package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/events"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Publisher publishes new-stock events to the optional output exchange. It
// connects lazily on the first publish, declares only its exchange (consumers
// own their queues and bindings), enables publisher confirms, and on failure
// tears the connection down and retries the same message once. It implements
// events.Publisher.
type Publisher struct {
	exchange     string
	exchangeType string
	routingKey   string
	url          string

	// dial opens a connection; injectable so tests never touch a broker.
	dial dialFunc

	mu     sync.Mutex
	conn   connection
	ch     channel
	closed bool
}

var _ events.Publisher = (*Publisher)(nil)

// NewPublisher returns the new-stock publisher for cfg. When output is
// disabled it logs and returns events.Noop without dialing. The URL is
// validated eagerly (parse errors are redacted so credentials never reach the
// log); the broker connection is opened lazily on the first publish.
func NewPublisher(cfg Config) (events.Publisher, error) {
	if !cfg.OutputEnabled() {
		log.Print("rabbitmq: output publisher disabled (RABBITMQ_OUTPUT_EXCHANGE not set)")
		return events.Noop{}, nil
	}
	if _, err := amqp.ParseURI(cfg.URL); err != nil {
		return nil, fmt.Errorf("rabbitmq: RABBITMQ_URL: %w", redactURLError(err))
	}
	if cfg.OutputExchange == cfg.Exchange {
		log.Print("rabbitmq: output exchange equals input exchange; verify the routing key does not create a feedback loop")
	}
	return &Publisher{
		exchange:     cfg.OutputExchange,
		exchangeType: cfg.OutputExchangeType,
		routingKey:   cfg.OutputRoutingKey,
		url:          cfg.URL,
		dial:         defaultDial,
	}, nil
}

// Publish sends the stock to the output exchange as a persistent protobuf
// message and waits for the broker confirmation. It is best-effort: after a
// failure it logs, tears the connection down, reconnects, and retries the
// same message once; a second failure is returned to the caller, which logs
// it. A cancelled context prevents both dialing and the retry. Publishing
// after Close returns an error.
func (p *Publisher) Publish(ctx context.Context, stock *stockv1.Stock) error {
	if stock == nil {
		return errors.New("rabbitmq: cannot publish a nil stock")
	}
	body, err := proto.Marshal(stock)
	if err != nil {
		return fmt.Errorf("marshal stock %s/%s: %w", stock.GetSymbol(), stock.GetExchange(), err)
	}
	msg := amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		MessageId:    stock.GetSymbol(),
		Type:         p.routingKey,
		Timestamp:    time.Now().UTC(),
		Headers:      amqp.Table{"symbol": stock.GetSymbol(), "exchange": stock.GetExchange()},
		Body:         body,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("rabbitmq: publisher is closed")
	}

	if err := p.ensureChannel(ctx); err != nil {
		return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, err)
	}
	err = p.publishOnce(ctx, msg)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		p.teardown()
		return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, ctx.Err())
	}

	log.Printf("rabbitmq: publish failed; reconnecting and retrying once symbol=%s", stock.GetSymbol())
	p.teardown()

	if err := p.ensureChannel(ctx); err != nil {
		return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, err)
	}
	if err := p.publishOnce(ctx, msg); err != nil {
		p.teardown()
		if ctx.Err() != nil {
			return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, ctx.Err())
		}
		return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, err)
	}
	return nil
}

// Close releases the channel and connection. It is idempotent: later calls
// return nil without side effects.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	if p.ch != nil {
		if cerr := p.ch.Close(); cerr != nil {
			err = cerr
		}
		p.ch = nil
	}
	if p.conn != nil {
		if cerr := p.conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
		p.conn = nil
	}
	return err
}

// ensureChannel returns a live channel with the output exchange declared and
// publisher confirms enabled, dialing lazily when no usable channel exists.
// A closed channel is torn down and replaced. It must be called with p.mu
// held.
func (p *Publisher) ensureChannel(ctx context.Context) error {
	if p.ch != nil && !p.ch.IsClosed() {
		return nil
	}
	p.teardown()
	if err := ctx.Err(); err != nil {
		return err
	}

	conn, err := p.dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	if err := ch.ExchangeDeclare(p.exchange, p.exchangeType, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return err
	}
	p.conn, p.ch = conn, ch
	return nil
}

// publishOnce publishes msg on the current channel and waits for the broker
// confirmation. It must be called with p.mu held.
func (p *Publisher) publishOnce(ctx context.Context, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, p.routingKey, false, false, msg)
	if err != nil {
		return err
	}
	if dc == nil {
		return errors.New("rabbitmq: publisher confirms are not enabled")
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("rabbitmq: broker negatively confirmed the publish")
	}
	return nil
}

// teardown closes and forgets the current channel and connection, if any.
// It must be called with p.mu held.
func (p *Publisher) teardown() {
	if p.ch != nil {
		p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}
