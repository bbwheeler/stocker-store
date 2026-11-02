// Package rabbitmq provides the RabbitMQ consumer that ingests stocks and the
// optional new-stock publisher.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/events"
	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// errInvalidMessage marks a message-level failure that is permanent: the
// message is logged and skipped rather than retried, because redelivery can
// never make it valid (decode or validation error).
var errInvalidMessage = errors.New("invalid rabbitmq stock message")

// Store is the subset of the stock store needed to ingest messages.
type Store interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error)
}

// Consumer consumes stock messages from RabbitMQ and writes them to the store.
type Consumer struct {
	cfg       Config
	store     Store
	publisher events.Publisher
	decoder   func([]byte) (*stockv1.Stock, error)

	// dial opens a connection; sleep waits between attempts (context-aware,
	// reporting whether the full delay elapsed). Both are injectable for
	// tests; retryMin/retryMax bound the exponential backoff.
	dial     dialFunc
	sleep    func(context.Context, time.Duration) bool
	retryMin time.Duration
	retryMax time.Duration
}

// New creates a Consumer. A nil publisher is replaced with events.Noop, so
// new stocks are written but not published.
func New(cfg Config, store Store, publisher events.Publisher) *Consumer {
	if publisher == nil {
		publisher = events.Noop{}
	}
	return &Consumer{
		cfg:       cfg,
		store:     store,
		publisher: publisher,
		decoder:   decodeStock,
		dial:      defaultDial,
		sleep:     sleepContext,
		retryMin:  time.Second,
		retryMax:  30 * time.Second,
	}
}

// Run consumes messages until ctx is done. If ingestion is disabled (an
// explicitly empty URL) it returns nil without dialing. A broker that is
// unreachable is retried with capped exponential backoff, so an outage never
// terminates the process. Run returns nil on context cancellation and the
// config validation error unchanged when the settings are invalid.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.cfg.IngestionEnabled() {
		return nil
	}
	if err := c.cfg.validate(); err != nil {
		return err
	}

	delay := c.retryMin
	for {
		connected, err := c.consumeOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if connected {
			delay = c.retryMin
			log.Printf("rabbitmq: consumer stopped: %v; reconnecting in %s", err, delay)
		} else {
			log.Printf("rabbitmq: connect failed: %v; retrying in %s", err, delay)
		}
		if !c.sleep(ctx, delay) {
			return nil
		}
		delay = min(delay*2, c.retryMax)
	}
}

// consumeOnce opens a connection and channel, declares the topology, and
// consumes deliveries until the context is cancelled, the channel is closed,
// or a delivery cannot be handled. connected reports whether the session got
// as far as consuming; it lets Run distinguish a setup failure from a lost
// session.
func (c *Consumer) consumeOnce(ctx context.Context) (connected bool, err error) {
	conn, err := c.dial(c.cfg.URL)
	if err != nil {
		return false, err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close rabbitmq connection: %w", cerr)
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}
	defer func() {
		if cerr := ch.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close rabbitmq channel: %w", cerr)
		}
	}()

	if err := ch.ExchangeDeclare(c.cfg.Exchange, c.cfg.ExchangeType, true, false, false, false, nil); err != nil {
		return false, err
	}
	if _, err := ch.QueueDeclare(c.cfg.Queue, true, false, false, false, nil); err != nil {
		return false, err
	}
	if err := ch.QueueBind(c.cfg.Queue, c.cfg.RoutingKey, c.cfg.Exchange, false, nil); err != nil {
		return false, err
	}
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		return false, err
	}

	notify := ch.NotifyClose(make(chan *amqp.Error, 1))
	deliveries, err := ch.Consume(c.cfg.Queue, "", false, false, false, false, nil)
	if err != nil {
		return false, err
	}

	log.Printf("rabbitmq: connected exchange=%q type=%q queue=%q routing_key=%q",
		c.cfg.Exchange, c.cfg.ExchangeType, c.cfg.Queue, c.cfg.RoutingKey)
	connected = true

	for {
		select {
		case <-ctx.Done():
			return connected, nil
		case amqpErr, ok := <-notify:
			if !ok {
				return connected, errors.New("rabbitmq: channel closed")
			}
			return connected, fmt.Errorf("rabbitmq: channel closed: %v", amqpErr)
		case d, ok := <-deliveries:
			if !ok {
				return connected, errors.New("rabbitmq: delivery channel closed")
			}
			if err := c.processDelivery(ctx, d); err != nil {
				return connected, err
			}
		}
	}
}

// processDelivery handles one delivery. Success acknowledges it after the
// store write. A permanent failure is logged once and nacked without requeue.
// A transient failure retries the same delivery with capped exponential
// backoff while it stays unacknowledged; cancellation returns without acking
// so the broker redelivers it.
func (c *Consumer) processDelivery(ctx context.Context, d amqp.Delivery) error {
	delay := c.retryMin
	for {
		err := c.handle(ctx, d)
		if err == nil {
			if aerr := d.Ack(false); aerr != nil {
				return fmt.Errorf("rabbitmq: ack message: %w", aerr)
			}
			return nil
		}

		symbol, exchange := deliveryIdentity(d)
		if isPermanent(err) {
			log.Printf("rabbitmq: skipping message symbol=%s exchange=%s message_id=%s: %v",
				symbol, exchange, d.MessageId, err)
			if nerr := d.Nack(false, false); nerr != nil {
				return fmt.Errorf("rabbitmq: nack message: %w", nerr)
			}
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("rabbitmq: transient failure for symbol=%s/%s, retrying in %s: %v",
			symbol, exchange, delay, err)
		if !c.sleep(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, c.retryMax)
	}
}

// handle decodes one delivery and upserts the stock. Decode and validation
// failures are wrapped with errInvalidMessage so the caller classifies them as
// permanent; store errors are returned unwrapped. When the upsert inserted a
// new (symbol, exchange) pair it also publishes a new-stock event
// (best-effort: a publish failure is logged, not returned).
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) error {
	m, err := c.decoder(d.Body)
	if err != nil {
		return fmt.Errorf("%w: %v", errInvalidMessage, err)
	}

	if m.GetSymbol() == "" || m.GetExchange() == "" {
		return fmt.Errorf("%w: stock message missing symbol or exchange", errInvalidMessage)
	}

	for _, e := range m.GetScores() {
		v := e.GetValue()
		cat := e.GetCategory()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w: stock %s/%s: score %s value %v is not finite", errInvalidMessage, m.GetSymbol(), m.GetExchange(), cat, v)
		}
		if v < -1.0 || v > 1.0 {
			return fmt.Errorf("%w: stock %s/%s: score %s value %v out of range [-1, 1]", errInvalidMessage, m.GetSymbol(), m.GetExchange(), cat, v)
		}
	}

	stored, inserted, err := c.store.UpdateStock(ctx, m.GetSymbol(), m.GetExchange(), toMap(m.GetScores()))
	if err != nil {
		return err
	}

	log.Printf("rabbitmq: applied symbol=%s exchange=%s scores=%d inserted=%v",
		stored.Symbol, stored.Exchange, len(m.GetScores()), inserted)
	if !inserted {
		return nil
	}
	if err := c.publisher.Publish(ctx, toProto(stored)); err != nil {
		log.Printf("rabbitmq: failed to publish new stock %s/%s: %v", stored.Symbol, stored.Exchange, err)
	}
	return nil
}

// deliveryIdentity returns the symbol and exchange recorded in the delivery
// headers for logging, falling back to the message id for the symbol.
func deliveryIdentity(d amqp.Delivery) (symbol, exchange string) {
	symbol = d.MessageId
	if v, ok := d.Headers["symbol"].(string); ok && v != "" {
		symbol = v
	}
	if v, ok := d.Headers["exchange"].(string); ok {
		exchange = v
	}
	return symbol, exchange
}

// isPermanent reports whether err is a message-level failure that must be
// skipped rather than retried: an invalid message (decode/validation) or a
// Postgres data exception (SQLSTATE class 22) / integrity constraint (class
// 23) violation. Everything else — connectivity, timeouts, pool exhaustion,
// serialization failures — is transient.
func isPermanent(err error) bool {
	if errors.Is(err, errInvalidMessage) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return len(pgErr.Code) >= 2 &&
			(strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23"))
	}
	return false
}

// sleepContext waits for d or until ctx is done, reporting whether the full
// delay elapsed (false means ctx was cancelled).
func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// decodeStock parses a protobuf stock message. On failure it reports the
// payload length and a truncated hex prefix so an undecodable message is
// diagnosable from logs alone.
func decodeStock(raw []byte) (*stockv1.Stock, error) {
	m := new(stockv1.Stock)
	if err := proto.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("decode stock message (len=%d prefix=%x): %w", len(raw), prefixBytes(raw, 32), err)
	}
	return m, nil
}

// prefixBytes returns at most the first n bytes of b for compact log output.
func prefixBytes(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

// toMap converts a repeated list of score entries into the map shape the store
// expects. A nil list yields a nil map (a scoreless upsert).
func toMap(entries []*stockv1.ScoreEntry) map[string]float64 {
	if entries == nil {
		return nil
	}
	m := make(map[string]float64, len(entries))
	for _, e := range entries {
		m[e.GetCategory()] = e.GetValue()
	}
	return m
}

// toProto converts a store.Stock into the stockv1.Stock message used for the
// optional new-stock publish.
func toProto(s *store.Stock) *stockv1.Stock {
	out := &stockv1.Stock{Symbol: s.Symbol, Exchange: s.Exchange}
	for _, e := range s.Scores {
		out.Scores = append(out.Scores, &stockv1.ScoreEntry{
			Category:  e.Category,
			Value:     e.Value,
			UpdatedAt: timestamppb.New(e.UpdatedAt),
		})
	}
	return out
}
