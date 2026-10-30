// Package kafka provides a consumer that ingests stocks published to a Kafka
// topic and writes them to the stock store.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/segmentio/kafka-go"
)

// Store is the subset of the stock store needed to ingest kafka messages.
type Store interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error)
}

// messageReader is the subset of *kafka.Reader the consumer needs. Client
// holds a factory producing it so tests can inject fake readers without a
// broker.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Config holds the settings required to consume the stocks topic.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Client consumes stocks from a Kafka topic and writes them to the store, and
// (when configured) publishes new-stock events on an output topic.
type Client struct {
	cfg       Config
	store     Store
	publisher Publisher
	decoder   func(raw []byte) (*stockv1.Stock, error)

	// newReader creates a fresh message reader; sleep waits between
	// reconnect attempts (context-aware, reporting whether the full delay
	// elapsed). Both are injectable for tests; retryMin/retryMax bound the
	// exponential reconnect backoff.
	newReader func() messageReader
	sleep     func(context.Context, time.Duration) bool
	retryMin  time.Duration
	retryMax  time.Duration
}

// New validates the configuration and creates a new Client. publisher may be
// nil (in which case it is replaced with a no-op); a nil publisher means new
// stocks are written to the store but not published.
func New(cfg Config, store Store, publisher Publisher) *Client {
	if publisher == nil {
		publisher = Noop{}
	}
	return &Client{
		cfg:       cfg,
		store:     store,
		publisher: publisher,
		decoder:   decodeStock,
		newReader: func() messageReader {
			return kafka.NewReader(kafka.ReaderConfig{
				Brokers:  cfg.Brokers,
				Topic:    cfg.Topic,
				GroupID:  cfg.GroupID,
				MinBytes: 1e3,
				MaxBytes: 1e6,
			})
		},
		sleep:    sleepContext,
		retryMin: time.Second,
		retryMax: 30 * time.Second,
	}
}

// Run consumes messages from the stocks topic until ctx is done. A fetch or
// commit error closes the current reader, logs the failure, and reconnects
// with a capped exponential backoff (retryMin doubling up to retryMax,
// reset to retryMin after any successful fetch), so a broker outage never
// stops ingestion permanently. Run returns nil on context cancellation and
// the config validation error unchanged when the settings are invalid.
func (c *Client) Run(ctx context.Context) error {
	if err := c.cfg.validate(); err != nil {
		return err
	}

	delay := c.retryMin
	for {
		r := c.newReader()
		progressed, err := c.consume(ctx, r)
		if cerr := r.Close(); cerr != nil {
			log.Printf("kafka: closing reader: %v", cerr)
		}
		if ctx.Err() != nil {
			return nil
		}
		if progressed {
			delay = c.retryMin
		}
		log.Printf("kafka: consumer stopped: %v; reconnecting in %s", err, delay)
		if !c.sleep(ctx, delay) {
			return nil
		}
		delay = min(delay*2, c.retryMax)
	}
}

// consume fetches and processes messages from r until a fetch or commit
// error, reporting whether at least one message was fetched before the
// failure (used by Run to reset the backoff).
func (c *Client) consume(ctx context.Context, r messageReader) (progressed bool, err error) {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return progressed, err
		}
		progressed = true

		// Committing before handling is intentional for this step: it
		// preserves the previous commit-before-handle delivery behavior
		// exactly. A later step of the fix plan moves the commit after
		// successful processing.
		if err := r.CommitMessages(ctx, msg); err != nil {
			return progressed, err
		}

		if err := c.handle(ctx, msg); err != nil {
			log.Printf("kafka: dropping message %s:%d: %v", msg.Topic, msg.Offset, err)
		}
	}
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

// handle decodes a single kafka message and upserts the stock into the store.
// When the upsert was an INSERT (a new (symbol, exchange) pair), it also
// publishes a new-stock event on the output topic (best-effort: a publish
// failure is logged, not returned).
func (c *Client) handle(ctx context.Context, msg kafka.Message) error {
	m, err := c.decoder(msg.Value)
	if err != nil {
		return err
	}

	if m.Symbol == "" || m.Exchange == "" {
		return errors.New("stock message missing symbol or exchange")
	}

	for _, e := range m.GetScores() {
		v := e.GetValue()
		cat := e.GetCategory()
		if v < -1.0 || v > 1.0 {
			return fmt.Errorf("score %s value %v out of range [-1, 1]", cat, v)
		}
	}

	stored, inserted, err := c.store.UpdateStock(ctx, m.Symbol, m.Exchange, toMap(m.GetScores()))
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	if err := c.publisher.Publish(ctx, toProto(stored)); err != nil {
		log.Printf("kafka: failed to publish new stock %s/%s: %v", stored.Symbol, stored.Exchange, err)
	}
	return nil
}

// decodeStock parses a protobuf stock message.
func decodeStock(raw []byte) (*stockv1.Stock, error) {
	m := new(stockv1.Stock)
	if err := proto.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("decode stock message: %w", err)
	}
	return m, nil
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

// toProto converts a store.Stock into the stockv1.Stock message used for gRPC
// responses and for the output-topic publish.
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

// validate reports the first misconfiguration in the kafka settings.
func (c Config) validate() error {
	switch {
	case len(c.Brokers) == 0:
		return errors.New("kafka: no brokers configured")
	case c.Topic == "":
		return errors.New("kafka: no topic configured")
	case c.GroupID == "":
		return errors.New("kafka: no consumer group configured")
	}
	return nil
}
