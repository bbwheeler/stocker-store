// Package kafka provides a consumer that ingests stocks published to a Kafka
// topic and writes them to the stock store.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

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
	}
}

// Run consumes messages from the stocks topic until ctx is done or an
// unrecoverable error occurs.
func (c *Client) Run(ctx context.Context) error {
	if err := c.cfg.validate(); err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  c.cfg.Brokers,
		Topic:    c.cfg.Topic,
		GroupID:  c.cfg.GroupID,
		MinBytes: 1e3,
		MaxBytes: 1e6,
	})
	defer reader.Close()

	for {
		if ctx.Err() != nil {
			return nil
		}

		msg, err := reader.ReadMessage(ctx)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read kafka message: %w", err)
		}

		if err := c.handle(ctx, msg); err != nil {
			log.Printf("kafka: dropping message %s:%d: %v", msg.Topic, msg.Offset, err)
			continue
		}
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
