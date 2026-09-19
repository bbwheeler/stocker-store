// Package kafka provides a consumer that ingests stocks published to a Kafka
// topic and writes them to the stock store.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	"stocker-store/internal/store"
	kafkastockv1 "stocker-store/proto/v1/kafka"

	"google.golang.org/protobuf/proto"

	"github.com/segmentio/kafka-go"
)

// Store is the subset of the stock store needed to ingest kafka messages.
type Store interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, error)
}

// Config holds the settings required to consume the stocks topic.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Client consumes stocks from a Kafka topic and writes them to the store.
type Client struct {
	cfg     Config
	store   Store
	decoder func(raw []byte) (*kafkastockv1.StockUpdate, error)
}

// New validates the configuration and creates a new Client.
func New(cfg Config, store Store) *Client {
	return &Client{
		cfg:     cfg,
		store:   store,
		decoder: decodeStock,
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

	_, err = c.store.UpdateStock(ctx, m.Symbol, m.Exchange, toMap(m.GetScores()))
	return err
}

// decodeStock parses a protobuf stock message.
func decodeStock(raw []byte) (*kafkastockv1.StockUpdate, error) {
	m := new(kafkastockv1.StockUpdate)
	if err := proto.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("decode stock message: %w", err)
	}
	return m, nil
}

// toMap converts a repeated list of score entries into the map shape the store
// expects. A nil list yields a nil map (a scoreless upsert).
func toMap(entries []*kafkastockv1.ScoreEntry) map[string]float64 {
	if entries == nil {
		return nil
	}
	m := make(map[string]float64, len(entries))
	for _, e := range entries {
		m[e.GetCategory()] = e.GetValue()
	}
	return m
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
