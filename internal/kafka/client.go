// Package kafka provides a consumer that ingests stocks published to a Kafka
// topic and writes them to the stock store.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// Message is the JSON payload published to the stocks topic. Scores are
// optional; a message with stock only still upserts the stock row.
type Message struct {
	Symbol   string             `json:"symbol"`
	Exchange string             `json:"exchange"`
	Scores   map[string]float64 `json:"scores,omitempty"`
}

// Store is the subset of the stock store needed to ingest kafka messages.
type Store interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) error
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
	decoder func(raw []byte) (*Message, error)
}

// New validates the configuration and creates a new Client.
func New(cfg Config, store Store) *Client {
	return &Client{
		cfg:     cfg,
		store:   store,
		decoder: decodeMessage,
	}
}

// Run consumes messages from the stocks topic until ctx is done or an
// unrecoverable error occurs.
func (c *Client) Run(ctx context.Context) error {
	if err := c.cfg.validate(); err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.cfg.Brokers,
		Topic:     c.cfg.Topic,
		GroupID:   c.cfg.GroupID,
		MinBytes:  1e3,
		MaxBytes:  1e6,
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

	for cat, val := range m.Scores {
		if val < -1.0 || val > 1.0 {
			return fmt.Errorf("score %s value %v out of range [-1, 1]", cat, val)
		}
	}

	return c.store.UpdateStock(ctx, m.Symbol, m.Exchange, m.Scores)
}

// decodeMessage parses a JSON stock message.
func decodeMessage(raw []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode stock message: %w", err)
	}
	if m.Scores == nil {
		m.Scores = map[string]float64{}
	}
	return &m, nil
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
