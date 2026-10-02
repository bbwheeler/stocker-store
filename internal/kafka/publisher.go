package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"google.golang.org/protobuf/proto"

	"github.com/segmentio/kafka-go"
)

// Publisher emits new-stock events on the configured output topic. It is
// consumed by the gRPC server and the kafka client whenever a new stock (an
// INSERT, not an UPDATE) is written to the store.
type Publisher interface {
	// Publish serializes stock with protobuf and writes it to the output
	// topic. It is best-effort: callers are expected to log (not return) an
	// error so that a publish failure does not abort the primary DB write or
	// the gRPC/Kafka handler response.
	Publish(ctx context.Context, stock *stockv1.Stock) error
	// Close flushes any pending writes and releases any resources.
	Close() error
}

// Noop is a Publisher that does nothing: Publish and Close return nil without
// side effects. It is returned by NewPublisher when brokers or the output
// topic are not configured.
type Noop struct{}

// Publish implements Publisher.
func (Noop) Publish(context.Context, *stockv1.Stock) error { return nil }

// Close implements Publisher.
func (Noop) Close() error { return nil }

// NewPublisher returns a Publisher for the given broker list and output
// topic, or a no-op Publisher if either is empty. The returned value is
// always non-nil.
func NewPublisher(brokers []string, topic string) Publisher {
	if len(brokers) == 0 || topic == "" {
		log.Printf("kafka output publisher disabled (need both brokers and an output topic)")
		return Noop{}
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Async:        true,
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
	})
	return &writerPublisher{w: w}
}

// writerPublisher publishes to a kafka.Writer.
type writerPublisher struct {
	w *kafka.Writer
}

// Publish implements Publisher.
func (p *writerPublisher) Publish(ctx context.Context, stock *stockv1.Stock) error {
	if stock == nil {
		return errors.New("kafka publisher: nil stock")
	}
	raw, err := proto.Marshal(stock)
	if err != nil {
		return fmt.Errorf("kafka publisher: marshal stock: %w", err)
	}
	// Key by symbol+exchange so a single stock's writes land on a single
	// partition; consumers can rely on ordering per-(symbol, exchange).
	key := stock.Symbol + "/" + stock.Exchange
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: raw,
	})
}

// Close implements Publisher.
func (p *writerPublisher) Close() error {
	return p.w.Close()
}
