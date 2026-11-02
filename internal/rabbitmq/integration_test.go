package rabbitmq_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/rabbitmq"
	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	amqp "github.com/rabbitmq/amqp091-go"
)

// memoryStore is an in-memory rabbitmq.Store double recording every update.
type memoryStore struct {
	mu      sync.Mutex
	updates []memoryUpdate
}

type memoryUpdate struct {
	symbol, exchange string
}

func (s *memoryStore) UpdateStock(_ context.Context, symbol, exchange string, _ map[string]float64) (*store.Stock, bool, error) {
	s.mu.Lock()
	s.updates = append(s.updates, memoryUpdate{symbol, exchange})
	s.mu.Unlock()
	return &store.Stock{Symbol: symbol, Exchange: exchange, Updated: time.Now()}, true, nil
}

func (s *memoryStore) received() []memoryUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]memoryUpdate(nil), s.updates...)
}

// TestIntegration_PublishConsumeRoundTrip publishes one stock through the
// RabbitMQ publisher and asserts the consumer delivers it to the store. It is
// gated on RABBITMQ_TEST_URL and uses only a unique throwaway exchange and
// queue derived from the timestamp and pid; it never touches stock.update.
func TestIntegration_PublishConsumeRoundTrip(t *testing.T) {
	testURL := os.Getenv("RABBITMQ_TEST_URL")
	if testURL == "" {
		t.Skip("RABBITMQ_TEST_URL not set; skipping RabbitMQ integration test")
	}

	unique := fmt.Sprintf("stocker-store.itest.%d.%d", time.Now().UnixNano(), os.Getpid())
	exchange := unique + ".exchange"
	queue := unique + ".queue"
	// The consumer's binding is a topic wildcard matching the publisher's
	// output routing key, so the published message reaches the queue while the
	// input and output routing keys stay distinct.
	bindingKey := unique + ".*"
	outputKey := unique + ".out"

	// The setup connection owns the throwaway topology; cleanups run in LIFO
	// order, so delete the queue and exchange before closing the connection.
	setup, err := amqp.Dial(testURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = setup.Close() })

	ch, err := setup.Channel()
	if err != nil {
		t.Fatalf("setup channel: %v", err)
	}
	// Declare with the same flags the consumer and publisher use so their
	// redeclarations are assertions, not 406 conflicts.
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(queue, bindingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	t.Cleanup(func() {
		if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
			t.Logf("delete queue: %v", err)
		}
		if err := ch.ExchangeDelete(exchange, false, false); err != nil {
			t.Logf("delete exchange: %v", err)
		}
	})

	cfg := rabbitmq.Config{
		URL:                testURL,
		Exchange:           exchange,
		ExchangeType:       "topic",
		RoutingKey:         bindingKey,
		Queue:              queue,
		Prefetch:           1,
		OutputExchange:     exchange,
		OutputExchangeType: "topic",
		OutputRoutingKey:   outputKey,
	}

	pub, err := rabbitmq.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	st := &memoryStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- rabbitmq.New(cfg, st, nil).Run(ctx) }()

	stock := &stockv1.Stock{Symbol: "ITEST", Exchange: "NYSEITEST"}
	if err := pub.Publish(ctx, stock); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := st.received(); len(got) > 0 {
			if got[0].symbol != stock.Symbol || got[0].exchange != stock.Exchange {
				t.Fatalf("store received %q/%q, want %q/%q",
					got[0].symbol, got[0].exchange, stock.Symbol, stock.Exchange)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer did not receive the published stock within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-consumerDone:
		if err != nil {
			t.Errorf("consumer Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not return after cancellation")
	}
}
