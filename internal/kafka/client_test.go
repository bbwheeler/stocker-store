package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"stocker-store/internal/store"
	kafkastockv1 "stocker-store/proto/v1/kafka"

	"google.golang.org/protobuf/proto"

	kafkaMsg "github.com/segmentio/kafka-go"
)

// fakeStore is a test double for the Store interface that records calls.
type fakeStore struct {
	called     bool
	lastSymbol string
	lastExch   string
	lastScores map[string]float64
	err        error
}

func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.called = true
	f.lastSymbol = symbol
	f.lastExch = exchange
	f.lastScores = scores
	return nil, nil
}

// TestDecodeStock_HappyPath verifies a full protobuf round-trip with all fields.
func TestDecodeStock_HappyPath(t *testing.T) {
	protoMsg := &kafkastockv1.StockUpdate{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores: map[string]float64{
			"momentum": 0.5,
			"value":    -0.25,
		},
	}

	raw, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	m, err := decodeStock(raw)
	if err != nil {
		t.Fatalf("decodeStock() unexpected error: %v", err)
	}
	if m.Symbol != "AAPL" {
		t.Errorf("Symbol = %q, want %q", m.Symbol, "AAPL")
	}
	if m.Exchange != "NASDAQ" {
		t.Errorf("Exchange = %q, want %q", m.Exchange, "NASDAQ")
	}
	if len(m.Scores) != 2 {
		t.Fatalf("len(Scores) = %d, want 2", len(m.Scores))
	}
	if got := m.Scores["momentum"]; got != 0.5 {
		t.Errorf("Scores[momentum] = %v, want 0.5", got)
	}
	if got := m.Scores["value"]; got != -0.25 {
		t.Errorf("Scores[value] = %v, want -0.25", got)
	}
}

// TestDecodeStock_ScoresAbsentDefaultsToEmptyMap verifies that when the proto
// message omits the scores field (nil), decodeStock returns an empty map.
func TestDecodeStock_ScoresAbsentDefaultsToEmptyMap(t *testing.T) {
	protoMsg := &kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"}
	raw, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	m, err := decodeStock(raw)
	if err != nil {
		t.Fatalf("decodeStock() unexpected error: %v", err)
	}
	if m.Scores == nil {
		t.Fatal("Scores = nil, want non-nil empty map")
	}
	if len(m.Scores) != 0 {
		t.Errorf("len(Scores) = %d, want 0", len(m.Scores))
	}
}

// TestDecodeStock_BadProtoReturnsError verifies that invalid protobuf bytes
// produce a decoded error containing "decode stock message".
func TestDecodeStock_BadProtoReturnsError(t *testing.T) {
	garbage := []byte{0x01, 0x02, 0xFF, 0xFE}

	_, err := decodeStock(garbage)
	if err == nil {
		t.Fatal("decodeStock() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "decode stock message") {
		t.Errorf("decodeStock() error = %q, want it to contain %q", err.Error(), "decode stock message")
	}
}

// ---- validate tests ----

func TestValidate_MissingBrokers(t *testing.T) {
	cfg := Config{Topic: "stocks", GroupID: "ingest"}
	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "brokers") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "brokers")
	}
}

func TestValidate_MissingTopic(t *testing.T) {
	cfg := Config{Brokers: []string{"localhost:9092"}, GroupID: "ingest"}
	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "topic") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "topic")
	}
}

func TestValidate_MissingGroupID(t *testing.T) {
	cfg := Config{Brokers: []string{"localhost:9092"}, Topic: "stocks"}
	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "group") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "group")
	}
}

func TestValidate_AllFieldsPresent(t *testing.T) {
	cfg := Config{Brokers: []string{"localhost:9092"}, Topic: "stocks", GroupID: "ingest"}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v, want nil", err)
	}
}

// ---- handle tests ----

func TestHandle_MissingSymbolOrExchange(t *testing.T) {
	testCases := []struct{ name, symbol, exchange string }{
		{"missing symbol", "", "NASDAQ"},
		{"missing exchange", "AAPL", ""},
		{"both missing", "", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			c := New(Config{}, store)
			msg := kafkastockv1.StockUpdate{Symbol: tc.symbol, Exchange: tc.exchange}
			raw, err := proto.Marshal(&msg)
			if err != nil {
				t.Fatalf("proto.Marshal() error = %v", err)
			}
			err = c.handle(context.Background(), kafkaMsg.Message{Value: raw})
			if err == nil {
				t.Fatal("handle() error = nil, want error")
			}
			if !strings.Contains(err.Error(), "symbol") && !strings.Contains(err.Error(), "exchange") {
				t.Errorf("handle() error = %q, want it to mention symbol or exchange", err.Error())
			}
			if store.called {
				t.Error("store.UpdateStock called, want it not to be called")
			}
		})
	}
}

func TestHandle_ScoreOutOfRange(t *testing.T) {
	testCases := []struct {
		name   string
		scores map[string]float64
	}{
		{"too high", map[string]float64{"momentum": 1.5}},
		{"too low", map[string]float64{"value": -2.0}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			c := New(Config{}, store)
			msg := kafkastockv1.StockUpdate{
				Symbol:   "AAPL",
				Exchange: "NASDAQ",
				Scores:   tc.scores,
			}
			raw, err := proto.Marshal(&msg)
			if err != nil {
				t.Fatalf("proto.Marshal() error = %v", err)
			}
			err = c.handle(context.Background(), kafkaMsg.Message{Value: raw})
			if err == nil {
				t.Fatal("handle() error = nil, want error")
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("handle() error = %q, want it to contain %q", err.Error(), "out of range")
			}
			if store.called {
				t.Error("store.UpdateStock called, want it not to be called")
			}
		})
	}
}

func TestHandle_StoreErrorPropagated(t *testing.T) {
	wantErr := fmt.Errorf("connection refused")
	store := &fakeStore{err: wantErr}
	c := New(Config{}, store)
	msg := kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"}
	raw, _ := proto.Marshal(&msg)
	err := c.handle(context.Background(), kafkaMsg.Message{Value: raw})
	if err == nil {
		t.Fatal("handle() error = nil, want error")
	}
	if err != wantErr {
		t.Errorf("handle() error = %v, want %v", err, wantErr)
	}
}

func TestHandle_HappyPath(t *testing.T) {
	wantScores := map[string]float64{"momentum": 0.5, "value": -0.25}
	store := &fakeStore{}
	c := New(Config{}, store)
	msg := kafkastockv1.StockUpdate{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   wantScores,
	}
	raw, err := proto.Marshal(&msg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	err = c.handle(context.Background(), kafkaMsg.Message{Value: raw})
	if err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if !store.called {
		t.Fatal("store.UpdateStock not called, want it called")
	}
	if store.lastSymbol != "AAPL" {
		t.Errorf("store lastSymbol = %q, want %q", store.lastSymbol, "AAPL")
	}
	if store.lastExch != "NASDAQ" {
		t.Errorf("store lastExchange = %q, want %q", store.lastExch, "NASDAQ")
	}
	if len(store.lastScores) != len(wantScores) {
		t.Fatalf("store lastScores count = %d, want %d", len(store.lastScores), len(wantScores))
	}
	for k, v := range wantScores {
		if got := store.lastScores[k]; got != v {
			t.Errorf("score[%s] = %v, want %v", k, got, v)
		}
	}
}

// TestDecodeStock_RoundTrip verifies a proto.Marshal + decodeStock round-trip.
func TestDecodeStock_RoundTrip(t *testing.T) {
	var tests []*kafkastockv1.StockUpdate
	tests = append(tests, &kafkastockv1.StockUpdate{
		Symbol:   "TSLA",
		Exchange: "NYSE",
		Scores:   map[string]float64{"momentum": 0.5, "value": -0.3},
	})
	tests = append(tests, &kafkastockv1.StockUpdate{
		Symbol:   "MSFT",
		Exchange: "XNAS",
	})

	for i, msg := range tests {
		msg := msg
		t.Run(fmt.Sprintf("idx_%d", i), func(t *testing.T) {
			raw, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("proto.Marshal() error = %v", err)
			}
			decoded, err := decodeStock(raw)
			if err != nil {
				t.Fatalf("decodeStock() unexpected error: %v", err)
			}
			wantScores := msg.Scores
			if wantScores == nil {
				wantScores = make(map[string]float64)
			}
			gotScores := decoded.Scores
			if gotScores == nil {
				gotScores = make(map[string]float64)
			}
			if len(gotScores) != len(wantScores) {
				t.Errorf("[%d] scores count = %d, want %d", i, len(gotScores), len(wantScores))
			}
			for k, v := range wantScores {
				if got := gotScores[k]; got != v {
					t.Errorf("[%d] score[%s] = %v, want %v", i, k, got, v)
				}
			}
		})
	}
}
