package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"stocker-store/internal/store"
	kafkastockv1 "stocker-store/proto/v1/kafka"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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

// scoreByCat finds the score entry with the given category, failing the test if
// it is not present.
func scoreByCat(t *testing.T, entries []*kafkastockv1.ScoreEntry, cat string) *kafkastockv1.ScoreEntry {
	t.Helper()
	for _, e := range entries {
		if e.GetCategory() == cat {
			return e
		}
	}
	t.Fatalf("score %q not found", cat)
	return nil
}

// TestDecodeStock_HappyPath verifies a full protobuf round-trip with all fields.
func TestDecodeStock_HappyPath(t *testing.T) {
	protoMsg := &kafkastockv1.StockUpdate{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores: []*kafkastockv1.ScoreEntry{
			{Category: "momentum", Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
			{Category: "value", Value: -0.25, UpdatedAt: timestamppb.New(time.Now())},
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
	if got := scoreByCat(t, m.Scores, "momentum").GetValue(); got != 0.5 {
		t.Errorf("Scores[momentum] = %v, want 0.5", got)
	}
	if got := scoreByCat(t, m.Scores, "value").GetValue(); got != -0.25 {
		t.Errorf("Scores[value] = %v, want -0.25", got)
	}
}

// TestDecodeStock_ScoresAbsentDefaultsToEmptySlice verifies that when the proto
// message omits the scores field (nil), decodeStock returns an empty
// (nil-tolerant) slice of scores.
func TestDecodeStock_ScoresAbsentDefaultsToEmptySlice(t *testing.T) {
	protoMsg := &kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"}
	raw, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	m, err := decodeStock(raw)
	if err != nil {
		t.Fatalf("decodeStock() unexpected error: %v", err)
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
		scores []*kafkastockv1.ScoreEntry
	}{
		{"too high", []*kafkastockv1.ScoreEntry{{Category: "momentum", Value: 1.5}}},
		{"too low", []*kafkastockv1.ScoreEntry{{Category: "value", Value: -2.0}}},
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
	wantScores := []*kafkastockv1.ScoreEntry{
		{Category: "momentum", Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
		{Category: "value", Value: -0.25, UpdatedAt: timestamppb.New(time.Now())},
	}
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
	// The fake store records the map the production code converted from the
	// repeated ScoreEntry slice: assert toMap preserved every category/value.
	wantMap := make(map[string]float64, len(wantScores))
	for _, e := range wantScores {
		wantMap[e.Category] = e.Value
	}
	if len(store.lastScores) != len(wantMap) {
		t.Fatalf("store lastScores count = %d, want %d", len(store.lastScores), len(wantMap))
	}
	for k, v := range wantMap {
		if got := store.lastScores[k]; got != v {
			t.Errorf("score[%s] = %v, want %v", k, got, v)
		}
	}
}

// TestDecodeStock_RoundTrip verifies a proto.Marshal + decodeStock round-trip.
func TestDecodeStock_RoundTrip(t *testing.T) {
	testMsgs := []*kafkastockv1.StockUpdate{
		{
			Symbol:   "TSLA",
			Exchange: "NYSE",
			Scores: []*kafkastockv1.ScoreEntry{
				{Category: "momentum", Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
				{Category: "value", Value: -0.3, UpdatedAt: timestamppb.New(time.Now())},
			},
		},
		{
			Symbol:   "MSFT",
			Exchange: "XNAS",
		},
	}

	for i, msg := range testMsgs {
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
			if len(decoded.Scores) != len(msg.Scores) {
				t.Errorf("[%d] scores count = %d, want %d", i, len(decoded.Scores), len(msg.Scores))
				return
			}
			for _, want := range msg.Scores {
				got := scoreByCat(t, decoded.Scores, want.Category)
				if got.GetValue() != want.GetValue() {
					t.Errorf("[%d] score[%s] = %v, want %v", i, want.Category, got.GetValue(), want.GetValue())
				}
			}
		})
	}
}
