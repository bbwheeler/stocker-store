package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

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
	inserted   bool
	err        error
}

func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	f.called = true
	f.lastSymbol = symbol
	f.lastExch = exchange
	f.lastScores = scores
	return &store.Stock{
		Symbol:   symbol,
		Exchange: exchange,
		Updated:  time.Now(),
		Scores:   toStoreScores(scores),
	}, f.inserted, nil
}

// toStoreScores inverts toMap for test fixtures: it expands a category->value
// map into the slice the production store returns.
func toStoreScores(m map[string]float64) []store.ScoreEntry {
	if m == nil {
		return nil
	}
	out := make([]store.ScoreEntry, 0, len(m))
	for cat, v := range m {
		out = append(out, store.ScoreEntry{Category: cat, Value: v, UpdatedAt: time.Now()})
	}
	return out
}

// fakePublisher is a test double for the Publisher interface that records
// calls and can optionally inject an error.
type fakePublisher struct {
	called    int
	lastStock *stockv1.Stock
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, stock *stockv1.Stock) error {
	f.called++
	f.lastStock = stock
	return f.err
}

func (f *fakePublisher) Close() error { return nil }

// scoreByCat finds the score entry with the given category, failing the test if
// it is not present.
func scoreByCat(t *testing.T, entries []*stockv1.ScoreEntry, cat string) *stockv1.ScoreEntry {
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
	protoMsg := &stockv1.Stock{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores: []*stockv1.ScoreEntry{
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
	protoMsg := &stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}
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
			c := New(Config{}, store, &fakePublisher{})
			msg := stockv1.Stock{Symbol: tc.symbol, Exchange: tc.exchange}
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
		scores []*stockv1.ScoreEntry
	}{
		{"too high", []*stockv1.ScoreEntry{{Category: "momentum", Value: 1.5}}},
		{"too low", []*stockv1.ScoreEntry{{Category: "value", Value: -2.0}}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			c := New(Config{}, store, &fakePublisher{})
			msg := stockv1.Stock{
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
	c := New(Config{}, store, &fakePublisher{})
	msg := stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}
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
	wantScores := []*stockv1.ScoreEntry{
		{Category: "momentum", Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
		{Category: "value", Value: -0.25, UpdatedAt: timestamppb.New(time.Now())},
	}
	store := &fakeStore{}
	c := New(Config{}, store, &fakePublisher{})
	msg := stockv1.Stock{
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
	testMsgs := []*stockv1.Stock{
		{
			Symbol:   "TSLA",
			Exchange: "NYSE",
			Scores: []*stockv1.ScoreEntry{
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

// ---- publisher tests ----

// TestHandle_PublishesOnNewStock verifies that when the store reports INSERT,
// the publisher is called exactly once with a stock matching the stored one.
func TestHandle_PublishesOnNewStock(t *testing.T) {
	store := &fakeStore{inserted: true}
	pub := &fakePublisher{}
	c := New(Config{}, store, pub)
	raw, err := proto.Marshal(&stockv1.Stock{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   []*stockv1.ScoreEntry{{Category: "momentum", Value: 0.5}},
	})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	if err := c.handle(context.Background(), kafkaMsg.Message{Value: raw}); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if !store.called {
		t.Fatal("store.UpdateStock not called, want it called")
	}
	if store.lastSymbol != "AAPL" || store.lastExch != "NASDAQ" {
		t.Errorf("store not called correctly: %q/%q", store.lastSymbol, store.lastExch)
	}
	if pub.called != 1 {
		t.Fatalf("publisher called %d times, want 1", pub.called)
	}
	if got := pub.lastStock.GetSymbol(); got != "AAPL" {
		t.Errorf("published symbol = %q, want %q", got, "AAPL")
	}
	if got := pub.lastStock.GetExchange(); got != "NASDAQ" {
		t.Errorf("published exchange = %q, want %q", got, "NASDAQ")
	}
	if got := len(pub.lastStock.GetScores()); got != 1 {
		t.Fatalf("published scores len = %d, want 1", got)
	}
	if got := scoreByCat(t, pub.lastStock.GetScores(), "momentum").GetValue(); got != 0.5 {
		t.Errorf("published scores[momentum] = %v, want 0.5", got)
	}
}

// TestHandle_DoesNotPublishOnUpdate verifies that when the store reports
// UPDATE (inserted=false), the publisher is not called.
func TestHandle_DoesNotPublishOnUpdate(t *testing.T) {
	store := &fakeStore{inserted: false}
	pub := &fakePublisher{}
	c := New(Config{}, store, pub)
	raw, err := proto.Marshal(&stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	if err := c.handle(context.Background(), kafkaMsg.Message{Value: raw}); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if !store.called {
		t.Fatal("store.UpdateStock not called, want it called")
	}
	if pub.called != 0 {
		t.Fatalf("publisher called %d times, want 0 (update of an existing stock)", pub.called)
	}
}

// TestHandle_DoesNotPublishWhenStoreErrors verifies that a store error is
// propagated and the publisher is not called.
func TestHandle_DoesNotPublishWhenStoreErrors(t *testing.T) {
	wantErr := fmt.Errorf("connection refused")
	store := &fakeStore{err: wantErr}
	pub := &fakePublisher{}
	c := New(Config{}, store, pub)
	raw, _ := proto.Marshal(&stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})
	err := c.handle(context.Background(), kafkaMsg.Message{Value: raw})
	if err != wantErr {
		t.Fatalf("handle() error = %v, want %v", err, wantErr)
	}
	if pub.called != 0 {
		t.Fatalf("publisher called %d times, want 0 (store failed)", pub.called)
	}
}

// TestHandle_PublishErrorDoesNotFailTheMessage verifies that a failed publish
// is logged (not returned): the handler still reports success because the DB
// write is authoritative.
func TestHandle_PublishErrorDoesNotFailTheMessage(t *testing.T) {
	store := &fakeStore{inserted: true}
	pub := &fakePublisher{err: fmt.Errorf("kafka unavailable")}
	c := New(Config{}, store, pub)
	raw, err := proto.Marshal(&stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	err = c.handle(context.Background(), kafkaMsg.Message{Value: raw})
	if err != nil {
		t.Fatalf("handle() returned %v, want nil (publish must be log-and-continue)", err)
	}
	if !store.called {
		t.Fatal("store.UpdateStock not called, want it called")
	}
	if pub.called != 1 {
		t.Fatalf("publisher called %d times, want 1", pub.called)
	}
}

// TestPublisher_RealAndNoop covers NewPublisher's two branches: the no-op
// returned when brokers or topic are missing, and the writer-backed impl
// returned when both are present.
func TestPublisher_RealAndNoop(t *testing.T) {
	// No-op branch.
	p := NewPublisher(nil, "")
	if _, ok := p.(Noop); !ok {
		t.Fatalf("NewPublisher(nil, \"\") = %T, want Noop", p)
	}
	if err := p.Publish(context.Background(), &stockv1.Stock{}); err != nil {
		t.Errorf("noop Publish error = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("noop Close error = %v, want nil", err)
	}

	// Real-writer branch: nothing is actually sent to a broker; we only check
	// that the returned publisher is the writer-backed impl and that close is
	// safe.
	p2 := NewPublisher([]string{"localhost:1"}, "topic")
	if _, ok := p2.(*writerPublisher); !ok {
		t.Fatalf("NewPublisher(brokers, topic) = %T, want *writerPublisher", p2)
	}
	if err := p2.Close(); err != nil {
		t.Errorf("writer Close error = %v, want nil (closed without writes)", err)
	}
}
