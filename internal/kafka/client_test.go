package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/jackc/pgx/v5/pgconn"
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

// TestHandle_PassesOnlyMessageScores verifies that handle forwards exactly the
// categories present in the message to the store, synthesizing none.
func TestHandle_PassesOnlyMessageScores(t *testing.T) {
	store := &fakeStore{}
	c := New(Config{}, store, &fakePublisher{})
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
	if got := len(store.lastScores); got != 1 {
		t.Fatalf("store lastScores count = %d, want 1: %v", got, store.lastScores)
	}
	if got := store.lastScores["momentum"]; got != 0.5 {
		t.Errorf("store lastScores[momentum] = %v, want 0.5", got)
	}
	for cat := range store.lastScores {
		if cat != "momentum" {
			t.Errorf("store lastScores has category %q, want only %q", cat, "momentum")
		}
	}
}

// TestHandle_ScorelessMessagePassesNilMap verifies that a message without scores
// reaches the store as a nil map (the documented toMap(nil) contract).
func TestHandle_ScorelessMessagePassesNilMap(t *testing.T) {
	store := &fakeStore{}
	c := New(Config{}, store, &fakePublisher{})
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
	if store.lastScores != nil {
		t.Fatalf("store lastScores = %v, want nil", store.lastScores)
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

// ---- Run tests (reconnecting consumer loop) ----

// fakeReader is a messageReader test double whose fetch behavior is supplied
// by each test. It records commits and close calls under a mutex so the test
// goroutine can inspect it while Run is executing.
type fakeReader struct {
	fetch     func(ctx context.Context) (kafkaMsg.Message, error)
	commitErr error
	events    *eventLog // optional; records a "commit" event per commit

	mu      sync.Mutex
	commits int
	closed  bool
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkaMsg.Message, error) {
	return f.fetch(ctx)
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafkaMsg.Message) error {
	f.mu.Lock()
	f.commits++
	f.mu.Unlock()
	if f.events != nil {
		f.events.record("commit")
	}
	return f.commitErr
}

func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeReader) commitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

func (f *fakeReader) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// countingStore is a Store test double that is safe to call from the Run
// goroutine: it records the number of calls and the last arguments under a
// mutex.
type countingStore struct {
	mu         sync.Mutex
	calls      int
	lastSymbol string
	lastExch   string
}

func (s *countingStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.lastSymbol = symbol
	s.lastExch = exchange
	return &store.Stock{
		Symbol:   symbol,
		Exchange: exchange,
		Updated:  time.Now(),
		Scores:   toStoreScores(scores),
	}, false, nil
}

func (s *countingStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *countingStore) last() (symbol, exchange string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSymbol, s.lastExch
}

// marshalStock builds a valid PROTOBUF stock payload for Run tests.
func marshalStock(t *testing.T, symbol, exchange string, scores map[string]float64) []byte {
	t.Helper()
	var entries []*stockv1.ScoreEntry
	for cat, v := range scores {
		entries = append(entries, &stockv1.ScoreEntry{
			Category:  cat,
			Value:     v,
			UpdatedAt: timestamppb.New(time.Now()),
		})
	}
	raw, err := proto.Marshal(&stockv1.Stock{Symbol: symbol, Exchange: exchange, Scores: entries})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	return raw
}

// yieldThenBlock returns a fetch function that delivers msg exactly once, then
// blocks until the context is done.
func yieldThenBlock(msg kafkaMsg.Message) func(context.Context) (kafkaMsg.Message, error) {
	var calls int32
	return func(ctx context.Context) (kafkaMsg.Message, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return msg, nil
		}
		<-ctx.Done()
		return kafkaMsg.Message{}, ctx.Err()
	}
}

// waitFor polls cond until it becomes true, failing the test after 2 s.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not satisfied within 2s")
}

// testRunConfig returns a config that passes validation; no broker is ever
// contacted because the reader factory is faked.
func testRunConfig() Config {
	return Config{Brokers: []string{"localhost:9092"}, Topic: "stocks", GroupID: "test"}
}

// TestRun_ReconnectsAfterFetchError verifies that a fetch failure does not
// terminate Run: the reader is recreated, the previously failing reader is
// closed, and messages from the new reader are still processed while the
// context is live.
func TestRun_ReconnectsAfterFetchError(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:     "stocks",
		Partition: 0,
		Offset:    1,
		Value:     marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
	}

	readerA := &fakeReader{fetch: func(context.Context) (kafkaMsg.Message, error) {
		return kafkaMsg.Message{}, errors.New("dial tcp: lookup kafka: no such host")
	}}
	readerB := &fakeReader{fetch: yieldThenBlock(validMsg)}

	st := &countingStore{}
	c := New(testRunConfig(), st, &fakePublisher{})

	var mu sync.Mutex
	factories := 0
	c.newReader = func() messageReader {
		mu.Lock()
		defer mu.Unlock()
		factories++
		if factories == 1 {
			return readerA
		}
		return readerB
	}
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return st.callCount() == 1 })

	mu.Lock()
	gotFactories := factories
	mu.Unlock()
	if gotFactories < 2 {
		t.Errorf("newReader called %d times, want at least 2", gotFactories)
	}
	if !readerA.isClosed() {
		t.Error("reader A not closed after fetch error")
	}
	if readerB.commitCount() != 1 {
		t.Errorf("reader B commits = %d, want 1", readerB.commitCount())
	}
	if sym, exch := st.last(); sym != "AAPL" || exch != "NASDAQ" {
		t.Errorf("store got %q/%q, want AAPL/NASDAQ", sym, exch)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live, want it to keep consuming", err)
	default:
	}
}

// TestRun_RetriesTransientFetchErrors verifies that several consecutive fetch
// errors are retried with backoff and that a later message is still processed.
func TestRun_RetriesTransientFetchErrors(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:  "stocks",
		Offset: 4,
		Value:  marshalStock(t, "TSLA", "NYSE", map[string]float64{"value": -0.25}),
	}

	var mu sync.Mutex
	step := 0

	st := &countingStore{}
	c := New(testRunConfig(), st, &fakePublisher{})
	c.newReader = func() messageReader {
		return &fakeReader{fetch: func(ctx context.Context) (kafkaMsg.Message, error) {
			mu.Lock()
			step++
			n := step
			mu.Unlock()
			if n <= 3 {
				return kafkaMsg.Message{}, errors.New("transient fetch failure")
			}
			if n == 4 {
				return validMsg, nil
			}
			<-ctx.Done()
			return kafkaMsg.Message{}, ctx.Err()
		}}
	}
	var sleeps int32
	c.sleep = func(ctx context.Context, _ time.Duration) bool {
		atomic.AddInt32(&sleeps, 1)
		return ctx.Err() == nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return st.callCount() == 1 })

	if got := atomic.LoadInt32(&sleeps); got < 3 {
		t.Errorf("sleep called %d times, want at least 3 (one per transient failure)", got)
	}
	if sym, exch := st.last(); sym != "TSLA" || exch != "NYSE" {
		t.Errorf("store got %q/%q, want TSLA/NYSE", sym, exch)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live, want it to keep retrying", err)
	default:
	}
}

// TestRun_ReturnsNilOnContextCancel verifies that cancelling the context while
// FetchMessage is blocked makes Run return nil.
func TestRun_ReturnsNilOnContextCancel(t *testing.T) {
	fetching := make(chan struct{})

	c := New(testRunConfig(), &countingStore{}, &fakePublisher{})
	c.newReader = func() messageReader {
		return &fakeReader{fetch: func(ctx context.Context) (kafkaMsg.Message, error) {
			close(fetching)
			<-ctx.Done()
			return kafkaMsg.Message{}, ctx.Err()
		}}
	}
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-fetching
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestRun_BackoffGrowsToMaxAndResetsAfterProgress pins the backoff schedule:
// the delay doubles (capped at retryMax) after failures with no progress and
// resets to retryMin once a message was fetched successfully.
func TestRun_BackoffGrowsToMaxAndResetsAfterProgress(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:  "stocks",
		Offset: 7,
		Value:  marshalStock(t, "MSFT", "XNAS", map[string]float64{"momentum": 0.1}),
	}

	errFetch := func(context.Context) (kafkaMsg.Message, error) {
		return kafkaMsg.Message{}, errors.New("broker down")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	step := 0
	var delays []time.Duration

	st := &countingStore{}
	c := New(testRunConfig(), st, &fakePublisher{})
	c.retryMin = time.Millisecond
	c.retryMax = 4 * time.Millisecond
	c.newReader = func() messageReader {
		mu.Lock()
		step++
		n := step
		mu.Unlock()
		switch n {
		case 1, 2, 3:
			return &fakeReader{fetch: errFetch}
		case 4:
			var calls int32
			return &fakeReader{fetch: func(context.Context) (kafkaMsg.Message, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					return validMsg, nil
				}
				return kafkaMsg.Message{}, errors.New("fetch error after progress")
			}}
		default:
			return &fakeReader{fetch: func(ctx context.Context) (kafkaMsg.Message, error) {
				<-ctx.Done()
				return kafkaMsg.Message{}, ctx.Err()
			}}
		}
	}
	c.sleep = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		delays = append(delays, d)
		n := len(delays)
		mu.Unlock()
		if n == 4 {
			// Expected full sequence observed (1ms, 2ms, 4ms capped, 1ms
			// reset); end the test.
			cancel()
			return false
		}
		return true
	}

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	mu.Lock()
	got := append([]time.Duration(nil), delays...)
	mu.Unlock()
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("backoff delays = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("backoff delay[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if st.callCount() != 1 {
		t.Errorf("store calls = %d, want 1", st.callCount())
	}
}

// TestRun_ReturnsConfigValidationError verifies Run surfaces the config
// validation error unchanged and never creates a reader.
func TestRun_ReturnsConfigValidationError(t *testing.T) {
	c := New(Config{}, &countingStore{}, &fakePublisher{})
	var factories int32
	c.newReader = func() messageReader {
		atomic.AddInt32(&factories, 1)
		return nil
	}

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want config validation error")
	}
	if !strings.Contains(err.Error(), "brokers") {
		t.Errorf("Run() error = %q, want it to contain %q", err.Error(), "brokers")
	}
	if got := atomic.LoadInt32(&factories); got != 0 {
		t.Errorf("newReader called %d times, want 0 for invalid config", got)
	}
}

// ---- Run tests (commit after success, permanent vs transient) ----

// eventLog is a mutex-protected ordered event log, used to assert the
// relative order of store updates and offset commits.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) record(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// scriptedStore records each UpdateStock call as an "update" event and fails
// the first failFirst calls with err before succeeding.
type scriptedStore struct {
	events    *eventLog
	failFirst int
	err       error

	mu    sync.Mutex
	calls int
}

func (s *scriptedStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	s.events.record("update")
	if n <= s.failFirst {
		return nil, false, s.err
	}
	return &store.Stock{
		Symbol:   symbol,
		Exchange: exchange,
		Updated:  time.Now(),
		Scores:   toStoreScores(scores),
	}, false, nil
}

func (s *scriptedStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// wantEvents asserts the exact ordered event sequence.
func wantEvents(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("events[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestRun_CommitsAfterSuccessfulHandle verifies at-least-once ordering: the
// offset is committed exactly once and strictly after the store write.
func TestRun_CommitsAfterSuccessfulHandle(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:     "stocks",
		Partition: 0,
		Offset:    5,
		Key:       []byte("AAPL"),
		Value:     marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
	}

	events := &eventLog{}
	r := &fakeReader{fetch: yieldThenBlock(validMsg), events: events}
	st := &scriptedStore{events: events}

	c := New(testRunConfig(), st, &fakePublisher{})
	c.newReader = func() messageReader { return r }
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return len(events.snapshot()) >= 2 })

	wantEvents(t, events.snapshot(), []string{"update", "commit"})
	if st.callCount() != 1 {
		t.Errorf("store calls = %d, want 1", st.callCount())
	}
	if r.commitCount() != 1 {
		t.Errorf("commits = %d, want 1", r.commitCount())
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live", err)
	default:
	}
}

// TestRun_DoesNotCommitOnTransientStoreErrorUntilSuccess verifies that a
// transient store failure retries the SAME message without committing, and
// commits only after the successful attempt.
func TestRun_DoesNotCommitOnTransientStoreErrorUntilSuccess(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:  "stocks",
		Offset: 6,
		Key:    []byte("TSLA"),
		Value:  marshalStock(t, "TSLA", "NYSE", map[string]float64{"value": -0.25}),
	}

	events := &eventLog{}
	r := &fakeReader{fetch: yieldThenBlock(validMsg), events: events}
	st := &scriptedStore{events: events, failFirst: 1, err: errors.New("connection refused")}

	c := New(testRunConfig(), st, &fakePublisher{})
	c.newReader = func() messageReader { return r }
	// Tiny injected bounds keep the retry sleep at ~1ms: fast and bounded.
	c.retryMin = time.Millisecond
	c.retryMax = 4 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return containsEvent(events.snapshot(), "commit") })

	wantEvents(t, events.snapshot(), []string{"update", "update", "commit"})
	if st.callCount() != 2 {
		t.Errorf("store calls = %d, want 2 (retry of the same message)", st.callCount())
	}
	if r.commitCount() != 1 {
		t.Errorf("commits = %d, want 1 (only after success)", r.commitCount())
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live", err)
	default:
	}
}

// TestRun_SkipsPermanentDecodeErrorAndCommits verifies that an undecodable
// payload is logged once, never reaches the store, and its offset is
// committed so it cannot block later messages.
func TestRun_SkipsPermanentDecodeErrorAndCommits(t *testing.T) {
	garbage := kafkaMsg.Message{
		Topic:     "stocks",
		Partition: 0,
		Offset:    9,
		Key:       []byte("bad-key"),
		Value:     []byte{0x01, 0x02, 0xff, 0xfe},
	}

	events := &eventLog{}
	r := &fakeReader{fetch: yieldThenBlock(garbage), events: events}
	st := &scriptedStore{events: events}

	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	t.Cleanup(func() { log.SetOutput(prev) })

	c := New(testRunConfig(), st, &fakePublisher{})
	c.newReader = func() messageReader { return r }
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return r.commitCount() == 1 })

	if st.callCount() != 0 {
		t.Errorf("store calls = %d, want 0 (decode error must not reach the store)", st.callCount())
	}
	if r.commitCount() != 1 {
		t.Errorf("commits = %d, want 1", r.commitCount())
	}
	out := logBuf.String()
	for _, want := range []string{"skipping message", "stocks:0:9", "key=bad-key", "decode stock message"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got:\n%s", want, out)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live", err)
	default:
	}
}

// TestRun_SkipsPgConstraintViolationAndCommits verifies that a Postgres
// integrity violation (CHECK, SQLSTATE 23514) is permanent: exactly one
// store attempt, one commit, no retry loop.
func TestRun_SkipsPgConstraintViolationAndCommits(t *testing.T) {
	validMsg := kafkaMsg.Message{
		Topic:  "stocks",
		Offset: 12,
		Key:    []byte("AAPL"),
		Value:  marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.6}),
	}

	events := &eventLog{}
	r := &fakeReader{fetch: yieldThenBlock(validMsg), events: events}
	st := &scriptedStore{
		events:    events,
		failFirst: 1,
		err: &pgconn.PgError{
			Code:    "23514",
			Message: `new row for relation "scores" violates check constraint "scores_value_check"`,
		},
	}

	c := New(testRunConfig(), st, &fakePublisher{})
	c.newReader = func() messageReader { return r }
	// Tiny bounds so a (wrong) transient classification fails fast instead
	// of sleeping 1s per retry.
	c.retryMin = time.Millisecond
	c.retryMax = 4 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return r.commitCount() == 1 })

	if st.callCount() != 1 {
		t.Errorf("store calls = %d, want 1 (constraint violation must not be retried)", st.callCount())
	}
	if r.commitCount() != 1 {
		t.Errorf("commits = %d, want 1", r.commitCount())
	}
	wantEvents(t, events.snapshot(), []string{"update", "commit"})
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while context is live", err)
	default:
	}
}

// TestIsPermanentError pins the permanent/transient classification table.
func TestIsPermanentError(t *testing.T) {
	decodeErr := fmt.Errorf("%w: decode stock message (len=4 prefix=0102fffe): proto: cannot parse invalid wire-format data", errInvalidMessage)
	validationErr := fmt.Errorf("%w: stock AAPL/NASDAQ: score momentum value 1.5 out of range [-1, 1]", errInvalidMessage)

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{"decode sentinel", decodeErr, true},
		{"validation sentinel", validationErr, true},
		{"pg check violation 23514", &pgconn.PgError{Code: "23514"}, true},
		{"pg fk violation 23503", &pgconn.PgError{Code: "23503"}, true},
		{"pg bad input 22P02", &pgconn.PgError{Code: "22P02"}, true},
		{"wrapped pg error", fmt.Errorf("upsert score: %w", &pgconn.PgError{Code: "23514"}), true},
		{"pg serialization failure 40001", &pgconn.PgError{Code: "40001"}, false},
		{"pg error with empty code", &pgconn.PgError{}, false},
		{"plain error", errors.New("connection refused"), false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPermanent(tc.err); got != tc.want {
				t.Errorf("isPermanent(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestHandle_ErrorsArePermanent verifies the real errors produced by handle
// (decode, missing identity, out-of-range score) classify as permanent.
func TestHandle_ErrorsArePermanent(t *testing.T) {
	st := &fakeStore{}
	c := New(Config{}, st, &fakePublisher{})

	testCases := []struct {
		name string
		msg  kafkaMsg.Message
	}{
		{"garbage payload", kafkaMsg.Message{Value: []byte{0x01, 0x02, 0xff, 0xfe}}},
		{"missing symbol", kafkaMsg.Message{Value: marshalStock(t, "", "NASDAQ", nil)}},
		{"missing exchange", kafkaMsg.Message{Value: marshalStock(t, "AAPL", "", nil)}},
		{"out of range score", kafkaMsg.Message{Value: marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 1.5})}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.handle(context.Background(), tc.msg)
			if err == nil {
				t.Fatal("handle() error = nil, want error")
			}
			if !isPermanent(err) {
				t.Errorf("isPermanent(handle error %v) = false, want true", err)
			}
			if st.called {
				t.Error("store.UpdateStock called, want it not to be called")
			}
		})
	}
}

// containsEvent reports whether the ordered event list contains e.
func containsEvent(events []string, e string) bool {
	for _, x := range events {
		if x == e {
			return true
		}
	}
	return false
}
