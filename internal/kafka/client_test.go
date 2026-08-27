package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

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

func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) error {
	if f.err != nil {
		return f.err
	}
	f.called = true
	f.lastSymbol = symbol
	f.lastExch = exchange
	f.lastScores = scores
	return nil
}

// --- decodeMessage ---

func TestDecodeMessage_ValidJSONAllFields(t *testing.T) {
	raw := `{"symbol":"AAPL","exchange":"NASDAQ","scores":{"momentum":0.5,"value":-0.25}}`

	m, err := decodeMessage([]byte(raw))
	if err != nil {
		t.Fatalf("decodeMessage() unexpected error: %v", err)
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

func TestDecodeMessage_ScoresAbsentDefaultsToEmptyMap(t *testing.T) {
	raw := `{"symbol":"AAPL","exchange":"NASDAQ"}`

	m, err := decodeMessage([]byte(raw))
	if err != nil {
		t.Fatalf("decodeMessage() unexpected error: %v", err)
	}
	if m.Scores == nil {
		t.Fatal("Scores = nil, want non-nil empty map")
	}
	if len(m.Scores) != 0 {
		t.Errorf("len(Scores) = %d, want 0", len(m.Scores))
	}
}

func TestDecodeMessage_BadJSONReturnsError(t *testing.T) {
	raw := `{"symbol": "AAPL",`

	_, err := decodeMessage([]byte(raw))
	if err == nil {
		t.Fatal("decodeMessage() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "decode stock message") {
		t.Errorf("decodeMessage() error = %q, want it to contain %q", err.Error(), "decode stock message")
	}
}

func TestDecodeMessage_NullAndEmptyScoresObject(t *testing.T) {
	t.Run("null scores", func(t *testing.T) {
		raw := `{"symbol":"AAPL","exchange":"NASDAQ","scores":null}`

		m, err := decodeMessage([]byte(raw))
		if err != nil {
			t.Fatalf("decodeMessage() unexpected error: %v", err)
		}
		if m.Scores == nil {
			t.Fatal("Scores = nil, want non-nil empty map")
		}
		if len(m.Scores) != 0 {
			t.Errorf("len(Scores) = %d, want 0", len(m.Scores))
		}
	})

	t.Run("empty object scores", func(t *testing.T) {
		raw := `{"symbol":"AAPL","exchange":"NASDAQ","scores":{}}`

		m, err := decodeMessage([]byte(raw))
		if err != nil {
			t.Fatalf("decodeMessage() unexpected error: %v", err)
		}
		if m.Scores == nil {
			t.Fatal("Scores = nil, want non-nil empty map")
		}
		if len(m.Scores) != 0 {
			t.Errorf("len(Scores) = %d, want 0", len(m.Scores))
		}
	})
}

// --- validate ---

func TestValidate_MissingBrokers(t *testing.T) {
	cfg := Config{
		Topic:   "stocks",
		GroupID: "ingest",
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "brokers") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "brokers")
	}
}

func TestValidate_MissingTopic(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		GroupID: "ingest",
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "topic") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "topic")
	}
}

func TestValidate_MissingGroupID(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "stocks",
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "group") {
		t.Errorf("validate() error = %q, want it to contain %q", err.Error(), "group")
	}
}

func TestValidate_AllFieldsPresent(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "stocks",
		GroupID: "ingest",
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v, want nil", err)
	}
}

// --- handle ---

func TestHandle_MissingSymbolOrExchange(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"missing symbol", `{"exchange":"NASDAQ"}`},
		{"missing exchange", `{"symbol":"AAPL"}`},
		{"both missing", `{}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			c := New(Config{}, store)

			msg := kafkaMsg.Message{Value: []byte(tc.value)}
			err := c.handle(context.Background(), msg)
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
	cases := []struct {
		name  string
		value string
	}{
		{"too high", `{"symbol":"AAPL","exchange":"NASDAQ","scores":{"momentum":1.5}}`},
		{"too low", `{"symbol":"AAPL","exchange":"NASDAQ","scores":{"value":-2.0}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			c := New(Config{}, store)

			msg := kafkaMsg.Message{Value: []byte(tc.value)}
			err := c.handle(context.Background(), msg)
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

	raw, err := json.Marshal(Message{Symbol: "AAPL", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	msg := kafkaMsg.Message{Value: raw}
	err = c.handle(context.Background(), msg)
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

	raw, err := json.Marshal(Message{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   wantScores,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	msg := kafkaMsg.Message{Value: raw}
	if err := c.handle(context.Background(), msg); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if !store.called {
		t.Fatal("store.UpdateStock not called, want it called")
	}
	if store.lastSymbol != "AAPL" {
		t.Errorf("store called with symbol %q, want %q", store.lastSymbol, "AAPL")
	}
	if store.lastExch != "NASDAQ" {
		t.Errorf("store called with exchange %q, want %q", store.lastExch, "NASDAQ")
	}
	if len(store.lastScores) != len(wantScores) {
		t.Fatalf("store called with %d scores, want %d", len(store.lastScores), len(wantScores))
	}
	for k, v := range wantScores {
		if got := store.lastScores[k]; got != v {
			t.Errorf("store called with score[%s] = %v, want %v", k, got, v)
		}
	}
}
