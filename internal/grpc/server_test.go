package grpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"stocker-store/internal/store"
	st "stocker-store/proto/v1"
)

// fakeStore is an in-memory implementation of the Store interface.
type fakeStore struct {
	stocks map[string]map[string]float64 // "exchange/symbol" -> scores
	getErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{stocks: make(map[string]map[string]float64)}
}

func key(symbol, exchange string) string { return exchange + "/" + symbol }

func splitKey(k string) (exchange, symbol string) {
	i := strings.IndexByte(k, '/')
	if i == -1 {
		return "", k
	}
	return k[:i], k[i+1:]
}

func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, error) {
	f.stocks[key(symbol, exchange)] = scores
	return &store.Stock{Symbol: symbol, Exchange: exchange, Scores: toDomainScores(scores), Created: time.Now()}, nil
}

func (f *fakeStore) RemoveStock(_ context.Context, symbol, exchange string) (bool, error) {
	k := key(symbol, exchange)
	if _, ok := f.stocks[k]; !ok {
		return false, nil
	}
	delete(f.stocks, k)
	return true, nil
}

func (f *fakeStore) GetStock(_ context.Context, symbol string, exchange *string) (*store.Stock, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	for k, scores := range f.stocks {
		ex, sym := splitKey(k)
		if sym != symbol || (exchange != nil && ex != *exchange) {
			continue
		}
		return &store.Stock{Symbol: sym, Exchange: ex, Scores: toDomainScores(scores), Created: time.Now()}, nil
	}
	return nil, errors.New("not found")
}

func (f *fakeStore) GetStocks(_ context.Context, limit int32, exchange *string, minScores, maxScores map[string]float64) ([]store.Stock, error) {
	var out []store.Stock
	for k, scores := range f.stocks {
		ex, sym := splitKey(k)
		if exchange != nil && ex != *exchange {
			continue
		}
		if matchesRange(scores, minScores, maxScores) {
			out = append(out, store.Stock{Symbol: sym, Exchange: ex, Scores: toDomainScores(scores), Created: time.Now()})
		}
	}
	if limit > 0 && int64(limit) < int64(len(out)) {
		out = out[:limit]
	}
	return out, nil
}

func matchesRange(scores, minScores, maxScores map[string]float64) bool {
	for cat, min := range minScores {
		if scores[cat] < min {
			return false
		}
	}
	for cat, max := range maxScores {
		if scores[cat] > max {
			return false
		}
	}
	return true
}

func toDomainScores(m map[string]float64) []store.ScoreEntry {
	entries := make([]store.ScoreEntry, 0, len(m))
	for cat, v := range m {
		entries = append(entries, store.ScoreEntry{Category: cat, Value: v})
	}
	return entries
}

var _ Store = (*fakeStore)(nil)

func newTestClient(t *testing.T, backend Store) st.StockStoreClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := NewServer(backend).GRPCServer()
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return st.NewStockStoreClient(conn)
}

func TestUpdateStock(t *testing.T) {
	fs := newFakeStore()
	client := newTestClient(t, fs)
	ctx := context.Background()

	stock, err := client.UpdateStock(ctx, &st.UpdateStockRequest{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   map[string]float64{"momentum": 0.5},
	})
	if err != nil {
		t.Fatalf("UpdateStock: %v", err)
	}
	if stock.GetSymbol() != "AAPL" || stock.GetExchange() != "NASDAQ" {
		t.Fatalf("unexpected stock: %v", stock)
	}
	scores := stock.GetScores()
	if len(scores) != 1 || scores[0].GetCategory() != "momentum" || scores[0].GetValue() != 0.5 {
		t.Fatalf("unexpected scores: %v", scores)
	}
}

func TestRemoveStock(t *testing.T) {
	client := newTestClient(t, newFakeStore())
	ctx := context.Background()

	if _, err := client.UpdateStock(ctx, &st.UpdateStockRequest{Symbol: "MSFT", Exchange: "NASDAQ"}); err != nil {
		t.Fatalf("UpdateStock: %v", err)
	}

	resp, err := client.RemoveStock(ctx, &st.RemoveStockRequest{Symbol: "MSFT", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("RemoveStock: %v", err)
	}
	if !resp.GetRemoved() {
		t.Fatal("expected removed=true")
	}

	resp, err = client.RemoveStock(ctx, &st.RemoveStockRequest{Symbol: "MSFT", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("RemoveStock: %v", err)
	}
	if resp.GetRemoved() {
		t.Fatal("expected removed=false for missing stock")
	}
}

func TestGetStock(t *testing.T) {
	client := newTestClient(t, newFakeStore())
	ctx := context.Background()

	if _, err := client.UpdateStock(ctx, &st.UpdateStockRequest{
		Symbol:   "TSLA",
		Exchange: "NASDAQ",
		Scores:   map[string]float64{"value": 0.25, "quality": -0.1},
	}); err != nil {
		t.Fatalf("UpdateStock: %v", err)
	}

	stock, err := client.GetStock(ctx, &st.GetStockRequest{Symbol: "TSLA"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if stock.GetSymbol() != "TSLA" || stock.GetExchange() != "NASDAQ" {
		t.Fatalf("unexpected stock: %v", stock)
	}
	if got := len(stock.GetScores()); got != 2 {
		t.Fatalf("expected 2 scores, got %d", got)
	}
}

func TestGetStock_NotFound(t *testing.T) {
	client := newTestClient(t, newFakeStore())
	_, err := client.GetStock(context.Background(), &st.GetStockRequest{Symbol: "NOPE"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestGetStocks_Filters(t *testing.T) {
	client := newTestClient(t, newFakeStore())
	ctx := context.Background()

	for _, tc := range []struct {
		symbol string
		exc    string
		mom    float64
	}{
		{"AAA", "EX1", 0.9},
		{"BBB", "EX1", 0.1},
		{"CCC", "EX2", 0.95},
	} {
		if _, err := client.UpdateStock(ctx, &st.UpdateStockRequest{
			Symbol:   tc.symbol,
			Exchange: tc.exc,
			Scores:   map[string]float64{"momentum": tc.mom},
		}); err != nil {
			t.Fatalf("UpdateStock %s: %v", tc.symbol, err)
		}
	}

	list, err := client.GetStocks(ctx, &st.GetStocksRequest{
		MinScores: map[string]float64{"momentum": 0.85},
		MaxScores: map[string]float64{"momentum": 1.0},
	})
	if err != nil {
		t.Fatalf("GetStocks: %v", err)
	}
	if got := len(list.GetStocks()); got != 2 {
		t.Fatalf("expected 2 stocks, got %d", got)
	}

	ex1 := "EX1"
	list, err = client.GetStocks(ctx, &st.GetStocksRequest{
		Exchange: &ex1,
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("GetStocks: %v", err)
	}
	if got := len(list.GetStocks()); got != 1 {
		t.Fatalf("expected 1 stock with limit, got %d", got)
	}
}

func TestAddStocks(t *testing.T) {
	fs := newFakeStore()
	client := newTestClient(t, fs)
	ctx := context.Background()

	stream, err := client.AddStocks(ctx)
	if err != nil {
		t.Fatalf("AddStocks: %v", err)
	}
	for i, sym := range []string{"S1", "S2", "S3"} {
		if err := stream.Send(&st.UpdateStockRequest{
			Symbol:   sym,
			Exchange: "EXX",
			Scores:   map[string]float64{"alpha": float64(i - 1)},
		}); err != nil {
			t.Fatalf("Send %s: %v", sym, err)
		}
	}

	stock, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if got := len(fs.stocks); got != 3 {
		t.Fatalf("expected 3 stocks in store, got %d", got)
	}
	if stock.GetExchange() != "EXX" {
		t.Fatalf("unexpected response stock: %v", stock)
	}
}

func TestAddStocks_EmptyStream(t *testing.T) {
	client := newTestClient(t, newFakeStore())
	stream, err := client.AddStocks(context.Background())
	if err != nil {
		t.Fatalf("AddStocks: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for empty stream, got %v", err)
	}
}
