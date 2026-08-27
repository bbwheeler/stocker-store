# Kafka Ingestor System — Implementation Plan

**Goal**: Verify that the Kafka ingestor system works correctly, including all codegen, build, runtime wiring, and test coverage required to confirm end-to-end correctness (message → decode → validate → persist).

---

## Phase 1 — Infrastructure Fixes (prevent compile/build/deploy from failing)

### Step 1: Add missing protobuf & gRPC dependencies to go.mod

The proto file exists at `proto/v1/stock_store.proto` but neither `google.golang.org/protobuf` nor `google.golang.org/grpc` appear in `go.mod`. Both are required for code generation and runtime.

**File**: `go.mod`
```go
// Change existing require block to:
require (
	github.com/jackc/pgx/v5 v5.7.1
	google.golang.org/grpc v1.69.4   // add
	google.golang.org/protobuf v1.36.3 // add
	github.com/klauspost/compress v1.15.9
	...
)
```

Then run `go mod tidy` to populate `go.sum`.

---

### Step 2: Generate `.pb.go` and `.grpc.pb.go` files from the proto definition

No generated Go code exists for `proto/v1/stock_store.proto`. Without this, the gRPC server cannot compile handlers.

**Approach**: Run protoc with correct plugin args:
```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/v1/stock_store.proto
```

This produces:
- `proto/v1/stockstorev1.pb.go`
- `proto/v1/stockstorev1_grpc.pb.go`

Both should live alongside the `.proto` file. The `go_package` option in the proto (`stocker-store/proto/v1;stockstorev1`) means imports use:
```go
import stockstorev1 "stocker-store/proto/v1"
```

**Files produced** (in addition to the source):
- `/home/brian/workspace/stocker-store/proto/v1/stockstorev1.pb.go`
- `/home/brian/workspace/stocker-store/proto/v1/stockstorev1_grpc.pb.go`

---

### Step 3: Fix Containerfile — wrong proto path (line 17)

**Current**: `proto/tsx/v1/tsx.proto`  
**Should be**: `proto/v1/stock_store.proto`

**File**: `Containerfile` line 14-17
```dockerfile
# Before (broken):
RUN protoc \
      --go_out=gen --go_opt=paths=source_relative \
      --go-grpc_out=gen --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
      -I /usr/include -I proto proto/tsx/v1/tsx.proto

# After (fixed):
RUN protoc \
      --go_out=gen --go_opt=paths=source_relative \
      --go-grpc_out=gen --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
      -I /usr/include -I proto proto/v1/stock_store.proto
```

---

### Step 4: Fix Containerfile — wrong build path (line 20)

**Current**: `./cmd/server` (directory that doesn't exist)  
**Should be**: `./cmd/main.go` (the actual file)

**File**: `Containerfile` line 20
```dockerfile
# Before (broken):
RUN CGO_ENABLED=0 go build -o /out/stocker-store ./cmd/server

# After (fixed):
RUN CGO_ENABLED=0 go build -o /out/stocker-store ./cmd/main.go
```

---

### Step 5: Fix Makefile — typo in path (line 4)

**Current**: `./cmd/storage/main.go`  
**Should be**: `./cmd/main.go`

**File**: `Makefile` line 4
```makefile
# Before (broken):
go build -o bin/stocker-store ./cmd/storage/main.go

# After (fixed):
go build -o bin/stocker-store ./cmd/main.go
```

---

## Phase 2 — Correctness Fixes (fix bugs that would cause runtime failure)

### Step 6: Fix `RemoveOldStocks` SQL interval type error

**File**: `/home/brian/workspace/stocker-store/internal/store/store.go` lines 136-151

The problem: `retention.String()` produces `"720h0m0s"` — Go's duration format — which PostgreSQL's interval parser cannot consume. The parameter `$1::interval` is bound as text, not as a pgx interval type.

**Fix**: Replace the `now() - $1::interval` pattern with `timestamp < $1` where `$1` is a `time.Time` value (e.g., `time.Now().Add(-retention)`). pgx natively maps Go `time.Time` to Postgres `TIMESTAMPTZ` without any format issues.

```go
// RemoveOldStocks deletes stocks (and their dependent scores) whose timestamp is
// older than the given retention window. A zero or negative retention window
// disables the check. Returns the number of stock rows removed.
func (s *Store) RemoveOldStocks(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}

	cutoff := time.Now().Add(-retention)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// First: delete dependent scores (old query used now()-$1::interval with $1 as "720h0m0s")
	_, err = tx.Exec(ctx, `
		WITH old AS (
			SELECT symbol, exchange FROM stocks
			WHERE timestamp < $1
		)
		DELETE FROM scores
		USING old
		WHERE scores.symbol = old.symbol AND scores.exchange = old.exchange
	`, cutoff)  // <-- time.Time, not string
	if err != nil {
		return 0, fmt.Errorf("delete old scores: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		DELETE FROM stocks
		WHERE timestamp < $1
	`, cutoff)  // <-- time.Time, not string
	if err != nil {
		return 0, fmt.Errorf("delete old stocks: %w", err)
	}
	removed := tag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	return removed, nil
}
```

**Same issue on two lines**: `store.go:139` and `store.go:151` — both changed at once.

---

## Phase 3 — Implement Missing Functionality (kafka ingestor can't work without this)

### Step 7: Implement real gRPC server handlers in internal/grpc/server.go

**File**: `/home/brian/workspace/stocker-store/internal/grpc/server.go`

The `GRPCServer()` method currently returns a noop stub. Replace it with real generated service binding so RPCs are processed instead of silently dropped.

```go
// import add at top:
import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"stocker-store/internal/store"
	st "stocker-store/proto/v1/stockstorev1"  // renamed import to avoid collision with store pkg
)

// Registered is the method called during NewServer.
type Registered interface {
	RegisterStockStoreServer(s *grpc.Server, srv StockStoreServer)
}

type StockStoreServer interface {
	AddStocks(stream st.StockStore_AddStocksServer) error
	*st.UnimplementedStockStoreServer // embed this in your actual impl for forward comp
}

// newGRPCServer constructs the real gRPC server from the generated pb code.
var newGRPCServer = func(srv StockStoreServer) Registered {
	s := grpc.NewServer()
	st.RegisterStockStoreServer(s, srv)
	return s
}
```

Then implement all RPCs as handlers on a `*server` struct:

```go
// Server holds the dependencies for the gRPC service.
type Server struct {
	store    Store  // store.Store interface
	srv      st.StockStoreServer
	stopFunc context.CancelFunc
	grpcsrv  Registered
}

func NewServer(st Store) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{store: st, stopFunc: cancel}
}
```

Key implementation patterns for each handler:

```go
// AddStocks implements the streaming server.
func (s *Server) AddStocks(stream st.StockStore_AddStocksServer) error {
	ctx := stream.Context()
	count := 0
	for {
		req, err := stream.Recv() // UpdateStockRequest
		if err == io.EOF {
			return stream.SendAndClose(&stock{Symbol: "", Exchange: ""}) // or empty Stock
		}
		if err != nil {
			return err
		}
		scores := make(map[string]float64)
		// Handle optional maps in proto3 (proto-gen-go sets map to non-nil if present)
		for c, v := range req.Scores {
			scores[c] = v
		}
		_, err = s.store.UpdateStock(ctx, req.Symbol, req.Exchange, scores)
		if err != nil {
			return err
		}
		count++
	}
}

// UpdateStock implements the unary RPC.
func (s *Server) UpdateStock(ctx context.Context, req *st.UpdateStockRequest) (*st.Stock, error) {
	scores := make(map[string]float64)
	for c, v := range req.Scores {
		scores[c] = v
	}
	stock, err := s.store.UpdateStock(ctx, req.Symbol, req.Exchange, scores)
	if err != nil {
		return empty, err
	}
	return toProtoStock(stock), nil
}

// RemoveStock, GetStock, GetStocks follow the same pattern.
```

### Conversion helpers needed:

```go
func toProtoScores(se []store.ScoreEntry) []*st.ScoreEntry {
	ps := make([]*st.ScoreEntry, len(se))
	for i, e := range se {
		ps[i] = &st.ScoreEntry{Category: e.Category, Value: e.Value}
	}
	return ps
}

func toProtoStock(s *store.Stock) *st.Stock {
	return &st.Stock{Symbol: s.Symbol, Exchange: s.Exchange, Scores: toProtoScores(s.Scores)}
}
```

---

## Phase 4 — Test Coverage (kafka client has zero tests)

### Step 8: Add kafka client tests with mock store

**Create**: `/home/brian/workspace/stocker-store/internal/kafka/client_test.go`

Test every public method and critical edge cases. Use an in-memory store mock, never require a real Kafka broker for unit tests (that's an integration test concern).

#### Test: `decodeMessage` — happy path
```go
func TestDecodeMessage(t *testing.T) {
	raw := []byte(`{"symbol":"AAPL","exchange":"NASDAQ","scores":{"bullish":0.8}}`)
	m, err := decodeMessage(raw)
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if m.Symbol != "AAPL" || m.Exchange != "NASDAQ" {
		t.Errorf("got symbol=%q exchange=%q; want AAPL NASDAQ", m.Symbol, m.Exchange)
	}
	if len(m.Scores) != 1 || m.Scores["bullish"] != 0.8 {
		t.Errorf("unexpected scores: %v", m.Scores)
	}
}
```

#### Test: `decodeMessage` — nil scores field defaults to empty map
```go
func TestDecodeMessage_nilScores(t *testing.T) {
	raw := []byte(`{"symbol":"GOOG","exchange":"NASDAQ"}`)
	m, err := decodeMessage(raw)
	if err != nil || m.Scores == nil {
		t.Errorf("want non-nil empty scores; got %v (err=%v)", m.Scores, err)
	}
}
```

#### Test: `validate` — errors without brokers/topic/groupID
```go
func TestValidate(t *testing.T) {
	tests := []struct {
		name       string
		give       Config
		wantErrSub string
	}{
		{"no brokers", Config{Topic: "t", GroupID: "g"}, "no brokers"},
		{"no topic",   Config{Brokers: []string{"h"}, GroupID: "g"}, "no topic"},
		{"no groupID",  Config{Brokers: []string{"h"}, Topic: "t"}, "no consumer group"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.give.validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Errorf("validate(%v): got %v; want substring %q", tt.give, err, tt.wantErrSub)
			}
		})
	}
}
```

#### Test: `handle` — missing symbol/exchange rejected
```go
func TestHandle(t *testing.T) {
	var mock badStore
	c := &Client{store: mock, decoder: decodeMessage}
	err := c.handle(context.Background(), kafka.Message{Key: nil, Value: []byte(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "missing symbol or exchange") {
		t.Errorf("want error about missing symbol/exchange; got %v", err)
	}
}
```

#### Test: `handle` — score out of range rejected
```go
func TestHandle_badScore(t *testing.T) {
	var mock badStore
	c := &Client{store: mock, decoder: decodeMessage}
	err := c.handle(context.Background(), kafka.Message{Value: []byte(`{"symbol":"X","exchange":"TSE","scores":{"bad":2.5}}`)})
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Errorf("want out-of-range error; got %v", err)
	}
}
```

**Mock type**: Create a tiny mock `store.Store` for testing:
```go
type fakeStore struct {
	lastSymbol   string
	lastExchange string
	scores       map[string]float64
	storeErr     error
}

func (f *fakeStore) UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) error {
	f.lastSymbol = symbol
	f.lastExchange = exchange
	f.scores = scores
	return f.storeErr
}
```

---

## Phase 5 — Kafka Consumer Integration Verification

### Step 9: Test main.go's Kafka + gRPC integration wiring

**File**: `/home/brian/workspace/stocker-store/cmd/main.go`

Verify three integration points in `runKafkaSubscriber()` and in the shutdown flow:

#### What to verify manually (or with an acceptance test):

1. **Env var parsing**:
   - Without `KAFKA_BROKERS` or `KAFKA_TOPIC`: returns nil, gRPC-only mode ✓ (no Kafka start)
   - With both vars present: creates client with correct GroupID (default `"stocker-store"`)  ✓
   - Broker list parsed by comma-split → `[]string{"localhost:9092","localhost:9093"}`

2. **Bridge store adapter**: Confirm `bridgeStore.UpdateStock` calls through to `*store.Store.UpdateStock` and unwraps the returned error correctly (it does today on line 147).

3. **Graceful shutdown wiring** (current):
   - gRPC: `gRPCServer.GracefulStop()` called after `<-ctx.Done()` ✓ (line 59)
   - Kafka: The client's `Run(ctx)` loop exits when ctx is canceled (line 68–73 in kafka/client.go uses `ctx.Err()`) ✓
   - Both are started as goroutines and both react to the same context (`ctx` line 24-25 of main.go).

4. **Retention goroutine**: Confirmed it runs separately with its own ticker, using the same `ctx`. When shutdown arrives (`<-ctx.Done()` line 56), the ticker loop exits and eventually the Kafka consumer sees `ctx.Err() != nil`. ✓

#### What's actually missing/wrong in current wiring:
- The **main goroutine panics if gRPC start returns an error** because it calls `lis interface{}` on a noop server. Fix by removing the stub completely with real grpc.Server (which we do in Step 7).
- There is no explicit Kafka graceful shutdown — after step 7's real gRPC server, the flow should be:
  ```go
  // After ctx done in main():
  cancel()                    // signals both goroutines to stop
  gRPCServer.GracefulStop()   // waits for gRPC listeners to drain
  // Kafka Run() will return as soon as ctx.Err() != nil on next ReadMessage cycle
  ```

This wiring is already correct because `client.go:68` checks `ctx.Err()` before each read and returns immediately. The only gap is no explicit "wait" goroutine — but since main falls through to the end of `main()`, the process will exit, which stops everything. To be more robust, add a short sleep or sync.WaitGroup:

```go
var wg sync.WaitGroup

wg.Add(2)
go func() { defer wg.Done(); gRPCServer.Serve(lis) }()

go func() { defer wg.Done(); _ = client.Run(ctx) }()

<-ctx.Done()

gRPCServer.GracefulStop()

wg.Wait() // ensure both goroutines exit cleanly before process ends
```

---

### Step 10: Verify `RemoveOldStocks` works against a real database

**Manually test**: Spin up a PostgreSQL instance (podman run or docker) and verify:

```bash
DATABASE_URL="postgres://user:pass@localhost:5432/stocker_test" go test -run TestRemoveOldStocks ./internal/store/ -v
```

Or manually:
```sql
-- Insert a stock with timestamp = now() - 7 days (assuming default 30-day retention)
INSERT INTO stocks(symbol, exchange, timestamp) VALUES ('TEST', 'EXCH', now() - interval '7 days');

-- Call RemoveOldStocks via program (it should delete the row above)

-- Confirm it's gone
SELECT * FROM stocks WHERE symbol = 'TEST';  -- should return empty
```

---

## Phase 6 — Verification Checklist (Step-by-step Confirmation)

Each item below is a **gating check** — none of the following Kafka ingestor functionality works until all pass:

### A. Compile & Build
- [ ] `go mod tidy` completes without errors (deps installed per Step 1)
- [ ] `protoc ... proto/v1/stock_store.proto` produces `.pb.go` files with no errors (Step 2)
- [ ] `go build ./...` compiles all packages, including the gRPC server (Steps 2+3)
- [ ] `make build` produces binary at `bin/stocker-store` without path errors (Step 5)

### B. Kafka Client Correctness
- [ ] `decodeMessage` parses valid JSON → struct ✓
- [ ] `decodeMessage` default-fills nil scores to empty map `{}` ✓
- [ ] `validate` returns error when any of brokers/topic/groupID is missing ✓
- [ ] `handle` rejects messages with empty symbol or exchange ✓
- [ ] `handle` rejects messages with scores outside [-1.0, 1.0] ✓
- [ ] `handle` successfully passes valid messages to store mock ✓
- [ ] `Client.Run(ctx)` exits cleanly when ctx is canceled ✓

### C. Kafka Infrastructure (end-to-end)
- [ ] Setting both `KAFKA_BROKERS` and `KAFKA_TOPIC` starts consumer in main.go (not no-op)
- [ ] Without those env vars, the service still runs as gRPC-only (no panic or crash)
- [ ] Broker comma-split parsing produces correct `[]string`
- [ ] Default GroupID `"stocker-store"` used when `KAFKA_GROUP_ID` not set

### D. Store Layer
- [ ] `RemoveOldStocks` works against a real PostgreSQL instance (interval → time.Time fix, Step 6)
- [ ] `UpdateStock` with scores upserts both stock row and score rows in one transaction
- [ ] Score values stored with `-1.0 ≤ value ≤ 1.0` constraint enforced by DB

### E. End-to-end Kafka Ingestor Flow
- [ ] Published `{symbol, exchange, scores}` to configured topic
- [ ] Consumer deserializes JSON → struct (decode) ✓
- [ ] Validates fields: symbol/exchange non-empty, scores in range (validate) ✓
- [ ] Calls `store.UpdateStock` which persists to PostgreSQL (persist) ✓
- [ ] No panics or data loss when score fields are absent ✓

### F. Containerization
- [ ] `Containerfile` line 17 builds with correct proto path (`proto/v1/stock_store.proto`) ✓
- [ ] `Containerfile` line 20 builds binary from `./cmd/main.go` ✓
- [ ] Image runs `stocker-store` and exposes port 50051 (gRPC) ✓

### G. Graceful Shutdown
- [ ] Ctrl+C (`os.Interrupt`) triggers `<-ctx.Done()` → cancellation propagated to both goroutines ✓
- [ ] gRPC listener drains requests before stopping ✓
- [ ] Kafka reader exits its read loop when `ctx.Err() != nil` ✓
