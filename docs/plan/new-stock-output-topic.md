# New-Stock Output Topic — Implementation Plan

**Goal:** When a new `(symbol, exchange)` pair is submitted for the first time (an `INSERT`, not an `UPDATE`), the service publishes a PROTOBUF-encoded `Stock` message (the same one gRPC returns) to a dedicated output Kafka topic named by `KAFKA_OUTPUT_TOPIC`. The existing consumer-facing topic variable `KAFKA_TOPIC` is renamed to `KAFKA_INPUT_TOPIC` (no backwards-compat).

**Architecture:** `Store.UpdateStock` gains a second return value — a `bool` indicating whether the upsert was an `INSERT` (`true`) or an `UPDATE` (`false`) — obtained via Postgres `ON CONFLICT ... DO UPDATE ... RETURNING (xmax = 0) AS inserted`. A new `kafka.Publisher` interface (implemented by a real `kafka-go` `Writer` and a no-op variant) is constructed once in `cmd/main.go` and injected into the gRPC server and the Kafka client; whichever path produced a new stock calls `Publish` (log-and-continue on failure).

**Tech Stack:** Go 1.25, `pgx/v5`, `segmentio/kafka-go` (writer + existing reader), `google.golang.org/protobuf`.

**Spec:** `docs/design.md` §12 (New-Stock Output Topic). Every constraint in §12 applies to this plan. In particular: no new libs, no new proto schema, input and output are independent no-ops, publish-failure is log-and-continue, no backward compatibility for the `KAFKA_TOPIC` rename.

## Global constraints

- Keep the code surface minimal; do not restructure packages we do not touch.
- No new dependencies in `go.mod`; `kafka-go` is already present.
- Publish must NOT roll back the DB write and must NOT cause the gRPC or Kafka-handler to return an error (log-and-continue).
- Input Kafka remains a no-op unless both `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set.
- Output Kafka is a no-op unless both `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set.
- The published message is the authoritative, server-stamped stock (the one the store returns after the upsert), not the raw input as received.
- The tree must compile (`go build ./…`) after every number-step in §Implementation steps.

### Prerequisites

**Steps 1–7 assume Step 0 is complete.** Step 0 restores the gRPC service registration and the missing `proto/v1/stock_store_grpc.pb.go` stub that were removed in commit `fc63d46`. Without Step 0 the gRPC test package cannot compile (client symbols are undefined) and the live gRPC service is unregistered — so Step 4's feature wiring would be a no-op in production.

---

## File structure (before defining steps)

| File | Change | Responsibility |
|---|---|---|
| `proto/v1/stock_store_grpc.pb.go` | **Regenerate** | Restore the missing gRPC service stub. |
| `internal/store/store.go` | Modify `UpdateStock` (lines 71–120) | Report `inserted bool` alongside the existing `*Stock`. |
| `internal/kafka/publisher.go` | **Create** | Define `Publisher` interface + `NewPublisher` + no-op and writer-backed impls. |
| `internal/kafka/client.go` | Modify `handle`, add optional `Publisher` field | Trigger `Publish` when the store reports `inserted=true`. |
| `internal/kafka/client_test.go` | Extend `fakeStore`, add `fakePublisher` | Add tests for new behavior; keep existing tests passing. |
| `internal/grpc/server.go` | Modify `Store` interface, `NewServer`, `AddStocks`, `UpdateStock` | Trigger `Publish` on new stock; tolerate publish errors. |
| `internal/grpc/server_test.go` | Extend `fakeStore`, add `fakePublisher` | Add tests for new behavior; keep existing tests passing. |
| `internal/store/store_test.go` | Add `TestUpdateStockInsertedFlag` | Assert `inserted=true` on first write, `false` on subsequent. |
| `cmd/main.go` | Rename env var, construct publisher, wire it in | Own the lifetime of the `Publisher`; pass it to both call sites. |
| `README.md` | Rename `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC`, add `KAFKA_OUTPUT_TOPIC` | Keep the operator-facing table and prose accurate. |

No new files under `proto/` or `internal/store`. No `go.mod` changes.

---

## Design decisions (summary, with rationale)

1. **Insert-detection mechanism — `RETURNING (xmax = 0)`.** A single round-trip inside the existing transaction, atomic with the write, deterministic under concurrent writers (Postgres serializes conflict resolution; exactly one writer sees `xmax = 0`). Rejected alternative: pre-check with `SELECT exists(...)` — that is racy under concurrency and adds a round-trip.
2. **API shape — `UpdateStock` returns `(*Stock, bool, error)`.** One method, one added return value. Rejected alternative: a second method `UpsertStock` — duplicates logic or requires `UpdateStock` to delegate, growing the surface. The `bool` reads as a direct answer to "did this create the row?".
3. **Publisher placement — inside the Kafka package, injected as an interface.** Keeps the `cmd` wiring trivial (`kafka.NewPublisher(...)`) and avoids a new internal package. The no-op variant is in the same package so the `Publisher` type has one concrete home.
4. **Who triggers the publish — the caller of `UpdateStock`, not the store.** The store stays agnostic of where events go; it just reports the outcome of its own write. Both ingestion paths (gRPC and Kafka-consumer) already sit right after the `UpdateStock` call; a two-line hook there is minimal.
5. **Failure semantics — log-and-continue.** A failed publish is logged at error level; the handler returns success to the caller. Rationale: DB write is authoritative; the output topic is a best-effort fan-out. Retries/DLQs are out of scope. Making a Kafka publish fatal to the primary write is disproportionate to the event's importance.
6. **Publish is synchronous (briefly blocking) with `kafka-go` `Writer.Async = true`.** Ordering is simple (the event is emitted at the moment of creation), cost is a queue-push not a network round-trip for high volume.

### Configuration matrix

| `KAFKA_BROKERS` set | `KAFKA_INPUT_TOPIC` set | `KAFKA_OUTPUT_TOPIC` set | Behavior |
|---|---|---|---|
| yes | yes | — | Consume from input topic; do not publish on new stocks (publish is logged as no-op). |
| yes | — | yes | Do not consume; publish on new stocks. |
| yes | yes | yes | Consume from input topic AND publish on new stocks. |
| yes | — | — | No Kafka at all; pure gRPC. |
| no  | any |  any  | No Kafka at all; pure gRPC. (A no-op publisher is constructed and `Close`ed.) |

### Message format

Same `Stock` PROTOBUF message gRPC returns (`stockstorev1.Stock`), serialized with `proto.Marshal`. No new schema, no JSON, no compression.

```protobuf
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }
```

### Code-change summary (file-by-file)

- **`internal/store/store.go`** — change `UpdateStock` signature and add one SQL line (`RETURNING (xmax = 0) AS inserted`); change `Exec` → `QueryRow`. All other store methods untouched.
- **`internal/kafka/publisher.go`** *(new)* — `type Publisher interface { Publish(ctx, *stockv1.Stock) error; Close() error }`. `NewPublisher(brokers, topic) Publisher` returns either a `*writerPublisher` (real `kafka.Writer`) or the shared `Noop` value based on config.
- **`internal/kafka/client.go`** — `New(cfg, store, publisher Publisher) *Client`; `handle` captures `*store.Stock, inserted, err := c.store.UpdateStock(...)` and, on `err == nil && inserted && c.publisher != nil`, calls `c.publisher.Publish(ctx, toProto(stock))` and logs (does not return) the error. Add a small `toProto(*store.Stock) *stockv1.Stock` helper (mirror of the one in `internal/grpc/server.go`).
- **`internal/kafka/client_test.go`** — `fakeStore.UpdateStock` gains the `inserted` return value; new `fakePublisher` records calls; add `TestHandle_PublishesOnNewStock`, `TestHandle_DoesNotPublishOnUpdate`, `TestHandle_DoesNotPublishWhenStoreErrors`, `TestHandle_PublishErrorDoesNotFailTheMessage`.
- **`internal/grpc/server.go`** — `Store` interface signature updated; `Server` gains `publisher kafka.Publisher`; `NewServer(store Store, publisher kafka.Publisher) *Server`; `AddStocks` and `UpdateStock` capture three-return value and publish when `inserted && s.publisher != nil`, logging (not returning) publish errors.
- **`internal/grpc/server_test.go`** — `fakeStore.UpdateStock` signature updated (fake returns `inserted=true` always, so existing tests still see a stock in the store); add `fakePublisher`; add `TestUpdateStock_PublishesOnNewStock`, `TestUpdateStock_DoesNotPublishOnUpdate`, `TestAddStocks_PublishesPerNewStock`, `TestUpdateStock_PublishErrorDoesNotFailResponse`.
- **`internal/store/store_test.go`** — add `TestUpdateStockInsertedFlag` (uses existing `testStore`, `clearStock` helpers).
- **`cmd/main.go`** — read `KAFKA_INPUT_TOPIC` (was `KAFKA_TOPIC`) and `KAFKA_OUTPUT_TOPIC`; construct `publisher := kafka.NewPublisher(brokers, outputTopic)` once; `defer publisher.Close()` after `gRPCServer.GracefulStop()`; pass it into `grpc.NewServer(st, publisher)` and `kafka.New(cfg, st, publisher)`.
- **`README.md`** — rename `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC` in the env table (line ~202) and prose (lines ~189, 205, 224, 371); add a new row for `KAFKA_OUTPUT_TOPIC`; expand the sample env file to include both topics; add a short "New-Stock Output" subsection to the `Kafka Ingestion` section.

---

## Implementation steps

*(Each step ends with the tree compiling (`go build ./...`) or tests passing (`go test ./...`) as noted. Run `go vet ./...` after each step as well.)*

### Step 0: Restore pre-existing gRPC service registration (prerequisite for step 4)

**Why this is required.** My feature step 4 wires the `Publisher` into the gRPC `AddStocks` / `UpdateStock` path, but the gRPC service on `main` is **not currently functional**: commit `fc63d46` deleted `proto/v1/stockstorev1_grpc.pb.go` (267 lines) and removed both `st.UnimplementedStockStoreServer` and `st.RegisterStockStoreServer(srv, s)` from `internal/grpc/server.go`. As a result `go test ./internal/grpc/...` cannot even compile (undefined `st.StockStoreClient` / `st.NewStockStoreClient`) and, even if it did, the service is unregistered in the running binary — every client call returns `Unimplemented: unknown service stockstore.v1.Stock`. Restoring those two lines and the missing stub is **not** new feature scope; it is a prerequisite for step 4 to have any effect in production.

**Files:**
- Regenerate: `proto/v1/stock_store_grpc.pb.go` — produce from `proto/v1/stock_store.proto` using the project toolchain `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (verified with `which protoc` and `$(go env GOPATH)/bin/protoc-gen-go --version`; the repo's existing `stock_store.pb.go` was generated by `protoc-gen-go v1.36.12`, which is the version in this environment).
- Modify: `internal/grpc/server.go` — restore the two lines that commit `fc63d46` deleted: (a) embed `st.UnimplementedStockStoreServer` in the `Server` struct, (b) call `st.RegisterStockStoreServer(srv, s)` inside `GRPCServer()`.

**Interfaces:**
- Consumes: existing `st.StockStoreServer`, `st.UnimplementedStockStoreServer`, `st.RegisterStockStoreServer`, `st.StockStore_ServiceDesc`, `st.StockStoreClient`, `st.NewStockStoreClient` (from the regenerated `proto/v1/stock_store_grpc.pb.go`).
- Produces: a functioning `stockstore.v1.StockStore` gRPC service in the running binary — `go build ./...` succeeds, `go vet ./...` succeeds, and the existing 7 gRPC tests in `internal/grpc/server_test.go` pass.

- **0.1 — Regenerate `proto/v1/stock_store_grpc.pb.go`.**

  Verify the toolchain is present:
  ```bash
  which protoc                                  # expected: /usr/bin/protoc
  protoc --version                              # expected: libprotoc 3.21.12 (matches existing stock_store.pb.go header)
  "$(go env GOPATH)/bin/protoc-gen-go" --version      # expected: protoc-gen-go v1.36.12
  "$(go env GOPATH)/bin/protoc-gen-go-grpc" --version # expected: 1.6.2
  ```
  (The `go_package` option in `proto/v1/stock_store.proto` is `proto/v1;stockstorev1`, so output lands in `proto/v1/` under the `stockstorev1` package — the same package the existing `stock_store.pb.go` is in; the server and message symbols share one Go package.)

  Run the exact `protoc` invocation from the repo root (the `proto/` directory is the include root so `google/protobuf/timestamp.proto` is resolved via the default include paths, and the output path preserves the `v1/` layout):
  ```bash
  protoc \
    --go_out=. \
    --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    -I proto \
    proto/v1/stock_store.proto
  ```
  - `paths=source_relative` keeps the file at `proto/v1/` (relative to the include root) rather than re-deriving it from the `go_package` import path — this is why the output is `proto/v1/stock_store.pb.go` and `proto/v1/stock_store_grpc.pb.go`, not nested under a module path.
  - `--go-out` regenerates `proto/v1/stock_store.pb.go` (messages) — this file is byte-identical to the one already in the tree, so it is a no-op content-wise but is produced anyway. **Do not hand-edit it.**
  - `--go-grpc-out` produces the file that was deleted: **`proto/v1/stock_store_grpc.pb.go`**.

  Verify the generated file defines all the symbols `internal/grpc/server.go` and `internal/grpc/server_test.go` need:
  ```bash
  grep -q "type UnimplementedStockStoreServer struct"    proto/v1/stock_store_grpc.pb.go
  grep -q "func RegisterStockStoreServer"                proto/v1/stock_store_grpc.pb.go
  grep -q "type StockStore_ServiceDesc"                   proto/v1/stock_store_grpc.pb.go
  grep -q "func NewStockStoreClient"                      proto/v1/stock_store_grpc.pb.go
  grep -q "type StockStoreClient interface"               proto/v1/stock_store_grpc.pb.go
  ```
  (The exact `grep -l "func RegisterStockStoreServer" proto/v1/stock_store_grpc.pb.go` check should print the path.)

- **0.2 — Restore the service registration in `internal/grpc/server.go`.**

  Apply these two edits — they reverse commit `fc63d46` exactly (`git show fc63d46 -- internal/grpc/server.go` is the reference diff):

  (a) Re-embed `st.UnimplementedStockStoreServer` as the first field of the `Server` struct:
  ```go
  // Server holds the dependencies for the gRPC service.
  type Server struct {
  	st.UnimplementedStockStoreServer
  	store Store
  }
  ```

  (b) Re-register the service in `GRPCServer()`:
  ```go
  // GRPCServer returns a new gRPC server with the StockStore service registered.
  func (s *Server) GRPCServer() *grpc.Server {
  	srv := grpc.NewServer()
  	st.RegisterStockStoreServer(srv, s)
  	return srv
  }
  ```

  No other changes to this file in step 0 (`AddStocks` / `UpdateStock` / `NewServer` keep their current shape — step 4 extends them).

- **0.3 — Verify.**

  ```bash
  go build ./...
  go vet ./...
  go test ./internal/grpc/...
  ```
  **All three must pass.** The existing 7 gRPC tests in `internal/grpc/server_test.go` are the acceptance test for this step: `TestUpdateStock`, `TestRemoveStock`, `TestGetStock`, `TestGetStock_NotFound`, `TestGetStocks_Filters`, `TestAddStocks`, `TestAddStocks_EmptyStream`. `TestUpdateStock` exercises `st.StockStoreClient` / `st.NewStockStoreClient` (which only exist once the grpc stub is regenerated), and every one of the seven exercises the registered service (which only works once `st.RegisterStockStoreServer(srv, s)` is back). If any of the seven still fails — `undefined: st.StockStoreClient`, `Unimplemented: unknown service stockstore.v1.Stock`, or otherwise — step 0 is **not complete**; do not proceed to step 1.

---

### Step 1: Report insert-vs-update from `Store.UpdateStock`

**Files:**
- Modify: `internal/store/store.go` (lines 71–120)
- Modify: `internal/store/store_test.go` (add one test)
- Modify: `internal/kafka/client_test.go:28–37` (**only** `fakeStore.UpdateStock` signature — required so the package still compiles)
- Modify: `internal/grpc/server_test.go:41–44` (**only** `fakeStore.UpdateStock` signature — required so the package still compiles)

**Interfaces:**
- Consumes: existing `pgx` `tx.QueryRow` (already in use elsewhere).
- Produces: `func (*Store) UpdateStock(ctx, symbol, exchange string, scores map[string]float64) (*Stock, bool, error)` where the `bool` is `inserted == true` on the first `INSERT` and `false` on a subsequent `DO UPDATE`.

- **1.1 — Change the SQL and the signature in `internal/store/store.go`.**

  Replace the current `UpdateStock` body (lines 70–120) with:

  ```go
  // UpdateStock upserts a stock and optionally its scores. Returns the updated
  // stock record and a bool indicating whether the upsert was an INSERT
  // (newly-created (symbol, exchange) pair) rather than an UPDATE of an
  // existing row.
  func (s *Store) UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*Stock, bool, error) {
  	tx, err := s.pool.Begin(ctx)
  	if err != nil {
  		return nil, false, fmt.Errorf("begin tx: %w", err)
  	}
  	defer tx.Rollback(ctx)

  	var inserted bool
  	err = tx.QueryRow(ctx, `
  		INSERT INTO stocks (symbol, exchange, timestamp)
  		VALUES ($1, $2, now())
  		ON CONFLICT (symbol, exchange) DO UPDATE SET timestamp = now()
  		RETURNING (xmax = 0) AS inserted
  	`, symbol, exchange).Scan(&inserted)
  	if err != nil {
  		return nil, false, fmt.Errorf("update stock: %w", err)
  	}

  	if len(scores) > 0 {
  		upsertScore := `
  			INSERT INTO scores (symbol, exchange, category, value, timestamp)
  			VALUES ($1, $2, $3, $4, now())
  			ON CONFLICT (symbol, exchange, category)
  			DO UPDATE SET value = EXCLUDED.value, timestamp = now()
  		`
  		for cat, val := range scores {
  			if _, err := tx.Exec(ctx, upsertScore, symbol, exchange, cat, val); err != nil {
  				return nil, false, fmt.Errorf("upsert score %s: %w", cat, err)
  			}
  		}
  	}

  	if err := tx.Commit(ctx); err != nil {
  		return nil, false, fmt.Errorf("commit tx: %w", err)
  	}

  	var ts time.Time
  	if err := s.pool.QueryRow(ctx,
  		`SELECT timestamp FROM stocks WHERE symbol = $1 AND exchange = $2`,
  		symbol, exchange).Scan(&ts); err != nil {
  		return nil, false, fmt.Errorf("get stock timestamp after update: %w", err)
  	}

  	stock := &Stock{Symbol: symbol, Exchange: exchange, Updated: ts}
  	stock.Scores, err = s.getStockScores(ctx, symbol, exchange)
  	if err != nil {
  		return nil, false, fmt.Errorf("get stock scores after update: %w", err)
  	}

  	return stock, inserted, nil
  }
  ```

- **1.2 — Update the two test fakes (signature only) so the tree compiles.**

  `internal/kafka/client_test.go:28` — signature gains the `bool` (the fake always reports `inserted=true`, which is what the new publish-on-new-stock tests need):
  ```go
  func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
  	if f.err != nil {
  		return nil, false, f.err
  	}
  	f.called = true
  	f.lastSymbol = symbol
  	f.lastExch   = exchange
  	f.lastScores = scores
  	return &store.Stock{Symbol: symbol, Exchange: exchange, Updated: time.Now()}, true, nil
  }
  ```

  `internal/grpc/server_test.go:41`:
  ```go
  func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
  	f.stocks[key(symbol, exchange)] = scores
  	return &store.Stock{Symbol: symbol, Exchange: exchange, Scores: toDomainScores(scores), Updated: time.Now()}, true, nil
  }
  ```

- **1.3 — Add `TestUpdateStockInsertedFlag`** to `internal/store/store_test.go`:

  ```go
  // TestUpdateStockInsertedFlag verifies that the second return value of
  // UpdateStock is true on the first write to a (symbol, exchange) pair
  // (INSERT) and false on subsequent writes to the same pair (UPDATE).
  func TestUpdateStockInsertedFlag(t *testing.T) {
  	s := testStore(t)
  	defer s.Close()

  	sym, exh := uniqueTag()
  	defer clearStock(t, s, sym, exh)

  	ctx := context.Background()
  	scores := map[string]float64{"momentum": 0.25}

  	first, inserted, err := s.UpdateStock(ctx, sym, exh, scores)
  	if err != nil {
  		t.Fatalf("first UpdateStock: %v", err)
  	}
  	if inserted != true {
  		t.Fatalf("inserted = %v on first write, want true", inserted)
  	}
  	if first == nil {
  		t.Fatalf("first stock is nil")
  	}

  	second, inserted2, err := s.UpdateStock(ctx, sym, exh, scores)
  	if err != nil {
  		t.Fatalf("second UpdateStock: %v", err)
  	}
  	if inserted2 != false {
  		t.Fatalf("inserted = %v on second write, want false", inserted2)
  	}
  	if second == nil {
  		t.Fatalf("second stock is nil")
  	}
  }
  ```

- **1.4 — Verify.**

  Run: `go build ./...` → success.
  Run: `go vet ./...` → success.
  Run: `go test ./...` → existing tests pass (the new store test skips when `DATABASE_URL` is unset; the two fake-signature updates keep the `kafka` and `grpc` test packages compiling and passing).

---

### Step 2: Add the `kafka.Publisher` interface + `NewPublisher` + no-op impl

**Files:**
- Create: `internal/kafka/publisher.go`

**Interfaces:**
- Consumes: `segmentio/kafka-go` `Writer`, `WriterConfig` (already in `go.mod`), `stockv1.Stock` (already in the tree).
- Produces:
  - `type Publisher interface { Publish(ctx context.Context, stock *stockv1.Stock) error; Close() error }`
  - `func NewPublisher(brokers []string, topic string) Publisher`

- **2.1 — Create `internal/kafka/publisher.go`:**

  ```go
  // This file defines the Publisher interface, its constructor, and a
  // shared no-op implementation. It is consumed by the gRPC server and the
  // kafka client whenever a new stock (an INSERT, not an UPDATE) is written
  // to the store.
  package kafka

  import (
  	"context"
  	"errors"
  	"fmt"
  	"log"
  	"time"

  	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

  	"github.com/segmentio/kafka-go"
  	"google.golang.org/protobuf/proto"
  )

  // Publisher emits new-stock events on the configured output topic.
  // The zero-value semantics are "no-op": Publish and Close both return nil
  // without side effects. Implementations are expected to be safe for
  // concurrent use.
  type Publisher interface {
  	// Publish serializes stock with protobuf and writes it to the output
  	// topic. It is best-effort: callers are expected to log (not return)
  	// an error so that a publish failure does not abort the primary DB
  	// write or the gRPC/Kafka handler response.
  	Publish(ctx context.Context, stock *stockv1.Stock) error
  	// Close flushes any pending writes and releases any resources.
  	// Calling Close on a no-op publisher returns nil.
  	Close() error
  }

  // Noop is the shared no-op Publisher: Publish and Close return nil without
  // side effects. Useful for callers that want to hold a non-nil Publisher
  // without configuring a broker/topic.
  type Noop struct{}

  func (Noop) Publish(context.Context, *stockv1.Stock) error { return nil }
  func (Noop) Close() error                                  { return nil }

  // NewPublisher returns a Publisher configured for the given output topic
  // and broker list, or a no-op Publisher if either argument is empty.
  // The returned value is always non-nil.
  func NewPublisher(brokers []string, topic string) Publisher {
  	if len(brokers) == 0 || topic == "" {
  		log.Printf("kafka output publisher disabled (need both brokers and an output topic)")
  		return Noop{}
  	}
  	w := kafka.NewWriter(kafka.WriterConfig{
  		Brokers:  brokers,
  		Topic:    topic,
  		Async:    true,
  		BatchMax: 100,
  		BatchMin: 1,
  		BatchTimeout: 100 * time.Millisecond,
  	})
  	return &writerPublisher{w: w, topic: topic}
  }

  // writerPublisher publishes to a kafka.Writer.
  type writerPublisher struct {
  	w     *kafka.Writer
  	topic string
  }

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

  func (p *writerPublisher) Close() error {
  	return p.w.Close()
  }
  ```

  (Note: `BatchMax`/`BatchMin`/`BatchTimeout` can be adjusted — they are reasonable defaults for a low-volume signal. The key choice is not required; the partition-ordered comment documents it as a convenience for idempotent consumers.)

- **2.2 — Verify.**

  Run: `go build ./...` → success.
  Run: `go vet ./...` → success.
  Run: `go test ./...` → existing tests pass (the new file is not yet referenced by tests).

---

### Step 3: Wire the publisher into the Kafka client (and add its tests)

**Files:**
- Modify: `internal/kafka/client.go` (add `publisher Publisher` field; extend `New`; extend `handle`; add `toProto`)
- Modify: `internal/kafka/client_test.go` (add `fakePublisher`; add publisher-related tests; keep existing tests passing)

**Interfaces:**
- Consumes: `Publisher` from `internal/kafka/publisher.go`; the three-return value from `Store.UpdateStock` (step 1).
- Produces: `func New(cfg Config, store Store, publisher Publisher) *Client` — the **new** `New` signature. `Client.handle` calls `c.publisher.Publish(ctx, toProto(stored))` when `stored != nil && inserted && c.publisher != nil`.

- **3.1 — Modify `internal/kafka/client.go`.**

  (a) Add a `publisher` field to the `Client` struct and accept it in `New`:
  ```go
  // Client consumes stocks from a Kafka topic and writes them to the store,
  // and (when configured) publishes new-stock events on an output topic.
  type Client struct {
  	cfg       Config
  	store     Store
  	publisher Publisher
  	decoder   func(raw []byte) (*stockv1.Stock, error)
  }

  // New validates the configuration and creates a new Client. publisher may
  // be nil (no output publishing) or a no-op value.
  func New(cfg Config, store Store, publisher Publisher) *Client {
  	if publisher == nil {
  		publisher = Noop{}
  	}
  	return &Client{
  		cfg:       cfg,
  		store:     store,
  		publisher: publisher,
  		decoder:   decodeStock,
  	}
  }
  ```

  (b) Update `handle` to capture the three-return value and publish on INSERT:
  ```go
  // handle decodes a single kafka message and upserts the stock into the
  // store. When the upsert was an INSERT (a new (symbol, exchange) pair), it
  // also publishes a new-stock event on the output topic (best-effort).
  func (c *Client) handle(ctx context.Context, msg kafka.Message) error {
  	m, err := c.decoder(msg.Value)
  	if err != nil {
  		return err
  	}

  	if m.Symbol == "" || m.Exchange == "" {
  		return errors.New("stock message missing symbol or exchange")
  	}

  	for _, e := range m.GetScores() {
  		v := e.GetValue()
  		cat := e.GetCategory()
  		if v < -1.0 || v > 1.0 {
  			return fmt.Errorf("score %s value %v out of range [-1, 1]", cat, v)
  		}
  	}

  	stored, inserted, err := c.store.UpdateStock(ctx, m.Symbol, m.Exchange, toMap(m.GetScores()))
  	if err != nil {
  		return err
  	}
  	if !inserted {
  		return nil
  	}
  	if err := c.publisher.Publish(ctx, toProto(stored)); err != nil {
  		// Log-and-continue: the DB write succeeded; the event is best-effort.
  		log.Printf("kafka: failed to publish new stock %s/%s: %v", stored.Symbol, stored.Exchange, err)
  	}
  	return nil
  }
  ```

  (c) Add a `toProto` helper at the bottom of the file:
  ```go
  // toProto converts a store.Stock into the stockv1.Stock message used for
  // gRPC responses and for the output-topic publish.
  func toProto(s *store.Stock) *stockv1.Stock {
  	out := &stockv1.Stock{Symbol: s.Symbol, Exchange: s.Exchange}
  	for _, e := range s.Scores {
  		out.Scores = append(out.Scores, &stockv1.ScoreEntry{
  			Category:  e.Category,
  			Value:     e.Value,
  			UpdatedAt: timestamppb.New(e.UpdatedAt),
  		})
  	}
  	return out
  }
  ```
  (Requires `"google.golang.org/protobuf/types/known/timestamppb"` in the import block.)

- **3.2 — Extend `internal/kafka/client_test.go`.**

  (a) Add a `fakePublisher` near `fakeStore`:
  ```go
  // fakePublisher is a test double for the Publisher interface.
  type fakePublisher struct {
  	called    int
  	lastStock *store.Stock
  	err       error
  }

  func (f *fakePublisher) Publish(_ context.Context, stock *stockv1.Stock) error {
  	f.called++
  	f.lastStock = stock
  	return f.err
  }
  func (f *fakePublisher) Close() error { return nil }
  ```

  (b) **Fix the existing `New(...)` call sites** in `client_test.go` — every `New(Config{}, store)` becomes `New(Config{}, store, &fakePublisher{})`. The current call sites are at lines 176, 207, 219, 236, 254. (The store-fake signature change from step 1 already keeps them compiling; this change just satisfies the new three-argument `New`.)

  (c) Add new tests (append to the file):
  ```go
  // TestHandle_PublishesOnNewStock verifies that when the store reports
  // INSERT, the publisher is called exactly once with the stored stock.
  func TestHandle_PublishesOnNewStock(t *testing.T) {
  	store := &fakeStore{}
  	pub := &fakePublisher{}
  	c := New(Config{}, store, pub)
  	raw, _ := proto.Marshal(&stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ",
  		Scores: []*stockv1.ScoreEntry{{Category: "momentum", Value: 0.5}}})
  	if err := c.handle(context.Background(), kafkaMsg.Message{Value: raw}); err != nil {
  		t.Fatalf("handle: %v", err)
  	}
  	if store.lastSymbol != "AAPL" || store.lastExch != "NASDAQ" {
  		t.Errorf("store not called correctly: %q/%q", store.lastSymbol, store.lastExch)
  	}
  	if pub.called != 1 {
  		t.Fatalf("publisher called %d times, want 1", pub.called)
  	}
  	if pub.lastStock.GetSymbol() != "AAPL" || pub.lastStock.GetExchange() != "NASDAQ" {
  		t.Errorf("published stock = %v", pub.lastStock)
  	}
  }

  // TestHandle_PublishErrorDoesNotFailTheMessage verifies that a failed
  //Publish returns nil and logs (the DB write is authoritative).
  func TestHandle_PublishErrorDoesNotFailTheMessage(t *testing.T) {
  	store := &fakeStore{}
  	pub := &fakePublisher{err: errors.New("kafka unavailable")}
  	c := New(Config{}, store, pub)
  	raw, _ := proto.Marshal(&stockv1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})
  	err := c.handle(context.Background(), kafkaMsg.Message{Value: raw})
  	if err != nil {
  		t.Fatalf("handle returned %v, want nil (publish must be log-and-continue)", err)
  	}
  	if !store.called {
  		t.Fatal("store.UpdateStock not called")
  	}
  	if pub.called != 1 {
  		t.Fatalf("publisher called %d times, want 1", pub.called)
  	}
  }

  // TestPublisher_RealAndNoop covers NewPublisher's two branches.
  func TestPublisher_RealAndNoop(t *testing.T) {
  	// No-op branch.
  	p := NewPublisher(nil, "")
  	if _, ok := p.(noopPublisher); !ok {
  		t.Fatalf("NewPublisher(nil,\"\") did not return noopPublisher, got %T", p)
  	}
  	if err := p.Publish(context.Background(), &stockv1.Stock{}); err != nil {
  		t.Errorf("noop Publish = %v, want nil", err)
  	}
  	if err := p.Close(); err != nil {
  		t.Errorf("noop Close = %v, want nil", err)
  	}

  	// Real-writer branch: we do not actually send to a broker; we only
  	// verify the returned interface is the writer-backed impl and that
  	// closing is safe.
  	p2 := NewPublisher([]string{"localhost:1"}, "topic")
  	if _, ok := p2.(*writerPublisher); !ok {
  		t.Fatalf("NewPublisher(brokers,topic) did not return *writerPublisher, got %T", p2)
  	}
  	if err := p2.Close(); err != nil {
  		t.Errorf("writer Close = %v, want nil (closed without writes)", err)
  	}
  }
  ```
  (Requires `"errors"` in the import block.)

- **3.3 — Verify.**

  Run: `go build ./...` → success.
  Run: `go vet ./...` → success.
  Run: `go test ./internal/kafka/...` → all pass (existing + new).

---

### Step 4: Wire the publisher into the gRPC server (and add its tests)

**Files:**
- Modify: `internal/grpc/server.go` (extend `Store`, extend `Server`, `NewServer`, `AddStocks`, `UpdateStock`; add `toProtoStock` re-uses existing)
- Modify: `internal/grpc/server_test.go` (add `fakePublisher`; fix existing `newTestClient` if needed; add publisher tests)

**Interfaces:**
- Consumes: `kafka.Publisher` from `internal/kafka/publisher.go`.
- Produces: `func NewServer(store Store, publisher kafka.Publisher) *Server` — the **new** signature. `Server.publisher` is always non-nil (a default no-op is installed when the caller passes nil).

- **4.1 — Modify `internal/grpc/server.go`.**

  (a) Add `kafka.Publisher` to the imports (the package `git.wheeli.ca/brian/stocker-store/internal/kafka`).

  (b) Update the `Store` interface signature and the `Server` struct / `NewServer`:
  ```go
  // Store exposes the data store methods needed by the gRPC handlers.
  type Store interface {
  	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error)
  	RemoveStock(ctx context.Context, symbol, exchange string) (bool, error)
  	GetStock(ctx context.Context, symbol string, exchange *string) (*store.Stock, error)
  	GetStocks(ctx context.Context, limit int32, exchange *string, minScores, maxScores map[string]float64) ([]store.Stock, error)
  }

  // Server holds the dependencies for the gRPC service.
  type Server struct {
  	store     Store
  	publisher kafka.Publisher
  }

  // NewServer creates a new Server with the given store and event publisher.
  // A nil publisher is accepted and replaced by a no-op.
  func NewServer(store Store, publisher kafka.Publisher) *Server {
  	if publisher == nil {
  		publisher = kafka.Noop{}
  	}
  	return &Server{store: store, publisher: publisher}
  }
  ```

  (c) Update `AddStocks` to publish on INSERT (log-and-continue):
  ```go
  func (s *Server) AddStocks(stream grpc.ClientStreamingServer[st.UpdateStockRequest, st.Stock]) error {
  	ctx := stream.Context()
  	var last *store.Stock

  	for {
  		req, err := stream.Recv()
  		if err == io.EOF {
  			break
  		}
  		if err != nil {
  			return status.Errorf(codes.InvalidArgument, "add stocks: %v", err)
  		}
  		stock, inserted, err := s.store.UpdateStock(ctx, req.GetSymbol(), req.GetExchange(), req.GetScores())
  		if err != nil {
  			log.Printf("update stock %s/%s: %v", req.GetSymbol(), req.GetExchange(), err)
  			return status.Errorf(codes.Internal, "update stock: %v", err)
  		}
  		if inserted {
  			if perr := s.publisher.Publish(ctx, toProtoStock(stock)); perr != nil {
  				log.Printf("add stocks: failed to publish new stock %s/%s: %v", stock.Symbol, stock.Exchange, perr)
  			}
  		}
  		last = stock
  	}

  	if last == nil {
  		log.Printf("add stocks: empty stream")
  		return status.Error(codes.InvalidArgument, "empty stream")
  	}
  	if err := stream.SendAndClose(toProtoStock(last)); err != nil {
  		return err
  	}
  	return nil
  }
  ```

  (d) Update `UpdateStock` to publish on INSERT (log-and-continue):
  ```go
  func (s *Server) UpdateStock(ctx context.Context, req *st.UpdateStockRequest) (*st.Stock, error) {
  	stock, inserted, err := s.store.UpdateStock(ctx, req.GetSymbol(), req.GetExchange(), req.GetScores())
  	if err != nil {
  		return nil, status.Errorf(codes.Internal, "update stock: %v", err)
  	}
  	if inserted {
  		if perr := s.publisher.Publish(ctx, toProtoStock(stock)); perr != nil {
  			log.Printf("update stock: failed to publish new stock %s/%s: %v", stock.Symbol, stock.Exchange, perr)
  		}
  	}
  	return toProtoStock(stock), nil
  }
  ```

- **4.2 — Extend `internal/grpc/server_test.go`.**

  (a) Add a `fakePublisher` (mirror of the one in `internal/kafka/client_test.go`, but here the published stock is the `*st.Stock` produced by `toProtoStock`):
  ```go
  type fakePublisher struct {
  	called    int
  	lastStock *st.Stock
  	err       error
  }

  func (f *fakePublisher) Publish(_ context.Context, stock *st.Stock) error {
  	f.called++
  	f.lastStock = stock
  	return f.err
  }
  func (f *fakePublisher) Close() error { return nil }
  ```
  (Note: the `kafka.Publisher` interface requires `Publish(ctx, *stockv1.Stock)`, where `stockv1` is the alias `git.wheeli.ca/brian/stocker-store/proto/v1` — the very same package this file imports as `st`. Both aliases name the identical compiler type, so `Publish(ctx, *st.Stock)` satisfies the interface and `fakePublisher` is assignable to `kafka.Publisher`.)

  (b) Update `newTestClient` (lines 110–126) to accept a publisher. Replace the anonymous struct/interface parameter with the concrete `kafka.Publisher` type (import `git.wheeli.ca/brian/stocker-store/internal/kafka`):
  ```go
  func newTestClient(t *testing.T, backend Store, publisher kafka.Publisher) st.StockStoreClient {
  	t.Helper()
  	lis := bufconn.Listen(1 << 20)
  	server := NewServer(backend, publisher).GRPCServer()
  	// ... rest of the helper is unchanged ...
  }
  ```
  Then update the existing call sites in this file to pass `kafka.Noop{}` (the shared no-op from step 2) — they are at the top of `TestUpdateStock`, `TestRemoveStock`, `TestGetStock`, `TestGetStock_NotFound`, `TestGetStocks_Filters`, `TestAddStocks`, and `TestAddStocks_EmptyStream`.

  (c) Add publisher tests:
  ```go
  // TestUpdateStock_PublishesOnNewStock verifies that a successful insert
  // triggers exactly one publish of the stored stock.
  func TestUpdateStock_PublishesOnNewStock(t *testing.T) {
  	fs := newFakeStore()
  	pub := &fakePublisher{}
  	client := newTestClient(t, fs, pub)
  	_, err := client.UpdateStock(context.Background(),
  		&st.UpdateStockRequest{Symbol: "AAPL", Exchange: "NASDAQ",
  			Scores: map[string]float64{"momentum": 0.5}})
  	if err != nil {
  		t.Fatalf("UpdateStock: %v", err)
  	}
  	if pub.called != 1 {
  		t.Fatalf("publisher called %d times, want 1", pub.called)
  	}
  	if pub.lastStock.GetSymbol() != "AAPL" || pub.lastStock.GetExchange() != "NASDAQ" {
  		t.Errorf("published stock = %v", pub.lastStock)
  	}
  	if got := len(pub.lastStock.GetScores()); got != 1 {
  		t.Errorf("published scores len = %d, want 1", got)
  	}
  }

  // TestUpdateStock_PublishErrorDoesNotFailResponse verifies that a Publish
  // failure is logged (not returned) — the response is still successful.
  func TestUpdateStock_PublishErrorDoesNotFailResponse(t *testing.T) {
  	fs := newFakeStore()
  	pub := &fakePublisher{err: errors.New("publish failed")}
  	client := newTestClient(t, fs, pub)
  	s, err := client.UpdateStock(context.Background(),
  		&st.UpdateStockRequest{Symbol: "AAPL", Exchange: "NASDAQ",
  			Scores: map[string]float64{"v": 0.1}})
  	if err != nil {
  		t.Fatalf("UpdateStock returned %v, want nil (publish is log-and-continue)", err)
  	}
  	if s.GetSymbol() != "AAPL" {
  		t.Errorf("stock = %v", s)
  	}
  	if pub.called != 1 {
  		t.Fatalf("publisher called %d times, want 1", pub.called)
  	}
  }

  // TestAddStocks_PublishesPerNewStock verifies each INSERT in the stream
  // triggers one publish.
  func TestAddStocks_PublishesPerNewStock(t *testing.T) {
  	fs := newFakeStore()
  	pub := &fakePublisher{}
  	client := newTestClient(t, fs, pub)
  	stream, err := client.AddStocks(context.Background())
  	if err != nil {
  		t.Fatalf("AddStocks: %v", err)
  	}
  	for i, sym := range []string{"S1", "S2", "S3"} {
  		if err := stream.Send(&st.UpdateStockRequest{Symbol: sym, Exchange: "EXX",
  			Scores: map[string]float64{"alpha": float64(i - 1)}}); err != nil {
  			t.Fatalf("Send %s: %v", sym, err)
  		}
  	}
  	if _, err := stream.CloseAndRecv(); err != nil {
  		t.Fatalf("CloseAndRecv: %v", err)
  	}
  	if pub.called != 3 {
  		t.Fatalf("publisher called %d times, want 3 (one per new stock)", pub.called)
  	}
  }
  ```

  (d) Optionally — but recommended — extend `fakeStore` with a `lastInserted bool` field and have `UpdateStock` return it from a per-key record, so a test can assert "update of existing stock → no publish". If you do this, the three-argument `UpdateStock` in `fakeStore` becomes:
  ```go
  type fakeStore struct {
  	stocks       map[string]map[string]float64
  	insertedOnce map[string]bool // "ex/sym" -> true once, second insert returns false
  	getErr       error
  }
  func (f *fakeStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
  	k := key(symbol, exchange)
  	if f.stocks[k] != nil && f.insertedOnce[k] {
  		return &store.Stock{...}, false, nil // update of existing
  	}
  	f.insertedOnce[k] = true
  	f.stocks[k] = scores
  	return &store.Stock{...}, true, nil // new
  }
  ```
  This lets you write `TestUpdateStock_DoesNotPublishOnUpdate` (the fake returns `inserted=false` for a second write to the same pair; assert `pub.called == 0`). This is a nice-to-have; the three required tests above already cover the core contract.

- **4.3 — Verify.**

  Run: `go build ./...` → success.
  Run: `go vet ./...` → success.
  Run: `go test ./internal/grpc/...` → all pass (existing + new).

---

### Step 5: Wire the publisher into `cmd/main.go` and rename `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC`

**Files:**
- Modify: `cmd/main.go` (the `stockStore` interface at line 22, `runKafkaSubscriber` at lines 123–144; `main` function to construct and pass the publisher)

**Interfaces:**
- Consumes: `kafka.NewPublisher` (step 2), `kafka.New` 3-arg (step 3), `grpc.NewServer` 2-arg (step 4).
- Produces: a running service where:
  - `KAFKA_INPUT_TOPIC` (renamed) drives the consumer (no-op unless `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are both set)
  - `KAFKA_OUTPUT_TOPIC` (new) drives the publisher (no-op unless `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are both set)
  - input and output are independent

- **5.1 — Update the local `stockStore` interface to match the new `UpdateStock` signature** (lines 20–23):
  ```go
  // Stock is the subset of the stock store needed to ingest kafka messages.
  type stockStore interface {
  	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error)
  }
  ```

- **5.2 — Construct the publisher once in `main` and pass it through.**

  Replace the section of `main` between the `st` initialization and the `kafka subscriber` start (roughly lines 42–69) with:

  ```go
  st, err := store.NewStore(ctx, dbUrl)
  if err != nil {
  	log.Fatalf("initialize store: %v", err)
  }
  defer st.Close()

  // Construct the publisher once; it is a no-op unless both KAFKA_BROKERS
  // and KAFKA_OUTPUT_TOPIC are set. It is shared by the gRPC server and the
  // kafka input subscriber so that *any* path that creates a new stock emits
  // the event.
  brokers := envList("KAFKA_BROKERS")
  publisher := kafka.NewPublisher(brokers, os.Getenv("KAFKA_OUTPUT_TOPIC"))

  server := grpc.NewServer(st, publisher)

  lis, err := net.Listen("tcp", ":3500")
  if err != nil {
  	log.Fatalf("listen on port 3500: %v", err)
  }
  gRPCServer := server.GRPCServer()
  go func() {
  	if err := gRPCServer.Serve(lis); err != nil {
  		log.Printf("gRPC serve error: %v", err)
  	}
  }()

  go runRetention(ctx, st)

  log.Println("stocker-store listening on :3500")

  if err := runKafkaSubscriber(ctx, st, publisher); err != nil {
  	log.Printf("kafka subscriber: %v", err)
  }

  <-ctx.Done()
  log.Println("shutting down...")

  gRPCServer.GracefulStop()
  // Flush in-flight publisher writes before exiting.
  if err := publisher.Close(); err != nil {
  	log.Printf("close publisher: %v", err)
  }
  cancel()
  ```

- **5.3 — Rename `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC` and pass the publisher into `runKafkaSubscriber`:**
  ```go
  func runKafkaSubscriber(ctx context.Context, st stockStore, publisher kafka.Publisher) error {
  	brokers := envList("KAFKA_BROKERS")
  	topic := os.Getenv("KAFKA_INPUT_TOPIC")
  	if len(brokers) == 0 || topic == "" {
  		log.Printf("KAFKA_BROKERS and KAFKA_INPUT_TOPIC required for kafka ingestion")
  		return nil
  	}
  	groupID := os.Getenv("KAFKA_GROUP_ID")
  	if groupID == "" {
  		groupID = "stocker-store"
  	}
  	client := kafka.New(kafka.Config{
  		Brokers: brokers,
  		Topic:   topic,
  		GroupID: groupID,
  	}, st, publisher)
  	log.Printf("kafka subscriber: consuming %q via %s", topic, strings.Join(brokers, ","))
  	return client.Run(ctx)
  }
  ```

  (Note: the `New(...)` now takes three arguments; `st` is the concrete `*store.Store`, which satisfies the `kafka.Store` interface.)

- **5.4 — Verify (no tests required for `cmd/main.go` directly; the integration contract is covered by the store, kafka, and grpc test packages).**

  Run: `go build ./...` → success.
  Run: `go vet ./...` → success.
  Run: `go test ./...` → all pass.

---

### Step 6: Update `README.md` for the rename + new variable

**Files:**
- Modify: `README.md` (line ~189, 202, 205, 224, 371 plus the features bullets and a new short "New-Stock Output" subsection under `Kafka Ingestion`)

**Interfaces:**
- Consumes: nothing.
- Produces: an updated operator-facing table and prose.

- **6.1 — Rename every `KAFKA_TOPIC` to `KAFKA_INPUT_TOPIC` in `README.md`.** Use the `edit` tool with `replaceAll` on `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC` (the README has ~6 occurrences; do **not** accidentally rename `KAFKA_BROKERS` or `KAFKA_GROUP_ID`).
  Verify afterwards with `grep -n KAFKA_TOPIC` — it should return **zero** matches in `README.md`.

- **6.2 — Add the new env var to the table (lines ~197–203)**. After the `KAFKA_GROUP_ID` row, add:
  ```markdown
  | `KAFKA_OUTPUT_TOPIC` | No (Kafka only) | — | Topic name (e.g. `stockers-new`) | Topic to **publish** new-stock `Stock` messages on. |
  ```

- **6.3 — Update the sample env file (lines ~222–225)** to include both:
  ```bash
  # Optional — Kafka ingestion (BROKERS and INPUT TOPIC must both be set to enable input).
  # KAFKA_BROKERS=localhost:9092
  # KAFKA_INPUT_TOPIC=stockers
  # KAFKA_GROUP_ID=stocker-store

  # Optional — New-stock output (BROKERS and OUTPUT TOPIC must both be set to enable output).
  # KAFKA_OUTPUT_TOPIC=stockers-new
  ```

- **6.4 — Update the "Kafka is optional" bullet** (line ~371) to mention the independence of input and output:
  ```markdown
  - **Kafka is optional:** ingestion is a no-op unless **both** `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set. New-stock publish is a no-op unless **both** `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set. The two are independent.
  ```

- **6.5 — Add a short "New-Stock Output" subsection** under the `Kafka Ingestion` section (after line ~189). Suggested text:
  ```markdown
  ### New-Stock Output

  In addition to ingesting on `KAFKA_INPUT_TOPIC`, the service can **publish** a `Stock` message (the same PROTOBUF-encoded message gRPC returns) to a separate `KAFKA_OUTPUT_TOPIC` whenever a **new** `(symbol, exchange)` pair is written for the first time (an `INSERT` in the `stocks` table, not an update to an existing one). The two directions are independent: enable input only, output only, both, or neither.

  Publish is best-effort — a failure is logged and the gRPC / Kafka handler still reports success. The published message is the authoritative, server-stamped stock (all scores including their `updated_at` timestamps), not the raw input as received.
  ```

  (This mirrors §12 of `docs/design.md` so the README stays source-of-truth for operators.)

- **6.6 — Verify.**

  Run: `grep -n KAFKA_TOPIC README.md | grep -v KAFKA_INPUT_TOPIC | grep -v KAFKA_OUTPUT_TOPIC` → empty.
  Manually scan the env section for `KAFKA_INPUT_TOPIC` and `KAFKA_OUTPUT_TOPIC`.

---

### Step 7: Final verification, commit, push

**Files:** none (verification + commit).

**Interfaces:**
- Consumes: all of the above.
- Produces: a single commit on `feature/kafka-new-stock-topic` with a clear message.

- **7.1 — Run the full test suite (database-backed tests will skip when `DATABASE_URL` is unset — that is the intended behavior and not a failure).**

  ```bash
  go build ./...
  go vet ./...
  go test ./...
  ```

  Expected: `build` and `vet` pass; `test` passes with the store tests reporting `skip` (not "fail"). To exercise the database-backed tests, set `DATABASE_URL` (or `DATABASE_URL_FILE`) in the environment.

- **7.2 — Smoke test (optional but recommended).**

  Start a disposable Postgres and a broker:
  ```bash
  export DATABASE_URL="postgres://user:pass@localhost:5432/testdb?sslmode=disable"
  export KAFKA_BROKERS="localhost:9092"
  export KAFKA_INPUT_TOPIC="stockers-in"
  export KAFKA_OUTPUT_TOPIC="stockers-out"
  make run
  ```
  In another terminal:
  ```bash
  # create the output topic if it does not already exist
  kafka-topics --create --topic stockers-out --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

  # send a message on the input topic for a *new* symbol
  kafka-console-producer --topic stockers-in --bootstrap-server localhost:9092
  # (paste a PROTOBUF-encoded Stock; or drive via grpcurl UpdateStock and observe the output topic)
  ```
  Then read a record from `stockers-out` and verify the symbol/exchange/scores match. A second upsert of the *same* pair should NOT produce a new record on `stockers-out`.

- **7.3 — Commit on `feature/kafka-new-stock-topic`** (already the current branch per the task setup):
  ```bash
  git add internal/store/store.go internal/store/store_test.go \
          internal/kafka/publisher.go internal/kafka/client.go internal/kafka/client_test.go \
          internal/grpc/server.go internal/grpc/server_test.go \
          cmd/main.go README.md
  git commit -m "feat(kafka): publish new-stock events on KAFKA_OUTPUT_TOPIC"

  # amend message to include the rename
  git commit --amend -m "feat(kafka): publish new-stock events on a dedicated output topic

  - Store.UpdateStock returns a third value (bool) indicating whether the
    upsert was an INSERT (new stock) or an UPDATE (existing stock),
    obtained via Postgres ON CONFLICT ... RETURNING (xmax = 0).
  - New kafka.Publisher interface with a no-op and a writer-backed
    implementation; constructed once in cmd/main.go and injected into
    both the gRPC server and the kafka input subscriber.
  - A failed publish is logged and does not fail the primary write or
    the gRPC / Kafka handler response (log-and-continue).
  - Rename KAFKA_TOPIC to KAFKA_INPUT_TOPIC (no backwards-compat);
    add KAFKA_OUTPUT_TOPIC for the new-stock publish (no-op unless
    both KAFKA_BROKERS and KAFKA_OUTPUT_TOPIC are set).
  - Update README.md: env table, sample env file, new 'New-Stock
    Output' subsection, prose rename."
  ```

- **7.4 — Push:**
  ```bash
  git push -u origin feature/kafka-new-stock-topic
  ```

---

## Verification checklist (run after every step)

After each numbered step, run:
- `go build ./...` — must succeed.
- `go vet ./...` — must succeed.
- `go test ./...` — must succeed (store tests will skip without `DATABASE_URL`; that is intended).

At step 7, also run:
- `grep -n KAFKA_TOPIC README.md | grep -v KAFKA_INPUT_TOPIC | grep -v KAFKA_OUTPUT_TOPIC` → empty.
- `grep -n KAFKA_TOPIC cmd/main.go internal grpc kafka store -r | grep -v KAFKA_INPUT_TOPIC | grep -v KAFKA_OUTPUT_TOPIC` → empty (no bare `KAFKA_TOPIC` references remain in code).
- (Optional) start the service with a real Postgres + Kafka and exercise the publish path end-to-end per step 7.2.
