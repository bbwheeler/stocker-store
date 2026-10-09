# Top Stocks — Implementation Plan

**Goal:** Add a read RPC `TopStocks(TopStocksRequest) returns (StockList)` to the `StockStore` service that returns the top `limit` stocks by score `value` in a `category`, optionally scoped to one `exchange`, with deterministic ordering (`value DESC, symbol ASC, exchange ASC`). A non-positive `limit` or an empty/unknown `category` returns an empty list, not an error.

**Architecture:** A new store method selects `(symbol, exchange)` rows from `scores` ordered by `value DESC, symbol ASC, exchange ASC` with `LIMIT`, then hydrates each stock's scores with the existing per-stock lookup. The gRPC handler is a thin unary wrapper that validates nothing itself (the store short-circuits) and maps `[]store.Stock` → `*stockstorev1.StockList` with the existing `toProtoStock` helper.

**Tech Stack:** Go 1.25, `pgx/v5`, `google.golang.org/protobuf`, gRPC/protobuf.

**Spec:** `docs/design.md` §13 (Top Stocks). Every constraint in §13 applies. In particular: no new dependencies, no schema change, no new index, and empty results (not errors) on the short-circuit paths.

## Global constraints

- Keep the code surface minimal; do not restructure packages we do not touch.
- No new dependencies in `go.mod`; `pgx` and the generated proto bindings are already present.
- No schema migration and no new index: the existing `idx_scores_category_value (category, value DESC)` covers the primary ordering.
- Ordering must be deterministic across identical requests (`value DESC, symbol ASC, exchange ASC`).
- `limit <= 0` and empty `category` short-circuit to an empty list without a query; an unknown category yields an empty list via the query.
- The tree must compile (`go build ./...`) after every numbered step.

### Prerequisites

Steps 1–3 are committed on branch `feat/top-scored-stocks-grpc`:
`2de29e6` (proto), `e6bf523` (store), `035a137` (gRPC handler). No earlier step is a prerequisite beyond the existing `StockStore` service and store package.

---

## File structure (before defining steps)

| File | Change | Responsibility |
|---|---|---|
| `proto/v1/stock_store.proto` | Modify | Add the `TopStocks` RPC and `TopStocksRequest` message. |
| `proto/v1/stock_store.pb.go` | Regenerate | Generated message/RPC descriptors (do not hand-edit). |
| `proto/v1/stock_store_grpc.pb.go` | Regenerate | Generated client/server stubs (do not hand-edit). |
| `internal/store/store.go` | Add `TopStocks` | Query + deterministic ordering + short-circuits + score hydration. |
| `internal/store/store_test.go` | Add `TestTopStocks` | Ordering, exchange scoping, tie-breaking, hydration, empty cases. |
| `internal/grpc/server.go` | Modify `Store` interface; add handler | Unary `TopStocks` handler mapping domain → proto. |
| `internal/grpc/server_test.go` | Extend `fakeStore`; add `TestTopStocks` | End-to-end handler behavior through the real gRPC client. |

No new files under `proto/` or `internal/`. No `go.mod` changes.

---

## Design decisions (summary, with rationale)

1. **Ordering and tiebreaker in SQL.** `ORDER BY value DESC, symbol ASC, exchange ASC` makes results deterministic (stable across identical requests) without application-side sorting. Rejected alternative: order in Go — it would force reading more rows than `limit` and cannot push the limit into the index.
2. **Short-circuits live in the store, not the handler.** `limit <= 0` and `category == ""` return `[]Stock{}, nil` before any query; the handler stays a pure mapper. This keeps the rule in one place and avoids a pointless round-trip. An unknown but non-empty category simply matches no rows and returns an empty list.
3. **`exchange` as `*string`.** `nil` means "all exchanges"; a non-nil pointer adds `AND exchange = $2`. This mirrors `GetStock`/`GetStocks` and keeps the optional nature explicit.
4. **Reuse the existing score-hydration helper.** After selecting the pairs, each stock's scores are fetched with the existing per-stock lookup so the response carries full `ScoreEntry` data (including `updated_at`), consistent with the other read paths.
5. **No new index.** `idx_scores_category_value (category, value DESC)` already serves the category equality predicate and the primary `value DESC` ordering; the `symbol`/`exchange` tiebreakers are a short in-memory/near-tail sort over the already-filtered rows.

### Request / response shape

```protobuf
message TopStocksRequest {
  int32 limit = 1;
  string category = 2;
  optional string exchange = 3;
}
```

Response is the existing `StockList { repeated Stock stocks = 1; }`.

### Exact SQL

```sql
SELECT symbol, exchange FROM scores
WHERE category = $1 [AND exchange = $2]
ORDER BY value DESC, symbol ASC, exchange ASC
LIMIT $n
```

The `[AND exchange = $2]` fragment is appended only when `exchange != nil`; the `LIMIT` placeholder index follows it.

### Code-change summary (file-by-file)

- **`proto/v1/stock_store.proto`** — add `rpc TopStocks(TopStocksRequest) returns (StockList);` to `service StockStore` and the `TopStocksRequest` message. Regenerate the two `.pb.go` files with the repo toolchain.
- **`internal/store/store.go`** — add `func (s *Store) TopStocks(ctx context.Context, limit int32, category string, exchange *string) ([]Stock, error)`.
- **`internal/grpc/server.go`** — add `TopStocks(...)` to the `Store` interface; implement `func (s *Server) TopStocks(ctx context.Context, req *st.TopStocksRequest) (*st.StockList, error)`.
- **`internal/store/store_test.go`** — add `TestTopStocks` (DB-backed; skips without `DATABASE_URL`).
- **`internal/grpc/server_test.go`** — extend `fakeStore` with `TopStocks` and add `TestTopStocks`.

---

## Implementation steps

*(Each step ends with the tree compiling; run `go build ./...`, `go vet ./...`, and `go test ./...`.)*

### Step 1: Add the `TopStocks` RPC and message to the proto

**Files:**
- Modify: `proto/v1/stock_store.proto`
- Regenerate: `proto/v1/stock_store.pb.go`, `proto/v1/stock_store_grpc.pb.go`

**Interfaces:**
- Produces: `stockstorev1.TopStocksRequest`, `StockStoreServer.TopStocks`, `StockStoreClient.TopStocks`, `StockStore_TopStocks_FullMethodName`.

- **1.1** — Add the RPC to `service StockStore` and the message after `GetStocksRequest`. Regenerate with the repo toolchain (`protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`; `paths=source_relative`, include root `proto`).
- **1.2 — Verify.** `go build ./...`, `go vet ./...`, `go test ./...` all pass.

### Step 2: Add `Store.TopStocks`

**Files:**
- Modify: `internal/store/store.go`
- Modify: `internal/store/store_test.go`

**Interfaces:**
- Consumes: existing `pgx` pool + `getStockScores`.
- Produces: `func (s *Store) TopStocks(ctx context.Context, limit int32, category string, exchange *string) ([]Stock, error)`.

- **2.1** — Implement the short-circuits (`limit <= 0`, `category == ""` → `[]Stock{}, nil`), build the query with the optional exchange predicate, scan `(symbol, exchange)` in order, then hydrate each stock's scores. Return a non-nil empty slice on the short-circuit paths.
- **2.2 — Verify.** `go build ./...`, `go vet ./...`, `go test ./...`.

### Step 3: Add the gRPC handler

**Files:**
- Modify: `internal/grpc/server.go`
- Modify: `internal/grpc/server_test.go`

**Interfaces:**
- Consumes: `Store.TopStocks` (step 2).
- Produces: `Store.TopStocks(...)` in the interface and `func (s *Server) TopStocks(ctx, req *st.TopStocksRequest) (*st.StockList, error)`.

- **3.1** — Add `TopStocks(...)` to the `Store` interface. Implement the handler: call the store, map internal errors to `codes.Internal`, and build `*st.StockList` via `toProtoStock`. A nil/empty result yields a non-nil `StockList` with no stocks.
- **3.2 — Verify.** `go build ./...`, `go vet ./...`, `go test ./...`.

---

## Test plan

**Store (`internal/store/store_test.go`, DB-backed; skips without `DATABASE_URL`):** seed four stocks across two exchanges with `sentiment` values `0.9 / 0.5 / 0.95 / 0.5` plus an unrelated `momentum` score on one. One `TestTopStocks` with subtests:

- **limit returns top ordered** — `limit=2` returns the highest-value pair, then the next.
- **exchange scoped** — non-nil `exchange` returns only that exchange's pairs in order.
- **all with deterministic tie break** — equal values break by `symbol` then `exchange`; the tied pair order is asserted. Verifies score hydration (the multi-score stock carries both entries, with the correct `sentiment` value).
- **non-positive limit** — `limit=0` returns a non-nil empty slice, no error.
- **empty category** — `""` returns an empty list, no error.
- **unknown category** — a made-up category returns an empty list, no error.

Helpers reused: `testStore`, `uniqueTag`, `seedStock`, `seedScore`, `clearStock`.

**gRPC (`internal/grpc/server_test.go`):** `fakeStore` gains a `TopStocks` method returning seeded stocks so the handler can be exercised through the real gRPC client (`newTestClient`). `TestTopStocks` writes three stocks via `UpdateStock` then asserts:

- ordered by `value DESC` (`limit=2` returns the two highest),
- a supplied `exchange` filters the result set,
- `limit=0` returns a non-nil empty list,
- an unknown category returns an empty list.

**Failure path:** store errors surface to the client as `codes.Internal` (covered by the handler's error mapping; no dedicated test).

---

## Verification steps

Run after every step, and once more before committing:

```bash
go build ./...
go vet ./...
go test ./...
```

Expected: `build` and `vet` pass; `test` passes with the DB-backed store test reporting `skip` when `DATABASE_URL` is unset (that is intended). To exercise the store subtests, set `DATABASE_URL` (or `DATABASE_URL_FILE`).

Functional spot-check (optional, with a reachable database):

```bash
grpcurl -plaintext -d '{ "limit": 10, "category": "sentiment", "exchange": "NYSE" }' \
  localhost:3500 stockstore.v1.StockStore/TopStocks
```

Verify the returned stocks are the highest `sentiment` scores on `NYSE`, in `value DESC` order.

## Non-goals

- No new index or schema migration.
- No pagination or offset parameter (the `limit` is the sole cap).
- No aggregation, averaging, or multi-category ranking — a single category at a time.
- No caching layer.
