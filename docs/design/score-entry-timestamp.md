# Design: `ScoreEntry` `updated_at` Timestamp

**Feature name:** `feature/score-entry-timestamp`
**Status:** Ready for implementation.

The user's request:

> Each `ScoreEntry` must have a last-updated timestamp, in addition to
> `category` and `value`. Update this in the proto (both gRPC and Kafka),
> as well as the database and all documentation. The last-updated
> timestamp must be updated whenever the score is updated. The updated
> timestamp should default to the current time. Keep the same version,
> V1 has not been deployed to production yet.

---

## 1. Goals

1. Every read of a `ScoreEntry` (gRPC response and `store.ScoreEntry`)
   exposes the time the score was last written.
2. Every write of a stock/score (gRPC and Kafka) stamps the written
   scores with the server's notion of "now."
3. No **new** database columns; the existing `scores.timestamp` becomes
   a first-class, read-back, per-score field in the API surface.
4. V1 semantics preserved in all other respects — the wire-level
   **shape** of one message and one field changes to make this work
   (documented below).
5. Keep `v1`. V1 has not been deployed to production.

---

## 2. Non-goals

- No `v2` proto; no schema version bump.
- No per-score history / audit trail; only the most-recent write
   timestamp per `(symbol, exchange, category)` tuple is exposed.
- No changes to retrieval by symbol / exchange / min-max scores.
- No changes to stock-level `timestamp` semantics or to the
   `RemoveOldStocks` retention logic.

---

## 3. Confusion to avoid (three distinct messages with `scores`)

| Message                        | Where          | Shape **before**              | Shape **after**                          |
|--------------------------------|----------------|-------------------------------|------------------------------------------|
| gRPC `ScoreEntry` (in `Stock`) | `stockstore.v1` | `category, value`             | `category, value, updated_at`            |
| gRPC `UpdateStockRequest`      | `stockstore.v1` | `symbol, exchange, scores: map<string,double>` | **UNCHANGED** (map) |
| Kafka `StockUpdate`            | `stockerstore.kafka.v1` | `symbol, exchange, scores: map<string,double>` | `symbol, exchange, scores: repeated ScoreEntry{category, value, updated_at}` |

**Only two messages change shape:** gRPC `ScoreEntry` and
Kafka `StockUpdate`. Everything else — gRPC `UpdateStockRequest`,
gRPC `GetStock*`, gRPC `RemoveStock*`, and the wire shape of the
Kafka consumer's view of the store — stays exactly the same.

---

## 4. Design decisions (summary)

### D1 — gRPC `ScoreEntry.gRPC` is the read/response shape; add `updated_at`

- **File:** `proto/v1/stock_store.proto`
- **Change:** `message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }` with matching `import "google/protobuf/timestamp.proto";`
- **Rationale:**
  - `ScoreEntry` appears **only** in the read/response `Stock` message.
  - `google.protobuf.Timestamp` is the standard gRPC/protobuf way to
    model an instant in time. The Go binding is
    `google.golang.org/protobuf/types/known/timestamppb` (already in
    `go.mod`). No new dependency.
  - Field number 3 is a safe continuation of the existing
    `category=1, value=2`.
  - Adding an optional-looking protobuf scalar-typed field to an
    existing message is a wire-compatible change: old clients reading a
    new response will see zero-value `updated_at`; new clients reading
    a new response are unaffected by adding a field. (In practice, V1
    is not deployed, so this consideration is academic here, but the
    shape is future-proof.)
- **Impact:** gRPC `Stock` message (and everything that returns
  `Stock` — `AddStocks`, `UpdateStock`, `GetStock`, `GetStocks`)
  returns `ScoreEntry` with a populated `updated_at`; the response
  bytes grow.

### D2 — gRPC `UpdateStockRequest.scores` is the write-request shape; **unchanged**

- **File:** `proto/v1/stock_store.proto`
- **Change:** none.
- **Rationale:** The write request only needs the category → value
  mapping; the server stamps the timestamp authoritatively (see D4).
  A `repeated ScoreEntry scores` here would mislead clients into
  sending `updated_at` values the server will not use. Keeping it a
  `map<string,double>` preserves the current wire shape and the
  "server is the source of truth for the write timestamp" guarantee.

### D3 — Kafka `StockUpdate.scores` changes from `map<string,double>` to `repeated ScoreEntry`

- **File:** `proto/v1/kafka/stock_message.proto`
- **Change:**
  - Add a local `message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }` that mirrors the gRPC one (field numbers and types).
  - Replace `map<string, double> scores = 3;` with `repeated ScoreEntry scores = 3;`.
- **Rationale:**
  - Maps cannot carry a per-entry `updated_at` — a `map<K,V>` is a
    single homogenous value type and there is no place to put a second
    value per entry. This is the core constraint that forces the
    change.
  - A locally-defined `ScoreEntry` in the Kafka proto is the idiomatic
    way to model a per-entry structured value. Protobuf does not
    support `import` of a message from another file's `package`; the
    gRPC `ScoreEntry` is in `stockstore.v1`, the Kafka message is in
    `stockerstore.kafka.v1`, and the Kafka file should remain
    portable (no cross-package dependency).
  - `repeated` + field number 3 (keeping the same wire number and
    position) is the minimal-blast-radius change. The wire layout of
    other fields (symbol=1, exchange=2) is untouched.
  - V1 has not been deployed → a clean cutover is acceptable. The
    "map → repeated" change is a **breaking** change for any
    pre-V1 producers publishing JSON/map-form; that is fine because
    per the user's statement, "V1 has not been deployed to
    production."
- **Impact:**
  - Any external producer of the Kafka topic must migrate from a map
    to repeated `ScoreEntry` (out of scope of this change; noted in
    the `proto/v1/kafka/README.md` update).
  - The in-repo consumer (`internal/kafka/client.go`) will adapt to
    the new shape.

### D4 — Server is the authoritative writer of `updated_at`

- **Guarantee:** *The last-updated timestamp must be updated whenever
  the score is updated; the updated timestamp should default to the
  current time.*
- **Approach:** The store always writes `scores.timestamp = now()`
  (already in the SQL upsert). The gRPC and Kafka server paths
  **ignore any client-provided `updated_at`**; if a Kafka message
  includes one, it is accepted but discarded. The server returns the
  **stored** timestamp read back from the DB.
- **Rationale:**
  - If we honored client-supplied timestamps, a misconfigured or
    malicious producer could stamp `updated_at` to the distant past
    (making a live score look stale) or the far future (making a
    dead score look live). The retention and "last updated"
    semantics would become meaningless.
  - "Default to current time" is exactly what the existing
    `scores.timestamp ... DEFAULT now()` already does. The write
    path in the DB is already correct; we only need to expose it.
- **Trade-off:** Clients that want to carry an "as-of" moment
  (e.g., scoring a snapshot at a specific historical instant) cannot
  do so through this API. That is out of the stated scope: the
  timestamp field is *last updated*, not *effective as of*.

**Consequence for the Kafka message:** `updated_at` in the Kafka
`ScoreEntry` is effectively **advisory / for shape parity**. Servers
use `now()`. We keep the field for:
- symmetry with the gRPC response shape (producers can emit
  what consumers expect),
- forward-compatibility if a future use-case requires it,
- a place for producers to *optionally* record the wall-clock time
  they observed the score (useful for log correlation even though
  the server will ignore it on write).

### D5 — No `v2`; version `v1` in every proto file is preserved

- gRPC file `proto/v1/stock_store.proto` → `package stockstore.v1;`
  unchanged.
- Kafka file `proto/v1/kafka/stock_message.proto` →
  `package stockerstore.kafka.v1;` unchanged.
- Both remain `v1`.

### D6 — Database `scores.timestamp` is already the source of truth

- The schema column exists; the upsert already updates it on every
  write; we add **one** line to `getStockScores`:
  `SELECT category, value, timestamp FROM scores ...`.
- `internal/store/models.go` gains `UpdatedAt time.Time` on
  `store.ScoreEntry`.
- No new migration, no new column, no `ALTER TABLE`.

---

## 5. Wire-shape change surface summary

| Wire item                              | Old shape                          | New shape                                |
|----------------------------------------|------------------------------------|------------------------------------------|
| gRPC `ScoreEntry`                       | `{category: string, value: double}` | `{category: string, value: double, updated_at: timestamppb}` |
| gRPC `UpdateStockRequest.scores`        | `map<string,double>` (unchanged)   | `map<string,double>` (unchanged)         |
| Kafka `StockUpdate`                     | `{symbol: string, exchange: string, scores: map<string,double>}` | `{symbol: string, exchange: string, scores: repeated ScoreEntry}` |
| Kafka `ScoreEntry`                      | (absent)                            | `{category: string, value: double, updated_at: timestamppb}` |

---

## 6. Impact on Go code (files touched)

| File | Change type |
|------|-------------|
| `proto/v1/stock_store.proto` | Add `import "google/protobuf/timestamp.proto";` and `updated_at` field on `ScoreEntry`. |
| `proto/v1/stock_message.proto` (Kafka) | Add `ScoreEntry` message; replace `map<string,double> scores` with `repeated ScoreEntry scores`. |
| `proto/v1/stockstorev1.pb.go` | Regenerated. |
| `proto/v1/kafka/stock_message.pb.go` | Regenerated. |
| `internal/store/models.go` | Add `UpdatedAt time.Time` to `ScoreEntry`. |
| `internal/store/store.go` | Update `getStockScores` SELECT to read `timestamp`; update `Scan` to take `UpdatedAt`. |
| `internal/grpc/server.go` | Update `toProtoScores` to include `UpdatedAt` (as `timestamppb.New(e.UpdatedAt)`). |
| `internal/kafka/client.go` | Add `toMap` helper or inline conversion `repeated ScoreEntry → map[string]float64`; iterate `m.ScoreEntry` for validation; pass the map to `c.store.UpdateStock`. |
| `cmd/main.go` | No change if the store signature is unchanged. |
| `internal/grpc/server_test.go` | Update `toDomainScores` to set `UpdatedAt: time.Now()`; add a test asserting `ScoreEntry.UpdatedAt != zero`. |
| `internal/kafka/client_test.go` | Update all tests to build `repeated ScoreEntry` payloads. |
| `internal/store/testhelper_test.go` | `seedScore` already writes `timestamp`; optionally assert non-zero `UpdatedAt` after a read. |
| `README.md` | Update Messages table, proto block, Kafka schema row; note the wire-shape change. |
| `design.md` | Update `ScoreEntry` message block to include `updated_at`. |
| `proto/v1/kafka/README.md` | Update Go example to build `repeated ScoreEntry`; update schema table; note that the server uses current time on write; note producer impact. |
| `docs/design/kafka-proto.md` | Mark the "map vs repeated ScoreEntry" rationale section as superseded. |
| `AGENTS.md` | Add a bullet under Requirements: "Each score has a last-updated timestamp." |

---

## 7. Risks & mitigations

1. **Wire-shape break on the Kafka `StockUpdate`.** Any external
   producer must migrate from `map<string,double>` to
   `repeated ScoreEntry`. Mitigation: V1 is not deployed (per user),
   and the `proto/v1/kafka/README.md` documents the new shape and
   the migration.
2. **gRPC `ScoreEntry` response bytes grow.** Clients using protobuf
   JSON/JSON-ish views may see an extra field. Mitigation: V1 is not
   deployed; new clients are written against the updated shape.
3. **Server ignoring `updated_at` on write** may surprise a
   Kafka producer who expected to control the timestamp. Mitigation:
   documented in `proto/v1/kafka/README.md` — "server stamps the
   score with its own clock; any `updated_at` provided by the
   producer is advisory."
4. **Field-number discipline.** `updated_at` is assigned field
   number 3 on both `ScoreEntry` messages, continuing the existing
   numbering. Do not use 1 or 2 (occupied). Do not use different
   numbers between the two `ScoreEntry` definitions (they are
   independent messages, but keeping the numbering identical
   documents the parity and reduces copy-paste bugs).
5. **No new `v2` proto.** The user's request is explicit: keep `v1`.
   Any future change of Kafka `StockUpdate` *shape* must go to a
   `v2` (or an in-place change with a documented breaking note and
   a clean cutover as we are doing now).

---

## 8. Out-of-scope (for this change)

- Producers of the Kafka topic: the schema change is consumed by
  in-repo code; external producers must migrate. Documented, not
  implemented.
- `GetStocks` min/max score filters: still operate on `value` (not
  `updated_at`). Correct — a score-filter is a semantic filter, not
  a recency filter.
- Retention: still based on `stocks.timestamp` (stock-level), not on
  per-score `updated_at`. The stock-level `timestamp` is advanced by
  any upsert of any of its scores (already in place), so retention
  behaves sensibly.

---

## 9. Implementation plan

See the numbered, dependency-ordered plan in
[**Step-by-step implementation plan**](#implementation-plan) below.

---

# Implementation Plan

> Header:
>
> **Goal:** add a per-score `updated_at` timestamp to gRPC and Kafka
> `ScoreEntry`, to the domain model, to the DB read path, to the
> gRPC server adapter, to the Kafka client adapter, and to every
> doc that describes the shape. Keep `v1`.
>
> **Architecture:** add `updated_at` to both `ScoreEntry` messages
> (gRPC and Kafka) via `google.protobuf.Timestamp`; make the Kafka
> `StockUpdate.scores` field a `repeated ScoreEntry` (not a map) so
> the per-entry timestamp has somewhere to live; surface the
> already-existing `scores.timestamp` column in the domain
> `store.ScoreEntry` and in the gRPC server adapter; leave the
> store signature `UpdateStock(... map[string]float64)` **unchanged**
> and have the Kafka adapter do the `[]*kafka ScoreEntry →
> map[string]float64` conversion; the server stamps the write
> timestamp with `now()` on its own clock (the existing
> `scores.timestamp DEFAULT now()` SQL already does this). Keep `v1`.
>
> **Tech Stack:** Go, gRPC, PostgreSQL (pgx v5), Kafka
> (segmentio/kafka-go), protobuf (protoc v29.3), `timestamppb`.
>
> **Spec:** this document (`docs/design/score-entry-timestamp.md`).

## Global constraints

- Keep proto **namespace `stockstore.v1`** (gRPC) and
  **namespace `stockerstore.kafka.v1`** (Kafka) unchanged.
- Keep **field numbers** on both `ScoreEntry` messages in the
  pattern `1=category 2=value 3=updated_at` on both messages for
  cross-implementation parity.
- Do **not** change the gRPC `UpdateStockRequest.scores` field — it
  stays a `map<string,double>`.
- Do **not** change the domain/store signature — it stays
  `UpdateStock(ctx, symbol, exchange string, scores map[string]float64)
  (*store.Stock, error)`. The timestamp is written by the DB (`now()`),
  not by the store.
- Do **not** add any new Go dependency. `google.golang.org/protobuf`
  v1.36.12 (already in `go.mod`) ships
  `google.golang.org/protobuf/types/known/timestamppb`.
- The server is the **authoritative** source of the write timestamp.
  Any `updated_at` provided in a Kafka request is accepted and
  discarded; the stored value read back from the DB is
  authoritative.
- Codegen toolchain: `protoc` v29.3 at
  `/home/brian/workspace/bin/protoc`; `protoc-gen-go` v1.36.12 and
  `protoc-gen-go-grpc` v1.6.2 in `$(go env GOPATH)/bin`.
- Verification commands: `go build ./...`, `go vet ./...`,
  `go test ./...`.

---

### Step 1 — gRPC proto: add `updated_at` to `ScoreEntry`

**Files:**
- Modify: `proto/v1/stock_store.proto`

**Interfaces:**
- Consumes: existing `proto/v1/stock_store.proto`.
- Produces: `ScoreEntry.UpdatedAt *timestamppb.Timestamp` in the
  regenerated Go code (`proto/v1/stockstorev1.pb.go`).

1. Add `import "google/protobuf/timestamp.proto";` after
   `option go_package = "stocker-store/proto/v1;stockstorev1";`.
2. Replace the line
   `message ScoreEntry { string category = 1; double value = 2; }`
   with
   `message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }`.
3. Regenerate the gRPC Go bindings from the repo root:

```bash
/home/brian/workspace/bin/protoc \
  --go_out=. --go_opt=paths=source_relative \
  proto/v1/stock_store.proto
```

4. Verify the regenerated file now exposes the getter:

```bash
grep -n "func (x \*ScoreEntry) GetUpdatedAt" proto/v1/stockstorev1.pb.go
```

   Expected to see one match.

5. Verify the build compiles:

```bash
go build ./...
```

6. Verify the vet is clean:

```bash
go vet ./...
```

7. **RISKY flag:** none at this step. The change is purely additive
   to an existing message.

---

### Step 2 — Kafka proto: add `ScoreEntry`, make `StockUpdate.scores` a repeated

**Files:**
- Modify: `proto/v1/kafka/stock_message.proto`

**Interfaces:**
- Consumes: the existing gRPC `ScoreEntry` message (as a shape
  reference — not referenced directly in the proto).
- Produces: a `kafkastockv1.ScoreEntry` type with `Category string`,
  `Value float64`, `UpdatedAt *timestamppb.Timestamp`; and
  `StockUpdate.Scores []*ScoreEntry`.

1. Add `import "google/protobuf/timestamp.proto";` after
   `option go_package = "stocker-store/proto/v1/kafka;kafkastockv1";`.
2. Replace `map<string, double> scores = 3;` with
   `repeated ScoreEntry scores = 3;`.
3. Add the new `ScoreEntry` message above `StockUpdate`
   (protobuf does not require forward references, but keep the shape
   parity with the gRPC `ScoreEntry` — same field names and numbers):

```proto
message ScoreEntry {
  string category = 1;
  double  value   = 2;
  google.protobuf.Timestamp updated_at = 3;
}

message StockUpdate {
  string symbol   = 1;
  string exchange = 2;
  repeated ScoreEntry scores = 3;
}
```

4. Update the comment above `StockUpdate` to mention the shape.
5. Regenerate the Kafka Go bindings from the repo root:

```bash
/home/brian/workspace/bin/protoc \
  --go_out=. --go_opt=paths=source_relative \
  proto/v1/kafka/stock_message.proto
```

6. Verify the regenerated file now exposes the repeated `Scores`
   field:

```bash
grep -n "func (x \*StockUpdate) GetScores" proto/v1/kafka/stock_message.pb.go
grep -n "Scores\s*\[\]\*ScoreEntry" proto/v1/kafka/stock_message.pb.go
```

   Expected: both matches present.

7. Verify the build compiles:

```bash
go build ./...
```

8. **RISKY flag:** this step is the single breaking wire-shape
   change. V1 is not deployed per the user's statement; the
   in-repo consumer is the only thing that must adapt.
   Mitigate by completing Step 4 (Kafka client) in the same
   change set before any Kafka traffic.

---

### Step 3 — Domain model: `store.ScoreEntry` gains `UpdatedAt`

**Files:**
- Modify: `internal/store/models.go`
- Modify: `internal/store/store.go` (the `getStockScores` SELECT
  and `Scan`)
- Modify: `internal/store/testhelper_test.go` (assert non-zero
  `UpdatedAt` in the new test helper)

**Interfaces:**
- Consumes: existing `Store` type with `ScoreEntry`.
- Produces: `store.ScoreEntry{Category string; Value float64;
  UpdatedAt time.Time}` — the canonical shape used by the gRPC
  server adapter and the Kafka client adapter.

1. In `internal/store/models.go`, add an `UpdatedAt time.Time`
   field to the `ScoreEntry` struct, placed after `Value`:

```go
type ScoreEntry struct {
    Category  string
    Value     float64
    UpdatedAt time.Time
}
```

2. In `internal/store/store.go` `getStockScores`, change the SELECT
   and the `rows.Scan`:

   - Before: `SELECT category, value FROM scores WHERE symbol = $1
     AND exchange = $2 ORDER BY category`
   - After: `SELECT category, value, timestamp FROM scores WHERE
     symbol = $1 AND exchange = $2 ORDER BY category`

   - Before: `rows.Scan(&score.Category, &score.Value)`
   - After: `rows.Scan(&score.Category, &score.Value,
     &score.UpdatedAt)`

   No SQL change elsewhere in the store; the upsert already writes
   `timestamp = now()`.

3. In `internal/store/testhelper_test.go`, update the `seedScore`
   helper to take an explicit `ts time.Time` and assert the write
   path, so tests can verify a non-zero `UpdatedAt` is read back:
   change the INSERT to set `timestamp` to the caller-supplied time,
   and add a sibling helper `readScore(t, s, symbol, exchange,
   category)` that returns a `store.ScoreEntry` (category/value are
   optional in the return).

4. Verify the build compiles:

```bash
go build ./...
go vet ./...
```

5. Run the store tests (skip if no DSN is set):

```bash
go test ./internal/store/...
```

   Expected: existing tests pass unchanged; the DB-backed tests are
   skipped if no `DATABASE_URL` is set.

6. **RISKY flag:** none — this is purely additive to the domain
   shape and a one-line SQL change.

---

### Step 4 — gRPC server adapter: map `UpdatedAt` to `timestamppb`

**Files:**
- Modify: `internal/grpc/server.go`

**Interfaces:**
- Consumes: `store.ScoreEntry{Category, Value, UpdatedAt}`.
- Produces: nothing new; only uses the updated shape.

1. In `internal/grpc/server.go`, import the timestamppb package:

```go
"google.golang.org/protobuf/types/known/timestamppb"
```

2. In `toProtoScores`, set `UpdatedAt` on each generated `ScoreEntry`.
   Before: `out = append(out, &st.ScoreEntry{Category: e.Category,
   Value: e.Value})`. After:
   `out = append(out, &st.ScoreEntry{Category: e.Category,
   Value: e.Value, UpdatedAt: timestamppb.New(e.UpdatedAt)})`.

3. Verify the build compiles:

```bash
go build ./...
go vet ./...
```

4. Update the gRPC server tests (see Step 6 below, not Step 4 —
   Step 4 is only source-code, tests are Step 6).

5. **RISKY flag:** none at this step. The `timestamppb.New` call
   returns a non-nil value for any `time.Time`, so the generated
   `ScoreEntry.UpdatedAt` will be non-nil. Existing client code that
   does `score.GetUpdatedAt().AsTime()` is safe —
   `timestamppb` `Get*` is nil-safe.

---

### Step 5 — Kafka client adapter: convert `repeated ScoreEntry →
map[string]float64`

**Files:**
- Modify: `internal/kafka/client.go`

**Interfaces:**
- Consumes: `kafkastockv1.ScoreEntry` and
  `kafkastockv1.StockUpdate.Scores []*ScoreEntry`.
- Produces: nothing new; the `Store` interface stays
  `UpdateStock(ctx, symbol, exchange string, scores map[string]float64)
  (*store.Stock, error)`.

1. In `internal/kafka/client.go`, add a small package-private helper
   to convert the repeated slice to a map (for the store):

```go
func toMap(entries []*kafkastockv1.ScoreEntry) map[string]float64 {
    if entries == nil {
        return nil
    }
    m := make(map[string]float64, len(entries))
    for _, e := range entries {
        m[e.GetCategory()] = e.GetValue()
    }
    return m
}
```

2. In `handle`, replace the iteration over `m.Scores` (a map) with
   an iteration over the slice:

   - Before: `for cat, val := range m.Scores { if val < -1.0 ||
     val > 1.0 { ... } }`
   - After:
     `for _, e := range m.GetScores() {
     v := e.GetValue(); cat := e.GetCategory();
     if v < -1.0 || v > 1.0 {
         return fmt.Errorf("score %s value %v out of range [-1, 1]",
         cat, v)
     } }`

3. In `handle`, change the `c.store.UpdateStock(...)` call argument
   `m.Scores` → `toMap(m.GetScores())`:
   `_, err = c.store.UpdateStock(ctx, m.Symbol, m.Exchange,
   toMap(m.GetScores()))`

4. In `decodeStock`, remove the `if m.Scores == nil { m.Scores =
   make(map[string]float64) }` block — `toMap` handles nil.

5. Verify the build compiles:

```bash
go build ./...
go vet ./...
```

6. Run the kafka tests (see Step 6 below for the actual test-file
   changes; the tests will not pass yet):

```bash
go test ./internal/kafka/...   # expected to FAIL (tests still use
map-typed payloads)
```

   This confirms the compile is correct but the tests need updating.
   Step 6 below updates them.

7. **RISKY flag:** the wire shape changed. If a Kafka producer is
   still sending the old `map<string,double>` shape, the producer
   **cannot** be consumed by this new consumer (and vice versa).
   Because V1 is not deployed, this is a deliberate cutover. The
   in-repo consumer is the only consumer we ship today.

---

### Step 6 — Tests: update all test files

**Files:**
- Modify: `internal/store/testhelper_test.go`
- Modify: `internal/store/store_test.go`
- Modify: `internal/grpc/server_test.go`
- Modify: `internal/kafka/client_test.go`

**Interfaces:**
- Consumes: the updated `store.ScoreEntry` and `kafkastockv1`
  types.
- Produces: tests that assert the new `UpdatedAt` field on every
  read of a `ScoreEntry` and a correct `repeated ScoreEntry`
  wire round-trip.

For each test file:

1. `internal/store/testhelper_test.go`:
   - Change `seedScore(t, s, sym, exh, cat, val float64)` to
     `seedScore(t, s, sym, exh, cat string, val float64, ts time.Time)`
     and write `ts` to `scores.timestamp`.
   - Add a new helper `readScore(t, s, sym, exh, cat
     string) (score store.ScoreEntry, ok bool)` that runs
     `SELECT category, value, timestamp FROM scores WHERE...`
     and returns the single row.

2. `internal/store/store_test.go`:
   - Update every `seedScore(...)` call to pass an explicit
     `time.Now()`.
   - In `TestUpdateStockRefreshesTimestamp` (or a new sibling test
     `TestUpdateStockRefreshesScoreTimestamp`), add an assertion that
     after `s.UpdateStock(...)` with one score,
     `s.getStockScores(ctx, sym, exh)[0].UpdatedAt` is not zero and
     is **not** before the seeded old `before` time.

3. `internal/grpc/server_test.go`:
   - Update `toDomainScores` so that each generated
     `store.ScoreEntry` sets `UpdatedAt: time.Now()`.
   - Add a sibling assertion in `TestUpdateStock` (or in a new
     `TestUpdateStockScoreUpdatedAt`) verifying that the response
     `Stock.Scores[0]` has non-zero
     `UpdatedAt` (`!scores[0].GetUpdatedAt().IsZero()`).
   - Update any existing test that constructs a
     `store.ScoreEntry{Category, Value}` literal to also set
     `UpdatedAt: time.Now()`.

4. `internal/kafka/client_test.go`:
   - Add an import:
     `"google.golang.org/protobuf/types/known/timestamppb"` and
     `"time"`.
   - Update `fakeStore.lastScores` from `map[string]float64` to
     `map[string]float64` **unchanged** (the store signature is
     unchanged). Only update the call sites where the test builds
     `kafkastockv1.StockUpdate`.
   - In `TestDecodeStock_HappyPath`, change the payload:

     `protoMsg := &kafkastockv1.StockUpdate{
     Symbol: "AAPL", Exchange: "NASDAQ",
     Scores: []*kafkastockv1.ScoreEntry{
         {Category: "momentum", Value: 0.5,
          UpdatedAt: timestamppb.New(time.Now())},
         {Category: "value", Value: -0.25,
          UpdatedAt: timestamppb.New(time.Now())},
     },
     }`

     Update the assertions to iterate `m.Scores` (slice) instead of
     indexing into a map: `if len(m.Scores) != 2 { ... }`; use a
     look-by-category helper or index into the slice.

   - In `TestDecodeStock_ScoresAbsentDefaultsToEmptyMap`, the
     behavior stays the same: `m.Scores` is empty (nil slice is OK).
     Keep the test name or rename it to
     `TestDecodeStock_ScoresAbsentDefaultsToEmptySlice`; use
     `len(m.Scores) != 0` for the assertion (and drop the
     `m.Scores == nil` check since `toMap` handles nil).

   - `TestDecodeStock_RoundTrip`: change the two test-case
     `StockUpdate` literals to build
     `Scores: []*kafkastockv1.ScoreEntry{ {Category: "momentum",
     Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
     {Category: "value", Value: -0.3,
     UpdatedAt: timestamppb.New(time.Now())} }`, and compare
     the slice element-by-element by category.

   - `TestHandle_ScoreOutOfRange`: change the tc shape from
     `map[string]float64` to `[]*kafkastockv1.ScoreEntry` and the
     `Scores:` field to `[]*kafkastockv1.ScoreEntry` in each
     `StockUpdate` literal.

   - `TestHandle_HappyPath`: change `wantScores` to a
     `[]*kafkastockv1.ScoreEntry` slice and assert the
     fakeStore's `lastScores` map reflects the conversion
     (e.g., `store.lastScores["momentum"] == 0.5`).

   - `TestHandle_StoreErrorPropagated`: no change needed —
     `Scores` slice may be empty.

5. Verify all packages build and vet clean:

```bash
go build ./...
go vet ./...
```

6. Run the full test suite (database-backed tests will skip with no
   `DATABASE_URL`):

```bash
go test ./...
```

7. **RISKY flag:** none — the changes are contained to test
   literals. The wire-shape impact is already flagged in Step 2 and
   Step 5.

---

### Step 7 — Docs: update all docs that describe the shape

**Files:**
- Modify: `README.md`
- Modify: `design.md`
- Modify: `proto/v1/kafka/README.md`
- Modify: `docs/design/kafka-proto.md`
- Modify: `AGENTS.md`

1. `README.md`:
   - Update the **Messages** table: the `ScoreEntry` row now lists
     three fields — `string category`, `double value`,
     `google.protobuf.Timestamp updated_at`. The
     `UpdateStockRequest` row stays `map<string,double> scores`.
   - Update the inline **proto block** to show the new
     `ScoreEntry` message with `updated_at` (and
     the import of `google/protobuf/timestamp.proto`).
   - Update the **Kafka Ingestion** message schema table: the
     `scores` row is now `repeated ScoreEntry` and add a new field
     row showing `ScoreEntry`'s three fields. Rename the proto
     block to reflect the new `ScoreEntry` message and the repeated
     field.
   - Update the **Kafka Validation** section to note that the server
     stamps each score with its own clock on write
     (the `updated_at` in the request is advisory only).
   - Update the **Repository layout** to add a mention of
     `docs/design/score-entry-timestamp.md`.

2. `design.md`:
   - Update the **Messages (key fields)** block: `ScoreEntry` now
     includes `updated_at = 3`.

3. `proto/v1/kafka/README.md`:
   - Update the **Consuming in Another Go Service** example:
     `msg := &kafkastockv1.StockUpdate{ Symbol: "AAPL",
     Exchange: "NASDAQ", Scores: []*kafkastockv1.ScoreEntry{
     {Category: "momentum", Value: 0.7,
     UpdatedAt: timestamppb.New(time.Now())} } }`.
   - Update the **Schema** table: the `scores` row is now
     "repeated ScoreEntry" and add a row for `ScoreEntry` with
     its three fields.
   - Add a **Versioning / migration** note: the V1 shape changed
     from `map<string,double>` to `repeated ScoreEntry` as a
     deliberate cutover; `updated_at` in the request is advisory —
     the server stamps each written score with its own clock.

4. `docs/design/kafka-proto.md`:
   - Add a short **Superseded by:** pointer to
     `docs/design/score-entry-timestamp.md` in the "Design
     Decisions" / "Explanation of choices" section. Mark the
     "Why a map field?" rationale as superseded by the
     "per-entry timestamp" requirement.

5. `AGENTS.md`:
   - Add a bullet under **Requirements**:
     `* Each score has a last-updated timestamp (set by the server
     on write).`

6. Verify markdown files are well-formed:

```bash
ls docs/design/score-entry-timestamp.md README.md design.md
proto/v1/kafka/README.md AGENTS.md docs/design/kafka-proto.md
```

7. **RISKY flag:** none — docs-only change.

---

### Step 8 — `cmd/main.go` bridgeStore (only if the store
signature changed)

**Files:**
- Modify: `cmd/main.go` — **only** if Step 5 changed
  `kafka.Store.UpdateStock`'s signature. In the current plan, the
  store signature is **unchanged**
  (`UpdateStock(ctx, symbol, exchange string, scores
  map[string]float64) (*store.Stock, error)`), so **no change** is
  needed in `cmd/main.go`.

1. Confirm the `stockStore` interface in `cmd/main.go` still
   matches the `store.Store` method:

   `type stockStore interface { UpdateStock(ctx context.Context,
   symbol, exchange string, scores map[string]float64)
   (*store.Stock, error) }` → matches.

2. Verify the build compiles:

```bash
go build ./...
go vet ./...
go test ./...
```

3. **RISKY flag:** none — the store signature is unchanged.

---

### Step 9 — Full verification

1. Clean rebuild and full test run:

```bash
go build ./...
go vet ./...
go test ./...
```

2. Spot-check the regenerated gRPC response shape by running the
   in-process integration test:
   `go test ./internal/grpc/... -run TestUpdateStock -v`.

3. Spot-check the Kafka round-trip:
   `go test ./internal/kafka/... -run TestDecodeStock_RoundTrip -v`.

4. Confirm the wire-shape change is the only wire-shape change
   (no accidental changes to other fields):

```bash
git diff --stat
```

5. Commit (see the **Execution Handoff** below).

---

## Execution Handoff

After all steps are complete, run from the repo root:

```bash
git add proto/v1/stock_store.proto proto/v1/kafka/stock_message.proto \
        proto/v1/stockstorev1.pb.go proto/v1/kafka/stock_message.pb.go \
        internal/store/models.go internal/store/store.go \
        internal/store/testhelper_test.go internal/store/store_test.go \
        internal/grpc/server.go internal/grpc/server_test.go \
        internal/kafka/client.go internal/kafka/client_test.go \
        README.md design.md AGENTS.md \
        proto/v1/kafka/README.md docs/design/kafka-proto.md \
        docs/design/score-entry-timestamp.md
git commit -m "feat(score-entry): add updated_at to ScoreEntry (gRPC
and Kafka)

- gRPC ScoreEntry gains google.protobuf.Timestamp updated_at
  (field 3); the read/response Stock.Scores now expose the time
  each score was last written.
- Kafka StockUpdate.scores changes from map<string,double> to
  repeated ScoreEntry{category,value,updated_at} so each entry has
  a per-entry timestamp.
- Store domain model gains store.ScoreEntry.UpdatedAt time.Time,
  populated by reading the existing scores.timestamp column.
- gRPC server adapter maps UpdatedAt into the timestamppb field.
- Kafka adapter converts []*ScoreEntry to map[string]float64 for
  the store; the server stamps the write timestamp with now() (the
  existing scores.timestamp DEFAULT now() SQL already does this).
- Keep v1 namespace/version. Update all docs.
"
```
