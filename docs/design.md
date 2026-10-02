# Stocker Store Service — Design Document

## 1. Overview

**Stocker Store** is a gRPC-backed service for managing stock data and scores. It supports thousands of stocks and flexible retrieval patterns.

```
┌───────────┐       Protocol Buffer            ┌──────────────┐    ┌────────────┐
│  Clients  │ ◄══════► gRPC (bidirectional)  ► │ stocker-store│◄──►│ PostgreSQL │
│ (gRPC,    │       & kafka streaming          │              │    │            │
│  Kafka)   │                                  └──────────────┘    └────────────┘
│           │                                      golang                    │
└───────────┘                                                     ┌────────────┐
                                                                  │ stocks,    │
                                                                  │ scores     │
                                                                  └────────────┘

## 2. Requirements Summary

- Retrieve stocks by: symbol, exchange, random (with filters), score ranges
- Submit stocks.
- Remove stocks from an exchange.
- Submit scores (dynamic categories, normalized -1.0 to 1.0).
- Thousands of stocks.
- Remove stocks older than a configurable duration (default 30 days) based on their timestamp.
- Self-hostable using Podman Quadlets
- simple, concise

---

## 3. Data Model

### 3.1 Entities and Relationships

```
┌─────────────────────────────────────┐     ┌───────────────────────────────────────┐
│ stocks                              │─────│     scores                            │
│ ( id (composite symbol + exchange)  │     │                                       │
│ symbol,                             │     │  (id (composite stock_id + category), │
│   exchange,                         │     │   symbol, exchange, category,         │
│   timestamp)                        │     │   value, change_timestamp             │
└─────────────────────────────────────┘     └───────────────────────────────────────┘
```
The stocks table has a composite key composed of the symbol + the exchange. Other columns include the symbol, the exchange, and the timestamp

The scores table has a composite key composed of the id from the stocks table + the category. Other columns include the symbol, exchange, category, value, timestamp

### 3.2 Schema (PostgreSQL)

#### `stocks` — master list of symbols

| Column      | Type        | Notes                 |
|-------------|-------------|-----------------------|
| `symbol`    | TEXT        | NOT NULL              |
| `exchange`  | TEXT        | NOT NULL              |
| `timestamp` | TIMESTAMPTZ | NOT NULL              |

Indexes:
`id` PRIMARY KEY (`symbol`, `exchange`)

#### `scores` — score snapshots

| Column           | Type         | Notes                                          |
|------------------|--------------|------------------------------------------------|
| `stock_id`       | TEXT         | FK → stocks.id                                 |
| `category`       | TEXT         | NOT NULL.                                      |
| `value`          | DOUBLE PREC. | CHECK: value BETWEEN -1.0 AND 1.0, DEFAULT 0.0 |
| `timestamp`      | TIMESTAMPTZ  | NOT NULL                                       |

Indexes:
`id` PRIMARY KEY (`stock_id`, `category`)
composite index on `(category, value DESC)`

---

## 4. gRPC API Definition (protobuf service)

### Service: `StockStore`

| RPC Name           | Request                       | Response                  | Notes                          |
|--------------------|-------------------------------|---------------------------|--------------------------------|
| AddStocks          | `stream UpdateStockRequest`   | `Stock`                   | Bulk create (client-streaming) |
| UpdateStock        |.`UpdateStockRequest`          | `Stock`                   | single create/update           |
| RemoveStock        | `RemoveStockRequest`          | `RemoveStockResponse`     | Delete from exchange           |
| GetStock           | `GetStockRequest`             | `Stock`                   | All exchanges or filtered      |
| GetStocks          | `GetStocksRequest`            | `List[Stock]`             | Gets Random List of Stocks     |

### Messages (key fields)

```protobuf
message UpdateStockRequest { string symbol = 1; string exchange = 2; optional map<string, double> scores = 3; }
message RemoveStockRequest { string symbol = 1; string exchange = 2; }
message GetStockRequest { string symbol = 1; optional string exchange = 2; }
message GetStocksRequest { int32 limit = 1; optional string exchange = 2; optional map<string, double> min_scores = 3; optional map<string, double> max_scores = 4; }
message RemoveStocksResponse { bool removed = 1; }
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }
```

---

## 5. Retrieval patterns and implementation approach

### 5.1 By symbol

Simple lookup: `stocks.symbol + stocks.exchange` → scores in single query.

### 5.2 By exchange

SELECT * FROM stocks WHERE exchange = $1 ORDER BY RANDOM()

### 5.4 By score range

`WHERE score.value BETWEEN min_val AND max_val AND score.category = $1`

---

## 6. Technology Decisions

### 6.1 Programming Language

| Option      | Verdict     | Why                              |
|-------------|-------------|----------------------------------|
| **Go**      | **SELECTED**| Strong gRPC ecosystem (gRPC-Go), compiled, single binary deployment, small memory footprint (~10 MB per server), fast at scale, excellent concurrency model. |

### 6.2 Database

| Option          | Verdict      | Why                              |
|-----------------|--------------|----------------------------------|
| **PostgreSQL**  | **SELECTED** | Best fit for both OLTP (company/exchange CRUD) and scoring analytics. Self-hostable via Docker Compose. Strong golang driver (`pgx`). |

### 6.3 Caching Layer

| Option                        | Verdict      | Why                              |
|-------------------------------|--------------|----------------------------------|
| None (cache-miss to Postgres) | **SELECTED initially** | At thousands of stocks, Postgres can handle the read load with proper indexing. Redis adds operational complexity not yet justified by performance needs. Add later if needed. |

### 6.4 Additional Infrastructure

| Component        | Choice                | Notes                            |
|------------------|-----------------------|----------------------------------|
| Config/Discovery | file-based config     | Fine for v1. |
| Schema migration | None | We can afford to lose  all data, just Drop and recreate the table |
| Container orchestration | Podman Quadlets | Keep v1 minimal. |
| Observability | OpenTelemetry, Prometheus metrics, structured logging | Standard Go ecosystem. |

---

## 7. Architecture and Deployment

### 7.1 Component diagram

```
                    ┌──────────────┐
    gRPC clients ──►│              │
     , kafka        │ stocker-store│ PostgreSQL
                    │   (Go)       │◄── Podman
                    └──────────────┘
```

Single `stocker-store` binary. Postgres runs separately or in its own container.

### 7.2 Normalization and validation

All score values get validated client-side via protobuf `double` with a server-side `CHECK(value BETWEEN -1.0 AND 1.0)` constraint. Default is `0.0`.

### 7.3 Error handling gRPC errors to return

| Scenario                            | gRPC code       |
|-------------------------------------|---------------------|
| Stock not found                     | NOT_FOUND           |
| Invalid score value                 | INVALID_ARGUMENT    |
| Database failure                    | INTERNAL + retry    |
| Conflict (e.g. concurrent writes)   | FAILED_PRECONDITION |

---

## 8. Data Retention

- Hard deletes: When a stock is "removed," the `stocks` row and its dependent `scores` rows get deleted.
- Automatic expiry: Stocks not added or updated within the retention window are deleted.
  - The retention window is a **configurable duration** (e.g. via a `STOCK_TTL` environment variable / config option) with a **default of 30 days**. A value of `0` disables expiry.
  - A stock's age is measured by its `timestamp` column — the time it was last added or updated. Every `AddStock`/`Update` refreshes this timestamp to "now," even when nothing else changed (the row is `INSERT`/upsert with `ON CONFLICT ... DO UPDATE SET timestamp = now()`).
  - Expiry runs as a background cleanup: a periodic task (default interval of 1 hour) deletes `stocks` (and, via the foreign key, their `scores`) where `timestamp < now() - <retention>`.
  - Example: with the 30-day default, a stock that has not been touched for 30 days is removed.

---

## 9. Scalability Considerations

### 9.1 At thousands of stocks (v1 target)

All query patterns fit comfortably in PostgreSQL with proper indexing. `pgx` provides excellent connection management. Memory footprint is ~20 MB per server process. No sharding needed.

---

## 10. Operations

### v1 Deployment (Podman Quadlet)

Stocker-Store should be deployed as a podman quadlet.
- The stocker-store container image should be pushed to the container registry at git.wheeli.ca
- The quadlet should pull and run the container image. PostgreSQL is deployed separately (not in scope of this repo/service).

### Schema migrations

None / Not Necessary

---

## 11. Future Extensions

- Pagination to avoid response limits

## 12. New-Stock Output Topic

When a new `(symbol, exchange)` pair is submitted for the **first time** — i.e. the upsert in `stocks` performs an `INSERT` rather than an `UPDATE` on the unique key — the service publishes a PROTOBUF-encoded `Stock` message (package `stockstore.v1`, the same message gRPC returns) to a dedicated **output** Kafka topic.

### 12.1 Purpose and scope

- Emit an event for downstream consumers (e.g. a scoring service that reacts to new tickers) without them having to poll `GetStocks`.
- Covers **all write paths into the store**: gRPC `AddStocks` / `UpdateStock`, and the Kafka input subscriber — any path that creates the row, not just Kafka-to-Kafka.
- Does **not** re-emit for subsequent updates of an existing `(symbol, exchange)` pair. The existing input topic already carries the full upsert stream; the output topic is a *creation signal*.
- After a `RemoveStock`, the next upsert of the same pair is a new INSERT and the event fires again (consistent with "new stock = new INSERT").

### 12.2 Naming

| Concern            | Old name | New name            | Notes                                                                                      |
|--------------------|----------|---------------------|--------------------------------------------------------------------------------------------|
| Inbound topic      | `KAFKA_TOPIC` | **`KAFKA_INPUT_TOPIC`** | Backwards-incompatible rename; operator updates all client env files. No alias kept.    |
| Outbound topic     | —        | **`KAFKA_OUTPUT_TOPIC`** | New variable.                                                                            |
| Broker list        | `KAFKA_BROKERS` | `KAFKA_BROKERS` | Unchanged; shared by the input consumer and the output producer (one cluster handles both). |
| Consumer group     | `KAFKA_GROUP_ID` | `KAFKA_GROUP_ID` | Unchanged; applies to the input consumer only.                                            |

The `KAFKA_INPUT_TOPIC` / `KAFKA_OUTPUT_TOPIC` pair matches the convention used by the partner service.

### 12.3 Detection mechanism (insert vs. update)

PostgreSQL's `ON CONFLICT ... DO UPDATE ... RETURNING` clause yields the affected tuple. Postgres exposes the tuple's `xmax` (the updating transaction's id) for every row it returns: `xmax = 0` means the tuple was *inserted* by this statement; `xmax != 0` means the tuple already existed and was *updated*. So the single upsert becomes:

```sql
INSERT INTO stocks (symbol, exchange, timestamp)
VALUES ($1, $2, now())
ON CONFLICT (symbol, exchange) DO UPDATE SET timestamp = now()
RETURNING (xmax = 0) AS inserted;
```

- `inserted = TRUE`  → newly created row; emit the output-topic message.
- `inserted = FALSE` → row pre-existed; silently skip the publish.

This is **one round-trip inside the existing transaction** (replaces the current `Exec` with a `QueryRow`), is atomic with the stock/score write, and is deterministic under concurrent writers (Postgres serializes conflict resolution and exactly one writer sees `xmax = 0`).

**API impact:** `Store.UpdateStock` gains a second return value — the `inserted` flag. Call sites (gRPC `AddStocks` / `UpdateStock`, Kafka `handle`) adapt: only the ones that hold a `Publisher` check the flag. The store stays agnostic of *where* the event goes — it just reports the outcome of its own write.

### 12.4 Component wiring

```
gRPC AddStocks   ─┐
gRPC UpdateStock ─┤        ┌─  publisher.Publish(ctx, *stockv1.Stock)  (best-effort)
Kafka handle     ─┴─► store.UpdateStock ──► (ctx, *store.Stock, bool inserted, error)
                                          │
                                 inserted==true ? → emit on output topic (log-and-continue on failure)
```

A small `Publisher` interface (in `internal/kafka/publisher.go`) exposes:

```go
type Publisher interface {
    Publish(ctx context.Context, stock *stockv1.Stock) error
    Close() error
}
```

Two implementations:

| Implementation    | Trigger                                              | `Publish` behavior                          |
|-------------------|------------------------------------------------------|---------------------------------------------|
| `*writerPublisher` | `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` both set   | `proto.Marshal` → `kafka.Writer.WriteMessages`. Async, buffered. |
| `noopPublisher`   | otherwise                                            | log a one-line notice, return nil.          |

`NewPublisher(brokers []string, topic string) Publisher` returns the appropriate impl; `cmd/main.go` constructs it **once** and injects it into `grpc.NewServer(st, publisher)` and `kafka.New(cfg, store, publisher)`.

Wiring in `cmd/main.go`:

1. Read `KAFKA_BROKERS` (shared by both directions), `KAFKA_INPUT_TOPIC` (renamed), `KAFKA_GROUP_ID` (input consumer), and `KAFKA_OUTPUT_TOPIC` (output producer).
2. Construct the publisher **once**: `publisher := kafka.NewPublisher(envList("KAFKA_BROKERS"), os.Getenv("KAFKA_OUTPUT_TOPIC"))`; `defer publisher.Close()` after the gRPC `GracefulStop`.
3. Hand it to `grpc.NewServer(st, publisher)` and `kafka.New(cfg, st, publisher)`.
4. The input consumer remains a **no-op** unless both `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set — the output publisher is a **no-op** unless both `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set. They are **independent**: enable input only, output only, both, or neither.

### 12.5 Failure semantics

- A **failure to publish** is **logged at error level** and the handler **returns success** to the caller. Rationale: the DB write is the authoritative state; the output topic is a best-effort fan-out. Retries, DLQs, and re-sends are out of scope — the consumer is expected to be tolerant (poll or use idempotent handlers). Making a Kafka publish failure fatal to the primary write is disproportionate to the event's importance.
- A **failure in the store write** (the `UpdateStock` error path) returns the error to the caller and **skips the publish** entirely — no point emitting an event for a stock that was not actually written.
- `Publish` is **synchronous** (blocks briefly) to keep ordering simple. For high volume, the underlying writer is `Async: true` so the cost is a queue push, not a network round-trip.
- `Close` is called in reverse order (after gRPC `GracefulStop`) on shutdown, so in-flight messages are flushed.

### 12.6 Configuration

| Variable            | Required | Default          | Format                                              | Purpose                                                            |
|---------------------|----------|------------------|-----------------------------------------------------|--------------------------------------------------------------------|
| `DATABASE_URL`      | Yes      | `postgres://…`   | PostgreSQL DSN                                      | The single Postgres connection string (unchanged).                 |
| `STOCK_TTL`         | No       | `720h`           | Go duration string; `0` disables                    | Retention window for stale-stock auto-removal (unchanged).         |
| `KAFKA_BROKERS`     | No       | —                | Comma-separated `host:port` list                    | Shared between the input consumer and the output producer.         |
| `KAFKA_INPUT_TOPIC` | No       | —                | Topic name (e.g. `stockers`)                        | Topic to **consume** stock events from (renamed from `KAFKA_TOPIC`). |
| `KAFKA_GROUP_ID`    | No       | `stocker-store`  | Consumer group id                                   | Kafka consumer group for the input subscriber (unchanged).         |
| `KAFKA_OUTPUT_TOPIC`| No       | —                | Topic name (e.g. `stockers-new`)                    | Topic to **publish** new-stock `Stock` messages to (new).          |

- Kafka **input** is a **no-op** unless **both** `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set.
- Kafka **output** is a **no-op** unless **both** `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set.
- Input and output are **independent** — enable either, both, or neither.

### 12.7 Message format

The published message is the same `Stock` PROTOBUF message gRPC returns (package `stockstore.v1`). No new schema, no new `proto/` tree:

```protobuf
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }
```

Serialized with `proto.Marshal` (no JSON, no compression). Consumers decode with the existing `proto/v1/stock_store.pb.go`. The payload is the **authoritative, server-stamped** stock (scores including their `updated_at` timestamps) — not the raw input as received.

### 12.8 Testing notes

- **Store:** new `TestUpdateStockInsertedFlag` asserts the `inserted` flag is `true` on the first write to a unique `(symbol, exchange)` pair and `false` on subsequent writes to the same pair. Uses the existing `testStore(t)` helper (skips when `DATABASE_URL` unset).
- **Kafka publisher:** no-op branch (neither `brokers` nor `topic` set → `Publish` returns nil, `Close` returns nil, no side effects); real-writer branch (type-asserts the impl; `Publish(nil)` returns a defensive error).
- **Kafka client / gRPC server:** `fakePublisher` test double records `Publish` calls; tests assert (a) publisher is called exactly once when `inserted=true`, (b) publisher is NOT called when `inserted=false`, (c) publisher NOT called when the store write itself errors, (d) a publish failure does NOT propagate as a handler error (log-and-continue).
- **Wiring:** covered by code review — `cmd/main.go` constructs one publisher and injects it into both `grpc.NewServer` and `kafka.New`.

### 12.9 Non-goals

- No new proto schema / new generated Go package.
- No per-score or per-category events on the output topic — the whole `Stock` is the message (the event is "a stock was created"; its scores are part of the snapshot).
- No retry queue or DLQ; a log line is the acknowledgment.
- No re-emission for *same* `(symbol, exchange)` across refreshes — only on the first INSERT.
- No backward compatibility for the `KAFKA_TOPIC` → `KAFKA_INPUT_TOPIC` rename.
- No change to the `KAFKA_GROUP_ID` or `KAFKA_BROKERS` semantics.
