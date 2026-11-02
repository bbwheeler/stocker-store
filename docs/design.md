# Stocker Store Service — Design Document

## 1. Overview

**Stocker Store** is a gRPC-backed service for managing stock data and scores. It supports thousands of stocks and flexible retrieval patterns.

```
┌───────────┐       Protocol Buffer            ┌──────────────┐    ┌────────────┐
│  Clients  │ ◄══════► gRPC (bidirectional)  ► │ stocker-store│◄──►│ PostgreSQL │
│ (gRPC,    │       & RabbitMQ ingestion       │              │    │            │
│ RabbitMQ) │                                  └──────────────┘    └────────────┘
│           │                                      golang                    │
└───────────┘                                                     ┌────────────┐
                                                                  │ stocks,    │
                                                                  │ scores     │
                                                                  └────────────┘
```

Stock writes arrive over gRPC and over RabbitMQ ingestion from the upstream
publisher. The RabbitMQ contract — exchange and queue topology, message schema,
configuration, and delivery semantics — is documented in
[`docs/design/rabbitmq-ingestion.md`](rabbitmq-ingestion.md).

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

#### Score upsert semantics

`UpdateStock` merges scores; it never replaces the set. Each supplied `(symbol, exchange, category)` is upserted inside the same transaction as the stock row:

```sql
INSERT INTO scores (symbol, exchange, category, value, timestamp)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (symbol, exchange, category)
DO UPDATE SET value = EXCLUDED.value, timestamp = now();
```

The statement never deletes score rows, so a partial update leaves categories omitted from the request with both their `value` and their `timestamp` intact. The stock-level `stocks.timestamp` is refreshed by every upsert, including one that supplies no scores. The only deletions of `scores` rows are `RemoveStock` (explicit) and `RemoveOldStocks` (retention).

Range checking is layered: broker ingestion rejects values outside `[-1.0, 1.0]` before calling the store, and the `CHECK (value BETWEEN -1.0 AND 1.0)` constraint on `scores.value` is the database-level backstop for every path.

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
 RabbitMQ ─────────►│ stocker-store│ PostgreSQL
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

## 12. New-Stock Output Publisher

When a new `(symbol, exchange)` pair is submitted for the **first time** — i.e. the upsert in `stocks` performs an `INSERT` rather than an `UPDATE` on the unique key — the service publishes a PROTOBUF-encoded `Stock` message (package `stockstore.v1`, the same message gRPC returns) to a dedicated RabbitMQ **output exchange**.

The full ingestion contract (topology, payload, configuration, delivery and failure semantics) lives in [`docs/design/rabbitmq-ingestion.md`](rabbitmq-ingestion.md). This section covers only the output publisher and the insert detection it relies on.

### 12.1 Purpose and scope

- Emit an event for downstream consumers (e.g. a scoring service that reacts to new tickers) without them having to poll `GetStocks`.
- Covers **all write paths into the store**: gRPC `AddStocks` / `UpdateStock`, and the RabbitMQ ingestion consumer — any path that creates the row.
- Does **not** re-emit for subsequent updates of an existing `(symbol, exchange)` pair. The input exchange already carries the full upsert stream; the output exchange carries a *creation signal*.
- After a `RemoveStock`, the next upsert of the same pair is a new INSERT and the event fires again (consistent with "new stock = new INSERT").

### 12.2 Output topology

The publisher declares only its output exchange (`RABBITMQ_OUTPUT_EXCHANGE`, type `RABBITMQ_OUTPUT_EXCHANGE_TYPE`, default `topic`, durable); consumers of that exchange own their queues and bindings. The default routing key `stock.created.v1` (`RABBITMQ_OUTPUT_ROUTING_KEY`) is deliberately different from the input routing key `stock.update.v1`: pointing the output exchange at `stock.update` with the input key would feed our own queue. The same exchange **and** routing key as the input is rejected at startup; when only the exchange names match, the service logs a warning and leaves topic-pattern overlap to the operator.

### 12.3 Detection mechanism (insert vs. update)

PostgreSQL's `ON CONFLICT ... DO UPDATE ... RETURNING` clause yields the affected tuple. Postgres exposes the tuple's `xmax` (the updating transaction's id) for every row it returns: `xmax = 0` means the tuple was *inserted* by this statement; `xmax != 0` means the tuple already existed and was *updated*. So the single upsert becomes:

```sql
INSERT INTO stocks (symbol, exchange, timestamp)
VALUES ($1, $2, now())
ON CONFLICT (symbol, exchange) DO UPDATE SET timestamp = now()
RETURNING (xmax = 0) AS inserted;
```

- `inserted = TRUE`  → newly created row; emit the output-exchange message.
- `inserted = FALSE` → row pre-existed; silently skip the publish.

This is **one round-trip inside the existing transaction** (replaces the current `Exec` with a `QueryRow`), is atomic with the stock/score write, and is deterministic under concurrent writers (Postgres serializes conflict resolution and exactly one writer sees `xmax = 0`).

**API impact:** `Store.UpdateStock` returns a second value — the `inserted` flag. Call sites (gRPC `AddStocks` / `UpdateStock`, the RabbitMQ consumer `handle`) adapt: only the ones that hold a `Publisher` check the flag. The store stays agnostic of *where* the event goes — it just reports the outcome of its own write.

### 12.4 Component wiring

```
gRPC AddStocks   ─┐
gRPC UpdateStock ─┤        ┌─  publisher.Publish(ctx, *stockv1.Stock)  (best-effort)
RabbitMQ handle  ─┴─► store.UpdateStock ──► (ctx, *store.Stock, bool inserted, error)
                                          │
                                 inserted==true ? → publish to the output exchange (log-and-continue on failure)
```

The `Publisher` contract lives in `internal/events/publisher.go`, owned by neither the gRPC server nor the broker package:

```go
type Publisher interface {
    Publish(ctx context.Context, stock *stockv1.Stock) error
    Close() error
}
```

Two implementations:

| Implementation | When | `Publish` behavior |
|----------------|------|--------------------|
| `*rabbitmq.Publisher` (`internal/rabbitmq/publisher.go`) | `RABBITMQ_OUTPUT_EXCHANGE` set | `proto.Marshal` → AMQP publish with publisher confirms; on failure it logs, reconnects, and retries the same message once. |
| `events.Noop` (`internal/events/publisher.go`) | otherwise | `Publish` and `Close` return nil; `NewPublisher` logs a one-line notice at construction. |

`rabbitmq.NewPublisher(cfg)` returns the appropriate implementation; `cmd/main.go` constructs it **once** and injects it into `grpc.NewServer(st, publisher)` and the consumer (`rabbitmq.New(cfg, st, publisher)`). Both constructors replace a nil publisher with `events.Noop{}`.

Wiring in `cmd/main.go`:

1. `rabbitmq.LoadConfig()` reads the RabbitMQ settings; the consumer and the output publisher share `RABBITMQ_URL`.
2. Construct the publisher **once**: `publisher, err := rabbitmq.NewPublisher(cfg)`; `publisher.Close()` runs after the gRPC `GracefulStop`.
3. Hand it to `grpc.NewServer(st, publisher)` and to `runRabbitMQConsumer(ctx, st, publisher, cfg)`.
4. Ingestion is disabled only by an explicitly empty `RABBITMQ_URL`; the output publisher is **disabled** unless `RABBITMQ_OUTPUT_EXCHANGE` is non-empty. Output requires a non-empty `RABBITMQ_URL` (a startup error otherwise), so the supported modes are ingestion only or ingestion plus output — never output alone.

### 12.5 Failure semantics

- A **failure to publish** is **logged and swallowed**: the gRPC handlers return success and the consumer still acknowledges the delivery. Rationale: the DB write is the authoritative state; the output exchange is a best-effort fan-out. A publish failure never fails the primary write (the gRPC response or the ingestion ack); retries, dead-letter queues, and re-sends are out of scope — the downstream consumer is expected to be tolerant.
- Inside `Publish`, a failed or unconfirmed publish tears down the connection, reconnects, and retries the **same message once**; a second failure is returned to the caller, which logs it (`rabbitmq: failed to publish new stock %s/%s: %v` on the ingestion path). A cancelled context prevents both dialing and the retry.
- A **failure in the store write** (the `UpdateStock` error path) returns the error to the caller and **skips the publish** entirely — no point emitting an event for a stock that was not actually written.
- `Publish` is **synchronous**: it waits for the broker's publisher confirm before returning, so a `nil` error means the broker accepted the message.
- `Close` runs after the gRPC `GracefulStop` on shutdown; it is idempotent and releases the publisher's channel and connection.

### 12.6 Configuration

| Variable                       | Required | Default                          | Format                                | Purpose                                                                                          |
|--------------------------------|----------|----------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------------|
| `GRPC_PORT`                    | No       | `3500`                           | Decimal TCP port, `1`–`65535`         | Port the gRPC server listens on. Invalid values abort startup.                                   |
| `DATABASE_URL`                 | Yes      | `postgres://…`                   | PostgreSQL DSN                        | The single Postgres connection string.                                                           |
| `STOCK_TTL`                    | No       | `720h`                           | Go duration string; `0` disables      | Retention window for stale-stock auto-removal.                                                   |
| `RABBITMQ_URL`                 | No       | `amqp://guest:guest@192.168.1.48:5672/` | `amqp://` / `amqps://` URI            | Broker connection shared by the consumer and the output publisher; explicitly empty disables ingestion (and therefore output). |
| `RABBITMQ_OUTPUT_EXCHANGE`     | No       | unset (output **disabled**)      | Exchange name                         | Output exchange; publishing happens only when this is non-empty.                                 |
| `RABBITMQ_OUTPUT_EXCHANGE_TYPE`| No       | `topic`                          | `direct`, `topic`, `fanout`, `headers`| Exchange type used when output is enabled.                                                       |
| `RABBITMQ_OUTPUT_ROUTING_KEY`  | No       | `stock.created.v1`               | Routing key                           | Key the new-stock event is published with.                                                       |

- The output publisher is a **no-op** (`events.Noop`) unless `RABBITMQ_OUTPUT_EXCHANGE` is set to a non-empty value; `NewPublisher` logs a one-line notice at startup.
- Output requires a non-empty `RABBITMQ_URL`, and the same exchange **and** routing key as the input is rejected at startup (self-feeding loop). Enabling output therefore implies ingestion is enabled; ingestion-only (output disabled) is the default.
- Ingestion settings (`RABBITMQ_EXCHANGE`, `RABBITMQ_EXCHANGE_TYPE`, `RABBITMQ_ROUTING_KEY`, `RABBITMQ_QUEUE`, `RABBITMQ_PREFETCH`) are documented in [`docs/design/rabbitmq-ingestion.md`](rabbitmq-ingestion.md) §4.
- `GRPC_PORT` is validated before the listener is created; an unset or empty value means `3500`. An unparseable or out-of-range value is fatal at startup rather than silently defaulted.

### 12.7 Message format

The published message is the same `Stock` PROTOBUF message gRPC returns (package `stockstore.v1`). No new schema, no new `proto/` tree:

```protobuf
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }
```

Serialized with `proto.Marshal` (no JSON, no compression). Consumers decode with the existing `proto/v1/stock_store.pb.go`. The payload is the **authoritative, server-stamped** stock (scores including their `updated_at` timestamps) — not the raw input as received.

### 12.8 Testing notes

- **Store:** `TestUpdateStockInsertedFlag` asserts the `inserted` flag is `true` on the first write to a unique `(symbol, exchange)` pair and `false` on subsequent writes to the same pair. Uses the existing `testStore(t)` helper (skips when `DATABASE_URL` unset).
- **RabbitMQ publisher** (`internal/rabbitmq/publisher_test.go`): disabled configuration returns `events.Noop` without dialing; exact exchange-declaration flags; message properties (content type, persistent delivery, `message_id` = symbol, `type` = output routing key, `symbol`/`exchange` headers, body round-trip); confirms enabled and awaited; a negative confirm, two consecutive failures, `Publish(nil)`, and publishing after `Close` are errors; URL parse errors redact credentials; a cancelled context skips dialing and the retry.
- **Consumer / gRPC server:** `fakePublisher` test doubles record `Publish` calls; tests assert (a) the publisher is called exactly once when `inserted=true` (`TestUpdateStock_PublishesOnNewStock`, `TestAddStocks_PublishesPerNewStock`, `TestProcessDelivery_PublishesOnlyOnInsert`), (b) it is NOT called when `inserted=false` (`TestUpdateStock_DoesNotPublishOnUpdate`), and (c) a publish failure does NOT propagate as a handler error or prevent the ack (log-and-continue: `TestUpdateStock_PublishErrorDoesNotFailResponse`, `TestProcessDelivery_PublishFailureIsLoggedAndAcked`). A store write failure returns early and never reaches the publish call.
- **Wiring:** covered by code review — `cmd/main.go` constructs one publisher and injects it into both `grpc.NewServer` and the RabbitMQ consumer.

### 12.9 Non-goals

- No new proto schema / new generated Go package.
- No per-score or per-category events on the output exchange — the whole `Stock` is the message (the event is "a stock was created"; its scores are part of the snapshot).
- No dead-letter queue and no retry queue; a log line is the acknowledgment. Dead-lettering remains an operator policy on the ingestion queue (see [`docs/design/rabbitmq-ingestion.md`](rabbitmq-ingestion.md)).
- No re-emission for *same* `(symbol, exchange)` across refreshes — only on the first INSERT.
- No change to the ingestion contract documented in [`docs/design/rabbitmq-ingestion.md`](rabbitmq-ingestion.md).

## 13. Top Stocks

`TopStocks(TopStocksRequest) returns (StockList)` returns the `limit` stocks with the highest score `value` in a given `category`, optionally scoped to a single `exchange`.

### 13.1 Request shape

```protobuf
message TopStocksRequest { int32 limit = 1; string category = 2; optional string exchange = 3; }
```

When `exchange` is omitted, all exchanges are considered; when set, only that exchange is queried.

### 13.2 Query

```sql
SELECT symbol, exchange FROM scores
WHERE category = $1 [AND exchange = $2]
ORDER BY value DESC, symbol ASC, exchange ASC
LIMIT $n
```

Ordering is deterministic: score `value` descending, then `symbol` ascending, then `exchange` ascending. This is the tiebreaker rule — results are stable across identical requests. Each returned stock has its scores hydrated via the existing per-stock score lookup.

### 13.3 Short-circuits

- `limit <= 0` → empty list (no query).
- empty `category` → empty list (no query).
- unknown (non-empty) category → empty list (the query returns no rows).

None of these cases is an error — the RPC returns an empty `StockList`.

### 13.4 Index

The existing index `idx_scores_category_value (category, value DESC)` covers the primary ordering; no new index is required.
