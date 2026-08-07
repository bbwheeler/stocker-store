# Stocker Store Service — Design Document

## 1. Overview

**Stocker Store** is a gRPC-backed service for managing stock data and multi-dimensional scores. It supports thousands of stocks with historical score tracking, weighted scoring queries, and flexible retrieval patterns.

```
┌───────────┐       Protocol Buffer            ┌──────────────┐    ┌────────────┐
│  Clients  │ ◄══════► gRPC (bidirectional)  ► │ stocker-store│◄──►│ PostgreSQL │
│ (gRPC     │       & kafka streaming          │              │    │            │
│  / REST   │       (future)                   └──────────────┘    └────────────┘
│   proxy ) │                                      golang                    │
└───────────┘                                                     schema history
                                                                              tables
```

## 2. Requirements Summary

- Retrieve stocks by: symbol, exchange, random (with filters), score range, top-score, weighted-score.
- Submit stocks.
- Remove stocks from an exchange.
- Submit/calculate scores (dynamic categories, normalized -1.0 to 1.0).
- Historical data with date-bound queries.
- Thousands of stocks.
- Self-hostable, simple operations, small footprint (no Kubernetes required for v1).

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
└─────────────────────────────────────┘     │   history[...])                       │
                                            └───────────────────────────────────────┘
```
The stocks table has a composite key composed of the symbol + the exchange. Other columns include the symbol, the exchange, and the timestamp

The scores table has a composite key composed of the id from the stocks table + the category. Other columns include the symbol, exchange, category, value, timestamp, and a history of older rows

### 3.2 Schema (PostgreSQL)

#### `stocks` — master list of symbols

| Column      | Type        | Notes                 |
|-------------|-------------|-----------------------|
| `id`        | composite   | symbol + exchange. PK |
| `symbol`    | TEXT        | NOT NULL              |
| `exchange`  | TEXT        | NOT NULL              |
| `timestamp` | TIMESTAMPTZ | DEFAULT now()         |


Indexes: `UNIQUE on (symbol, exchange)`.

#### `scores` — score snapshots

| Column           | Type         | Notes                                          |
|------------------|--------------|------------------------------------------------|
| `id`             | composite    | PK, stock_id + category                        |
| `stock_id`       | TEXT         | FK → stocks.id                                 |
| `category`       | TEXT         | NOT NULL.                                      |
| `value`          | DOUBLE PREC. | CHECK: value BETWEEN -1.0 AND 1.0, DEFAULT 0.0 |
| `timestamp`      | TIMESTAMPTZ  | NOT NULL                                       |

Indexes: unique constraint on `(stock_id, category)` for current score. composite index on `(category, value DESC)`

---

### 3.3 Historical data approach

1. The current active row gets its `value` and `timestamp` updated (in-place).
2. A new row is written with the previous values for history.
3. This gives O(1) lookups for "latest score".

Queries like "score of symbol X on date Y" iterate through the history table to get the score.

---

## 4. gRPC API Definition (protobuf service)

### Service: `StockStore`

| RPC Name           | Request                       | Response                  | Notes                        |
|--------------------|-------------------------------|---------------------------|------------------------------|
| AddStock           | `AddStockRequest`             | `Stock`                   | Creates symbol + exchange    |
| RemoveStock        | `RemoveStockRequest`          | `RemoveStockResponse`     | Delete from exchange         |
| GetStockBySymbol   | `GetStockBySymbolRequest`     | `Stock`                   | All exchanges or filtered    |
| GetStocksByExchange| `GetStocksByExchange`         | `List[Stock]`             |                              |
| GetRandomStocks    | `GetRandomStocksRequest`      | `List[Stock]`             | Needs reasonable upper limit |
| UpdateScore        | `UpdateScoreRequest`          | `ScoreSnapshot`           | Creates/updates score + hist.|
| GetScores          | `GetScoresRequest`            | `ScoreHistory`            | Latest + optional timebound  |
| GetTopStocks       | `GetTopStocksRequest`         | `List[StockWithScore]`    | Reasonable upper limit       |

### Messages (key fields)

```protobuf
message AddStockRequest { string symbol = 1; string exchange = 2; }
message RemoveStockRequest { string symbol = 1; string exchange = 2; }
message GetStockBySymbolRequest { string symbol = 1; optional string exchange = 2; } // nullable

message UpdateScoreRequest { string symbol = 1; string exchange = 2; string category = 3; double value = 4; }
message GetScoresRequest { string symbol = 1; string exchange = 2; string category = 3; optional google.protobuf.Timestamp date = 4; }

message GetRandomStocksRequest { string exchange = 1; int32 count = 2; map<string, double> score_filters = 3; }
// score_filters: "category_y => min_value" — filters to stocks with score > min in that category

message GetTopStocksRequest { string category = 1; int32 limit = 2; string exchange = 3; } // exchange optional

message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; }
message ScoreSnapshot { string category = 1; double value = 2; repeated ScoreHistoryEntry history = 3; }
```

---

## 5. Retrieval patterns and implementation approach

### 5.1 By symbol

Simple lookup: `stocks.symbol + stocks.exchange` → scores in single query.

### 5.2 By exchange

SELECT * FROM stocks WHERE exchange = $1 ORDER BY stocks.symbol

### 5.3 Random stocks

On a database this size: `ORDER BY random() LIMIT count`.

### 5.4 By score range

`WHERE score.value BETWEEN min_val AND max_val AND score.category = $1`.

### 5.5 Top-score

`ORDER BY score.value DESC LIMIT x`. Simple index-sorted scan on `(category, value DESC)`.

---

## 6. Technology Decisions

### 6.1 Programming Language

| Option      | Verdict     | Why                              |
|-------------|-------------|----------------------------------|
| **Go**      | **SELECTED**| Strong gRPC ecosystem (gRPC-Go), compiled, single binary deployment, small memory footprint (~10 MB per server), fast at scale, excellent concurrency model. |
| Python      | Rejected    | Slower at scale, gRPC support works but runtime overhead is higher. Async gRPC is immature. Good for prototyping (v1 of scoring engine maybe) but not ideal long-term. |
| Java        | Rejected    | Heavy heap (~300 MB minimum), slow startup, overkill for this workload complexity. |
| Rust        | Considered  | Fantastic performance but no first-class gRPC support that's as mature as Go/Java. tonic-rs exists but ecosystem is less battle-tested. |

### 6.2 Database

| Option          | Verdict      | Why                              |
|-----------------|--------------|----------------------------------|
| **PostgreSQL**  | **SELECTED** | Best fit for both OLTP (company/exchange CRUD) and time-series (score history). Self-hostable via Docker Compose. Strong golang driver (`pgx`). |
| Cassandra       | Rejected     | Good for time-series writes at scale but poor on joins needed for "company + exchange + scores" lookups. CQL lacks rich query flexibility for scoring filters. |
| MongoDB         | Rejected     | Document model maps reasonably well but: (1) no ACID transactions across collections in shared hosting scenarios, (2) weaker indexing for time-series queries vs Timescale, (3) historically higher resource footprint than PostgreSQL. |
| CouchDB         | Rejected     | Replication is nice but query flexibility is limited (MapReduce views). Not suited for numeric range filters on scores. No continuous aggregation concept. |
| MariaDB         | Rejected     | Same as Postgres minus the time-series extensions and materialized view capabilities. Would need manual triggers/policies for history retention. |
| SQLite          | Considered   | Zero config, embedded. Works fine at thousands of stocks. **But** no native time-series extensions, limited concurrent write throughput, no self-hosting story if we want separate deployment from the API. |
| Flat files      | Rejected     | No ACID, no querying, not production-worthy. |

### 6.3 Caching Layer

| Option                        | Verdict      | Why                              |
|-------------------------------|--------------|----------------------------------|
| None (cache-miss to Postgres) | **SELECTED initially** | At thousands of stocks, Postgres can handle the read load with proper indexing. Redis adds operational complexity not yet justified by performance needs. Add later if needed. |
| Redis           | Deferred    | Excellent for hot score lookups and random stock pre-computation. Adds one more infrastructure dependency. Consider when QPS > 1000 sustained. |

### 6.4 Additional Infrastructure

| Component        | Choice                | Notes                            |
|------------------|-----------------------|----------------------------------|
| Config/Discovery | file-based config     | Fine for v1. |
| Schema migration | None | We can afford to lose historic data, just Drop and recreate the table |
| Container orchestration | Podman Quadlets (v1), Kubernetes later | Keep v1 minimal. |
| Observability | OpenTelemetry, Prometheus metrics, structured logging | Standard Go ecosystem. |

---

## 7. Architecture and Deployment

### 7.1 Component diagram

```
                    ┌──────────────┐
    gRPC clients ──►│              │
     / HTTP proxy   │ stocker-store│ PostgreSQL
                    │   (Go)       │◄── Podman
                    └──────────────┘
```

Single `stocker-store` binary, no microservice decomposition at this scale. Postgres runs separately or in its own container. No service mesh or sidecars needed — keep it dead simple.

### 7.2 Normalization and validation

All score values get validated client-side via protobuf `double` with a server-side `CHECK(value BETWEEN -1.0 AND 1.0)` constraint. Default is `0.0`.

### 7.3 Error handling gRPC errors to return

| Scenario                            | gRPC code       |
|-------------------------------------|---------------------|
| Symbol already exists               | ALREADY_EXISTS      |
| Stock not found                     | NOT_FOUND           |
| Invalid score value                 | INVALID_ARGUMENT    |
| Database failure                    | INTERNAL + retry    |
| Conflict (e.g. concurrent writes)   | FAILED_PRECONDITION |

---

## 8. Data Retention and History

- Score history should be pruned beyond a configurable time (eg 1 year)
- Hard deletes: When a stock is "removed," the `stock_exchanges` row gets deleted.

---

## 9. Scalability Considerations

### 9.1 At thousands of stocks (v1 target)

All query patterns fit comfortably in PostgreSQL with proper indexing. `pgx` provides excellent connection management. Memory footprint is ~20 MB per server process. No sharding needed.

---

## 10. Operations

### v1 Deployment (Podman Quadlet)

TODO

### Schema migrations

None / Not Necessary

---

## 11. Future Extensions

- REST-gateway over gRPC for non-gRPC clients
- Authentication via mTLS or JWT tokens on the server side
- Batch operations (gRPC streaming AddStock in bulk)
- Pagination to avoid response limits
- Category weight presets / templates
