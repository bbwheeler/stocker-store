# Stocker Store Service — Design Document

## 1. Overview

**Stocker Store** is a gRPC-backed service for managing stock data and multi-dimensional scores. It supports millions of stocks with historical score tracking, weighted scoring queries, and flexible retrieval patterns.

```
┌───────────┐       Protocol Buffer       ┌──────────────┐       ┌────────────┐
│  Clients   │ ◄══════► gRPC (bidirectional)  ► │ stocker-store │◄──►│ PostgreSQL │
│ (gRPC      │       & streaming             │              │       │ + Timescale│
│  / REST    │       (future)                └──────────────┘       └────────────┘
│   proxy? ) │                              golang, GoCDR                    │
└────────────┘                              etcd                       schema history
                                                                              tables
```

## 2. Requirements Summary

- Retrieve stocks by: symbol, exchange, random (with filters), score range, top-score, weighted-score.
- Submit stocks.
- Remove stocks from an exchange.
- Submit/calculate scores (dynamic categories, normalized -1.0 to 1.0).
- Historical data with date-bound queries.
- Thousands of stocks, millions of stock-exchange combinations expected at scale.
- Self-hostable, simple operations, small footprint (no Kubernetes required for v1).

---

## 3. Data Model

### 3.1 Entities and Relationships

```
┌───────────────┐     ┌──────────────────┐     ┌──────────────────┐
│    companies    │<────│ stock_exchanges   │────│     scores       │
│  (id, symbol)   │     │ (company_id,    │     │ (stock_exchange_│
└───────────────┘      │   exchange,      │     │   id, category,  │
                       │   submitted_at)   │     │   value,         │
                       └──────────────────┘     │   history[...])   │
                                                └──────────────────┘
```

### 3.2 Schema (PostgreSQL + Timescale Extension)

#### `companies` — master list of symbols

| Column      | Type     | Notes                   |
|-------------|----------|-------------------------|
| `id`        | BIGSERIAL| PK, auto-incrementing   |
| `symbol`    | TEXT     | NOT NULL UNIQUE         |
| `created_at`| TIMESTAMPTZ | DEFAULT now()        |

Indexes: unique on `(symbol)`.

#### `stock_exchanges` — each submitted stock (one row per symbol+exchange)

| Column           | Type      | Notes                   |
|------------------|-----------|-------------------------|
| `id`             | BIGSERIAL | PK                      |
| `company_id`     | BIGINT    | FK → companies.id       |
| `exchange`       | TEXT      | e.g. "NYSE", "NASDAQ"   |
| `submitted_at`   | TIMESTAMPTZ | DEFAULT now()         |

Indexes: unique on `(company_id, exchange)`. Composite index on `(exchange)`.

#### `scores` — score snapshots (Timescale hypertable for time-series)

| Column           | Type      | Notes                   |
|------------------|-----------|-------------------------|
| `id`             | BIGSERIAL | PK                      |
| `stock_exchange_id` | BIGINT | FK → stock_exchanges.id |
| `category`       | TEXT      | NOT NULL                |
| `value`          | DOUBLE PREC. | CHECK: value BETWEEN -1.0 AND 1.0, DEFAULT 0.0 |
| `timestamp`      | TIMESTAMPTZ | NOT NULL (hypertable time column) |

Indexes: unique constraint on `(stock_exchange_id, category)` for current score. Timescale partial index on un-aggregated raw scores for historical queries. A trigger ensures only one active per company+category, keeping older rows in the hypertable.

#### `score_categories` — dynamic score categories (lookups)

| Column   | Type    | Notes              |
|----------|---------|--------------------|
| `id`     | BIGSERIAL| PK                |
| `name`   | TEXT    | NOT NULL UNIQUE    |
| `created_at`| TIMESTAMPTZ| DEFAULT now()  |

---

### 3.3 Historical data approach

Scores use a **hypertable** in TimescaleDB to store every version of every score. On score updates:

1. The current active row gets its `value` and `timestamp` updated (in-place).
2. A new row is written with the previous values for history.
3. This gives O(1) lookups for "latest score" with complete audit trail.

Queries like "score of symbol X on date Y" use Timescale's time-bounded queries:

```sql
SELECT value FROM scores WHERE stock_exchange_id = $1 AND category = $2
  AND timestamp <= $3 ORDER BY timestamp DESC LIMIT 1;
```

---

## 4. gRPC API Definition (protobuf service)

### Service: `StockStore`

| RPC Name           | Request                       | Response                  | Notes                        |
|--------------------|-------------------------------|---------------------------|------------------------------|
| AddStock           | `AddStockRequest`             | `Stock`                   | Creates company + exchange   |
| RemoveStock        | `RemoveStockRequest`          | `RemoveStockResponse`     | Soft delete from exchange    |
| GetStockBySymbol   | `GetStockBySymbolRequest`     | `Stock`                   | All exchanges or filtered    |
| GetStocksByExchange| `GetStocksByExchangeRequest`  | `stream Stock`            | Server-side streaming        |
| GetRandomStocks    | `GetRandomStocksRequest`      | `stream Stock`            | Server-side streaming        |
| UpdateScore        | `UpdateScoreRequest`          | `ScoreSnapshot`           | Creates/updates score + hist.|
| GetScores          | `GetScoresRequest`            | `ScoreHistory`            | Latest + optional timebound  |
| GetTopStocks       | `GetTopStocksRequest`         | `stream StockWithScore`   | Server-side streaming        |
| GetWeightedScores  | `GetWeightedScoresRequest`    | `stream WeightedResult`   | Aggregates scores at query time|

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

message GetWeightedScoresRequest { map<string, double> categories = 1; // "category => weight"
    int32 limit = 2; string exchange = 3; bool ascending = 4; }

message Stock { string symbol = 1; string exchange = 2; double score = 3; repeated ScoreEntry scores = 4; }
message ScoreEntry { string category = 1; double value = 2; }
message ScoreSnapshot { string category = 1; double value = 2; repeated ScoreHistoryEntry history = 3; }
message WeightedResult { Stock stock; double weighted_score = 2; }
```

---

## 5. Retrieval patterns and implementation approach

### 5.1 By symbol

Simple lookup: `companies.symbol` → `stock_exchanges` → scores in single query.

### 5.2 By exchange

SELECT * FROM stock_exchanges WHERE exchange = $1 JOIN companies ... ORDER BY companies.symbol LIMIT offset? No, pagination with cursor (`id > last_id`).

### 5.3 Random stocks

On a database this size: `ORDER BY random() LIMIT count`. For larger datasets: pre-compute an array of primary keys and pick from it (pg_advisory_lock for concurrency), or use a sampling index. At thousands of rows, `random()` is fine.

### 5.4 By score range

`WHERE score.value BETWEEN min_val AND max_val AND score.category = $1`. With Timescale's continuous aggregates for common categories, this hits pre-computed views at query time for speed.

### 5.5 Top-score

`ORDER BY score.value DESC LIMIT x`. Simple index-sorted scan on `(category, value DESC)`.

### 5.6 Weighted scores (dynamic aggregations)

This is the most compute-intensive pattern. Strategy:

- **On-the-fly calculation**: For each query, fetch all categories for stocks that have at least one matching score, multiply by weights, and sort. This requires an index lookup → N score fetches → in-memory sort.
  - Pro: simple, no stale data, O(1) writes
  - Con: O(n) reads per query; expensive on large datasets

- **Continuous aggregate (Timescale)**: Pre-compute rolling windows of scores for heavy categories at defined intervals. Queryable via materialized views.
  - Pro: very fast reads
  - Con: staleness between refreshes, requires managing refresh policies

**Decision**: Start with on-the-fly calculation (kitchensink query approach). It is the simplest and correct by construction. Add continuous aggregates later if query latency exceeds SLOs. The weighted score pattern doesn't need to be a write-critical path.

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
| **PostgreSQL + Timescale** | **SELECTED**| Best fit for both OLTP (company/exchange CRUD) and time-series (score history). Self-hostable via Docker Compose. Strong golang driver (`pgx`). Timescale adds hypertables and continuous aggregates with zero schema migration complexity. |
| Cassandra       | Rejected   | Good for time-series writes at scale but poor on joins needed for "company + exchange + scores" lookups. CQL lacks rich query flexibility for scoring filters. |
| MongoDB         | Rejected   | Document model maps reasonably well but: (1) no ACID transactions across collections in shared hosting scenarios, (2) weaker indexing for time-series queries vs Timescale, (3) historically higher resource footprint than PostgreSQL. |
| CouchDB         | Rejected   | Replication is nice but query flexibility is limited (MapReduce views). Not suited for numeric range filters on scores. No continuous aggregation concept. |
| MariaDB         | Rejected   | Same as Postgres minus the time-series extensions and materialized view capabilities. Would need manual triggers/policies for history retention. |
| SQLite          | Considered  | Zero config, embedded. Works fine at thousands of stocks. **But** no native time-series extensions, limited concurrent write throughput, no self-hosting story if we want separate deployment from the API. |
| Flat files      | Rejected   | No ACID, no querying, not production-worthy. |

### 6.3 Caching Layer

| Option          | Verdict      | Why                              |
|-----------------|--------------|----------------------------------|
| None (cache-miss to Postgres) | **SELECTED initially** | At thousands of stocks, Postgres can handle the read load with proper indexing. Redis adds operational complexity not yet justified by performance needs. Add later if needed. |
| Redis           | Deferred     | Excellent for hot score lookups and random stock pre-computation. Adds one more infrastructure dependency. Consider when QPS > 1000 sustained. |

### 6.4 Additional Infrastructure

| Component       | Choice      | Notes                            |
|-----------------|-------------|----------------------------------|
| Config/Discovery | etcd or file-based config | Both fine for v1. |
| Schema migration | Goose or sqlx/migrate | Both well-supported with Go. |
| Container orchestration | Docker Compose (v1), Kubernetes later | Keep v1 minimal. |
| Observability | OpenTelemetry, Prometheus metrics, structured logging | Standard Go ecosystem. |

---

## 7. Architecture and Deployment

### 7.1 Component diagram

```
                    ┌──────────────┐
    gRPC clients ──►│              │
     / HTTP proxy   │ stocker-store│ PostgreSQL + Timescale
                      │   (Go)       │◄── Docker Compose
                     └──────────────┘
```

Single `stocker-store` binary, no microservice decomposition at this scale. Postgres/Timescale runs in its own container. No service mesh or sidecars needed — keep it dead simple.

### 7.2 Indexing strategy

| Table            | Index                          | Purpose                            |
|------------------|--------------------------------|------------------------------------|
| `companies`      | UNIQUE on `symbol`             | Fast symbol lookup                 |
| `stock_exchanges`| UNIQUE on `(company_id, exchange)` | Ensure one row per stock+exchange |
| `stock_exchanges`| B-tree on `exchange`           | Exchange-based queries             |
| `scores`         | UNIQUE on `(stock_exchange_id, category)` | Current score enforcement    |
| `scores`         | B-tree on `(category, value DESC)` | Top-score / range queries (hypertable time column) |
| scores hypertable | Compression via Timescale policies | Keep storage lean over history    |

### 7.3 Normalization and validation

All score values get validated client-side via protobuf `double` with a server-side `CHECK(value BETWEEN -1.0 AND 1.0)` constraint. Default is `0.0`. A scoring category gets created on demand (INSERT … ON CONFLICT DO NOTHING into `score_categories`).

### 7.4 Error handling gRPC errors to return

| Scenario                            | gRPC code       |
|-------------------------------------|--------------------|
| Symbol already exists               | ALREADY_EXISTS     |
| Stock not found                     | NOT_FOUND          |
| Invalid score value                 | INVALID_ARGUMENT   |
| Database failure                    | INTERNAL + retry  |
| Conflict (e.g. concurrent writes)   | FAILED_PRECONDITION |

---

## 8. Data Retention and History

- Score history is unlimited by default. Use Timescale's [hypertable retention policies](https://docs.timescale.com/latest/using-timescaledb/data-retention/) for automatic pruning after N years.
- Soft deletes: When a stock is "removed," the `stock_exchanges` row gets archived (flagged or moved to an archive table). Existing score history remains intact for historical queries.

---

## 9. Scalability Considerations

### 9.1 At thousands of stocks (v1 target)

All query patterns fit comfortably in PostgreSQL with proper indexing. `pgx` provides excellent connection management. Memory footprint is ~20 MB per server process. No sharding needed.

### 9.2 Expansion paths (if scale > hundreds of thousands to millions)

| Path                       | Description                              | Effort  |
|----------------------------|------------------------------------------|---------|
| **Connection pool**        | pgx pool size tuned to Postgres work_mem | None    |
| **Read replicas**          | Timescale supports this natively          | Medium  |  
| **Partitioning by exchange**| Auto-partition on `exchange` if one gets disproportionately large | Low |
| **Caching (Redis)**        | Cache top-score and weighted-score queries | Low |
| **Background pre-computation** | Cron or Timescale continuous aggregates for weighted scores           | Medium  |

Sharding is not needed until we hit >1M stock-exchange rows, which would be ~1 GB on disk — fine for a single PostgreSQL instance.

---

## 10. Operations

### v1 Deployment (Docker Compose)

```yaml
services:
  stocker-store:
    build: .
    ports: ["50051:50051"]        # gRPC port
    depends_on: [postgres]
    environment:
      - DB_URL=postgresql://stocker:password@postgres/postgres

  postgres:
    image: timescale/timescaledb:latest-pg16
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]
```

### Schema migrations

Use `sqlx` or `goose`. Migrations live in a `migrations/` directory alongside the code. Applied on startup (first run only). No hot migration support needed yet.

---

## 11. Future Extensions

- REST-gateway over gRPC for non-gRPC clients
- Authentication via mTLS or JWT tokens on the server side
- Batch operations (gRPC streaming AddStock in bulk)
- Real-time score update push via gRPC bidirectional streams
- Category weight presets / templates
- Export API for downstream analytics
