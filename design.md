# Stocker Store Service — Design Document

## 1. Overview

**Stocker Store** is a gRPC-backed service for managing stock data and scores. It supports thousands of stocks and flexible retrieval patterns.

```
┌───────────┐       Protocol Buffer            ┌──────────────┐    ┌────────────┐
│  Clients  │ ◄══════► gRPC (bidirectional)  ► │ stocker-store│◄──►│ PostgreSQL │
│ (gRPC     │       & kafka streaming          │              │    │            │
│  / REST   │       (future)                   └──────────────┘    └────────────┘
│   proxy ) │                                      golang                    │
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
message ScoreEntry { string category = 1; double value = 2; }
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
     / HTTP proxy   │ stocker-store│ PostgreSQL
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

- Pagination to avoid response limits
