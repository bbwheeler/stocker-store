# Stocker Store

A simple, self-hostable Go gRPC service for storing stocks and their normalized scores, with optional Kafka ingestion.

**Tech:** Go 1.25 · gRPC/protobuf · PostgreSQL (pgx v5) · Kafka (segmentio/kafka-go) · Podman Quadlets

```
        ┌────────────┐   gRPC :3500   ┌────────────────────────────────┐
        │  client /  │ ─────────────▶ │            stocker-store        │
        │  scoring   │ ◀───────────── │  StockStore service            │
        │  services  │                │  (upsert / remove / retrieve)   │
        └────────────┘                │  retention loop  (STOCK_TTL)     │
                                     │  kafka subscriber (optional)     │
        ┌────────────┐   protobuf   │                                  │
        │  kafka     │ ────────────▶│                                  │
        │  brokers   │   topic      └───────────┬──────────────────────┘
        └────────────┘                          │
                                                ▼
                                        ┌───────────────┐
                                        │  PostgreSQL   │
                                        │ stocks / scores│
                                        └───────────────┘
```

Stocker Store stores thousands of `(symbol, exchange)` stocks plus **dynamic** score categories, each score normalized to `[-1.0, 1.0]`. You can retrieve stocks by symbol, by exchange, or by min/max score ranges; when a request's `limit` is smaller than the number of matching stocks, a random subset is returned. Stocks and scores are upserted, and stale stocks are automatically removed after a configurable retention window (default 30 days). Ingestion happens over gRPC and, optionally, Kafka — the service is fully functional as pure gRPC with only a `DATABASE_URL`.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [gRPC API](#grpc-api)
  - [Methods](#methods)
  - [Messages](#messages)
  - [Example](#example)
- [Kafka Ingestion](#kafka-ingestion)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Database](#database)
- [Development](#development)
- [Troubleshooting & Notes](#troubleshooting--notes)
- [Further Reading](#further-reading)

## Overview

Stocker Store is a single Go binary that keeps a live store of stocks and their scores. Each stock is identified by a `(symbol, exchange)` pair and may carry any number of scores, grouped by category. Categories are dynamic — there is no fixed set you must conform to — and every score must fall within the range `[-1.0, 1.0]`.

The service exposes a gRPC `StockStore` interface (package `stockstore.v1`) for all reads and writes, and runs two background concerns: a retention loop that deletes stale stocks, and an optional Kafka subscriber that ingests `StockUpdate` messages. Nothing in the background work is mandatory: with just `DATABASE_URL` the service runs as a pure gRPC stock and score store.

## Features

- **Bulk and single upserts** — send one stock or a stream of stocks; both upsert the stock and its scores atomically.
- **Dynamic score categories** — scores are keyed by an arbitrary category string, each normalized to `[-1.0, 1.0]`.
- **Flexible retrieval** — filter by symbol, exchange, and per-category min/max score ranges.
- **Random sampling** — when `limit` is less than the total matching count, a random subset is returned (`ORDER BY RANDOM()`).
- **Stale-stock removal** — stocks older than `STOCK_TTL` (default 30 days) are deleted along with their scores; disabled when set to `0`.
- **Optional Kafka ingestion** — consume PROTOBUF-encoded `StockUpdate` messages; a no-op unless both brokers and topic are configured.
- **Zero-migration database** — tables are created automatically via `CREATE TABLE IF NOT EXISTS` on startup.
- **Self-hostable** — run from source, as a container, or via Podman Quadlets.

## gRPC API

The service is `StockStore` in package `stockstore.v1`. Pre-generated Go bindings are committed at `proto/v1` and `proto/v1/kafka`.

### Methods

| Method | Request → Response | Style | Notes |
| --- | --- | --- | --- |
| `AddStocks` | `stream UpdateStockRequest → Stock` | client-streaming | Bulk upsert; returns the last upserted stock. |
| `UpdateStock` | `UpdateStockRequest → Stock` | unary | Upsert one stock. |
| `RemoveStock` | `RemoveStockRequest → RemoveStockResponse` | unary | Remove a stock and its scores. |
| `GetStock` | `GetStockRequest → Stock` | unary | By `symbol`, with an optional `exchange` filter. |
| `GetStocks` | `GetStocksRequest → StockList` | unary | Random set filtered by `exchange` + min/max score ranges + `limit`. |

> **Callout:** `GetStocks.limit` must be a positive value. When `limit` is unset or `0`, `GetStocks` returns an **empty** list. Always set a positive `limit`.

### Messages

| Message | Fields |
| --- | --- |
| `UpdateStockRequest` | `string symbol`, `string exchange`, `map<string,double> scores` |
| `RemoveStockRequest` | `string symbol`, `string exchange` |
| `GetStockRequest` | `string symbol`, `optional string exchange` |
| `GetStocksRequest` | `int32 limit`, `optional string exchange`, `map<string,double> min_scores`, `map<string,double> max_scores` |
| `RemoveStockResponse` | `bool removed` |
| `Stock` | `string symbol`, `string exchange`, `repeated ScoreEntry scores` |
| `ScoreEntry` | `string category`, `double value` |
| `StockList` | `repeated Stock stocks` |

The full service definition (`proto/v1/stock_store.proto`):

```proto
syntax = "proto3";

package stockstore.v1;
option go_package = "stocker-store/proto/v1;stockstorev1";

service StockStore {
	rpc AddStocks(stream UpdateStockRequest) returns (Stock);
	rpc UpdateStock(UpdateStockRequest) returns (Stock);
	rpc RemoveStock(RemoveStockRequest) returns (RemoveStockResponse);
	rpc GetStock(GetStockRequest) returns (Stock);
	rpc GetStocks(GetStocksRequest) returns (StockList);
}

message UpdateStockRequest { string symbol = 1; string exchange = 2; map<string, double> scores = 3; }
message RemoveStockRequest { string symbol = 1; string exchange = 2; }
message GetStockRequest { string symbol = 1; optional string exchange = 2; }
message GetStocksRequest { int32 limit = 1; optional string exchange = 2; map<string, double> min_scores = 3; map<string, double> max_scores = 4; }
message RemoveStockResponse { bool removed = 1; }
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; }
message StockList { repeated Stock stocks = 1; }
```

### Example

The following `grpcurl` call is **illustrative only** — substitute your own `:3500` target and fields:

```bash
grpcurl -plaintext \
  -d '{ "limit": 10, "exchange": "NYSE", "min_scores": { "momentum": 0.5 } }' \
  localhost:3500 stockstore.v1.StockStore/GetStocks
```

## Kafka Ingestion

Stocks may be ingested from a Kafka topic. Messages are **PROTOBUF-encoded** (not JSON), using the shared message definition at `proto/v1/kafka/stock_message.proto`.

### Message schema

Message `StockUpdate` in package `stockerstore.kafka.v1`:

| Field | Type | Required |
| --- | --- | --- |
| `symbol` | `string` | Yes |
| `exchange` | `string` | Yes |
| `scores` | `map<string,double>` | No |

```proto
syntax = "proto3";

package stockerstore.kafka.v1;
option go_package = "stocker-store/proto/v1/kafka;kafkastockv1";

message StockUpdate {
  string symbol            = 1;
  string exchange          = 2;
  map<string, double> scores = 3;
}
```

### Validation

On receipt, each message is validated before it is written:

- `symbol` and `exchange` must be non-empty.
- Every score value must be within `[-1.0, 1.0]`.

If validation fails (or the raw bytes are not valid PROTOBUF), the message is **dropped and logged**, and the consumer continues with the next message.

Kafka ingestion is a **no-op unless both `KAFKA_BROKERS` and `KAFKA_TOPIC` are set**; otherwise the service runs as pure gRPC. See [Configuration](#configuration). See `proto/v1/kafka/README.md` for details on producing these messages (via Go module, git submodule, or copy).

## Configuration

Configuration is entirely via environment variables. The **single most important variable is `DATABASE_URL`** — the service falls back to a default DSN if it is unset (so the process still starts), but you **must set it** to connect to your real database.

### Environment Variables

| Variable | Required | Default | Format | Purpose |
| --- | --- | --- | --- | --- |
| `DATABASE_URL` | Yes (in practice) | `postgres://localhost:5432/stocker?sslmode=disable` | PostgreSQL DSN: `scheme://user:pass@host:port/db?sslmode=...` | The single Postgres connection string. Set this to point at your database. |
| `STOCK_TTL` | No | `720h` (30 days) | Go duration string, e.g. `48h`, `720h`; `0` (or any non-positive value) **disables** expiry | Retention window for stale-stock auto-removal. Cleanup runs once at startup, then hourly. |
| `KAFKA_BROKERS` | No (Kafka only) | — | Comma-separated `host:port` list, e.g. `broker1:9092,broker2:9092` | Kafka brokers to consume from. |
| `KAFKA_TOPIC` | No (Kafka only) | — | Topic name (e.g. `stockers`) | The topic to consume stock events from. |
| `KAFKA_GROUP_ID` | No (Kafka only) | `stocker-store` | Consumer group id (string) | Kafka consumer group, for partition assignment across replicas. |

> **Key rule:** Kafka ingestion is a **no-op unless BOTH `KAFKA_BROKERS` and `KAFKA_TOPIC` are set.** Otherwise the service runs as pure gRPC.
>
> **Minimal deployment requires only `DATABASE_URL`.**

There are **no** separate `DB_USER`, `DB_PASSWORD`, or `DB_HOST` variables — there is only `DATABASE_URL`.

### Sample `.env.podman`

This is the environment file a deployment reads (the Quadlet example loads `~/.config/stocker-store/.env.podman`) — it is **not** shipped in the repo; the operator creates it:

```bash
# Required — the single Postgres connection string.
DATABASE_URL=postgres://user:PASSWORD@localhost:5432/stocker?sslmode=disable

# Optional — stock retention as a Go duration (default 720h / 30 days); "0" disables.
# STOCK_TTL=720h

# Optional — Kafka ingestion (BROKERS and TOPIC must both be set to enable).
# KAFKA_BROKERS=localhost:9092
# KAFKA_TOPIC=stockers
# KAFKA_GROUP_ID=stocker-store
```

### Test-only variable

> **Not a deployment variable.** `DATABASE_URL_FILE` is read **only by the test suite** to load a DSN from a file. Set it only when running tests; it has no effect on running the service.

## Deployment

### Prerequisites

- Go 1.25+ (for building/running from source).
- A reachable PostgreSQL instance.
- (Optional) Kafka brokers and a topic, if you want Kafka ingestion.
- (Optional) Podman, for containerized runs or Quadlets.

### Option A — Run locally (Go)

```bash
make build    # -> go build -o bin/stocker-store ./cmd/main.go
make run      # build, then run (must point at a reachable DATABASE_URL)
make test     # go test ./...
make clean    # remove bin/
```

Ensure the process can reach a database — the running binary reads `DATABASE_URL` at startup.

### Option B — Run as a container

```bash
podman build -t stocker-store .
```

The `Containerfile` is multi-stage:

- **build** — `golang:1.25-bookworm`; installs `protoc` and the `protoc-gen-go` / `protoc-gen-go-grpc` plugins, compiles the proto, then `CGO_ENABLED=0 go build`.
- **runtime** — `gcr.io/distroless/static-debian12`, with `ENTRYPOINT ["/stocker-store"]`.

Pre-generated proto code is already in-tree, so the build has no external codegen dependency at run time.

> **Note:** the `Containerfile` contains `EXPOSE 50051`, which is **stale, misleading metadata**. The actual gRPC server binds **`:3500`** (see `cmd/main.go`). Connect on **port 3500**.

Run with the required environment variables:

```bash
podman run --rm \
  --network=host \
  -e DATABASE_URL="postgres://user:PASSWORD@localhost:5432/stocker?sslmode=disable" \
  stocker-store
```

### Option C — Podman Quadlets (self-host)

Quadlet units live in `deploy/quadlet/`:

- `stocker-store.build` — `File=%h/stocker-store/Containerfile`, `ImageTag=stocker-store:latest`.
- `stocker-store.container` — `Image=containers.wheeli.ca/stocker-store:latest`, `Network=host`, `EnvironmentFile=%h/.config/stocker-store/.env.podman`, `Restart=always`, `RestartSec=5`.

Because the container type uses `Network=host`, the gRPC service is reachable on the host's **`:3500`**.

> **The operator must create** `~/.config/stocker-store/.env.podman` — it is **not** shipped in the repo. Populate it with `DATABASE_URL` and the optional `STOCK_TTL` / `KAFKA_*` values (see the [sample `.env.podman`](#sample-envpodman)).

To use the Quadlets, place the unit files (for example in `/etc/systemd/user` or per-user `~/.config/containers/systemd`), create the env file, then reload and enable the service.

### Quickstart (Quadlets, end-to-end)

1. **Write the env file:** create `~/.config/stocker-store/.env.podman` — at minimum `DATABASE_URL` (see the [sample](#sample-envpodman)).
2. **Build the image:** `podman build -t stocker-store .`
3. **Enable & start:** `systemctl enable --now stocker-store`
4. **Verify:** `podman logs stocker-store` and/or `systemctl status stocker-store`

### Push to registry

`deploy/push.sh` builds and pushes the image:

```bash
./deploy/push.sh   # -> podman push containers.wheeli.ca/stocker-list:latest
```

> **Known inconsistency:** the pushed image name `stocker-list:latest` does not match the Quadlet's `stocker-store:latest`. Neither is asserted here as the canonical name.

## Database

Stocker Store uses PostgreSQL. Tables are created automatically at startup with `CREATE TABLE IF NOT EXISTS` — there are **no manual migrations**.

**`stocks`:**

```
symbol    TEXT        NOT NULL,
exchange  TEXT        NOT NULL,
timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
PRIMARY KEY (symbol, exchange)
```

**`scores`:**

```
symbol   TEXT             NOT NULL,
exchange TEXT             NOT NULL,
category TEXT             NOT NULL,
value    DOUBLE PRECISION CHECK(value BETWEEN -1.0 AND 1.0) DEFAULT 0.0,
timestamp TIMESTAMPTZ      NOT NULL DEFAULT now(),
UNIQUE (symbol, exchange, category),
FOREIGN KEY (symbol, exchange) REFERENCES stocks(symbol, exchange)
```

An index `idx_scores_category_value` is created on `scores(category, value DESC)`.

> **Note:** `GetStocks` uses `ORDER BY RANDOM() LIMIT n` to select the random subset.

## Development

### Build & test

```bash
make build    # go build -o bin/stocker-store ./cmd/main.go
make test     # go test ./...
go vet ./...
make run      # build, then run
make clean    # remove bin/
```

Pre-generated proto bindings are committed under `proto/v1` and `proto/v1/kafka`, so you do not need codegen tooling to build.

### Repository layout

```text
cmd/main.go                    # entrypoint: env config, gRPC server, retention loop, kafka subscriber
internal/grpc/                 # gRPC server (StockStore handlers) + tests
internal/kafka/                # Kafka consumer (StockUpdate ingestion) + tests
internal/store/                # PostgreSQL (pgx pool), models, table init, + tests
proto/v1/                      # StockStore service + messages (stock_store.proto) and generated Go
proto/v1/kafka/                # StockUpdate message (stock_message.proto), consumer README.md, generated Go
deploy/quadlet/                # Quadlet unit files (stocker-store.build / stocker-store.container)
deploy/push.sh                 # build + push image to registry
deploy/install.sh              # legacy install script
Containerfile                  # multi-stage build (note the stale EXPOSE 50051)
Makefile                       # build / run / test / clean
design.md                      # historical design doc (partially stale)
AGENTS.md                      # agent-oriented notes
```

## Troubleshooting & Notes

- **Minimal configuration:** only `DATABASE_URL` is required. `STOCK_TTL` and the `KAFKA_*` variables are optional.
- **Kafka is optional:** ingestion is a no-op unless **both** `KAFKA_BROKERS` and `KAFKA_TOPIC` are set.
- **The real port is 3500, not 50051:** the gRPC server listens on `:3500`. The `EXPOSE 50051` line in the `Containerfile` is stale metadata — connect on **`:3500`**.
- **`GetStocks` requires `limit > 0`:** an unset or `0` `limit` returns an empty list.
- **Authoritative deployment variables** are the ones in the [Configuration](#configuration) section: `DATABASE_URL`, `STOCK_TTL`, and the `KAFKA_*` variables.

### Known inconsistencies to fix

These are known issues in the deployment scripts and metadata; none of them is asserted as the canonical source of truth:

- **Port:** `EXPOSE 50051` in the `Containerfile` vs. the real `:3500` binding in `cmd/main.go`.
- **Image name:** `deploy/push.sh` pushes `stocker-list:latest` vs. the Quadlet's `stocker-store:latest`.
- **`deploy/install.sh`:** references legacy variables `DB_USER` / `DB_PASSWORD`, the legacy service name `tsx-tracker`, and some missing/renamed files. It is legacy and should not be relied upon.

## Further Reading

- `design.md` — original design doc (partially stale).
- `AGENTS.md` — agent-oriented project notes.
- `proto/v1/kafka/README.md` — how to produce/consume the Kafka `StockUpdate` message.
- Source: `cmd/main.go`, `internal/{grpc,kafka,store}`, `proto/v1*`.
