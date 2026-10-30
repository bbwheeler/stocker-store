# Stocker Store

A simple, self-hostable Go gRPC service for storing stocks and their normalized scores, with optional Kafka ingestion.

**Tech:** Go 1.25 · gRPC/protobuf · PostgreSQL (pgx v5) · Kafka (segmentio/kafka-go) · Podman Quadlets

```
┌──────────┐   gRPC :3500 (GRPC_PORT) ┌──────────────────────────────┐
│ client / │ ────▶                    │ stocker-store                │
│ scoring  │ ◀────                    │ StockStore service           │
│ services │                          │ upsert / remove / retrieve   │
└──────────┘                          │ retention loop (STOCK_TTL)   │
                                      │ kafka subscriber (optional)  │
┌──────────┐   protobuf               │                              │
│ kafka    │ ────▶                    │                              │
│ brokers  │   topic                  └──────────────────────────────┘
└──────────┘                                            ▼
                                               ┌──────────────────┐
                                               │ PostgreSQL       │
                                               │ stocks / scores  │
                                               └──────────────────┘
```

Stocker Store stores thousands of `(symbol, exchange)` stocks plus **dynamic** score categories, each score normalized to `[-1.0, 1.0]` and carrying a per-score last-updated timestamp (set by the server on write). You can retrieve stocks by symbol, by exchange, or by min/max score ranges; when a request's `limit` is smaller than the number of matching stocks, a random subset is returned. Stocks and scores are upserted, and stale stocks are automatically removed after a configurable retention window (default 30 days). Ingestion happens over gRPC and, optionally, Kafka — the service is fully functional as pure gRPC with only a `DATABASE_URL`.

## Table of Contents

- [Stocker Store](#stocker-store)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Features](#features)
  - [gRPC API](#grpc-api)
    - [Methods](#methods)
    - [Messages](#messages)
    - [Example](#example)
  - [Kafka Ingestion](#kafka-ingestion)
    - [Message schema](#message-schema)
    - [Validation and delivery semantics](#validation-and-delivery-semantics)
    - [New-Stock Output](#new-stock-output)
  - [Configuration](#configuration)
    - [Environment Variables](#environment-variables)
    - [Sample `.env.podman`](#sample-envpodman)
    - [Test-only variable](#test-only-variable)
  - [Deployment](#deployment)
    - [Prerequisites](#prerequisites)
    - [Option A — Run locally (Go)](#option-a--run-locally-go)
    - [Option B — Run as a container](#option-b--run-as-a-container)
    - [Option C — Podman Quadlets (self-host)](#option-c--podman-quadlets-self-host)
    - [Quickstart (Quadlets, end-to-end)](#quickstart-quadlets-end-to-end)
    - [Push to registry](#push-to-registry)
  - [Database](#database)
  - [Development](#development)
    - [Build \& test](#build--test)
    - [Repository layout](#repository-layout)
  - [Troubleshooting \& Notes](#troubleshooting--notes)
    - [Verifying Kafka ingestion](#verifying-kafka-ingestion)
    - [Known inconsistencies to fix](#known-inconsistencies-to-fix)
  - [Further Reading](#further-reading)

## Overview

Stocker Store is a single Go binary that keeps a live store of stocks and their scores. Each stock is identified by a `(symbol, exchange)` pair and may carry any number of scores, grouped by category. Categories are dynamic — there is no fixed set you must conform to — and every score must fall within the range `[-1.0, 1.0]`.

The service exposes a gRPC `StockStore` interface (package `stockstore.v1`) for all reads and writes, and runs two background concerns: a retention loop that deletes stale stocks, and an optional Kafka subscriber that ingests PROTOBUF-encoded `Stock` messages. Nothing in the background work is mandatory: with just `DATABASE_URL` the service runs as a pure gRPC stock and score store.

## Features

- **Bulk and single upserts** — send one stock or a stream of stocks; both upsert the stock and its scores atomically. Upserts **merge** scores: only the categories present in the request are written, and categories not mentioned are left unchanged, including their last-updated timestamps.
- **Dynamic score categories** — scores are keyed by an arbitrary category string, each normalized to `[-1.0, 1.0]`, each with a last-updated timestamp set by the server on write.
- **Flexible retrieval** — filter by symbol, exchange, and per-category min/max score ranges.
- **Random sampling** — when `limit` is less than the total matching count, a random subset is returned (`ORDER BY RANDOM()`).
- **Top stocks** — fetch the N stocks with the highest score in a category, optionally scoped to an exchange, with deterministic ordering (value DESC, then symbol, then exchange).
- **Stale-stock removal** — stocks older than `STOCK_TTL` (default 30 days) are deleted along with their scores; disabled when set to `0`.
- **Optional Kafka ingestion** — consume PROTOBUF-encoded `Stock` messages (`stockstore.v1.Stock`); offsets are committed only after a successful write, transient failures are retried with capped backoff, invalid messages are skipped and logged, and the consumer reconnects automatically; a no-op unless both brokers and topic are configured.
- **Zero-migration database** — tables are created automatically via `CREATE TABLE IF NOT EXISTS` on startup.
- **Self-hostable** — run from source, as a container, or via Podman Quadlets.

## gRPC API

The service is `StockStore` in package `stockstore.v1`. Pre-generated Go bindings are committed at `proto/v1`.

### Methods

| Method | Request → Response | Style | Notes |
| --- | --- | --- | --- |
| `AddStocks` | `stream UpdateStockRequest → Stock` | client-streaming | Bulk upsert; returns the last upserted stock. |
| `UpdateStock` | `UpdateStockRequest → Stock` | unary | Upsert one stock. |
| `RemoveStock` | `RemoveStockRequest → RemoveStockResponse` | unary | Remove a stock and its scores. |
| `GetStock` | `GetStockRequest → Stock` | unary | By `symbol`, with an optional `exchange` filter. |
| `GetStocks` | `GetStocksRequest → StockList` | unary | Random set filtered by `exchange` + min/max score ranges + `limit`. |
| `TopStocks` | `TopStocksRequest → StockList` | unary | Top N stocks by score in a category; optional exchange filter. |

> **Callout:** `GetStocks.limit` must be a positive value. When `limit` is unset or `0`, `GetStocks` returns an **empty** list. Always set a positive `limit`.

#### Partial score updates

`AddStocks` and `UpdateStock` treat `scores` as a **merge**, not a replacement:

- A category present in the request is inserted or updated, and gets a fresh server timestamp.
- A category absent from the request is left untouched — same value, same `updated_at`.
- An empty or omitted `scores` map updates only the stock-level timestamp.
- Scores are removed only by `RemoveStock` or by retention (`STOCK_TTL`); an upsert never removes a score.

Kafka ingestion follows the same rule: a Kafka `Stock` message merges its scores into the stored set.

### Messages

| Message | Fields |
| --- | --- |
| `UpdateStockRequest` | `string symbol`, `string exchange`, `map<string,double> scores` |
| `RemoveStockRequest` | `string symbol`, `string exchange` |
| `GetStockRequest` | `string symbol`, `optional string exchange` |
| `GetStocksRequest` | `int32 limit`, `optional string exchange`, `map<string,double> min_scores`, `map<string,double> max_scores` |
| `TopStocksRequest` | `int32 limit`, `string category`, `optional string exchange` |
| `RemoveStockResponse` | `bool removed` |
| `Stock` | `string symbol`, `string exchange`, `repeated ScoreEntry scores` |
| `ScoreEntry` | `string category`, `double value`, `google.protobuf.Timestamp updated_at` |
| `StockList` | `repeated Stock stocks` |

The full service definition (`proto/v1/stock_store.proto`):

```proto
syntax = "proto3";

package stockstore.v1;
option go_package = "proto/v1;stockstorev1";
import "google/protobuf/timestamp.proto";

service StockStore {
	rpc AddStocks(stream UpdateStockRequest) returns (Stock);
	rpc UpdateStock(UpdateStockRequest) returns (Stock);
	rpc RemoveStock(RemoveStockRequest) returns (RemoveStockResponse);
	rpc GetStock(GetStockRequest) returns (Stock);
	rpc GetStocks(GetStocksRequest) returns (StockList);
	rpc TopStocks(TopStocksRequest) returns (StockList);
}

message UpdateStockRequest { string symbol = 1; string exchange = 2; map<string, double> scores = 3; }
message RemoveStockRequest { string symbol = 1; string exchange = 2; }
message GetStockRequest { string symbol = 1; optional string exchange = 2; }
message GetStocksRequest { int32 limit = 1; optional string exchange = 2; map<string, double> min_scores = 3; map<string, double> max_scores = 4; }
message TopStocksRequest { int32 limit = 1; string category = 2; optional string exchange = 3; }
message RemoveStockResponse { bool removed = 1; }
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry { string category = 1; double value = 2; google.protobuf.Timestamp updated_at = 3; }
message StockList { repeated Stock stocks = 1; }
```

### Example

The following `grpcurl` call is **illustrative only** — substitute your own target and fields. `localhost:3500` is the default port; set `GRPC_PORT` to change it:

```bash
grpcurl -plaintext \
  -d '{ "limit": 10, "exchange": "NYSE", "min_scores": { "momentum": 0.5 } }' \
  localhost:3500 stockstore.v1.StockStore/GetStocks
```

Top `limit` stocks by score in a category (optionally scoped to one exchange):

```bash
grpcurl -plaintext -d '{ "limit": 10, "category": "sentiment", "exchange": "NYSE" }' localhost:3500 stockstore.v1.StockStore/TopStocks
```

## Kafka Ingestion

Stocks may be ingested from a Kafka topic. Messages are **PROTOBUF-encoded** (not JSON), using the shared `stockstore.v1.Stock` message defined in `proto/v1/stock_store.proto`.

### Message schema

The consumer decodes `stockstore.v1.Stock`:

| Field | Type | Required |
| --- | --- | --- |
| `symbol` | `string` | Yes |
| `exchange` | `string` | Yes |
| `scores` | `repeated ScoreEntry` | No |
| `ScoreEntry.category` | `string` | — |
| `ScoreEntry.value` | `double` | — |
| `ScoreEntry.updated_at` | `google.protobuf.Timestamp` | — |

Field layout (from `proto/v1/stock_store.proto`):

```proto
message Stock { string symbol = 1; string exchange = 2; repeated ScoreEntry scores = 3; }
message ScoreEntry {
  string category = 1;
  double value    = 2;
  google.protobuf.Timestamp updated_at = 3;
}
```

A payload built from the gRPC `UpdateStockRequest` message (which uses `map<string, double> scores` instead of `repeated ScoreEntry`) is **wire-compatible** with `Stock`, so producers may use either shape. **JSON is not accepted** — an unparseable payload is skipped and logged with a `decode stock message` reason.

### Validation and delivery semantics

On receipt, each message is validated before it is written:

- `symbol` and `exchange` must be non-empty.
- Every score value must be within `[-1.0, 1.0]`.
- Every score value must be **finite** — `NaN` and `±Inf` are rejected explicitly (a `NaN` would otherwise slip past the range check and have the whole message rolled back by the database `CHECK` constraint).

**Permanently invalid messages are skipped, not retried.** A message that fails to decode or fails validation — or that the database rejects with a data/integrity violation — is logged exactly once and its offset is committed, so a single bad message can never block later ones:

```
kafka: skipping message <topic>:<partition>:<offset> key=<key>: <reason>
```

Decode errors include the payload length and a hex prefix for diagnosis; validation errors name the stock (`symbol/exchange`).

**Transient failures are retried.** A store failure that is not permanently invalid (database down, connection refused, timeout) is retried **on the same message** with capped exponential backoff (1 s doubling to 30 s), logged as:

```
kafka: transient failure for <topic>:<partition>:<offset> key=<key>, retrying in <delay>: <err>
```

If the connection to the broker itself fails (fetch or offset commit), the consumer logs `kafka: consumer stopped: <err>; reconnecting in <delay>`, recreates its reader with the same capped backoff, and resumes — a broker outage never requires a process restart.

**Offsets are committed only after the write succeeds** (or the message is deliberately skipped). The resulting delivery semantics are:

- **At-least-once for transient failures** — a crash between the database commit and the offset commit redelivers the message.
- **Skip-and-log for permanently invalid messages** — they are never applied and never retried; the log line is the audit trail.
- **Redelivery is safe** — upserts are idempotent (`ON CONFLICT` merges), so re-applied messages (including catch-up replays) are harmless.

> **Timestamps:** the server stamps each written score with its **own clock** (`now()`) on write; the stored value read back in gRPC responses is authoritative. Any `updated_at` a Kafka producer sends is **advisory only** — the server accepts and discards it.

Kafka ingestion (**input**) is a **no-op unless BOTH `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set**. New-stock publishing (**output**) is a no-op unless BOTH `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set. The two directions are independent — see [Configuration](#configuration). Produce these messages from any Kafka producer that can emit the PROTOBUF-encoded `Stock` (for Go producers, the types in `proto/v1`). See [Verifying Kafka ingestion](#verifying-kafka-ingestion) for how to confirm end-to-end that messages are landing.

### New-Stock Output

In addition to ingesting on `KAFKA_INPUT_TOPIC`, the service can **publish** a `Stock` message (the same PROTOBUF-encoded message gRPC returns) to a separate `KAFKA_OUTPUT_TOPIC` whenever a **new** `(symbol, exchange)` pair is written for the first time (an `INSERT` into the `stocks` table, not an update to an existing one). The two directions are independent: enable input only, output only, both, or neither.

Publish is best-effort — a failure is logged and the gRPC / Kafka handler still reports success (the DB write is authoritative). The published message is the authoritative, server-stamped stock (all scores including their `updated_at` timestamps), not the raw input as received.

## Configuration

Configuration is entirely via environment variables. The **single most important variable is `DATABASE_URL`** — the service does not start without it.

### Environment Variables

| Variable | Required | Default | Format | Purpose |
| --- | --- | --- | --- | --- |
| `DATABASE_URL` | Yes | — | Full DSN `postgres://user:pass@host:port/db?sslmode=...` (or `postgresql://...`) **or** legacy `host:port/db?sslmode=...` | The Postgres connection string. A full DSN is used verbatim; the legacy form is prefixed with `postgres://` (see the note below). |
| `DATABASE_USERNAME` | No | — | Postgres user name | Companion of the **legacy** `DATABASE_URL` form only: when set **together with** `DATABASE_PASSWORD` the credentials are embedded in the DSN. Ignored when `DATABASE_URL` is a full DSN. |
| `DATABASE_PASSWORD` | No | — | Postgres password | Companion of the **legacy** `DATABASE_URL` form only: when set **together with** `DATABASE_USERNAME` the credentials are embedded in the DSN. Ignored when `DATABASE_URL` is a full DSN. |
| `GRPC_PORT` | No | `3500` | Decimal TCP port, `1`–`65535` | Port the gRPC server binds to (`:PORT`). Invalid values abort startup. |
| `STOCK_TTL` | No | `720h` (30 days) | Go duration string, e.g. `48h`, `720h`; `0` (or any non-positive value) **disables** expiry | Retention window for stale-stock auto-removal. Cleanup runs once at startup, then hourly. |
| `KAFKA_BROKERS` | No (Kafka only) | — | Comma-separated `host:port` list, e.g. `broker1:9092,broker2:9092` | Kafka brokers, used for **both** consuming stock events (input) and publishing new-stock `Stock` messages (output). |
| `KAFKA_INPUT_TOPIC` | No (Kafka only) | — | Topic name (e.g. `stockers`) | The topic to **consume** stock events from. |
| `KAFKA_GROUP_ID` | No (Kafka only) | `stocker-store` | Consumer group id (string) | Kafka consumer group, for partition assignment and offset tracking across restarts. |
| `KAFKA_OUTPUT_TOPIC` | No (Kafka only) | — | Topic name (e.g. `stockers-new`) | The topic to **publish** new-stock `Stock` messages on. |

> **Key rule:** Kafka ingestion (input) is a **no-op unless BOTH `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set.** New-stock publish (output) is a **no-op unless BOTH `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set.** The two directions are independent — enable input only, output only, both, or neither. Otherwise the service runs as pure gRPC.
>
> **Minimal deployment requires only `DATABASE_URL`.**

> **DSN forms:** `DATABASE_URL` accepts a **full DSN** (`postgres://user:pass@host:5432/stocker?sslmode=disable` or `postgresql://...`), which is used verbatim, **or** the **legacy form** `host:5432/stocker?sslmode=disable`. For the legacy form only, setting **both** `DATABASE_USERNAME` and `DATABASE_PASSWORD` embeds the credentials; for a full DSN they are ignored (the DSN is authoritative). At startup the service logs the sanitized target — e.g. `database: postgres://postgres@pg:5432/stocker` — scheme, user, host and path only; **the password is never logged**.
>
> There are no separate `DB_USER`, `DB_PASSWORD`, or `DB_HOST` variables — `DATABASE_USERNAME`/`DATABASE_PASSWORD` are companions of the legacy `DATABASE_URL` form, not replacements for it.

### Sample `.env.podman`

This is the environment file a deployment reads (the Quadlet example loads `~/.config/stocker-store/.env.podman`) — it is **not** shipped in the repo; the operator creates it:

```bash
# Required — the Postgres connection string: a full DSN …
DATABASE_URL=postgres://user:PASSWORD@localhost:5432/stocker?sslmode=disable

# … or the legacy host:port/db form, with optional credentials applied only
# when BOTH variables are set (full DSNs ignore them):
# DATABASE_URL=localhost:5432/stocker?sslmode=disable
# DATABASE_USERNAME=user
# DATABASE_PASSWORD=PASSWORD

# Optional — gRPC listen port (default 3500).
# GRPC_PORT=3500

# Optional — stock retention as a Go duration (default 720h / 30 days); "0" disables.
# STOCK_TTL=720h

# Optional — Kafka input/ingestion (BROKERS and INPUT topic both required to enable input).
# KAFKA_BROKERS=localhost:9092
# KAFKA_INPUT_TOPIC=stockers
# KAFKA_GROUP_ID=stocker-store

# Optional — New-stock output (BROKERS and OUTPUT topic both required to enable output).
# KAFKA_OUTPUT_TOPIC=stockers-new
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

> **Port:** the `Containerfile` declares `EXPOSE 3500`, matching the default `GRPC_PORT`. `EXPOSE` is static image metadata and does not follow `GRPC_PORT`. With `--network=host` (below) no port publishing is needed; on a bridge network publish the port with `-p 3500:3500` — or `-p $GRPC_PORT:$GRPC_PORT` when overridden, passing `GRPC_PORT` into the container (`-e`) as well.

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
- `stocker-store.container` — `Image=containers.wheeli.ca/stocker-store:latest`, `EnvironmentFile=%h/.config/stocker-store/.env.podman`, `Restart=always`, `RestartSec=5`.

The shipped unit uses the default bridge network and publishes no ports, so the gRPC port is not reachable from the host as-is. Either add `Network=host` to it (the service then listens on the host's **`GRPC_PORT`**, default `3500`, and that port must be free on the host), or publish the port with `PublishPort` (e.g. `PublishPort=3500:3500`, keeping the container side equal to `GRPC_PORT` from `.env.podman`).

> **The operator must create** `~/.config/stocker-store/.env.podman` — it is **not** shipped in the repo. Populate it with `DATABASE_URL` and the optional `STOCK_TTL` / `KAFKA_*` values (see the [sample `.env.podman`](#sample-envpodman)).

To use the Quadlets, place the unit files (for example in `/etc/systemd/user` or per-user `~/.config/containers/systemd`), create the env file, then reload and enable the service.

### Quickstart (Quadlets, end-to-end)

1. **Write the env file:** create `~/.config/stocker-store/.env.podman` — at minimum `DATABASE_URL`; optionally set `GRPC_PORT` (default `3500`) (see the [sample](#sample-envpodman)).
2. **Build the image:** `podman build -t stocker-store .`
3. **Enable & start:** `systemctl enable --now stocker-store`
4. **Verify:** `podman logs stocker-store` and/or `systemctl status stocker-store` — the startup line `stocker-store listening on :<port>` reports the port actually bound.

### Push to registry

`deploy/push.sh` builds and pushes the image:

```bash
./deploy/push.sh   # -> podman push containers.wheeli.ca/stocker-store:latest
```

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

> **Note:** TopStocks orders by score value DESC with a deterministic tiebreaker (symbol, then exchange); a limit <= 0 or an empty/unknown category returns an empty list.

## Development

### Build & test

```bash
make build    # go build -o bin/stocker-store ./cmd/main.go
make test     # go test ./...
go vet ./...
make run      # build, then run
make clean    # remove bin/
```

Pre-generated proto bindings are committed under `proto/v1`, so you do not need codegen tooling to build.

`go test ./...` (what `make test` runs) includes the config tests in `cmd/main_test.go` — `GRPC_PORT` parsing, `DATABASE_URL`/DSN handling, and DSN log sanitization; `go test ./cmd` runs just those.

### Repository layout

```text
cmd/main.go                    # entrypoint: env config (incl. GRPC_PORT, DATABASE_URL), gRPC server, retention loop, kafka subscriber
cmd/main_test.go               # GRPC_PORT parser + DATABASE_URL/DSN tests
internal/grpc/                 # gRPC server (StockStore handlers) + tests
internal/kafka/                # Kafka consumer (Stock ingestion: reconnect, at-least-once commit, retries) + tests
internal/store/                # PostgreSQL (pgx pool), models, table init, + tests
proto/v1/                      # StockStore service + messages (stock_store.proto) and generated Go
deploy/quadlet/                # Quadlet unit files (stocker-store.build / stocker-store.container)
deploy/push.sh                 # build + push image to registry
deploy/install.sh              # legacy install script
docs/plan/                     # design/fix plans (kafka-score-ingestion-fix.md, partial-score-updates.md, …)
Containerfile                  # multi-stage build (EXPOSE 3500)
Makefile                       # build / run / test / clean
docs/design.md                 # historical design doc (partially stale)
AGENTS.md                      # agent-oriented notes
```

## Troubleshooting & Notes

- **Minimal configuration:** only `DATABASE_URL` is required. `STOCK_TTL` and the `KAFKA_*` variables are optional.
- **Kafka is optional:** ingestion (**input**) is a no-op unless **both** `KAFKA_BROKERS` and `KAFKA_INPUT_TOPIC` are set, and new-stock publishing (**output**) is a no-op unless **both** `KAFKA_BROKERS` and `KAFKA_OUTPUT_TOPIC` are set — the two directions are independent.
- **Port:** the gRPC server listens on `GRPC_PORT` (default `3500`), logged at startup as `stocker-store listening on :<port>`. `EXPOSE 3500` in the `Containerfile` reflects only that default.
- **`GetStocks` requires `limit > 0`:** an unset or `0` `limit` returns an empty list.
- **TopStocks limit/category:** a non-positive limit or an empty/unknown category returns an empty list (not an error).
- **Authoritative deployment variables** are the ones in the [Configuration](#configuration) section: `DATABASE_URL`, `DATABASE_USERNAME`, `DATABASE_PASSWORD`, `GRPC_PORT`, `STOCK_TTL`, and the `KAFKA_*` variables.

### Verifying Kafka ingestion

A quick runbook for confirming that Kafka messages actually reach the `scores` table. All log lines below are emitted verbatim by the service — a one-liner to watch them:

```bash
podman logs stocker-store | grep -E 'database:|kafka'
```

**Healthy startup** should show, in order:

```
database: postgres://<user>@<host><path>          # sanitized target — never the password
kafka subscriber: consuming "<topic>" via <brokers>
stocker-store listening on :<port>
```

**Per-message success** — one line per applied message:

```
kafka: applied <topic>:<partition>:<offset> <symbol>/<exchange> scores=<n> inserted=<bool>
```

If this line never appears while messages are being produced, work through the checks below.

**Failure lines and what they mean:**

| Log line (pattern) | Meaning |
| --- | --- |
| `kafka: transient failure for <topic>:<partition>:<offset> key=<key>, retrying in <delay>: <err>` | The store (database) failed with a transient error. The **same message** is retried with capped backoff (1 s → 30 s) and is **not committed** until it succeeds — nothing is lost; fix the database. |
| `kafka: consumer stopped: <err>; reconnecting in <delay>` | The broker connection failed (fetch or offset commit). The reader is recreated with the same capped backoff and ingestion resumes automatically — **no restart needed**. If this repeats at startup, the broker is down or unreachable from the container (check its advertised listener). |
| `kafka: skipping message <topic>:<partition>:<offset> key=<key>: <reason>` | A **permanently invalid** message: undecodable payload (`decode stock message …`), empty symbol/exchange, out-of-range or non-finite (`NaN`/`±Inf`) score, or a database data/constraint violation. Logged once, offset committed, later messages unaffected. |

**Consumer-group lag** — a healthy consumer has a member and no lag:

```bash
kafka-consumer-groups.sh --bootstrap-server <brokers> --describe --group <KAFKA_GROUP_ID>
```

- `CONSUMER-ID` present and `LAG 0` — healthy.
- Group missing or no member — the subscriber is not running (check the failure lines above).
- `LAG` growing — messages are produced but not consumed.

**Topic sanity** — a typo'd `KAFKA_INPUT_TOPIC` is auto-created by the broker and stays empty while real messages pile up on the real topic:

```bash
kafka-topics.sh --bootstrap-server <brokers> --list
```

**Payload sanity** — consume one message and inspect its first bytes:

```bash
kafka-console-consumer.sh --bootstrap-server <brokers> --topic <topic> --from-beginning --max-messages 1 | xxd | head
# or, with kcat:
kcat -b <brokers> -C -t <topic> -o beginning -c 1 | xxd | head
```

A valid PROTOBUF `Stock` starts with `0a …` (field 1, the `symbol` string). JSON starts with `7b 22` (`{"`) and will be skipped with a `decode stock message` reason.

**Database sanity:**

```sql
SELECT symbol, exchange, category, value, timestamp FROM scores ORDER BY timestamp DESC LIMIT 10;
```

**Re-ingestion runbook.** If the database was reset (or messages were skipped) while `KAFKA_GROUP_ID` kept its offsets, the consumer will **not** re-read old messages — the group resumes where it left off. To replay the topic:

1. Stop the service.
2. Set `KAFKA_GROUP_ID` to a **fresh** value, e.g. `stocker-store-reingest-2026-10-30`.
3. Start the service: a new group begins at the earliest offset the topic still **retains** and replays from there.
4. Revert `KAFKA_GROUP_ID` to the normal value afterwards.

Re-ingestion re-applies every retained message; the upserts are idempotent, so this is harmless — but be aware it only replays what topic retention still holds, and a large replay is best done off-peak.

### Known inconsistencies to fix

These are known issues in the deployment scripts and metadata; none of them is asserted as the canonical source of truth:

- **Image name:** `deploy/push.sh` pushes `stocker-list:latest` vs. the Quadlet's `stocker-store:latest`.
- **`deploy/install.sh`:** references legacy variables `DB_USER` / `DB_PASSWORD`, the legacy service name `tsx-tracker`, and some missing/renamed files. It is legacy and should not be relied upon.

## Further Reading

- `docs/design.md` — historical design doc (partially stale).
- `docs/plan/` — design and fix plans, including `kafka-score-ingestion-fix.md`, the root-cause analysis and fix plan that defines the current consumer behavior.
- `AGENTS.md` — agent-oriented project notes.
- `proto/v1/stock_store.proto` — the service and message definitions; `Stock` is also the Kafka message schema.
- Source: `cmd/main.go`, `internal/{grpc,kafka,store}`, `proto/v1`.
