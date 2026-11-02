# Stocker Store

A simple, self-hostable Go gRPC service for storing stocks and their normalized scores, with RabbitMQ ingestion enabled by default.

**Tech:** Go 1.25 · gRPC/protobuf · PostgreSQL (pgx v5) · RabbitMQ (amqp091-go) · Podman Quadlets

```
┌──────────────┐   publish    ┌────────────────────────────────────────┐
│ stocker-list │ ───────────► │ RabbitMQ broker                        │
└──────────────┘              │                                        │
                              │ exchange "stock.update" (topic)        │
                              │   └─ routing key "stock.update.v1"     │
                              │        │                               │
                              │        ▼                               │
                              │ queue "stocker-store.stock.update.v1"  │
                              └────────┬───────────────────────────────┘
                                       │ deliver (manual ack)
                                       ▼
┌──────────┐   gRPC :3500  ┌──────────────────────────────┐   upsert   ┌──────────────┐
│ clients  │ ◄───────────► │ stocker-store                │ ─────────► │ PostgreSQL   │
│ (gRPC)   │               │ gRPC + RabbitMQ consumer     │            │ stocks /     │
└──────────┘               │ retention loop (STOCK_TTL)   │            │ scores       │
                           └──────────────┬───────────────┘            └──────────────┘
                                          │ new stock only
                                          ▼ optional, disabled by default
                              exchange $RABBITMQ_OUTPUT_EXCHANGE
                              routing key $RABBITMQ_OUTPUT_ROUTING_KEY
```

Stocker Store stores thousands of `(symbol, exchange)` stocks plus **dynamic** score categories, each score normalized to `[-1.0, 1.0]` and carrying a per-score last-updated timestamp (set by the server on write). You can retrieve stocks by symbol, by exchange, or by min/max score ranges; when a request's `limit` is smaller than the number of matching stocks, a random subset is returned. Stocks and scores are upserted, and stale stocks are automatically removed after a configurable retention window (default 30 days). Ingestion happens over gRPC and, by default, over RabbitMQ — the service also runs as pure gRPC when ingestion is disabled with `RABBITMQ_URL=`.

## Table of Contents

- [Stocker Store](#stocker-store)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Features](#features)
  - [gRPC API](#grpc-api)
    - [Methods](#methods)
    - [Messages](#messages)
    - [Example](#example)
  - [RabbitMQ Ingestion](#rabbitmq-ingestion)
    - [Wire contract](#wire-contract)
    - [Message schema](#message-schema)
    - [Validation and delivery semantics](#validation-and-delivery-semantics)
    - [New-stock output](#new-stock-output)
  - [Configuration](#configuration)
    - [Environment variables](#environment-variables)
    - [Sample `.env.podman`](#sample-envpodman)
    - [Test-only variables](#test-only-variables)
  - [Deployment](#deployment)
    - [Prerequisites](#prerequisites)
    - [Option A — Run locally (Go)](#option-a--run-locally-go)
    - [Option B — Run as a container](#option-b--run-as-a-container)
    - [Option C — Podman Quadlets (self-host)](#option-c--podman-quadlets-self-host)
    - [Quickstart (Quadlets, end-to-end)](#quickstart-quadlets-end-to-end)
    - [Push to registry](#push-to-registry)
  - [Database](#database)
  - [Development](#development)
    - [Build & test](#build--test)
    - [Repository layout](#repository-layout)
  - [Troubleshooting & Notes](#troubleshooting--notes)
    - [Verifying RabbitMQ ingestion](#verifying-rabbitmq-ingestion)
  - [Further Reading](#further-reading)

## Overview

Stocker Store is a single Go binary that keeps a live store of stocks and their scores. Each stock is identified by a `(symbol, exchange)` pair and may carry any number of scores, grouped by category. Categories are dynamic — there is no fixed set you must conform to — and every score must fall within the range `[-1.0, 1.0]`.

The service exposes a gRPC `StockStore` interface (package `stockstore.v1`) for all reads and writes, and runs two background concerns: a retention loop that deletes stale stocks, and a RabbitMQ consumer that ingests PROTOBUF-encoded `Stock` messages. Ingestion is enabled by default against the shared broker; setting `RABBITMQ_URL` to the empty string explicitly disables it and the service runs as a pure gRPC stock and score store.

An unreachable broker is **not** a startup failure: the process keeps serving gRPC and retention while the consumer reconnects with backoff.

## Features

- **Bulk and single upserts** — send one stock or a stream of stocks; both upsert the stock and its scores atomically. Upserts **merge** scores: only the categories present in the request are written, and categories not mentioned are left unchanged, including their last-updated timestamps.
- **Dynamic score categories** — scores are keyed by an arbitrary category string, each normalized to `[-1.0, 1.0]`, each with a last-updated timestamp set by the server on write.
- **Flexible retrieval** — filter by symbol, exchange, and per-category min/max score ranges.
- **Random sampling** — when `limit` is less than the total matching count, a random subset is returned (`ORDER BY RANDOM()`).
- **Top stocks** — fetch the N stocks with the highest score in a category, optionally scoped to an exchange, with deterministic ordering (value DESC, then symbol, then exchange).
- **Stale-stock removal** — stocks older than `STOCK_TTL` (default 30 days) are deleted along with their scores; disabled when set to `0`.
- **RabbitMQ ingestion (default on)** — consume PROTOBUF-encoded `Stock` messages (`stockstore.v1.Stock`) from a durable queue; at-least-once with the ack sent only after the database write, capped in-process retries for transient failures, permanent/poison messages logged and nacked without requeue, and automatic reconnect with backoff. Set `RABBITMQ_URL=` to disable.
- **Optional new-stock output** — publish a `Stock` event when a write inserts a new `(symbol, exchange)` row; disabled unless `RABBITMQ_OUTPUT_EXCHANGE` is set.
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

RabbitMQ ingestion follows the same rule: an ingested `Stock` message merges its scores into the stored set.

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
message ScoreEntry {
  string category = 1;
  double value    = 2;
  google.protobuf.Timestamp updated_at = 3;
}
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

## RabbitMQ Ingestion

Stocks are ingested from RabbitMQ by default, using the shared broker at the default
`RABBITMQ_URL`. Messages are **binary protobuf** (not JSON), using the shared
`stockstore.v1.Stock` message defined in `proto/v1/stock_store.proto`. The upstream
publisher is `stocker-list`.

### Wire contract

**Input (we consume):**

| Item | Value |
| --- | --- |
| Exchange name | `stock.update` (`RABBITMQ_EXCHANGE`) |
| Exchange type | `topic` (`RABBITMQ_EXCHANGE_TYPE`) |
| Exchange flags | `durable=true`, `autoDelete=false`, `internal=false`, `noWait=false`, `args=nil` |
| Routing key | `stock.update.v1` (`RABBITMQ_ROUTING_KEY`) |
| Publisher flags | `mandatory=false`, `immediate=false`, `deliveryMode=2` (persistent) |
| Queue name | `stocker-store.stock.update.v1` (`RABBITMQ_QUEUE`) |
| Queue flags | `durable=true`, `exclusive=false`, `autoDelete=false`, `noWait=false`, `args=nil` |
| Binding | `source=stock.update`, `destination=stocker-store.stock.update.v1`, `key=stock.update.v1` |
| Prefetch | `1` (`RABBITMQ_PREFETCH`), single consumer worker |
| Consumer flags | `autoAck=false`, `exclusive=false`, `noLocal=false`, `noWait=false`, `args=nil` |
| Payload | `proto.Marshal` of `stockstore.v1.Stock` |

**Message properties published by the upstream producer:**

| Property | Value |
| --- | --- |
| `content_type` | `application/x-protobuf` |
| `message_id` | the stock symbol only (for example `AC`) — **not unique** |
| `type` | `stock.update.v1` |
| `timestamp` | publish time (Unix seconds granularity) |
| `headers` | `{"symbol": <symbol>, "exchange": <exchange>}` |
| `delivery_mode` | `2` (persistent) |
| `body` | binary protobuf `stockstore.v1.Stock` |

> **Warning — the exchange is `stock.update`, not `stock.update.v1`.**
> `stock.update.v1` is the **routing key** and the AMQP `type` property. It is *not* the
> exchange name. Declaring an exchange called `stock.update.v1` creates a second, empty
> exchange that receives nothing. Redeclaring `stock.update` with any different flag or
> argument yields `406 PRECONDITION_FAILED` and closes the channel.

On every connect the service declares the exchange with exactly the upstream flags, then
declares and binds its own queue. It never deletes or re-args the shared exchange.
Changing `RABBITMQ_EXCHANGE`, `RABBITMQ_EXCHANGE_TYPE`, or `RABBITMQ_ROUTING_KEY` only
works if the broker's existing exchange and the upstream binding use the same values: a
mismatch fails the declaration with `406 PRECONDITION_FAILED` (exchange) or silently
routes nothing (routing key).

The durable, non-exclusive queue survives both broker and service restarts, so messages
accumulate while the service is down. Queue redeclaration is also an assertion: changing
durability, exclusivity, auto-delete, or arguments against an existing queue yields
`406 PRECONDITION_FAILED`. To change those flags, drain and delete the queue, or use a
new `RABBITMQ_QUEUE` name.

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

Upstream sets only `symbol` and `exchange`; **`scores` is normally absent**. A `scores`
field from another producer is accepted and validated like any gRPC write. A payload
built from the gRPC `UpdateStockRequest` message (which uses `map<string, double> scores`
instead of `repeated ScoreEntry`) is **wire-compatible** with `Stock`. **JSON is not
accepted** — an unparseable payload is skipped and logged with a `decode stock message`
reason.

Because `scores` is absent, an ingested message is a **scoreless upsert**: existing score
rows keep their values and their timestamps, and only the stock-level timestamp is
refreshed. A scoreless message never deletes scores; the only deletions are `RemoveStock`
and retention.

> **Timestamps:** the server stamps each written score with its **own clock** (`now()`) on
> write; the stored value read back in gRPC responses is authoritative. Any `updated_at` a
> producer sends is **advisory only** — the server accepts and discards it.

There is no per-message sequence number, revision, snapshot marker, or deletion signal.
Ordering across symbols is not guaranteed, and the AMQP `timestamp` is seconds precision
and regenerated on a publisher retry, so it is not a deduplication key. Deduplicate on
`(symbol, exchange)` — the store's primary key.

### Validation and delivery semantics

On receipt, each message is validated before it is written:

- `symbol` and `exchange` must be non-empty.
- Every score value must be within `[-1.0, 1.0]`.
- Every score value must be **finite** — `NaN` and `±Inf` are rejected explicitly (a `NaN`
  would otherwise slip past the range check and have the whole message rolled back by the
  database `CHECK` constraint).

**Ack after write (at-least-once).** The delivery is acknowledged only after the database
write commits. A crash between commit and ack redelivers the message; upserts are
idempotent (`INSERT ... ON CONFLICT`), so duplicates are expected and safe.

**Permanently invalid messages are skipped, not retried.** A message that fails to decode
or validate — or that the database rejects with a data/integrity violation (SQLSTATE class
`22` or `23`) — is logged exactly once and nacked **without requeue**, so it can never
block later messages:

```
rabbitmq: skipping message symbol=<symbol> exchange=<exchange> message_id=<id>: <reason>
```

The service configures no dead-letter exchange or `x-*` queue arguments. An operator may
attach a dead-letter policy to `stocker-store.stock.update.v1`; without one, a skipped
message is dropped. Never add `x-*` arguments or an alternate exchange to the shared
`stock.update` exchange — that breaks the upstream declaration with a 406.

**Transient failures are retried in-process.** A store failure that is not permanently
invalid (database down, connection refused, timeout, serialization failure — SQLSTATE
class `40` included) is retried **on the same delivery** with capped exponential backoff
(1 s doubling to 30 s) while the delivery stays unacknowledged, logged as:

```
rabbitmq: transient failure for symbol=<symbol>/<exchange>, retrying in <delay>: <err>
```

**The consumer reconnects automatically.** If the channel or connection is lost, the
service closes both, logs `rabbitmq: consumer stopped: <err>; reconnecting in <delay>`, and
retries with the same capped backoff. Unacknowledged deliveries are requeued by the broker
and redelivered. An unreachable broker at startup is not fatal either
(`rabbitmq: connect failed: <err>; retrying in <delay>`): gRPC and retention keep running
while ingestion recovers. Only malformed configuration is fatal.

Ingestion is a **single worker** per process; `RABBITMQ_PREFETCH` bounds unacknowledged
deliveries (default `1`), and deliveries are processed one at a time.

### New-stock output

The service can additionally **publish** a `Stock` message — the same PROTOBUF-encoded
message gRPC returns — whenever a **new** `(symbol, exchange)` row is inserted, from either
the gRPC path or the RabbitMQ consumer. Updates to existing stocks do not emit.

Output is **disabled by default**. It is enabled only when `RABBITMQ_OUTPUT_EXCHANGE` is
set to a non-empty value (which also requires a non-empty `RABBITMQ_URL`):

| Item | Value |
| --- | --- |
| Exchange | `$RABBITMQ_OUTPUT_EXCHANGE`, type `$RABBITMQ_OUTPUT_EXCHANGE_TYPE` (default `topic`) |
| Exchange flags | `durable=true`, `autoDelete=false`, `internal=false`, `noWait=false`, `args=nil` |
| Routing key | `$RABBITMQ_OUTPUT_ROUTING_KEY` (default `stock.created.v1`) |
| Delivery | `deliveryMode=2` (persistent), `mandatory=false`, publisher confirms awaited |
| Properties | `content_type=application/x-protobuf`, `message_id=<symbol>`, `type=<output routing key>`, `timestamp=now UTC`, headers `{symbol, exchange}` |
| Payload | the authoritative stored stock, including all scores and their server-stamped `updated_at` values |

The publisher connects lazily on the first publish and declares **only its exchange** —
consumers of that exchange own their queues and bindings. It is best-effort: one failure
tears down the connection, logs
`rabbitmq: publish failed; reconnecting and retrying once symbol=<symbol>`, reconnects, and
retries the same message once. A second failure is logged
(`rabbitmq: failed to publish new stock <symbol>/<exchange>: <err>`) and never fails the
database write, the gRPC response, or the ingestion ack.

> **Loop warning.** The default output routing key (`stock.created.v1`) deliberately
> differs from the input routing key. Pointing the output exchange at `stock.update` with
> the input routing key would feed our own queue and create a loop; that exact
> same-exchange/same-key case is rejected at startup. When the output and input exchange
> names match, the service logs
> `rabbitmq: output exchange equals input exchange; verify the routing key does not create a feedback loop`
> and leaves broader topic-pattern overlaps to the operator's responsibility.

## Configuration

Configuration is entirely via environment variables. The **single most important variable
is `DATABASE_URL`** — the service does not start without it.

### Environment variables

| Variable | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `DATABASE_URL` | string | — | **Yes** | Postgres DSN: full `postgres://user:pass@host:port/db?sslmode=...` (or `postgresql://...`), or the legacy `host:port/db?sslmode=...` form. A full DSN is used verbatim; the legacy form is prefixed with `postgres://`. |
| `DATABASE_USERNAME` | string | — | No | Legacy-form companion only: when set **together with** `DATABASE_PASSWORD`, the credentials are embedded in the DSN. Ignored when `DATABASE_URL` is a full DSN. |
| `DATABASE_PASSWORD` | string | — | No | Legacy-form companion only: when set **together with** `DATABASE_USERNAME`, the credentials are embedded in the DSN. Ignored when `DATABASE_URL` is a full DSN. |
| `GRPC_PORT` | integer | `3500` | No | Port the gRPC server binds to (`:PORT`); must be `1`–`65535`. Invalid values abort startup. |
| `STOCK_TTL` | Go duration | `720h` (30 days) | No | Retention window for stale-stock removal, e.g. `48h`, `720h`. Any non-positive value (e.g. `0`) disables expiry. An unparseable value logs a warning and falls back to the default. Cleanup runs once at startup, then hourly. |
| `RABBITMQ_URL` | AMQP URI | `amqp://guest:guest@192.168.1.48:5672/` | No | Broker connection URI (`amqp://` or `amqps://`). **Absent** → the shared default broker; **explicitly empty** → ingestion disabled (no dial). A malformed URI is fatal at startup and the error is redacted. The URI path selects the vhost (`/` by default). |
| `RABBITMQ_EXCHANGE` | string | `stock.update` | No | Input exchange name. Must match the broker's existing exchange; empty falls back to the default. |
| `RABBITMQ_EXCHANGE_TYPE` | string | `topic` | No | Input exchange type: `direct`, `topic`, `fanout`, or `headers` (case-insensitive). Anything else is a startup error. |
| `RABBITMQ_ROUTING_KEY` | string | `stock.update.v1` | No | Binding key for the input queue. Empty falls back to the default. |
| `RABBITMQ_QUEUE` | string | `stocker-store.stock.update.v1` | No | Durable, non-exclusive input queue name. Empty falls back to the default. |
| `RABBITMQ_PREFETCH` | integer | `1` | No | Maximum unacknowledged deliveries (`>= 1`). Non-numeric or `< 1` is a startup error. |
| `RABBITMQ_OUTPUT_EXCHANGE` | string | unset | No | Output exchange for new-stock events. **Output is disabled unless this is non-empty**; enabling it requires `RABBITMQ_URL`. |
| `RABBITMQ_OUTPUT_EXCHANGE_TYPE` | string | `topic` | No | Output exchange type, validated like `RABBITMQ_EXCHANGE_TYPE`. |
| `RABBITMQ_OUTPUT_ROUTING_KEY` | string | `stock.created.v1` | No | Output routing key. Rejected if it equals `RABBITMQ_ROUTING_KEY` while the output exchange equals the input exchange (self-feeding loop). |

> **How empty values are read.** `RABBITMQ_URL` uses `os.LookupEnv` so the absent and
> empty cases differ: **absent → the default broker**, **explicitly empty (or
> whitespace-only) → ingestion disabled**. Every other variable treats empty as absent and
> falls back to its default. `RABBITMQ_OUTPUT_EXCHANGE` treats absent *and* empty as
> "output disabled".
>
> **Configuration errors are fatal; broker errors are not.** A malformed URL, an invalid
> exchange type or prefetch, or enabling output without a URL aborts startup with a
> message. An unreachable broker does not: the consumer retries with capped backoff.

> **DSN forms:** `DATABASE_URL` accepts a **full DSN**
> (`postgres://user:pass@host:5432/stocker?sslmode=disable` or `postgresql://...`), which is
> used verbatim, **or** the **legacy form** `host:5432/stocker?sslmode=disable`. For the
> legacy form only, setting **both** `DATABASE_USERNAME` and `DATABASE_PASSWORD` embeds the
> credentials; for a full DSN they are ignored (the DSN is authoritative). At startup the
> service logs the sanitized target — e.g. `database: postgres://postgres@pg:5432/stocker` —
> scheme, user, host and path only; **the password is never logged**.

> **Broker account.** A stock RabbitMQ refuses the `guest` account from non-loopback
> addresses, so a remote deployment must supply a dedicated user and vhost in
> `RABBITMQ_URL`, preferably with `amqps://`. The service needs permission to declare and
> bind its own queue, read from it, read the input exchange to bind, and (when output is
> enabled) declare and write the output exchange. Do not grant administrator rights.

### Sample `.env.podman`

This is the environment file a deployment reads (the Quadlet unit loads
`~/.config/stocker-store/.env.podman`) — it is **not** shipped in the repo; the operator
creates it:

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

# Optional — RabbitMQ ingestion. Ingestion is ON by default against the shared broker;
# set RABBITMQ_URL= (empty) to disable it entirely, or point it at your own broker.
# RABBITMQ_URL=amqp://stocker:PASSWORD@rabbitmq.example:5672/stocks
# RABBITMQ_EXCHANGE=stock.update
# RABBITMQ_EXCHANGE_TYPE=topic
# RABBITMQ_ROUTING_KEY=stock.update.v1
# RABBITMQ_QUEUE=stocker-store.stock.update.v1
# RABBITMQ_PREFETCH=1

# Optional — new-stock output. Disabled unless the exchange is set.
# RABBITMQ_OUTPUT_EXCHANGE=stock.created
# RABBITMQ_OUTPUT_EXCHANGE_TYPE=topic
# RABBITMQ_OUTPUT_ROUTING_KEY=stock.created.v1
```

### Test-only variables

> **Not deployment variables.** These are read only by the test suite and have no effect on
> the running service.
>
> - `DATABASE_URL_FILE` — path to a file containing a Postgres DSN for the store tests.
> - `RABBITMQ_TEST_URL` — enables the RabbitMQ integration test, run with
>   `go test ./internal/rabbitmq -run Integration`. Without it that test skips. Point it at a
>   disposable broker: the test uses unique throwaway exchanges and queues and never touches
>   `stock.update`.

## Deployment

### Prerequisites

- Go 1.25+ (for building/running from source).
- A reachable PostgreSQL instance.
- A reachable RabbitMQ broker. Ingestion is on by default, so a deployment either points
  `RABBITMQ_URL` at your broker or sets `RABBITMQ_URL=` to run gRPC-only. The broker may be
  remote; the consumer reconnects with backoff if it is down.
- (Optional) Podman, for containerized runs or Quadlets.

### Option A — Run locally (Go)

```bash
make build    # -> go build -o bin/stocker-store ./cmd/main.go
make run      # build, then run (must point at a reachable DATABASE_URL)
make test     # go test ./...
make clean    # remove bin/
```

Ensure the process can reach a database — the running binary reads `DATABASE_URL` at
startup.

### Option B — Run as a container

```bash
podman build -t git.wheeli.ca/brian/stocker-store:latest .
```

The `Containerfile` is multi-stage:

- **build** — `golang:1.25-bookworm`; installs `protoc` and the `protoc-gen-go` /
  `protoc-gen-go-grpc` plugins, compiles the proto, then `CGO_ENABLED=0 go build`.
- **runtime** — `gcr.io/distroless/static-debian12`, with `ENTRYPOINT ["/stocker-store"]`.

Pre-generated proto code is already in-tree, so the build has no external codegen
dependency at run time.

> **Port:** the `Containerfile` declares `EXPOSE 3500`, matching the default `GRPC_PORT`.
> `EXPOSE` is static image metadata and does not follow `GRPC_PORT`. With `--network=host`
> (below) no port publishing is needed; on a bridge network publish the port with
> `-p 3500:3500` — or `-p $GRPC_PORT:$GRPC_PORT` when overridden, passing `GRPC_PORT` into
> the container (`-e`) as well.

Run with the required environment variables:

```bash
podman run --rm \
  --network=host \
  -e DATABASE_URL="postgres://user:PASSWORD@localhost:5432/stocker?sslmode=disable" \
  -e RABBITMQ_URL="amqp://stocker:PASSWORD@rabbitmq.example:5672/stocks" \
  git.wheeli.ca/brian/stocker-store:latest
```

### Option C — Podman Quadlets (self-host)

The Quadlet unit lives in `deploy/quadlet/`:

- `stocker-store.container` — `Image=git.wheeli.ca/brian/stocker-store:latest`
  (the same image name `deploy/push.sh` pushes), `EnvironmentFile=%h/.config/stocker-store/.env.podman`,
  `AutoUpdate=registry`, `Restart=always`, `RestartSec=10`. It orders after
  `network-online.target` only; the broker is remote, so no broker unit is ordered and the
  consumer reconnects with backoff.

The shipped unit uses the default bridge network and publishes no ports, so the gRPC port
is not reachable from the host as-is. Either add `Network=host` to it (the service then
listens on the host's **`GRPC_PORT`**, default `3500`, and that port must be free on the
host), or publish the port with `PublishPort` (e.g. `PublishPort=3500:3500`, keeping the
container side equal to `GRPC_PORT` from `.env.podman`).

> **The operator must create** `~/.config/stocker-store/.env.podman` — it is **not** shipped
> in the repo. Populate it with `DATABASE_URL`, the broker settings (`RABBITMQ_URL` or
> `RABBITMQ_URL=` to disable ingestion), and the optional `STOCK_TTL` / `RABBITMQ_*` values
> (see the [sample `.env.podman`](#sample-envpodman)).

To use the Quadlet, install the unit (for example in
`~/.config/containers/systemd/`), create the env file, then reload and enable the service.

### Quickstart (Quadlets, end-to-end)

1. **Write the env file:** create `~/.config/stocker-store/.env.podman` — at minimum
   `DATABASE_URL`, plus a reachable `RABBITMQ_URL` (or `RABBITMQ_URL=` to disable ingestion)
   (see the [sample](#sample-envpodman)).
2. **Build (or pull) the image:**
   `podman build -t git.wheeli.ca/brian/stocker-store:latest .`
   (or `podman pull git.wheeli.ca/brian/stocker-store:latest`).
3. **Enable & start:** `systemctl --user enable --now stocker-store`
4. **Verify:** `podman logs stocker-store` and/or `systemctl --user status stocker-store` —
   the startup line `stocker-store listening on :<port>` reports the port actually bound,
   and `rabbitmq: connected ...` reports the topology once the broker is reached.

### Push to registry

`deploy/push.sh` builds and pushes the image used by the Quadlet unit:

```bash
./deploy/push.sh   # -> podman push git.wheeli.ca/brian/stocker-store:latest
```

## Database

Stocker Store uses PostgreSQL. Tables are created automatically at startup with
`CREATE TABLE IF NOT EXISTS` — there are **no manual migrations**.

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

> **Note:** `TopStocks` orders by score value DESC with a deterministic tiebreaker (symbol,
> then exchange); a limit <= 0 or an empty/unknown category returns an empty list.

## Development

### Build & test

```bash
make build    # go build -o bin/stocker-store ./cmd/main.go
make test     # go test ./...
go vet ./...
make run      # build, then run
make clean    # remove bin/
```

Pre-generated proto bindings are committed under `proto/v1`, so you do not need codegen
tooling to build.

`go test ./...` (what `make test` runs) includes the `cmd` configuration tests
(`GRPC_PORT` parsing, `DATABASE_URL`/DSN handling, DSN log sanitization) and the
`internal/rabbitmq` unit tests. The RabbitMQ integration test skips unless
`RABBITMQ_TEST_URL` is set:

```bash
go test ./cmd
go test ./internal/rabbitmq -run Integration -v   # --- SKIP without RABBITMQ_TEST_URL
```

### Repository layout

```text
cmd/main.go                    # entrypoint: env config, RabbitMQ config/publisher, gRPC server, retention loop, consumer
cmd/main_test.go               # GRPC_PORT parser + DATABASE_URL/DSN tests
internal/grpc/                 # gRPC server (StockStore handlers) + tests
internal/rabbitmq/             # RabbitMQ consumer, optional publisher, config, unit + gated integration tests
internal/events/               # neutral Publisher contract and Noop implementation
internal/store/                # PostgreSQL (pgx pool), models, table init, + tests
proto/v1/                      # StockStore service + messages (stock_store.proto) and generated Go
deploy/quadlet/                # Quadlet unit (stocker-store.container)
deploy/push.sh                 # build + push image to registry
docs/design/                   # permanent design docs (rabbitmq-ingestion.md)
docs/plan/                     # implementation plans
Containerfile                  # multi-stage build (EXPOSE 3500)
Makefile                       # build / run / test / clean
docs/design.md                 # service design doc
AGENTS.md                      # agent-oriented notes
```

## Troubleshooting & Notes

- **Minimal database configuration:** only `DATABASE_URL` is required to start. `STOCK_TTL`
  and the `RABBITMQ_*` variables are optional.
- **RabbitMQ ingestion is on by default:** it uses the default `RABBITMQ_URL` unless you
  set it. Set `RABBITMQ_URL=` (explicitly empty) to disable ingestion and run as pure gRPC.
- **New-stock output is off by default:** it is enabled only by a non-empty
  `RABBITMQ_OUTPUT_EXCHANGE` (which requires `RABBITMQ_URL`).
- **Port:** the gRPC server listens on `GRPC_PORT` (default `3500`), logged at startup as
  `stocker-store listening on :<port>`. `EXPOSE 3500` in the `Containerfile` reflects only
  that default.
- **`GetStocks` requires `limit > 0`:** an unset or `0` `limit` returns an empty list.
- **TopStocks limit/category:** a non-positive limit or an empty/unknown category returns an
  empty list (not an error).
- **Authoritative deployment variables** are the ones in the [Configuration](#configuration)
  section: `DATABASE_URL`, `DATABASE_USERNAME`, `DATABASE_PASSWORD`, `GRPC_PORT`,
  `STOCK_TTL`, and the `RABBITMQ_*` variables.
- **Queue flags are an assertion:** if you change `RABBITMQ_QUEUE` durability/exclusivity
  expectations (or previously declared it differently), the declaration fails with 406.
  Drain and delete the queue or choose a new name.

### Verifying RabbitMQ ingestion

A quick runbook for confirming that messages actually reach the `stocks`/`scores` tables.
It uses `rabbitmqadmin` from the RabbitMQ management plugin; substitute your own host and
credentials (`-H`, `-u`, `-p`, and `-V <vhost>` when not `/`).

Watch the service log:

```bash
podman logs stocker-store | grep -E 'database:|rabbitmq:'
```

**Healthy startup** should show, in order:

```
database: postgres://<user>@<host><path>                  # sanitized target — never the password
rabbitmq: output publisher disabled (RABBITMQ_OUTPUT_EXCHANGE not set)
stocker-store listening on :<port>
rabbitmq: consuming queue="stocker-store.stock.update.v1" exchange="stock.update" routing_key="stock.update.v1" prefetch=1
rabbitmq: connected exchange="stock.update" type="topic" queue="stocker-store.stock.update.v1" routing_key="stock.update.v1"
```

With `RABBITMQ_URL=` the fourth line is instead
`rabbitmq: ingestion disabled (RABBITMQ_URL explicitly empty)`. The output-disabled line is
present whenever `RABBITMQ_OUTPUT_EXCHANGE` is unset; output cannot be enabled without a
non-empty URL.

**Per-message success** — one line per applied message:

```
rabbitmq: applied symbol=<symbol> exchange=<exchange> scores=<n> inserted=<bool>
```

**Topology sanity** — the exchange, queue, and binding should exist with the documented
flags:

```bash
rabbitmqadmin -H <host> -u <user> -p <password> list exchanges name type durable
rabbitmqadmin -H <host> -u <user> -p <password> list queues name messages messages_ready messages_unacknowledged consumers
rabbitmqadmin -H <host> -u <user> -p <password> list bindings source destination routing_key
```

A healthy queue has a `consumers` count of at least `1` while the service is running. A
growing `messages_ready` with no consumers means the service is not consuming (check the
log lines above). `messages_unacknowledged = 1` with prefetch `1` is normal while a
message is being written.

**Declare the queue and binding by hand** (idempotent; useful when the service has never
connected and you want to prove the upstream is publishing):

```bash
rabbitmqadmin -H <host> -u <user> -p <password> declare queue name=stocker-store.stock.update.v1 durable=true
rabbitmqadmin -H <host> -u <user> -p <password> declare binding \
  source=stock.update destination=stocker-store.stock.update.v1 \
  destination_type=queue routing_key=stock.update.v1
```

**Payload sanity** — fetch one message without removing it, then inspect the first bytes:

```bash
rabbitmqadmin -H <host> -u <user> -p <password> get \
  queue=stocker-store.stock.update.v1 count=1 ackmode=ack_requeue_true \
  payload_file=/tmp/stocker-msg.bin
xxd /tmp/stocker-msg.bin | head
```

`ackmode=ack_requeue_true` leaves the message on the queue. A valid PROTOBUF `Stock` starts
with `0a` (field 1, `symbol`, length-delimited) followed by its length and UTF-8 bytes. JSON
starts with `7b 22` (`{"`) and will be skipped with a `decode stock message` reason. If the
service is running, it may claim the delivery first, so `get` can return nothing; stop the
service for a deterministic check, or accept a retry.

**Purge the queue** (destructive; drops everything currently queued, including anything the
service has not yet consumed):

```bash
rabbitmqadmin -H <host> -u <user> -p <password> purge queue name=stocker-store.stock.update.v1
```

**Failure lines and what they mean:**

| Log line (pattern) | Meaning |
| --- | --- |
| `rabbitmq: transient failure for symbol=<symbol>/<exchange>, retrying in <delay>: <err>` | The database failed with a transient error. The **same delivery** is retried with capped backoff (1 s → 30 s) while unacknowledged — nothing is lost; fix the database. |
| `rabbitmq: skipping message symbol=<symbol> exchange=<exchange> message_id=<id>: <reason>` | A **permanently invalid** message: undecodable payload, empty symbol/exchange, out-of-range or non-finite (`NaN`/`±Inf`) score, or a database data/constraint violation. Nacked without requeue and logged once; later messages are unaffected. |
| `rabbitmq: connect failed: <err>; retrying in <delay>` | The initial dial or topology declaration failed. Retried with capped backoff; the process stays up and gRPC keeps serving. If it repeats, the broker is down or unreachable (check the host, port, vhost, and credentials). |
| `rabbitmq: consumer stopped: <err>; reconnecting in <delay>` | An established session lost its channel/connection. The consumer reconnects with backoff; unacknowledged deliveries are redelivered. |
| `rabbitmq: failed to publish new stock <symbol>/<exchange>: <err>` | The optional output publish failed after one reconnect-and-retry. Logged only; the database write and ingestion ack succeeded. |
| `rabbitmq: publish failed; reconnecting and retrying once symbol=<symbol>` | The output publish failed and the publisher is reconnecting for its single retry. |

**Database sanity:**

```sql
SELECT symbol, exchange, timestamp FROM stocks ORDER BY timestamp DESC LIMIT 10;
SELECT symbol, exchange, category, value, timestamp FROM scores ORDER BY timestamp DESC LIMIT 10;
```

**Re-ingestion runbook.** Consumed messages are gone — AMQP has no replay of acknowledged
deliveries, and this service keeps no copy. Recovery is driven by the upstream publisher,
which republishes the **entire symbol universe** every cycle (default 24 hours; no deltas,
deletes, or snapshot markers). Therefore:

1. If the database was reset, leave the service running (or let its durable queue hold
   messages while it is down) and let the next upstream cycle re-ingest the universe. The
   upserts are idempotent, so re-applying everything is harmless.
2. To confirm a cycle independently, bind a **fresh temporary queue** to the same exchange
   with the same routing key and consume from it, then delete that queue:
   ```bash
   rabbitmqadmin ... declare queue name=stocker-store.reingest-$(date +%Y%m%d) durable=false
   rabbitmqadmin ... declare binding source=stock.update \
     destination=stocker-store.reingest-$(date +%Y%m%d) destination_type=queue routing_key=stock.update.v1
   # inspect with get … ackmode=ack_requeue_true, then remove the temporary queue
   rabbitmqadmin ... delete queue name=stocker-store.reingest-$(date +%Y%m%d)
   ```
   This observes the next cycle without disturbing the service's own queue.
3. **Never delete or re-arg the shared `stock.update` exchange.** It belongs to the upstream
   publisher; deleting it or redeclaring it with different flags breaks the upstream with
   `406 PRECONDITION_FAILED`. The only object this service owns is its queue (and the
   optional output exchange).

## Further Reading

- `docs/design/rabbitmq-ingestion.md` — the authoritative RabbitMQ contract: topology,
  configuration, failure semantics, and log lines.
- `docs/design.md` — the service design document (data model, gRPC API, store behaviour).
- `AGENTS.md` — agent-oriented project notes.
- `proto/v1/stock_store.proto` — the service and message definitions; `Stock` is also the
  RabbitMQ message schema.
- Source: `cmd/main.go`, `internal/{grpc,rabbitmq,events,store}`, `proto/v1`.
