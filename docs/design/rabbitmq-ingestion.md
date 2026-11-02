# RabbitMQ Ingestion — Design

## 1. Context

Stocker Store is a Go gRPC service backed by PostgreSQL. It stores `(symbol, exchange)`
stocks and dynamic score categories. Writes arrive over gRPC and, optionally, over
RabbitMQ from the upstream `stocker-list` service. This document defines the RabbitMQ
consumer and the optional new-stock publisher.

The broker is remote. Messages are binary protobuf. Delivery is at-least-once and the
store is idempotent (`INSERT ... ON CONFLICT`), so duplicate delivery is expected and
safe. The upstream publisher republishes the entire symbol universe every cycle
(default 24 hours); there are no deltas, deletes, or snapshot markers.

Ingestion is enabled by default against the shared broker. Setting `RABBITMQ_URL` to
the empty string explicitly disables ingestion and performs no dial.

## 2. Wire contract

### 2.1 Exchange and routing

| Item | Value |
| --- | --- |
| Exchange name | `stock.update` (`RABBITMQ_EXCHANGE`) |
| Exchange type | `topic` (`RABBITMQ_EXCHANGE_TYPE`) |
| Exchange flags | `durable=true`, `autoDelete=false`, `internal=false`, `noWait=false`, `args=nil` |
| Routing key | `stock.update.v1` (`RABBITMQ_ROUTING_KEY`) |
| Publisher flags | `mandatory=false`, `immediate=false`, `deliveryMode=2` (persistent) |

`stock.update.v1` is the **routing key** and the AMQP `type` property. It is **not**
the exchange name. Declaring an exchange named `stock.update.v1` creates a second,
empty exchange and receives nothing. Redeclaring `stock.update` with any different
flag or argument yields `406 PRECONDITION_FAILED` and closes the channel.

We declare the exchange with exactly the upstream flags on every connect, then declare
and bind our own queue. We never delete or re-arg the shared exchange.

Production uses exchange `stock.update`, type `topic`, routing key `stock.update.v1`.
Changing `RABBITMQ_EXCHANGE`, `RABBITMQ_EXCHANGE_TYPE`, or `RABBITMQ_ROUTING_KEY` only
works if the broker's existing exchange and the publisher's binding use the same
values. A mismatch fails the declaration with `406 PRECONDITION_FAILED` (exchange) or
silently routes nothing (routing key).

### 2.2 Our queue

| Item | Value |
| --- | --- |
| Queue name | `stocker-store.stock.update.v1` (`RABBITMQ_QUEUE`) |
| Queue flags | `durable=true`, `exclusive=false`, `autoDelete=false`, `noWait=false`, `args=nil` |
| Binding | `source=stock.update`, `destination=<queue>`, `key=stock.update.v1` |
| Prefetch | `1` (`RABBITMQ_PREFETCH`) |
| Consumer flags | `autoAck=false`, `exclusive=false`, `noLocal=false`, `noWait=false`, `args=nil` |

A durable queue plus persistent messages survive a broker restart. Because the queue
is durable and non-exclusive, it survives our process restart and accumulates messages
while we are down. Queue redeclaration is also an assertion: changing durability,
exclusivity, auto-delete, or arguments against an existing queue yields
`406 PRECONDITION_FAILED`. To change those flags, drain and delete the queue, or use a
new `RABBITMQ_QUEUE` name.

### 2.3 Message properties

Every upstream message carries these properties:

| Property | Value |
| --- | --- |
| `content_type` | `application/x-protobuf` |
| `content_encoding` | empty |
| `message_id` | the stock symbol only (for example `AC`) — **not unique** |
| `type` | `stock.update.v1` |
| `timestamp` | publish time, Unix seconds granularity |
| `headers` | `{"symbol": <symbol>, "exchange": <exchange>}` |
| `priority` | `0` |
| `correlation_id`, `reply_to`, `expiration`, `app_id`, `user_id` | unset |
| `body` | `proto.Marshal` of `stockstore.v1.Stock` |

### 2.4 Payload

The body is the bare protobuf message defined in `proto/v1/stock_store.proto`:

```protobuf
message Stock {
  string symbol   = 1;
  string exchange = 2;
  repeated ScoreEntry scores = 3;
}
```

Upstream sets only `symbol` and `exchange`; `scores` is always absent. There is no
envelope, no JSON, and no compression. A `scores` field may appear from another
producer; it is validated like any gRPC write.

Because `scores` is absent, a message is a **scoreless upsert**. `Store.UpdateStock`
skips the score loop when the map is empty and only refreshes `stocks.timestamp`;
existing score rows keep their values and timestamps. A scoreless message never
deletes scores. The only deletions are `RemoveStock` and retention.

`message_id` is the symbol only, so it collides across exchanges. Deduplicate on
`(symbol, exchange)` — which is the store's primary key.

### 2.5 Identifiers the consumer must not rely on

There is no per-message sequence number, revision, snapshot marker, or deletion
signal. Ordering across symbols is not guaranteed. The AMQP `timestamp` is seconds
precision and is regenerated on a publisher retry, so it is not a deduplication key.

## 3. Topology

```
                    ┌──────────────────────────────────────────┐
                    │ RabbitMQ broker                          │
                    │                                          │
 stocker-list ─────►│ exchange "stock.update" (topic, durable) │
   publish          │        │ routing key "stock.update.v1"   │
                    │        ▼                                 │
                    │ queue "stocker-store.stock.update.v1"    │
                    │        (durable, prefetch 1)             │
                    └────────┬─────────────────────────────────┘
                             │ deliver (autoAck=false)
                             ▼
                    ┌──────────────────────┐      upsert      ┌────────────┐
                    │ stocker-store        │─────────────────►│ PostgreSQL │
                    │ RabbitMQ consumer    │                  │ stocks /   │
                    │ ack after write      │                  │ scores     │
                    └──────────┬───────────┘                  └────────────┘
                               │ inserted?
                               ▼ optional
                    exchange $RABBITMQ_OUTPUT_EXCHANGE
                    routing key $RABBITMQ_OUTPUT_ROUTING_KEY
```

The output publisher declares only its exchange. Consumers of that exchange own their
queues and bindings.

## 4. Configuration

All configuration is environment based. `RABBITMQ_URL` uses `os.LookupEnv`: an absent
variable selects the default broker, while an explicitly empty value disables
ingestion. Every other variable treats empty as absent and falls back to its default.

The URI path selects the vhost; the default URL's path is `/`, so the default vhost is
`/`. A URL such as `amqp://user:pass@host:5672/stocks` selects vhost `stocks`.

| Variable | Default | Validation / behaviour |
| --- | --- | --- |
| `RABBITMQ_URL` | `amqp://guest:guest@192.168.1.48:5672/` | Must parse as `amqp://` or `amqps://`. Explicitly empty disables ingestion (no dial). |
| `RABBITMQ_EXCHANGE` | `stock.update` | Non-empty; empty falls back to the default. |
| `RABBITMQ_EXCHANGE_TYPE` | `topic` | Trimmed and lowercased; one of `direct`, `topic`, `fanout`, `headers`; anything else is a startup error. |
| `RABBITMQ_ROUTING_KEY` | `stock.update.v1` | Non-empty; empty falls back to the default. |
| `RABBITMQ_QUEUE` | `stocker-store.stock.update.v1` | Non-empty; empty falls back to the default. |
| `RABBITMQ_PREFETCH` | `1` | Integer `>= 1`; anything else is a startup error. |
| `RABBITMQ_OUTPUT_EXCHANGE` | unset | Output is **disabled** unless this is explicitly set to a non-empty value. |
| `RABBITMQ_OUTPUT_EXCHANGE_TYPE` | `topic` | Same validation as `RABBITMQ_EXCHANGE_TYPE`; used when output is enabled. |
| `RABBITMQ_OUTPUT_ROUTING_KEY` | `stock.created.v1` | Must differ from the input routing key when the output exchange equals the input exchange. |

Additional validation, all fatal at startup:

- `RABBITMQ_URL` parses successfully; parse errors are redacted so credentials never
  reach the log.
- If output is enabled, `RABBITMQ_URL` must be non-empty.
- Output must not form a direct self-loop: the same exchange **and** the same routing
  key as the input is rejected. Topic-pattern overlaps remain the operator's
  responsibility and are documented in the output section.

A broker that is unreachable at startup is **not** a configuration error. The process
stays up, logs the failure, and reconnects with capped exponential backoff. Only
malformed configuration is fatal.

## 5. Consumption lifecycle

1. **Load configuration.** Validate as in §4. A validation error is fatal.
2. **Disabled check.** If `RABBITMQ_URL` is explicitly empty, log the disabled state
   and return without dialing.
3. **Dial.** `amqp.Dial(url)`. On failure, log and retry with backoff
   (1 s doubling to 30 s). The process does not exit.
4. **Open a channel.**
5. **Declare the exchange.** `ExchangeDeclare(exchange, type, true, false, false, false, nil)`.
6. **Declare the queue.** `QueueDeclare(queue, true, false, false, false, nil)`.
7. **Bind.** `QueueBind(queue, routingKey, exchange, false, nil)`.
8. **Set prefetch.** `Qos(prefetch, 0, false)`.
9. **Consume.** `Consume(queue, "", false, false, false, false, nil)`.
10. **Process one delivery at a time.** Decode protobuf, validate, upsert, optionally
    publish a new-stock event, then acknowledge.
11. **Ack.** `delivery.Ack(false)` only after the store write succeeds. A publish
    failure is logged but does not prevent the ack, because the database is
    authoritative.
12. **Permanent failure.** Log the reason and `delivery.Nack(false, false)`. The
    message is dropped, or dead-lettered if an operator policy attaches a dead-letter
    target to our queue.
13. **Transient failure.** Retry the same delivery in-process with capped exponential
    backoff (1 s doubling to 30 s) while it remains unacknowledged. Do not requeue it,
    which would create a hot loop.
14. **Connection loss.** Close the channel and connection, log, wait with backoff, and
    return to step 3. Unacknowledged deliveries are requeued by the broker and
    redelivered; the upsert is idempotent.
15. **Shutdown.** On context cancellation, stop consuming, close the channel and
    connection, and return. Unacknowledged deliveries are requeued.

The consumer is a single worker. Prefetch `1` means at most one unacknowledged
delivery at a time, which keeps the retry path simple and bounds in-flight work.

## 6. Failure and semantics matrix

| Event | Detection | Action | Result |
| --- | --- | --- | --- |
| Success | decode, validation, and `UpdateStock` succeed | `Ack(false)` | At-least-once; a crash before ack causes redelivery. |
| Scoreless message | `scores` empty or absent | Normal upsert and ack | Existing scores are preserved; stock timestamp is refreshed. |
| Undecodable payload | `proto.Unmarshal` fails | Validation error, nack without requeue | Dropped (or dead-lettered by policy); logged once. |
| Missing symbol/exchange | validation | Validation error, nack without requeue | Dropped (or dead-lettered by policy); logged once. |
| Non-finite or out-of-range score | validation | Validation error, nack without requeue | Dropped (or dead-lettered by policy); logged once. |
| Database data/integrity violation | PG SQLSTATE class `22` or `23` | Permanent, nack without requeue | Dropped (or dead-lettered by policy); logged once. |
| Transient database/network error | any other error (including class `40`) | Retry the same delivery with backoff while unacked | No loss; delivery stays unacknowledged. |
| Channel/connection loss | consume channel closes or ack/declare fails | Close, log, reconnect with backoff | Unacked deliveries are redelivered; duplicates are safe. |
| Broker unreachable | dial error | Log and retry with backoff | Process stays up; gRPC and retention continue. |
| Publish of a new-stock event fails | publisher error | Log and continue | Ingestion still acks; output is best-effort. |
| Shutdown | context cancelled | Close channel and connection | Unacked deliveries are requeued. |

Duplicate delivery is expected from publisher retries, broker restarts, and our own
reconnect path. Every write is an idempotent upsert keyed by `(symbol, exchange)`.

Dead-lettering is not configured by this service. An operator may attach a policy to
our queue. Never add `x-*` arguments or an alternate exchange to the shared
`stock.update` exchange: that would break the upstream declaration with a 406.

## 7. Optional output publisher

The output publisher is **disabled by default**. It is enabled only when
`RABBITMQ_OUTPUT_EXCHANGE` is explicitly set to a non-empty value. It emits one
`stockstore.v1.Stock` when a write **inserts** a new `(symbol, exchange)` row (the
`inserted` flag from `Store.UpdateStock`), not on updates.

The contract lives in `internal/events` as `Publisher` (`Publish` and `Close`), with
`events.Noop` for the disabled case. `internal/rabbitmq.Publisher` implements it.
`internal/grpc` and the consumer depend only on `internal/events`, never on the broker
package.

- Declares `RABBITMQ_OUTPUT_EXCHANGE` with `RABBITMQ_OUTPUT_EXCHANGE_TYPE`
  (default `topic`), `durable=true`, `autoDelete=false`, `internal=false`, `noWait=false`,
  `args=nil`.
- Publishes with routing key `RABBITMQ_OUTPUT_ROUTING_KEY` (default `stock.created.v1`),
  `mandatory=false`, `immediate=false`, `deliveryMode=2`.
- Properties: `content_type=application/x-protobuf`, `message_id=<symbol>`,
  `type=<output routing key>`, `timestamp=now UTC`, headers
  `{"symbol": <symbol>, "exchange": <exchange>}`.
- Enables publisher confirms and waits for each confirm. A failed or unconfirmed
  publish tears down the connection and retries once. A second failure is returned to
  the caller, which logs it.
- Connects lazily on the first publish. It declares no queue and no binding.
- The payload is the authoritative stored stock, including all scores and their
  server-stamped `updated_at` values. This differs from the inbound upstream payload,
  which carries only symbol and exchange.

The default output routing key is deliberately different from the input routing key.
Pointing the output exchange at `stock.update` with `stock.update.v1` would feed our
own queue and create a loop. The same-exchange/same-key case is rejected at startup;
broader topic overlaps are the operator's responsibility. The service logs a warning
when output and input share an exchange name.

Output is best-effort: a failure never fails the gRPC response or the RabbitMQ
ingestion ack.

## 8. Observability

The service uses the standard `log` package. All identifiers are symbols, exchanges,
queues, and routing keys; delivery is tracked per message, not by any stream position.
The URL is never logged.

| Log line | When |
| --- | --- |
| `rabbitmq: ingestion disabled (RABBITMQ_URL explicitly empty)` | Startup with ingestion disabled. |
| `rabbitmq: consuming queue=%q exchange=%q routing_key=%q prefetch=%d` | Startup, before connecting. |
| `rabbitmq: connected exchange=%q type=%q queue=%q routing_key=%q` | Channel, exchange, queue, and binding are ready. |
| `rabbitmq: connect failed: %v; retrying in %s` | Dial or setup failed; retrying. |
| `rabbitmq: consumer stopped: %v; reconnecting in %s` | An established consumer lost its channel/connection. |
| `rabbitmq: applied symbol=%s exchange=%s scores=%d inserted=%v` | One line per successfully stored message. |
| `rabbitmq: skipping message symbol=%s exchange=%s message_id=%s: %v` | Permanent failure; message nacked without requeue. |
| `rabbitmq: transient failure for symbol=%s/%s, retrying in %s: %v` | Transient failure; same delivery retried. |
| `rabbitmq: failed to publish new stock %s/%s: %v` | Output publish failed; ingestion continued. |
| `rabbitmq: output publisher disabled (RABBITMQ_OUTPUT_EXCHANGE not set)` | Startup with output disabled. |
| `rabbitmq: output exchange equals input exchange; verify the routing key does not create a feedback loop` | Startup with output enabled while `RABBITMQ_OUTPUT_EXCHANGE` equals `RABBITMQ_EXCHANGE`. |
| `rabbitmq: publish failed; reconnecting and retrying once symbol=%s` | Output publish failed; one reconnect-and-retry. |

Operational checks use broker tooling (`rabbitmqadmin list queues`, `list bindings`,
`get`, `purge`) rather than stream-position tooling. Queue depth, ready count,
unacknowledged count, and consumer count are the useful signals.

## 9. Security

- Credentials live in `RABBITMQ_URL`; the URL is never logged. AMQP URI parse errors
  are unwrapped from `*url.Error` so the embedded password is not printed.
- Prefer `amqps://` for remote brokers. `amqp.Dial` performs the TLS handshake for the
  `amqps` scheme and honours the standard URI query parameters.
- Stock RabbitMQ refuses the `guest` account from non-loopback addresses. A remote
  deployment must supply a dedicated user and vhost.
- Grant the minimum permissions on the vhost: the service needs to declare and bind
  its own queue, read from it, read the input exchange to bind, and declare/write the
  output exchange when output is enabled. Do not grant administrator rights.
- No credentials are committed to the repository; deployment env files are
  operator-created.

## 10. Non-goals

- No protobuf or generated-code changes. The wire type stays
  `stockstore.v1.Stock`.
- No envelope, JSON, compression, or schema negotiation.
- No deletion or snapshot messages; stale stocks are aged out by `STOCK_TTL` on our
  side.
- No scores from the upstream publisher; scoreless messages preserve existing scores.
- No exactly-once delivery; at-least-once plus idempotent upserts is the contract.
- No multiple consumer workers; one queue and prefetch `1`.
- No dead-letter exchange, alternate exchange, or `x-*` arguments on the shared
  exchange. Dead-lettering remains an operator policy on our own queue.
- No metrics or health endpoint; logs plus broker queue metrics are the observability
  surface.
- No broker-side replay of consumed messages. The upstream full-universe cycle is the
  recovery mechanism.
