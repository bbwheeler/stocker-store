# RabbitMQ Migration Plan

> **Temporary execution artifact.** This plan is written for the developer agents who
> perform the migration. It necessarily names the legacy broker package and contains
> the final zero-reference sweep commands. The final step removes this file; the
> permanent architecture is documented in `docs/design/rabbitmq-ingestion.md`.

## Goal

Replace every broker integration in the repository with RabbitMQ:

- New `internal/rabbitmq` consumer and publisher on
  `github.com/rabbitmq/amqp091-go v1.16.0`.
- A neutral `internal/events.Publisher` contract owned by neither the broker package
  nor `internal/grpc`.
- Removal of the legacy broker package, dependency, environment variables, docs,
  deployment ordering, and plans.
- At the end, `git grep -ri kafka` and `go list -m all | grep -i kafka` return nothing.

## Hard constraints

- Do **not** change `proto/v1/stock_store.proto` or generated Go files. The wire type
  stays `stockstore.v1.Stock`.
- Preserve the upstream wire contract exactly: exchange `stock.update` (topic,
  durable), routing key/type `stock.update.v1`, binary protobuf, persistent delivery.
- Every step must leave the tree buildable and all tests passing.
- Each step is executed by one developer agent and committed separately.
- Use `test-driven-development` for behaviour changes and
  `verification-before-completion` before every commit.
- Do not introduce broker-shaped naming (offsets, partitions, consumer groups) in
  new code or docs.

## Standard verification (run at the end of every step)

```bash
gofmt -l .          # must print nothing; run gofmt -w on any listed file
go build ./...
go vet ./...
go test ./...
```

---

## Step 1 — Add the neutral publisher contract and decouple gRPC

**Goal:** Move the publisher interface out of the broker package so `internal/grpc`
never imports a broker implementation.

**Files**

- Create `internal/events/publisher.go`.
- Modify `internal/grpc/server.go`.
- Modify `internal/grpc/server_test.go`.

**Implementation**

1. `internal/events/publisher.go`:
   - `package events`.
   - `type Publisher interface { Publish(ctx context.Context, stock *stockv1.Stock) error; Close() error }`
     with a doc comment stating that `Publish` is best-effort and `Close` releases
     resources.
   - `type Noop struct{}` with `Publish` returning `nil` and `Close` returning `nil`.
   - Import `context` and `stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"`.
2. `internal/grpc/server.go`:
   - Replace the broker import with `internal/events`.
   - Change the `Server.publisher` field and `NewServer` parameter to
     `events.Publisher`.
   - Replace the nil fallback with `events.Noop{}`.
   - Do not change handler behaviour or publish call sites.
3. `internal/grpc/server_test.go`:
   - Replace the broker import with `internal/events`.
   - Change `fakePublisher` assertions and `newTestClient` to use `events.Publisher`.
   - Replace every `kafka.Noop{}` value with `events.Noop{}`.
   - Keep all publisher-wiring tests and their assertions unchanged.

The legacy package still exists and its `Publisher` interface has the same method set,
so `cmd/main.go` continues to compile during this step.

**Tests**

- Existing gRPC publisher tests must pass unchanged in behaviour:
  publish once on insert, no publish on update, no publish when the store write fails,
  and a publish error does not fail the response.
- Add `var _ events.Publisher = (*fakePublisher)(nil)`.

**Verify:** standard verification, plus:

```bash
go test ./internal/grpc -run Publish -v
```

---

## Step 2 — Add RabbitMQ configuration and the consumer

**Goal:** Implement the RabbitMQ consumer, its environment configuration, and unit
tests behind an injectable AMQP seam. Do not wire it into `cmd/main.go` yet.

**Files**

- Modify `go.mod` and `go.sum` (add `github.com/rabbitmq/amqp091-go v1.16.0`,
  for example with `go get github.com/rabbitmq/amqp091-go@v1.16.0`).
- Create `internal/rabbitmq/config.go`.
- Create `internal/rabbitmq/amqp.go`.
- Create `internal/rabbitmq/consumer.go`.
- Create `internal/rabbitmq/config_test.go`.
- Create `internal/rabbitmq/consumer_test.go`.

**Implementation**

### Configuration (`config.go`)

- Constants: `defaultURL = "amqp://guest:guest@192.168.1.48:5672/"`,
  `defaultExchange = "stock.update"`, `defaultExchangeType = "topic"`,
  `defaultRoutingKey = "stock.update.v1"`,
  `defaultQueue = "stocker-store.stock.update.v1"`, `defaultPrefetch = 1`,
  `defaultOutputExchangeType = "topic"`,
  `defaultOutputRoutingKey = "stock.created.v1"`.
- `type Config struct { URL, Exchange, ExchangeType, RoutingKey, Queue string;
  Prefetch int; OutputExchange, OutputExchangeType, OutputRoutingKey string }`.
- `func LoadConfig() (Config, error)`:
  - `RABBITMQ_URL` uses `os.LookupEnv`: absent → default; present empty → `""`
    (disabled); present non-empty → that value, validated with `amqp.ParseURI`.
  - `RABBITMQ_EXCHANGE`, `RABBITMQ_ROUTING_KEY`, `RABBITMQ_QUEUE`,
    `RABBITMQ_EXCHANGE_TYPE` use `getEnv(key, fallback)` where an empty value falls
    back to the default.
  - `RABBITMQ_PREFETCH`: parse a base-10 integer; empty → `1`; anything `< 1` or
    non-numeric is an error.
  - `RABBITMQ_OUTPUT_EXCHANGE` uses `LookupEnv`: absent or empty → output disabled.
  - `RABBITMQ_OUTPUT_EXCHANGE_TYPE` and `RABBITMQ_OUTPUT_ROUTING_KEY` use
    `getEnv` with their defaults.
  - Trim surrounding whitespace from every string value. A whitespace-only value is
    treated as empty (default fallback, or output disabled).
  - Exchange types are trimmed and lowercased; only
    `direct|topic|fanout|headers` are accepted.
  - If output is enabled and `URL == ""`, return an error.
  - If output is enabled and
    `OutputExchange == Exchange && OutputRoutingKey == RoutingKey`, return an error
    (self-feeding loop).
  - On `amqp.ParseURI` error, unwrap `*url.Error` so credentials embedded in the URI
    never appear in the returned error.
  - Add `func (c Config) IngestionEnabled() bool { return c.URL != "" }` and
    `func (c Config) OutputEnabled() bool { return c.OutputExchange != "" }`.
- Add `func (c Config) validate() error` for programmatic misuse; `LoadConfig` calls
  it.

### AMQP seam (`amqp.go`)

Define unexported interfaces so tests inject fakes:

```go
type connection interface {
    Channel() (channel, error)
    Close() error
}

type confirmation interface {
    WaitContext(ctx context.Context) (bool, error)
}

type channel interface {
    ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
    QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
    QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
    Qos(prefetchCount, prefetchSize int, global bool) error
    Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
    NotifyClose(c chan *amqp.Error) chan *amqp.Error
    Confirm(noWait bool) error
    PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error)
    IsClosed() bool
    Close() error
}

type dialFunc func(string) (connection, error)
```

- Add `amqpConnection` and `amqpChannel` adapters over `*amqp.Connection` and
  `*amqp.Channel`. The adapter is required because `*amqp.Channel` returns a concrete
  `*amqp.DeferredConfirmation`, which cannot satisfy an interface method returning the
  `confirmation` interface.
- `defaultDial` calls `amqp.Dial`.

### Consumer (`consumer.go`)

- `type Store interface { UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) }`.
- `type Consumer struct` holds `cfg Config`, `store Store`, `publisher events.Publisher`,
  `decoder func([]byte) (*stockv1.Stock, error)`, `dial dialFunc`,
  `sleep func(context.Context, time.Duration) bool`, `retryMin`, `retryMax`.
- `func New(cfg Config, store Store, publisher events.Publisher) *Consumer`:
  nil publisher → `events.Noop{}`; decoder → `decodeStock`; dial → `defaultDial`;
  sleep → `sleepContext`; retry bounds 1 s and 30 s.
- `func (c *Consumer) Run(ctx context.Context) error`:
  - If `!cfg.IngestionEnabled()`, return `nil` (the caller logs the disabled state).
  - Return `cfg.validate()` for programmatic misconfiguration.
  - Loop: `connected, err := c.consumeOnce(ctx)`. On context cancellation return
    `nil`. If `connected`, log
    `rabbitmq: consumer stopped: %v; reconnecting in %s`; otherwise log
    `rabbitmq: connect failed: %v; retrying in %s`. Sleep with capped backoff (start
    1 s, double to 30 s) and retry. Reset the delay after a session that successfully
    connected.
- `consumeOnce(ctx) (connected bool, err error)`:
  - Dial; open a channel; defer close of both.
  - `ExchangeDeclare(cfg.Exchange, cfg.ExchangeType, true, false, false, false, nil)`.
  - `QueueDeclare(cfg.Queue, true, false, false, false, nil)`.
  - `QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, false, nil)`.
  - `Qos(cfg.Prefetch, 0, false)`.
  - `Consume(cfg.Queue, "", false, false, false, false, nil)`.
  - Log `rabbitmq: connected exchange=%q type=%q queue=%q routing_key=%q` and mark the
    session connected.
  - Loop over `select { case <-ctx.Done(): return nil; case amqpErr, ok := <-notify: ...; case d, ok := <-deliveries: ... }`.
    A closed deliveries channel or a broker close notification returns an error so
    `Run` reconnects.
  - For each delivery call `processDelivery`.
- `processDelivery`:
  - Retry loop around `handle`.
  - Success: `delivery.Ack(false)`. An ack error is returned so the connection is torn
    down and the message is redelivered.
  - Permanent error: log
    `rabbitmq: skipping message symbol=%s exchange=%s message_id=%s: %v`, then
    `delivery.Nack(false, false)`, then return `nil`. If the nack fails, return the
    error so the session reconnects and the broker redelivers.
  - Transient error: log
    `rabbitmq: transient failure for symbol=%s/%s, retrying in %s: %v`, sleep with
    capped backoff while the delivery remains unacked; on context cancellation return
    the context error without acking.
- `handle`:
  - Decode protobuf; wrap decode/validation errors with `errInvalidMessage`.
  - Require non-empty `symbol` and `exchange`.
  - Validate every score is finite and within `[-1.0, 1.0]` (scores are normally
    absent).
  - `store.UpdateStock(ctx, symbol, exchange, toMap(scores))`.
  - Log `rabbitmq: applied symbol=%s exchange=%s scores=%d inserted=%v`.
  - If `inserted`, call `publisher.Publish(ctx, toProto(stored))`; log
    `rabbitmq: failed to publish new stock %s/%s: %v` on error and continue.
- `isPermanent`: `errors.Is(err, errInvalidMessage)` or a `*pgconn.PgError` whose
  SQLSTATE starts with `22` or `23`. Everything else is transient (including class
  `40`).
- Reuse the existing helpers `sleepContext`, `prefixBytes`, `toMap`, `toProto` with
  the same semantics; `toMap(nil)` returns a nil map.
- Never log the broker URL.

**Tests**

`config_test.go`:

- Defaults when no `RABBITMQ_*` variable is set. Use a helper that records and clears
  all relevant variables with `t.Cleanup`.
- `RABBITMQ_URL` absent vs explicitly empty (`LookupEnv` distinction).
- Invalid exchange type and invalid prefetch values are errors.
- Output disabled by default; enabled by a non-empty `RABBITMQ_OUTPUT_EXCHANGE`.
- Output with empty URL is an error; same exchange plus same routing key is an error.
- URL parse error redacts the password.

`consumer_test.go` (fakes: scripted connection/channel, fake `amqp.Acknowledger` on
`amqp.Delivery`, scripted store, injected sleep):

- Exchange declaration flags are exactly `(name, kind, durable=true,
  autoDelete=false, internal=false, noWait=false, args=nil)`.
- Queue declaration flags are exactly `(name, durable=true, autoDelete=false,
  exclusive=false, noWait=false, args=nil)`; bind uses `(queue, routingKey, exchange,
  noWait=false, args=nil)`.
- `Qos` receives `(prefetch, 0, false)`.
- A successful message acks exactly once after the store write.
- A transient store error is retried with delays `1s`, `2s`, `4s`, … capped at `30s`,
  then acked on success.
- A permanent decode/validation/data error is nacked with `requeue=false` and is not
  retried.
- Missing symbol/exchange, non-finite scores, and out-of-range scores are permanent.
- A scoreless message calls the store with a nil map and still acks.
- Cancelling the context during a transient retry returns without acking.
- A closed delivery channel causes a redial; the next session consumes successfully.
- Cancelling the context closes the channel and connection.
- Run with an explicitly empty URL returns `nil` without dialing.
- Log assertions for the applied, skip, transient, connected, and reconnect lines.

**Verify:** standard verification, plus:

```bash
go test ./internal/rabbitmq -v
```

---

## Step 3 — Add the RabbitMQ publisher and integration test

**Goal:** Implement the optional new-stock publisher and a broker-gated integration
test. Still do not wire it into `cmd/main.go`.

**Files**

- Create `internal/rabbitmq/publisher.go`.
- Create `internal/rabbitmq/publisher_test.go`.
- Create `internal/rabbitmq/integration_test.go`.

**Implementation**

- `type Publisher struct` holds the output exchange, type, routing key, URL, dial
  seam, mutex, connection, channel, and closed flag.
- `func NewPublisher(cfg Config) (events.Publisher, error)`:
  - If `!cfg.OutputEnabled()`, log
    `rabbitmq: output publisher disabled (RABBITMQ_OUTPUT_EXCHANGE not set)` and
    return `events.Noop{}`.
  - Validate the URL with `amqp.ParseURI`, redacting `*url.Error`.
  - If `cfg.OutputExchange == cfg.Exchange`, log a warning:
    `rabbitmq: output exchange equals input exchange; verify the routing key does not create a feedback loop`.
  - Perform no network I/O; dial on the first publish.
- `Publish(ctx, stock)`:
  - Nil stock returns an error.
  - `proto.Marshal`.
  - Build `amqp.Publishing` with:
    - `ContentType: "application/x-protobuf"`
    - `DeliveryMode: amqp.Persistent`
    - `MessageId: stock.Symbol`
    - `Type: cfg.OutputRoutingKey`
    - `Timestamp: time.Now().UTC()`
    - `Headers: amqp.Table{"symbol": stock.Symbol, "exchange": stock.Exchange}`
    - `Body: body`
  - Ensure a live channel: dial, open channel, declare the output exchange with
    `(name, type, durable=true, autoDelete=false, internal=false, noWait=false, args=nil)`,
    enable confirms with `Confirm(false)`.
  - Publish with `mandatory=false`, `immediate=false`, and wait for the deferred
    confirmation.
  - On publish/confirm failure: log
    `rabbitmq: publish failed; reconnecting and retrying once symbol=%s`, close the
    resources, reconnect, and retry the same `amqp.Publishing` once. A second failure
    is returned wrapped as `publish to rabbitmq exchange %q: %w`.
  - Honour context cancellation before dialing, before publishing, and after a
    failure; a cancelled context means no retry.
  - A publish after `Close` returns an error.
- `Close()` closes the channel and connection and is idempotent.
- Add `var _ events.Publisher = (*Publisher)(nil)`.

**Tests**

Unit tests live in `package rabbitmq` so they can inject the unexported seams. The
integration test lives in `package rabbitmq_test` and uses only the exported API.

`publisher_test.go` (fake connection/channel/confirmation):

- Disabled configuration returns `events.Noop` and never dials.
- Exchange declaration flags are exact.
- Message properties: content type, persistent delivery, message id equals the
  symbol, type equals the output routing key, timestamp is set, headers contain
  `symbol` and `exchange`, body round-trips through `proto.Unmarshal`.
- Confirms are enabled once per connection and awaited; `acked=false` is an error.
- A first publish failure tears down, redials, and succeeds on the retry.
- Two consecutive failures return an error and make no third attempt.
- A closed channel triggers a fresh dial on the next publish.
- `Publish(nil)` returns an error.
- `Close` closes resources and is safe to call twice.
- A URL parse error redacts credentials.
- Context cancellation prevents a dial/retry.

`integration_test.go`:

- Skip cleanly when `RABBITMQ_TEST_URL` is unset:
  `t.Skip("RABBITMQ_TEST_URL not set; skipping RabbitMQ integration test")`.
- Use a unique throwaway exchange and queue derived from
  `time.Now().UnixNano()` and `os.Getpid()`; never touch `stock.update`.
- Declare the exchange, queue, and binding; start the consumer in a goroutine against
  an in-memory fake `Store`; publish one `Stock` with the publisher; assert the fake
  store receives the correct symbol/exchange within a timeout; cancel the consumer and
  assert it returns; delete the queue and exchange in cleanup.
- The default `go test ./...` run must show this test as skipped, not failed.

**Verify:** standard verification, plus:

```bash
go test ./internal/rabbitmq -run Integration -v   # SKIP without RABBITMQ_TEST_URL
```

---

## Step 4 — Wire `cmd/main.go` to RabbitMQ

**Goal:** Replace the legacy subscriber and publisher wiring with the new consumer and
publisher while keeping the tree buildable. The legacy package remains on disk until
Step 5.

**Files**

- Modify `cmd/main.go`.
- Modify `cmd/main_test.go` (add a disabled-ingestion test).

**Implementation**

1. Imports: replace the legacy broker import with `internal/events` and
   `internal/rabbitmq`.
2. After the store is created:
   - `cfg, err := rabbitmq.LoadConfig()`; on error `log.Fatalf("rabbitmq config: %v", err)`.
   - `publisher, err := rabbitmq.NewPublisher(cfg)`; on error
     `log.Fatalf("rabbitmq publisher: %v", err)`.
   - `server := grpc.NewServer(st, publisher)`.
3. Replace `runKafkaSubscriber` with:

   ```go
   func runRabbitMQConsumer(ctx context.Context, st stockStore, publisher events.Publisher, cfg rabbitmq.Config) error {
       if !cfg.IngestionEnabled() {
           log.Printf("rabbitmq: ingestion disabled (RABBITMQ_URL explicitly empty)")
           return nil
       }
       log.Printf("rabbitmq: consuming queue=%q exchange=%q routing_key=%q prefetch=%d",
           cfg.Queue, cfg.Exchange, cfg.RoutingKey, cfg.Prefetch)
       return rabbitmq.New(cfg, st, publisher).Run(ctx)
   }
   ```

4. Call it after the gRPC server and retention loop start:
   `if err := runRabbitMQConsumer(ctx, st, publisher, cfg); err != nil { log.Fatalf("rabbitmq consumer: %v", err) }`.
5. Delete `envList` and its comment.
6. Update the `stockStore` comment to say "ingest broker messages".
7. Keep the shutdown sequence: `GracefulStop`, then `publisher.Close()`, then `cancel()`.
8. Update the comment above publisher construction: it is a no-op unless
   `RABBITMQ_OUTPUT_EXCHANGE` is set.

**Tests**

- Add `TestRunRabbitMQConsumerDisabled`: a config with an explicitly empty URL and nil
  store/publisher returns `nil` and does not panic.
- Existing `GRPC_PORT` and DSN tests remain unchanged.

**Verify:** standard verification, plus:

```bash
go test ./cmd -v
```

The tree still builds because `internal/kafka` remains present and compiles, even
though `cmd/main.go` no longer imports it.

---

## Step 5 — Delete the legacy package and dependencies

**Goal:** Remove the old broker implementation and its module dependencies.

**Files**

- Delete `internal/kafka/` (all files).
- Modify `go.mod`, `go.sum` via `go mod tidy`.

**Implementation**

1. `rm -rf internal/kafka`.
2. `go mod tidy`.
3. Confirm `github.com/rabbitmq/amqp091-go v1.16.0` is a direct requirement and the
   legacy client plus its compression indirects are gone.
4. Do not touch docs in this step; Step 6 and Step 7 handle them.

**Tests**

- All tests must pass without the deleted package.

**Verify**

```bash
gofmt -l .
go build ./...
go vet ./...
go test ./...
go mod tidy
git diff --exit-code go.mod go.sum   # no diff after a second tidy
go list -m all | grep -i kafka       # expected: no output
```

Also confirm no Go source imports the deleted package:

```bash
grep -rn "internal/kafka" --include='*.go' .   # expected: no output
```

---

## Step 6 — Rewrite README and fix deployment metadata

**Goal:** Make the operator-facing documentation describe RabbitMQ only, and fix stale
deployment references.

**Files**

- Rewrite `README.md`.
- Modify `deploy/quadlet/stocker-store.container`.

**README requirements**

- Title, description, tech line, and ASCII diagram: RabbitMQ (`amqp091-go`), not the
  legacy broker. Show `stocker-list → exchange → queue → stocker-store → PostgreSQL`
  and the optional output exchange.
- Overview and features: ingestion is enabled by default against the shared broker;
  `RABBITMQ_URL=` disables it. At-least-once, ack-after-write, transient retry, poison
  skip, automatic reconnect. Output publisher is disabled unless
  `RABBITMQ_OUTPUT_EXCHANGE` is set.
- Keep the gRPC API section unchanged except for removing broker references.
- Replace the legacy ingestion section with a RabbitMQ section:
  - Contract table: exchange name/type/flags, routing key, queue name/flags, binding,
    prefetch, payload, persistent delivery, properties.
  - Explicit warning: `stock.update.v1` is the routing key and `type` property, not
    the exchange name.
  - Message schema: `stockstore.v1.Stock`, binary protobuf, `scores` normally absent.
  - Validation and delivery semantics: ack after write, nack without requeue for
    permanent failures, in-process retry for transient failures while unacked,
    reconnect on connection loss, at-least-once and idempotent.
  - Document that a scoreless message preserves existing scores and refreshes only the
    stock timestamp.
  - New-stock output: disabled by default, explicit enabling variable, default
    `stock.created.v1` routing key, loop warning, best-effort semantics.
- Configuration table: `DATABASE_URL`, `DATABASE_USERNAME`, `DATABASE_PASSWORD`,
  `GRPC_PORT`, `STOCK_TTL`, `RABBITMQ_URL`, `RABBITMQ_EXCHANGE`,
  `RABBITMQ_EXCHANGE_TYPE`, `RABBITMQ_ROUTING_KEY`, `RABBITMQ_QUEUE`,
  `RABBITMQ_PREFETCH`, `RABBITMQ_OUTPUT_EXCHANGE`,
  `RABBITMQ_OUTPUT_EXCHANGE_TYPE`, `RABBITMQ_OUTPUT_ROUTING_KEY`.
  State the `LookupEnv` rule: absent URL → default broker; explicitly empty URL →
  ingestion disabled; all other empty values fall back to defaults.
- Replace the sample `.env.podman` broker block with the RabbitMQ variables.
- Deployment prerequisites: a reachable RabbitMQ broker instead of a legacy broker.
- Keep the test-only variable section and add `RABBITMQ_TEST_URL`: it gates the
  broker integration test and is not read by the service. `DATABASE_URL_FILE` remains
  test-only.
- Repository layout: `internal/rabbitmq/`, `internal/events/`, remove references to
  non-existent `deploy/install.sh` and `deploy/quadlet/stocker-store.build`, and make
  the Quadlet image name match the shipped unit.
- Replace the offset/lag runbook with a `rabbitmqadmin` runbook:
  - `list exchanges name type durable`
  - `list queues name messages messages_ready messages_unacknowledged consumers`
  - `list bindings source destination routing_key`
  - declare queue and binding commands for `stocker-store.stock.update.v1`
  - `get queue=... count=1 ackmode=ack_requeue_true payload_file=/tmp/msg.bin` followed
    by `xxd /tmp/msg.bin | head` to verify the protobuf prefix `0a`
  - `purge queue name=...`
  - How to read the service log lines from the design document.
  - Re-ingestion: consumed messages are not replayable; the upstream publisher
    republishes the full universe every cycle, or bind a fresh temporary queue for the
    next cycle. Never delete or re-arg the shared exchange.
- Remove the "Known inconsistencies" section and any stale `deploy/install.sh`,
  `stocker-store.build`, or wrong image-name references.
- Further reading should point to `docs/design/rabbitmq-ingestion.md` and
  `docs/design.md`.

**Quadlet**

Replace the legacy broker ordering line with network ordering only:

```ini
[Unit]
After=network-online.target
Wants=network-online.target
```

Add a short comment: the broker is remote; the consumer reconnects with backoff, so no
broker unit is ordered. Keep the existing image, environment file, restart policy, and
install target.

**Verify**

```bash
git grep -ri kafka -- README.md deploy   # expected: no output
gofmt -l .
go build ./...
go vet ./...
go test ./...
```

---

## Step 7 — Rewrite design and metadata docs; delete obsolete plans

**Goal:** Remove the legacy broker from the permanent design and metadata surfaces, and
delete the two obsolete implementation plans.

**Files**

- Modify `docs/design.md`.
- Modify `AGENTS.md`.
- Modify `proto/v1/README.md`.
- Modify `docs/plan/partial-score-updates.md`.
- Modify `docs/plan/configurable-grpc-port.md`.
- Delete `docs/plan/kafka-score-ingestion-fix.md`.
- Delete `docs/plan/new-stock-output-topic.md`.

**Implementation**

1. `docs/design.md`:
   - §1 overview diagram: replace the legacy broker label with RabbitMQ.
   - §3.2 score upsert semantics: say the broker ingestion path rejects values
     outside `[-1.0, 1.0]`; keep the database `CHECK` as the backstop.
   - §7.1 component diagram: replace the legacy broker node with a RabbitMQ node.
   - §12: rewrite as "New-Stock Output Publisher":
     - insert detection via `xmax = 0` remains unchanged;
     - `Publisher` interface now lives in `internal/events`;
     - implementation is `internal/rabbitmq.Publisher`;
     - configuration uses `RABBITMQ_OUTPUT_EXCHANGE`,
       `RABBITMQ_OUTPUT_EXCHANGE_TYPE`, `RABBITMQ_OUTPUT_ROUTING_KEY`;
     - publisher confirms, persistent delivery, protobuf, symbol/exchange headers;
     - failure semantics: log and continue, one reconnect-and-retry, never fails the
       primary write;
     - non-goals: no new schema, no dead-letter queue, no re-emission on update.
   - Add a short "RabbitMQ Ingestion" section (or a pointer in §1/§12) to
     `docs/design/rabbitmq-ingestion.md` for the full contract.
2. `AGENTS.md`:
   - Requirement: "Receive stocks through RabbitMQ".
   - Tech list: replace the legacy broker with "RabbitMQ".
3. `proto/v1/README.md`:
   - Line 17: state that this `Stock` message is also the RabbitMQ ingestion message
     and that the consumer on `RABBITMQ_QUEUE` decodes exactly this PROTOBUF schema.
4. Historical plans:
   - `docs/plan/partial-score-updates.md`: update the tech-stack line, package paths
     (`internal/rabbitmq/consumer.go`, `internal/rabbitmq/consumer_test.go`), the
     no-op publisher name (`events.Noop{}`), and replace "Kafka" with "RabbitMQ" or
     "broker" as appropriate. Keep the historical narrative but make it accurate for
     the current tree.
   - `docs/plan/configurable-grpc-port.md`: replace the legacy output-topic variable
     with `RABBITMQ_OUTPUT_EXCHANGE`, and replace "Kafka" adjectives with "broker"
     or "RabbitMQ".
5. Delete the two obsolete plans with `git rm`.

**Verify**

```bash
git grep -ri kafka -- docs AGENTS.md proto   # expected: only docs/plan/rabbitmq-migration.md
gofmt -l .
go build ./...
go vet ./...
go test ./...
```

---

## Step 8 — Integration verification and zero-reference sweep

**Goal:** Prove the consumer works against a real broker (when one is available), then
remove the final references and confirm the repository is clean.

**Files**

- Delete `docs/plan/rabbitmq-migration.md` at the end of this step.

**Integration verification (optional but recommended)**

Podman is available. Run a disposable broker and the gated integration test:

```bash
podman run -d --rm --name stocker-store-rabbitmq -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=stocker -e RABBITMQ_DEFAULT_PASS=stocker \
  rabbitmq:3-management
RABBITMQ_TEST_URL=amqp://stocker:stocker@127.0.0.1:5672/ go test ./internal/rabbitmq -run Integration -v
podman stop stocker-store-rabbitmq
```

The test must use only its unique throwaway exchange and queue. Do not run it against
the production broker or the shared `stock.update` exchange.

If no broker is available, confirm the test skips cleanly:

```bash
go test ./internal/rabbitmq -run Integration -v   # --- SKIP
```

**Zero-reference sweep**

Run the sweep. At this point the only expected match is this plan file:

```bash
git grep -ri kafka
```

Expected output: only `docs/plan/rabbitmq-migration.md` (this file, because it contains
the legacy package name and the sweep commands). Then remove this plan and re-run:

```bash
git rm docs/plan/rabbitmq-migration.md
git grep -ri kafka          # expected: no output
grep -rI --exclude-dir=.git -i kafka .   # expected: no output, including untracked files
go list -m all | grep -i kafka           # expected: no output
go mod graph | grep -i kafka             # expected: no output
```

`grep` exits 1 when there are no matches; that is the expected result, not a failure.

**Dependency and formatting sweep**

```bash
go mod tidy
git diff --exit-code go.mod go.sum   # expected: no diff
gofmt -l .                           # expected: no output
go build ./...
go vet ./...
go test ./...
```

**Final checks**

- `git status --short` shows only the intended deletions/modifications from this step.
- The gRPC publisher tests, RabbitMQ consumer/publisher tests, config tests, and store
  tests all pass.
- The integration test skips without `RABBITMQ_TEST_URL` and passes with a disposable
  broker.
- `docs/design/rabbitmq-ingestion.md` is accurate against the final code.
- Commit with a `docs(rabbitmq):` or `refactor(rabbitmq):` message and push to
  `origin feature/rabbitmq-migration`.

## Definition of done

- `internal/kafka` no longer exists.
- `go.mod` requires `github.com/rabbitmq/amqp091-go v1.16.0`; no legacy broker
  dependency or its indirects remain.
- `cmd/main.go` loads RabbitMQ configuration, constructs the optional publisher, runs
  the consumer, and shuts down cleanly.
- `internal/grpc` imports only `internal/events` for publishing.
- `internal/rabbitmq` has unit tests for both consumer and publisher and a gated
  integration test.
- README, `docs/design.md`, `AGENTS.md`, `proto/v1/README.md`, the historical plans,
  and the Quadlet describe RabbitMQ only.
- `git grep -ri kafka`, `go list -m all | grep -i kafka`, and the untracked-file grep
  all return nothing.
- `gofmt -l .` is empty; `go build ./...`, `go vet ./...`, and `go test ./...` pass.
