# Kafka Score Ingestion — Root Cause and Fix Plan

**Goal:** Explain why PROTOBUF score messages for existing stocks appear on `KAFKA_INPUT_TOPIC` but never reach the `scores` table, and specify a small, testable set of fixes that make ingestion resilient, observable, and at-least-once for transient failures — without changing the gRPC/proto API, the `scores` schema, or the documented partial-update merge semantics.

**Scope:** `internal/kafka/client.go`, `cmd/main.go`, their tests, and `README.md`. No new dependencies. No schema changes. No proto changes.

**Verdict up front:** a well-formed PROTOBUF message for an existing stock **does** write scores (verified end-to-end). The realistic causes of the operator's symptom are, in order: (1) the subscriber silently exits and never reconnects after the first Kafka error, (2) messages that fail decoding/validation/DB writes are committed and dropped with a single log line, and (3) `MinBytes: 1e3` delays small messages by several seconds. Operational causes (wrong topic, stale consumer-group offsets after a DB reset, wrong DSN/target) are equally worth checking with the operator's logs; the runbook is part of this plan.

---

## 1. Root causes, ranked

| Rank | Cause | Kind | Where | Evidence |
|---|---|---|---|---|
| **1** | `Client.Run` returns on the first `ReadMessage` error. `main` only logs the returned error and then blocks on `<-ctx.Done()`, so the process (and gRPC) stays healthy but Kafka ingestion is **dead until restart**. No retry, no reconnect. | **Code bug** | `internal/kafka/client.go:77-83`, `cmd/main.go:82-84` | H2 below: subscriber exited with `no such host` after ~10 s; process stayed alive; after Kafka returned and a message was produced, the group had no member, `LAG=1`, DB unchanged. |
| **2** | `ReadMessage` **commits the offset before `handle` runs** (kafka-go `reader.go:792-805`). `handle` errors are logged as `kafka: dropping message` and the loop continues. So decode errors, validation errors, transient DB errors, and poison values lose the message **permanently**. | **Code bug** (at-most-once) | `internal/kafka/client.go:85-88`, `:96-125` | H3: out-of-range, JSON, and garbage payloads each logged once, offset advanced `1→4`, DB unchanged. H5: NaN passed validation, DB `CHECK` failed, the **whole** message rolled back and was dropped, offset advanced. |
| **3** | `MinBytes: 1e3` makes each fetch wait for 1 KB or `MaxWait` (10 s default, kafka-go `reader.go:655-656`). A quiet topic therefore looks stalled for several seconds. | Code/UX bug | `internal/kafka/client.go:67` | H1: the score appeared only after the next fetch window (4 s in the cleanest run; not within the first 3 s); the write itself was correct. |
| **4** | No positive log line on a successful write, so `podman logs` cannot distinguish "no messages" from "messages were dropped". | Observability gap | `internal/kafka/client.go:114-124` | Logs contain only startup lines plus `dropping message`; no line ever says a stock/score was applied. |
| **5** | Validation accepts `NaN`/`±Inf` (`v < -1 || v > 1` is false for NaN). Postgres then rejects the `CHECK`, rolling back **every** score in that message, and the message is dropped. | **Code bug** (validation gap) | `internal/kafka/client.go:106-112` | H5: `momentum=0.6,sentiment=NaN` → `SQLSTATE 23514`, neither score applied, offset advanced. |
| **6** | Operational/config: wrong `KAFKA_INPUT_TOPIC` auto-creates an empty topic while messages pile up on the real one; a re-created database retains the old consumer-group offsets; a full `DATABASE_URL` DSN is rejected by the current builder (see rank 7); an old JSON producer is dropped as invalid protobuf. | Operational + doc bug | `cmd/main.go:34-45`, `README.md` | H5: typo topic `stockers-typo` was auto-created and the consumer sat idle. DSN: `DATABASE_URL=postgres://…` exits with `lookup postgres: no such host`. |
| **7** | `DATABASE_URL` handling re-prefixes the value with `postgres://` unless **both** `DATABASE_USERNAME` and `DATABASE_PASSWORD` are set. The README documents a full DSN, which therefore fails at startup (crash loop). | **Code + doc bug** | `cmd/main.go:34-45` | Full DSN → `initialize store: … lookup postgres: no such host`, exit 1. Only `host:port/db` + `DATABASE_USERNAME`/`DATABASE_PASSWORD` works, and those variables are undocumented. |

**H1 is not a bug:** an existing stock with valid scores is updated correctly (`momentum=0.1 → 0.7`, `value` inserted). The failure modes are around it, not in `Store.UpdateStock`.

**Most probable explanation of the operator's report:** rank 1. The deployment history supports it — commit `3ccefc6` added `After=kafka-broker-1.container`, which orders *startup* but does not wait for broker readiness, and Kafka's advertised listener/quorum may not be usable yet (or at all). One failed fetch at startup permanently disables ingestion while the container reports healthy. Rank 2 then guarantees that any message arriving during that period, or any message with a bad value/format, is committed and lost. Ranks 1 and 2 are both defects in this repository.

---

## 2. Reproduction (what was actually run)

### 2.1 Environment

The sandbox's Podman daemon runs on a different network namespace than the shell, so `-p` publishing and `--network=host` were not reachable from the shell. Postgres, Kafka, the service, and the producer were therefore placed on one user-defined Podman network and addressed by container name. A reviewer on a normal machine can use `localhost:<port>` instead; the commands are otherwise identical.

```bash
podman network create repro-net

# PostgreSQL 16
podman run -d --rm --name pg-repro --network repro-net \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=stocker docker.io/library/postgres:16-alpine

# Single-node KRaft Kafka on 19092 (9092 was occupied by an unrelated kafka-broker-2)
podman run -d --rm --name kafka-repro --network repro-net \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:19092,CONTROLLER://0.0.0.0:19093' \
  -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://kafka-repro:19092' \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:19093' \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk' \
  docker.io/apache/kafka:latest

podman exec kafka-repro /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-repro:19092 --create --topic stockers --partitions 1 --replication-factor 1
```

The service binary (`CGO_ENABLED=0 go build -o stocker-store ./cmd/main.go`) and a small producer that marshals `stockv1.Stock` were copied into a helper container (`svc-h2`, `svc-h4`) with `podman exec -i … cat > /tmp/svc`, then started with:

```bash
DATABASE_URL="pg-repro:5432/stocker?sslmode=disable" \
DATABASE_USERNAME=postgres DATABASE_PASSWORD=test \
KAFKA_BROKERS=kafka-repro:19092 KAFKA_INPUT_TOPIC=stockers \
KAFKA_GROUP_ID=<group> GRPC_PORT=1350x /tmp/svc
```

Note the **legacy DSN form** (`host:port/db` + credentials). The README's full-DSN form does not start (rank 7).

Seed an existing stock before producing:

```sql
INSERT INTO stocks(symbol,exchange) VALUES ('REPRO1','NYSE');
INSERT INTO scores(symbol,exchange,category,value) VALUES ('REPRO1','NYSE','momentum',0.1);
```

### 2.2 H1 — valid message for an existing stock

```bash
podman exec <helper> /tmp/producer -brokers kafka-repro:19092 -topic stockers \
  -symbol REPRO1 -exchange NYSE -scores 'momentum=0.7,value=-0.4'
# produced 81 bytes to stockers
```

Immediately after producing, the DB was unchanged (`momentum=0.1`). After waiting for the fetch window:

```
 symbol  | exchange | category | value |          timestamp
---------+----------+----------+-------+------------------------------
 REPRO1  | NYSE     | momentum |   0.7 | 2026-10-30 18:28:13.60147+00
 REPRO1  | NYSE     | value    |  -0.4 | 2026-10-30 18:28:13.60147+00
```

The service log showed **only** startup lines; no line reported the write. `MinBytes: 1e3` is why the write was not visible immediately (rank 3).

### 2.3 H3 — messages dropped, offsets still committed

```bash
producer … -scores 'momentum=1.5'                              # out of range
producer … -raw '{"symbol":"REPRO1","exchange":"NYSE","scores":{"momentum":0.2}}'  # old JSON
producer … -hex '0102fffe'                                     # garbage
```

Service log:

```
2026/10/30 18:28:49 kafka: dropping message stockers:1: score momentum value 1.5 out of range [-1, 1]
2026/10/30 18:28:49 kafka: dropping message stockers:2: decode stock message: proto: cannot parse invalid wire-format data
2026/10/30 18:28:49 kafka: dropping message stockers:3: decode stock message: proto: cannot parse invalid wire-format data
```

Consumer group after the drops:

```
GROUP      TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
repro-h1   stockers   0          4               4               0
```

DB still `momentum=0.7, value=-0.4`. The three messages are gone forever. (A protobuf message built from the gRPC `UpdateStockRequest` map shape is **wire-compatible** with `Stock` — verified — so the gRPC request type is not the problem; JSON is.)

### 2.4 H2 — subscriber dies when Kafka is unavailable (the leading cause)

Kafka was stopped, then the service was started with `KAFKA_GROUP_ID=repro-h2`:

```
2026/10/30 18:29:24 stocker-store listening on :13501
2026/10/30 18:29:24 kafka subscriber: consuming "stockers" via kafka-repro:19092
2026/10/30 18:29:34 kafka subscriber: read kafka message: fetching message: failed to dial: failed to open connection to kafka-repro:19092: dial tcp: lookup kafka-repro on 10.89.0.1:53: no such host
```

`pgrep` showed the process still alive (`/tmp/svc`, pid 7), i.e. the gRPC server and retention loop kept running. Kafka was then restarted, the topic recreated, and `momentum=0.9,sentiment=0.3` produced:

* service log: no new lines at all (no reconnect, no read);
* DB: still `momentum=0.7, value=-0.4` (the pending message was never applied);
* `kafka-consumer-groups --describe --group repro-h2`: `GroupIdNotFoundException: Group repro-h2 not found`;
* the message remained on the topic (`LOG-END-OFFSET=1`, no consumer).

A transient broker blip **after** startup has the same effect: any `ReadMessage` error returns from `Run` and ingestion is dead until the pod restarts.

### 2.5 H4 — consumer-group offsets

* **Fresh group replays history.** A new group `repro-h4` on a topic with one pending message read offset `0 → 1` and applied it (`momentum=0.9`, `sentiment=0.3` appeared). This is kafka-go's default `StartOffset: FirstOffset` (`reader.go:481-488`).
* **Same group resumes.** The consumer was stopped, `momentum=0.2` produced (log-end `2`), and the same group restarted: it consumed only offset `1 → 2` (final `momentum=0.2`), not the old message.
* **New group replays everything.** `repro-h4b` advanced `0 → 2` with no new message produced.

Consequence: if the database is re-created/reset while `KAFKA_GROUP_ID` stays the same, old messages are never re-read; a fresh group ID is the re-ingestion lever.

### 2.6 H5 — additional findings

**NaN poisons the whole message.** `momentum=0.6,sentiment=NaN`:

```
2026/10/30 18:33:17 kafka: dropping message stockers:2: upsert score sentiment: ERROR: new row for relation "scores" violates check constraint "scores_value_check" (SQLSTATE 23514)
```

Neither `momentum=0.6` nor `sentiment` was applied (the transaction rolled back), and the offset advanced. Validation must reject non-finite values before the store call.

**Topic typo auto-creates an empty topic.** Starting with `KAFKA_INPUT_TOPIC=stockers-typo` produced `kafka-topics --list` → `stockers`, `stockers-typo`; the typo topic was empty, the group never appeared, and messages on `stockers` were unconsumed. No error was logged.

**`DATABASE_URL` re-prefixing.** With `DATABASE_URL=postgres://postgres:test@pg-repro:5432/stocker?sslmode=disable` (and no `DATABASE_USERNAME`/`DATABASE_PASSWORD`), the service exits:

```
initialize store: initialize tables: failed to connect to `user=root database=postgres:test@pg-repro:5432/stocker`:
  hostname resolving error: lookup postgres on 10.89.0.1:53: no such host
```

With credentials set too, it exits similarly. Only `DATABASE_URL=pg-repro:5432/stocker?sslmode=disable` + `DATABASE_USERNAME` + `DATABASE_PASSWORD` works.

**Delivery semantics.** Because `ReadMessage` commits first, the current consumer is at-most-once even for the happy path: a crash after commit but before the DB commit loses the message.

---

## 3. Design decisions (with rationale)

1. **At-least-once for transient failures, explicit skip for permanent ones.**
   Use `FetchMessage` + `CommitMessages` and commit **only after** `handle` succeeds (or after a permanent message is logged and skipped). A transient DB/broker failure retries the same message with capped exponential backoff. Upserts are idempotent (`ON CONFLICT` on `(symbol, exchange)` and `(symbol, exchange, category)`), so redelivery is safe. This is the smallest change that stops silent loss.

2. **A poison message must not block the partition forever.**
   Decode errors and validation errors are **permanent**: log them at error level with `topic:partition:offset`, key/headers, and the decoded symbol/exchange when available, then commit and move on. This matches the documented "drop and log" behavior but makes it loud and diagnosable. A malformed message is a producer bug; blocking the partition would let one bad producer halt all score ingestion. No dead-letter topic is added (it would be a new topic/config surface and is out of scope); the full log line is the audit trail.

3. **DB integrity violations are permanent; connectivity/timeouts are transient.**
   Classify with `errors.As` against `*pgconn.PgError`: SQLSTATE class `22` (data exception, includes the `CHECK` violation) and class `23` (integrity constraint) are permanent → skip and log. Everything else from the store (connection refused, pool timeout, context deadline) is transient → retry. `pgconn` ships with `pgx`, so no new dependency. `pgx.ErrNoRows`-style cases are covered by class 22/23 or the transient default.

4. **Reconnect by recreating the reader.**
   `Run` wraps `consume` in a loop: create a reader, consume until a fetch/commit error, close it, back off, and start a new reader. This survives "broker was down at startup" (H2) and also re-joins the group cleanly after rebalance/commit errors. A reader is cheap; recreating avoids relying on undocumented internal recovery behavior. Backoff is 1 s doubling to 30 s.

5. **Keep the consumer single-threaded.**
   One partition is processed at a time; per-message upserts are already fast and transactional. Concurrency would add ordering/commit complexity for no requirement. (Kafka partitions are assigned across replicas by the group; that is unchanged.)

6. **Remove the `MinBytes` trap.**
   Set `MinBytes: 1` (kafka-go's default). A quiet topic should deliver immediately; `MaxBytes` remains 1 MB. `MaxWait` can stay at its 10 s default because it no longer applies to small messages.

7. **Log every accepted message.**
   `log.Printf("kafka: applied %s:%d:%d %s/%s scores=%d inserted=%v", …)` is the direct answer to "did my message land?" It is one line per message; this service ingests stock updates, not a firehose, and the operator explicitly needs this visibility. If log volume ever becomes a problem, gate it later; do not hide it now.

8. **Reject non-finite scores in validation.**
   `math.IsNaN(v) || math.IsInf(v, 0)` is a permanent validation error. It prevents the DB `CHECK`-triggered whole-message rollback (H5) and gives a precise log line.

9. **Fix the DSN builder rather than the docs.**
   Accept both documented forms: a full `postgres://…`/`postgresql://…` DSN is used verbatim; otherwise the legacy `host:port/db` form is prefixed and `DATABASE_USERNAME`/`DATABASE_PASSWORD` (when both set) are embedded with `url.UserPassword`. This is backward compatible and makes the README true. Log the sanitized target (scheme/host/path/user, never the password) so the operator can confirm which database is being written.

10. **Fail fast on an unexpected subscriber return.**
    After the reconnect loop, `Run` only returns on context cancellation or a configuration error. Treat a non-nil return as fatal in `main` (`log.Fatalf`) so systemd's `Restart=always` surfaces misconfiguration instead of leaving a healthy-looking process with dead ingestion.

11. **Do not touch the store, schema, proto, or merge semantics.**
    `Store.UpdateStock` is correct for valid input (proven by `TestUpdateStockPreservesUnmentionedScores`). All fixes are in the consumer shell and configuration.

### Non-goals

- Changing `proto/v1/stock_store.proto`, `UpdateStockRequest`/`Stock`, or the gRPC service.
- Changing the `scores` table, its constraints, or retention.
- Adding a dead-letter topic, schema registry, metrics endpoint, or a new dependency.
- Multi-message batching or parallel processing.
- Changing `KAFKA_GROUP_ID` semantics or the documented merge behavior.

---

## 4. File structure (planned changes)

| File | Change | Responsibility |
|---|---|---|
| `internal/kafka/client.go` | Add reader abstraction, reconnect loop, explicit commit, error classification, NaN/Inf validation, accepted/skip logs, `MinBytes: 1` | Resilient, observable, at-least-once consumer |
| `internal/kafka/client_test.go` | Add fake reader + tests for reconnect, commit ordering, classification, logging, non-finite scores | Unit proof without a broker |
| `cmd/main.go` | `databaseDSN` helper + sanitized target log; fatal on unexpected subscriber return | Correct DSN handling and visible startup/config failures |
| `cmd/main_test.go` | `TestDatabaseDSN` table test | Pin both DSN forms |
| `README.md` | Correct Kafka schema section, DSN/env docs, ingestion verification + re-ingestion runbook | Operator-facing truth |

No changes to `internal/store`, `internal/grpc`, `proto/`, `go.mod`, the `Containerfile`, or the Quadlets are required.

---

## 5. Implementation steps

Run `gofmt -l .`, `go vet ./...`, `go build ./...`, and `go test ./...` after every step. Steps 1–4 touch the same file; implement in order and keep the tree green after each.

### Step 1: Reconnecting consumer loop

**Files:** `internal/kafka/client.go`, `internal/kafka/client_test.go`.

**Do:**

1. Add a narrow interface and a factory field:
   ```go
   type messageReader interface {
       FetchMessage(ctx context.Context) (kafka.Message, error)
       CommitMessages(ctx context.Context, msgs ...kafka.Message) error
       Close() error
   }
   ```
   Add to `Client`: `newReader func() messageReader`, `sleep func(context.Context, time.Duration) bool`, `retryMin`, `retryMax time.Duration`.
2. In `New`, default `newReader` to `func() messageReader { return kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.Brokers, Topic: cfg.Topic, GroupID: cfg.GroupID, MinBytes: 1e3, MaxBytes: 1e6}) }` (preserving the current fetch settings; Step 5 changes `MinBytes`), `sleep` to a context-aware timer helper, `retryMin = time.Second`, `retryMax = 30 * time.Second`.
3. Replace the `Run` body with: validate config; loop { create reader; `consume`; close; if `ctx.Err() != nil` return nil; log `kafka: consumer stopped: <err>; reconnecting in <delay>`; sleep with context; double the delay up to `retryMax`; reset to `retryMin` after any successful fetch }.
4. Implement `consume(ctx, r) (progressed bool, err error)` that calls `FetchMessage` in a loop and returns `(true, err)` on a fetch/commit failure. **In this step, commit immediately after fetching** (`r.CommitMessages(ctx, msg)` then `c.handle(ctx, msg)`), which preserves today's commit-before-handle behavior so the tree stays correct; Step 2 moves the commit after processing.
5. Keep `cfg.validate()` unchanged so a genuinely invalid config still returns an error (which Step 6 makes fatal).

**Acceptance:**

- New tests pass:
  - `TestRun_ReconnectsAfterFetchError` — factory returns reader A (whose `FetchMessage` returns an error) then reader B (which returns one valid message then blocks). Assert the factory was called at least twice, `handle` ran for the message, and `Run` did not return while the context was live.
  - `TestRun_RetriesTransientFetchErrors` — several consecutive fetch errors then a message; assert no return and eventual processing.
  - `TestRun_ReturnsNilOnContextCancel` — cancel while `FetchMessage` blocks; assert `Run` returns nil.
- `go test ./internal/kafka` passes; existing tests unchanged.

### Step 2: Commit after success, classify permanent vs transient

**Depends on Step 1.**

**Files:** `internal/kafka/client.go`, `internal/kafka/client_test.go`.

**Do:**

1. Add a sentinel `var errInvalidMessage = errors.New("invalid kafka stock message")`. Wrap decode and validation errors with `fmt.Errorf("%w: …", errInvalidMessage, …)` while preserving the existing substrings (`decode stock message`, `out of range`, `symbol`, `exchange`) so current tests keep passing. Include the symbol/exchange in validation errors (`stock %s/%s: …`) so the skip log identifies the stock without a second decode.
2. Have `consume` call `processMessage(ctx, msg)` and, **only if it returns nil**, call `r.CommitMessages(ctx, msg)`. Return on commit error (the reconnect loop will re-create the reader; the message is redelivered if the commit did not take).
3. Implement `processMessage` as a retry loop around `handle`:
   - success → return nil;
   - `isPermanent(err)` → log `kafka: skipping message <topic>:<partition>:<offset> key=<key>: <err>` and return nil (commit/skip);
   - context cancelled → return `ctx.Err()`;
   - otherwise log `kafka: transient failure for <topic>:<partition>:<offset>, retrying in <delay>: <err>`, sleep with backoff, retry the same message. Never drop on a transient error.
4. Implement `isPermanent(err) bool`: `errors.Is(err, errInvalidMessage)` or `errors.As(err, &pgErr)` with SQLSTATE class `22`/`23` (`len(code) >= 2`; `github.com/jackc/pgx/v5/pgconn`). Keep `handle` returning store errors unwrapped so `TestHandle_StoreErrorPropagated` still works.

**Acceptance:**

- New tests pass:
  - `TestRun_CommitsAfterSuccessfulHandle` — one message, healthy fake store; assert `CommitMessages` called exactly once **after** `UpdateStock` (record call order).
  - `TestRun_DoesNotCommitOnTransientStoreErrorUntilSuccess` — store fails once with `errors.New("connection refused")` then succeeds; assert `UpdateStock` called twice, `CommitMessages` once, after the successful attempt.
  - `TestRun_SkipsPermanentDecodeErrorAndCommits` — garbage `Value`; assert `CommitMessages` called once, `UpdateStock` never called.
  - `TestRun_SkipsPgConstraintViolationAndCommits` — store returns `&pgconn.PgError{Code: "23514"}`; assert one `UpdateStock` call, one commit (no infinite retry).
  - `TestIsPermanentError` — table for decode/validation sentinel, `23514`, `23503`, `22P02`, plain error, `context.DeadlineExceeded`.
- `TestHandle_StoreErrorPropagated` still passes unchanged; `go test ./internal/kafka` passes.

### Step 3: Reject non-finite scores

**Files:** `internal/kafka/client.go`, `internal/kafka/client_test.go`.

**Do:** in the score loop of `handle`, change the condition to reject `math.IsNaN(v) || math.IsInf(v, 0)` in addition to `v < -1 || v > 1`, wrapping the error with `errInvalidMessage`. Add `"math"` to imports.

**Acceptance:**

- `TestHandle_RejectsNaNScore` and `TestHandle_RejectsInfScore` assert an error mentioning the category and `"out of range"` (or a new `"not finite"` phrase; keep it descriptive), and that `fakeStore.called` is false.
- `go test ./internal/kafka` passes.

### Step 4: Accepted/skip/retry logging with full context

**Files:** `internal/kafka/client.go`, `internal/kafka/client_test.go`.

**Do:**

1. After a successful `UpdateStock`, log:
   ```
   log.Printf("kafka: applied %s:%d:%d %s/%s scores=%d inserted=%v",
       msg.Topic, msg.Partition, msg.Offset, stored.Symbol, stored.Exchange, len(m.GetScores()), inserted)
   ```
   Do this before the publish branch so a publish failure still records the write.
2. Ensure the skip/retry lines from Step 2 include `topic:partition:offset` and `key=%s` (`msg.Key`). Put the stock identity/payload description into the wrapped error in `handle` instead of re-decoding in `processMessage`: validation errors already carry `symbol/exchange`; for decode failures, include the byte length and a truncated hex prefix (first 32 bytes) in the decode error (`fmt.Errorf("%w: decode stock message (len=%d prefix=%x): %v", …)`).
3. Keep messages single-line so `grep` works.

**Acceptance:**

- `TestRun_LogsAppliedMessage` captures `log` output into a `bytes.Buffer` (`log.SetOutput`, restore with `t.Cleanup`) and asserts the line contains `kafka: applied`, the topic, offset, symbol, exchange, and the score count.
- `TestRun_LogsSkippedMessage` asserts the skip line contains `skipping message`, topic, partition, and offset.
- `go test ./internal/kafka` passes.

### Step 5: Remove the `MinBytes` latency trap

**Files:** `internal/kafka/client.go`.

**Do:** set `MinBytes: 1` in the reader config created in `New` (Step 1). Do not set `MaxWait`; it becomes irrelevant for small messages.

**Acceptance:**

- `go build ./...` and `go test ./...` pass.
- End-to-end: with a caught-up consumer, produce one small message and confirm the `kafka: applied …` log line appears within ~1 s (the fixed behavior) instead of after the fetch window. Record the command/output in the PR description.

### Step 6: Make an unexpected subscriber return fatal; fix `DATABASE_URL`

**Files:** `cmd/main.go`, `cmd/main_test.go`.

**Do:**

1. In `main`, keep the synchronous call but use `log.Fatalf("kafka subscriber: %v", err)` when `runKafkaSubscriber` returns a non-nil error. (It returns nil when Kafka input is not configured and on context cancellation.)
2. Extract the DSN logic into `func databaseDSN(raw, username, password string) string`:
   - `raw == ""` → `""` (caller keeps the existing "required" fatal);
   - starts with `postgres://` or `postgresql://` → return `raw` verbatim;
   - otherwise, if both `username` and `password` are non-empty → `"postgres://" + url.UserPassword(username, password).String() + "@" + raw`;
   - otherwise → `"postgres://" + raw`.
3. Use `databaseDSN` in `main`. After parsing with `url.Parse`, log the sanitized target once: scheme, host, path, and username — **never** the password. Example: `database: postgres://postgres@pg:5432/stocker`.
4. Leave the `DATABASE_URL`-empty fatal behavior unchanged.

**Acceptance:**

- `TestDatabaseDSN` table:
  - full `postgres://user:pass@host:5432/db?sslmode=disable` → unchanged (with and without username/password env values);
  - `host:5432/db?sslmode=disable` + creds → `postgres://user:pass@host:5432/db?sslmode=disable`;
  - same without creds → `postgres://host:5432/db?sslmode=disable`;
  - password containing `@`/`:` is correctly escaped (assert it does not appear raw);
  - empty raw → `""`.
- `go test ./cmd` passes.

### Step 7: Correct and extend the README

**Files:** `README.md`.

**Do:**

1. **Kafka message schema:** replace the stale `proto/v1/kafka` / `StockUpdate` (package `stockerstore.kafka.v1`) section with the truth: the consumer decodes `stockstore.v1.Stock` (`proto/v1/stock_store.proto`), PROTOBUF-encoded; field layout `symbol=1`, `exchange=2`, `repeated ScoreEntry scores=3`; `ScoreEntry{category=1, value=2, updated_at=3}`. Note that a payload built from `UpdateStockRequest` (`map<string,double>`) is wire-compatible, and that JSON is **not** accepted.
2. **Validation:** state that non-finite (`NaN`/`±Inf`) and out-of-range values are rejected and the message is skipped and logged with `topic:partition:offset`; transient store errors are retried without committing.
3. **Delivery semantics:** add a short line: at-least-once for transient failures, skip-and-log for permanently invalid messages; upserts are idempotent, so redelivery is safe.
4. **Configuration:** document that `DATABASE_URL` accepts a full `postgres://…` DSN **or** the legacy `host:port/db` form, and that `DATABASE_USERNAME`/`DATABASE_PASSWORD` are only applied to the legacy form.
5. **New troubleshooting subsection "Verifying Kafka ingestion":**
   - healthy startup lines: `kafka subscriber: consuming "<topic>" via <brokers>` and `stocker-store listening on :<port>`;
   - per-message success line: `kafka: applied <topic>:<partition>:<offset> <symbol>/<exchange> scores=<n> inserted=<bool>`;
   - failure lines: `kafka: transient failure … retrying`, `kafka: consumer stopped … reconnecting`, `kafka: skipping message …`;
   - consumer-group lag: `kafka-consumer-groups.sh --bootstrap-server <brokers> --describe --group stocker-store` (a healthy consumer has a `CONSUMER-ID` and `LAG 0`; a missing group / no member means the subscriber is not running);
   - topic sanity: `kafka-topics.sh --list` — a typo'd `KAFKA_INPUT_TOPIC` is auto-created and stays empty;
   - message format check with `kcat`/`kafka-console-consumer` + `xxd` (valid protobuf starts with `0a …`);
   - DB check: `SELECT symbol, exchange, category, value, timestamp FROM scores ORDER BY timestamp DESC LIMIT 10;`;
   - **re-ingestion:** stop the service, set a fresh `KAFKA_GROUP_ID` (e.g. `stocker-store-reingest-2026-10-30`), start it; a new group starts at the earliest retained offset (`StartOffset` default `FirstOffset`) and replays the topic. Warn that this re-applies messages (idempotent) and only works while the topic still retains them.
6. Remove or correct any other statement made false by this plan (e.g. "no separate DB_USER/DB_PASSWORD" while `DATABASE_USERNAME`/`DATABASE_PASSWORD` exist).

**Acceptance:**

- `grep -n "StockUpdate\|proto/v1/kafka" README.md` returns no claims that those files exist (historical wording is fine only if clearly marked removed).
- `grep -n "kafka: applied\|re-ingest\|KAFKA_GROUP_ID" README.md` finds the new runbook.
- Markdown tables still render (same column counts).

### Step 8: End-to-end verification (manual, no code)

**Files:** none.

**Do:** run the reproduction from §2 against the fixed binary:

1. Valid update for an existing stock → `kafka: applied …` appears within ~1 s, DB shows the new value, offset advances.
2. Start the service with Kafka down → log shows `consumer stopped … reconnecting` every few seconds; start Kafka and produce → the same process applies the message (no restart).
3. Produce garbage / out-of-range / NaN / JSON → one `kafka: skipping message <topic>:<partition>:<offset> …` line each, offsets advance, the next valid message applies.
4. Stop Postgres briefly while producing → `transient failure … retrying` repeats; start Postgres → the message applies and commits (no loss).
5. Full suite: `gofmt -l .` (empty), `go vet ./...`, `go build ./...`, `DATABASE_URL=… go test ./...` (DB-backed store tests execute, not skip).

**Acceptance:** all five scenarios match; capture the log/DB output for the PR.

---

## 6. Acceptance criteria

- [ ] The subscriber never exits because a broker is unavailable; it logs and retries, and resumes consuming when the broker returns (verified by H2 reproduction).
- [ ] Offsets are committed **after** a successful `handle`; a transient store error retries the same message and does not commit until it succeeds.
- [ ] A permanently invalid message (decode error, empty symbol/exchange, out-of-range/NaN/Inf score, SQLSTATE 22/23) is logged once with `topic:partition:offset` (plus key/symbol/exchange where available), committed, and does not block later messages.
- [ ] Every accepted message logs `kafka: applied <topic>:<partition>:<offset> <symbol>/<exchange> scores=<n> inserted=<bool>`.
- [ ] `MinBytes` no longer delays small messages; observed apply latency is ~1 s on a quiet topic.
- [ ] A full `postgres://…` `DATABASE_URL` starts successfully; the legacy `host:port/db` form with `DATABASE_USERNAME`/`DATABASE_PASSWORD` still works; the password never appears in logs.
- [ ] README's Kafka schema/DSN sections are accurate, and it contains a verification + re-ingestion runbook.
- [ ] No changes to `proto/`, the gRPC service, the `scores` schema, or partial-update merge semantics.
- [ ] `gofmt -l .` empty; `go vet ./...`, `go build ./...`, `go test ./...` pass (store tests run against Postgres, not skipped).

---

## 7. Risks and edge cases

1. **At-least-once means duplicates.** A crash between DB commit and offset commit redelivers the message; upserts make this harmless. Do not add "exactly once" machinery.
2. **Unbounded retry on a persistent transient error.** If the DB is down, the partition stalls (correct: no data loss) and the log repeats with capped backoff. A message that is permanently invalid but misclassified as transient would block the partition; the `pgconn` class check plus the validation sentinel cover the known cases. Review the classifier when adding new store errors.
3. **Log volume.** One line per accepted message could be noisy under heavy write rates. This is intentional for the current incident; gating it behind a flag is a future change, not part of this fix.
4. **Recreating the reader on every fetch/commit error resets the group session.** Under a flapping broker this can cause repeated rebalances. The 1–30 s backoff bounds it.
5. **Skipping malformed messages is still data loss.** It is now visible and actionable, but recovery requires the producer to re-send. A dead-letter topic is deliberately out of scope.
6. **`DATABASE_URL` behavior change.** Full DSNs now work; legacy `host/db` values still work. Do not "normalize" a full DSN by re-adding credentials — the DSN is authoritative.
7. **`CommitMessages` after a rebalance can fail.** The reconnect loop re-creates the reader; the message is redelivered and re-applied (idempotent).
8. **`StartOffset` default is `FirstOffset`.** A fresh group replays everything the topic retains; document this in the re-ingestion runbook so an operator does not accidentally trigger a large replay during peak load.
9. **The producer format is outside this repo.** Confirm whether the operator's producer emits protobuf `Stock` (or the wire-compatible `StockUpdate`/map shape) and not JSON; the new skip log will say `decode stock message` for JSON.
10. **Operational log access.** All of this is diagnosable only with `podman logs stocker-store` and the Kafka consumer-group tooling; the runbook must be followed on the live host.

---

## 8. Verification checklist (reviewer)

- [ ] Branch is `fix/kafka-score-ingestion`, not `main`.
- [ ] Reproduce H2 with the pre-fix binary (subscriber exits; process stays up) and confirm the post-fix binary reconnects and applies a message produced after Kafka returns.
- [ ] Reproduce H3 (out-of-range/JSON/NaN) and confirm each is skipped **with** a contextual log line and the next valid message is applied.
- [ ] Confirm no `kafka: dropped` wording remains; skip lines are `kafka: skipping message <topic>:<partition>:<offset>`.
- [ ] Confirm commit ordering in tests (commit after store success, not before).
- [ ] Confirm `TestDatabaseDSN` covers both forms and escaping.
- [ ] `gofmt -l .` empty; `go vet ./...`, `go build ./...` pass; `DATABASE_URL=<dsn> go test ./...` passes with store tests executed (no `SKIP`).
- [ ] `git status` clean after commit/push; commit message `fix(kafka): make score ingestion resilient and observable` (plan commit uses `docs(plan): …`).
- [ ] README runbook commands are copy-pasteable and match the actual log strings.

---

## 9. Operator checks to run on the live deployment (before/after the fix)

These are the checks that distinguish the ranked causes using only the deployed host:

1. **Is the subscriber alive?**
   `podman logs stocker-store | grep -E 'kafka subscriber|kafka: (applied|skipping|transient|consumer stopped)'`
   - `kafka subscriber: consuming "<topic>" via …` followed by silence and no `applied` lines → suspect rank 1 or a wrong topic.
   - `kafka subscriber: read kafka message: …` → rank 1 confirmed; restart the container only *after* Kafka is healthy, and fix the code.
   - `kafka: dropping message …` → rank 2/5; the log states the reason (decode/out-of-range/constraint).
2. **Is the consumer group joined and caught up?**
   `kafka-consumer-groups.sh --bootstrap-server <brokers> --describe --group stocker-store`
   - No group / no `CONSUMER-ID` → the subscriber is not running (rank 1).
   - `LAG` growing → messages are being produced but not consumed.
3. **Is the topic name correct?**
   `kafka-topics.sh --bootstrap-server <brokers> --list`
   - A near-duplicate topic (e.g. `stockers-typo`) that is empty → `KAFKA_INPUT_TOPIC` mismatch (rank 6).
4. **Which database is the service using?**
   Compare the newly logged `database: postgres://<user>@<host><path>` line (Step 6) with the host you are inspecting. If they differ, you are looking at the wrong DB.
5. **Was the DB re-created while the group kept its offsets?**
   If `scores` is empty but `LAG=0` and the group is old, re-ingest with a fresh `KAFKA_GROUP_ID` (rank 6 / H4). Confirm the topic retention still holds the messages.
6. **Is the broker cluster healthy?**
   `podman logs kafka-broker-1` (and any peers) plus `kafka-topics.sh --describe`. On this build host a `kafka-broker-2` container is running in a 2-node KRaft quorum whose node 1 (`192.168.1.75:9093`) is unreachable — a half-cluster can produce leader/metadata errors that kill the current subscriber on its first fetch. Verify broker 1 is up and its advertised listener is reachable **from inside the stocker-store container** before restarting the service.
