# Partial Score Updates — Implementation Plan

**Goal:** Lock in and document the guarantee that a stock upsert **merges** the incoming score categories into the stored set: only the categories present in the message are inserted or updated, and every existing score whose category is absent from the message keeps both its value and its last-updated timestamp. The deliverable is regression coverage plus explicit documentation, not a behavior change.

**Architecture:** The guarantee already holds structurally. `Store.UpdateStock` performs one `INSERT ... ON CONFLICT (symbol, exchange) DO UPDATE SET value = EXCLUDED.value, timestamp = now()` per supplied category inside a single transaction; it never issues a `DELETE` against `scores` and skips the score loop entirely when the incoming map is empty. Both ingestion paths (gRPC `AddStocks`/`UpdateStock` and Kafka `Client.handle`) pass exactly the message's scores to `UpdateStock`. The only score deletions in the codebase are the explicit `RemoveStock` and the retention sweep `RemoveOldStocks`, neither of which is on the upsert path. This plan adds DB-backed store tests that prove merge semantics, handler-level pass-through tests that prove no unioning or clearing, and documentation of the contract.

**Tech Stack:** Go 1.25, `pgx/v5`, PostgreSQL 16, gRPC/protobuf, `segmentio/kafka-go` (tests only, no new dependency).

**Spec:** This plan is the spec for the verification work; it defines the contract to add to `README.md` and `docs/design.md`. No schema change, no proto change, no new dependency, and no behavior change to `internal/store`, `internal/grpc`, or `internal/kafka` are expected. If a test exposes a real violation, fixing it is in scope for Step 4.

## Global constraints

- Do **not** change the upsert SQL to "delete then insert", and do not add a read-modify-write of the score set in Go. The per-row `ON CONFLICT` upsert is what makes the merge safe under concurrency.
- Do not add schema migrations, indexes, or protobuf changes. `UpdateStockRequest.scores` is already `map<string, double>` (`proto/v1/stock_store.proto:16`).
- DB-backed tests live in `internal/store/store_test.go` and must use the existing `testStore(t)` gate so they skip cleanly when no DSN is configured. They must clean up with `defer clearStock(...)` and unique keys from `uniqueTag()`.
- Handler tests use the existing in-memory fakes in `internal/grpc/server_test.go` and `internal/kafka/client_test.go`; they must not require a database.
- The tree must build (`go build ./...`) after every numbered step.
- Test names must be stable and descriptive; assertions must cover **both** the value and the timestamp of untouched scores.

---

## Current behavior (verified against `a087bd2`)

| Location | Behavior |
|---|---|
| `internal/store/store.go:74-125` `UpdateStock` | Opens a transaction, upserts the `stocks` row (always `timestamp = now()`), then for each `(category, value)` in `scores` runs `INSERT INTO scores ... ON CONFLICT (symbol, exchange, category) DO UPDATE SET value = EXCLUDED.value, timestamp = now()`. `if len(scores) > 0` guards the loop, so `nil` and empty maps touch no score rows. No `DELETE` appears in the function. |
| `internal/store/store.go:118-122` | Returns the stock hydrated by `getStockScores`, so a partial update response already contains the *full* merged score set. |
| `internal/store/store.go:171-192` `RemoveStock` | Explicitly deletes all scores for the pair, then the stock. Intentional destructive path. |
| `internal/store/store.go:130-168` `RemoveOldStocks` | Deletes scores for stocks past retention, then the stocks. Intentional retention path. |
| `internal/grpc/server.go:70` (`AddStocks`) and `:96` (`UpdateStock`) | Call `s.store.UpdateStock(ctx, symbol, exchange, req.GetScores())` — the request's map verbatim. |
| `internal/kafka/client.go:114` `handle` | Calls `c.store.UpdateStock(ctx, m.Symbol, m.Exchange, toMap(m.GetScores()))`; `toMap(nil)` returns a `nil` map (`client.go:136-147`). |
| `internal/store/store.go:50-58` | `scores` has `UNIQUE (symbol, exchange, category)` and `CHECK (value BETWEEN -1.0 AND 1.0)`; the unique key is what the conflict target relies on. |

**Conclusion:** the merge guarantee is already satisfied. The gap is that no test asserts that *unmentioned* score rows survive with their original timestamps, and no document states the guarantee. A future refactor (for example "replace the score set" or a `DELETE ... WHERE category <> ALL($3)`) would pass the current suite.

---

## File structure (before defining steps)

| File | Change | Responsibility |
|---|---|---|
| `internal/store/store_test.go` | Add `TestUpdateStockPreservesUnmentionedScores` (DB-backed) | Authoritative proof of merge semantics at the SQL layer, including timestamps. |
| `internal/grpc/server_test.go` | Extend `fakeStore` with a score-call recorder; add three tests | Prove handlers forward exactly the request/message scores. |
| `internal/kafka/client_test.go` | Add two tests using the existing `fakeStore.lastScores` | Prove `handle` forwards exactly the message scores and a nil map when scoreless. |
| `README.md` | Features bullet + `Partial score updates` subsection under gRPC API | State the operator/client-facing contract. |
| `docs/design.md` | New paragraph after the `scores` table (§3) | State the storage-layer contract and where deletions are allowed. |

No changes to production Go code, `proto/`, `go.mod`, schema, or deployment files are planned.

---

## Design decisions (summary, with rationale)

1. **DB-backed store tests are the authoritative proof.** Merge semantics are a property of the SQL (`ON CONFLICT` per category, no `DELETE`), including the server-assigned `timestamp` columns. Only a real Postgres can prove that absent categories keep their stored row and timestamp; a fake would just re-encode the assumption under test. `testStore(t)` already provides the connection, schema bootstrap, and skip behavior.
2. **Read the baseline rows before the update, then compare after.** Seeded timestamps are captured by reading them back with `readScore` (not by reusing the `time.Time` passed to `seedScore`), so the comparison is against Postgres's stored `TIMESTAMPTZ` value and is immune to precision/location differences. Timestamps compare with `.Equal(...)`; values compare with `==` using fixtures that round-trip exactly through Postgres `DOUBLE PRECISION` (`0.25`, `-0.4`, `0.9` are all stored and read back as the identical `float64`).
3. **Assert the response too.** `UpdateStock` hydrates its return value with `getStockScores`, so the response must list untouched categories as well. This catches a regression where the write survives but the read path filters by "categories in this request".
4. **Case split: subset, new category, no scores.** These are the three ways a client exercises partial updates. "Subset" proves preservation of value and timestamp; "new category" proves additive merge; "no scores" proves a scoreless touch neither clears nor rewrites the set. `nil` and empty map are tested separately because they take the same branch but arrive from different callers (`req.GetScores()` on an absent proto map vs. `toMap(nil)`).
5. **Pass-through tests belong at the handler layer, not the store.** The handlers' only job is transport → store argument. A recording fake proves no unioning (accumulating categories across stream messages), no clearing (dropping categories the message omitted), and correct nil propagation. The gRPC fake gets a `lastScores` snapshot (copy, not alias) because it currently overwrites `stocks[k]`; the Kafka fake already records `lastScores`.
6. **Documentation states the contract once per audience.** `README.md` gets the client-facing promise ("omitted categories are untouched"); `docs/design.md` gets the storage-level rule plus the explicit list of the only two deletion paths. Neither document claims a behavior the tests do not assert.

### Non-goals

- Changing the upsert SQL, the proto, or the schema.
- Adding a "replace all scores" API or a `DELETE`-on-omit mode.
- Changing score-range validation. Kafka validates `[-1, 1]` before calling the store (`internal/kafka/client.go:106-112`); gRPC relies on the `scores.value` `CHECK` constraint, which fails the whole transaction (including the stock timestamp refresh) and surfaces as `codes.Internal`.
- Per-category timestamps on the `stocks` row, score history/audit tables, or change data capture.
- Backfilling timestamps or reconciling existing rows.

---

## Implementation steps

Each step is independently verifiable. Run `gofmt -l .` after any Go edit (it must print nothing).

### Step 1: Add DB-backed merge regression tests

**Files:** modify `internal/store/store_test.go` only.

**Do:** add one top-level test, `TestUpdateStockPreservesUnmentionedScores`, using `s := testStore(t)`, `defer s.Close()`, a fresh `sym, exh := uniqueTag()`, and `defer clearStock(t, s, sym, exh)`. Capture `ctx := context.Background()` and a distinct old timestamp per seeded score (for example `base := time.Now().UTC().Add(-3 * time.Hour)`, with `base` and `base.Add(time.Hour)` so the two baselines differ). Seed with `seedStock` and `seedScore`. Before each update, read the rows to be preserved with `readScore` and keep those `ScoreEntry` values as the baseline.

Use subtests (each on its own `uniqueTag()` pair so they cannot interfere, or reset state between subtests with `clearStock` + reseed):

- **`subset update keeps unmentioned value and timestamp`** — seed `momentum=0.25` and `sentiment=0.9` with distinct old timestamps; call `s.UpdateStock(ctx, sym, exh, map[string]float64{"momentum": -0.4})`. Assert: `momentum` value is `-0.4`; `momentum.UpdatedAt` is strictly after the old baseline (the touched row is refreshed); `sentiment` value is still `0.9`; `sentiment.UpdatedAt.Equal(baselineSentiment.UpdatedAt)` is true; `scoreCount(t, s, sym, exh) == 2`; and the returned `*Stock.Scores` contains both categories (use the existing `scoreByCategory` helper) with the same values.
- **`new category keeps existing scores`** — seed `momentum=0.25` only; call `UpdateStock` with `{"quality": 0.5}`. Assert `momentum` value and `UpdatedAt` are unchanged (`Equal` against the baseline read before the call), `quality` exists with `0.5` and a non-zero timestamp, `scoreCount == 2`, and the response hydrates both.
- **`nil scores keeps all scores`** — seed two categories; call `UpdateStock(ctx, sym, exh, nil)`. Assert both categories keep their exact values and `UpdatedAt` (`Equal` against baselines), `scoreCount == 2`, and the returned stock's `Updated` (stock-level timestamp) is after the seeded stock timestamp — i.e. the stock is touched while the scores are not.
- **`empty map keeps all scores`** — same as the nil case with `map[string]float64{}`, asserting the same preservation.

**Expected assertions:** untouched rows preserve **both** `Value` and `UpdatedAt`; touched rows change value and advance `UpdatedAt`; no row is ever removed by an upsert; the returned stock hydrates the merged set. On the current implementation all subtests must pass without production changes.

**Acceptance:** `DATABASE_URL=postgres://postgres:test@localhost:5432/stocker?sslmode=disable go test -run TestUpdateStockPreservesUnmentionedScores -v ./internal/store` shows `PASS` (not `SKIP`) for all four subtests; `go test ./internal/store` still passes.

### Step 2: Add handler and Kafka pass-through tests

**Files:** modify `internal/grpc/server_test.go` and `internal/kafka/client_test.go`. Do not modify production code.

**Do (gRPC):**

1. Extend the existing `fakeStore` (not a new type) with a `lastScores map[string]float64` field. In `UpdateStock`, store a **copy** of the incoming map (so later mutation by a test or caller cannot change the recorded value) in addition to the existing `f.stocks[k] = scores` assignment. Keep all existing behavior, including the `inserted` calculation, unchanged.
2. Add `TestUpdateStock_PassesOnlyRequestScores`: create `newFakeStore()` and a client via `newTestClient(t, fs, kafka.Noop{})`. First call `client.UpdateStock` with `{"momentum": 0.5, "sentiment": 0.9}`; then call it again for the same symbol/exchange with `{"momentum": -0.4}`. Assert the second `fs.lastScores` has length 1, contains `momentum = -0.4`, and has **no** `sentiment` key (no unioning), and that the request map was not mutated.
3. Add `TestUpdateStock_PassesNilScores`: call `client.UpdateStock` with `Symbol`/`Exchange` and no `Scores`; assert `fs.lastScores == nil` (nil, not an empty non-nil map) and that the call succeeds.
4. Add `TestAddStocks_PassesPerMessageScores`: open `client.AddStocks(ctx)`, send two requests for the same `(symbol, exchange)` with disjoint categories (`{"alpha": 0.1}` then `{"beta": -0.2}`), then one scoreless request for a second symbol; `CloseAndRecv`. Assert via `client.GetStock` that the first stock's scores contain exactly the last message's category (`beta`) — proving the handler forwards each message independently rather than accumulating (`alpha` must be absent) — and that the second (scoreless) stock reports zero scores. (The fake replaces `stocks[k]` per call, so a handler that unioned or cleared would change the observed sets.)

**Do (Kafka):**

5. Add `TestHandle_PassesOnlyMessageScores`: build a `stockv1.Stock{Symbol, Exchange, Scores: []*stockv1.ScoreEntry{{Category: "momentum", Value: 0.5}}}`, marshal it with `proto.Marshal`, call `(&Client{...}).handle` via `New(Config{}, store, &fakePublisher{})` and a `kafkaMsg.Message{Value: raw}`. Assert `store.called`, `len(store.lastScores) == 1`, and `store.lastScores["momentum"] == 0.5`; assert no extra categories were synthesized.
6. Add `TestHandle_ScorelessMessagePassesNilMap`: marshal a `Stock` with no scores; call `handle`; assert no error, `store.called`, and `store.lastScores == nil` (this is the `toMap(nil)` contract; a non-nil empty map would still be behaviorally equivalent in the store, but the test pins the documented shape).

**Acceptance:** `go test ./internal/grpc ./internal/kafka` passes with no database. Existing tests in both packages still pass unchanged.

### Step 3: Document the partial-update contract

**Files:** modify `README.md` and `docs/design.md`. Documentation only.

**Do (README):**

- Extend the **Bulk and single upserts** feature bullet (`README.md:66`) to say that upserts merge: only the score categories in the request are written, and categories not mentioned are left unchanged, including their last-updated timestamps.
- Add a short `#### Partial score updates` subsection immediately after the gRPC **Methods** table (after the existing `GetStocks.limit` callout, around `README.md:91`). State: `AddStocks` and `UpdateStock` treat `scores` as a merge, not a replacement; a category present in the request is inserted or updated with a fresh server timestamp; a category absent from the request is untouched (same value, same `updated_at`); an empty or omitted `scores` map updates only the stock-level timestamp; removing scores is done only by `RemoveStock` or retention. Include one sentence noting the rule also applies to Kafka ingestion.
- Keep the existing `UpdateStockRequest` message row unchanged (`map<string,double> scores`).

**Do (design):**

- After the `scores` table definition and its index list (`docs/design.md:61-72`), add a `#### Score upsert semantics` paragraph (or an equivalent bolded paragraph consistent with the file's style) stating the storage-level rule: `UpdateStock` upserts each supplied `(symbol, exchange, category)` with `ON CONFLICT ... DO UPDATE SET value = EXCLUDED.value, timestamp = now()`; it never deletes score rows, so omitted categories retain value and timestamp; the stock-level `stocks.timestamp` is always refreshed even for a scoreless upsert; the only deletions of `scores` rows are `RemoveStock` (explicit) and `RemoveOldStocks` (retention).
- Do not claim validation behavior that does not exist; if score-range validation is mentioned, attribute it correctly (Kafka pre-validates `[-1, 1]`, the database `CHECK` is the backstop).

**Acceptance:** `grep -n -i "partial" README.md docs/design.md` finds the new sections; `grep -n "merge" README.md docs/design.md` shows the guarantee; no statement contradicts `internal/store/store.go`. Markdown tables/lists still render (same column counts).

### Step 4: Full verification against a real Postgres, then commit

**Files:** none planned; fix code/tests only if verification exposes a defect.

**Do:**

1. Start the local test database: `podman start stocker-postgres-test` (image `docker.io/library/postgres:16-alpine`, `POSTGRES_DB=stocker`, `POSTGRES_PASSWORD=test`, host port `5432`). If the container is unavailable, start a disposable equivalent: `podman run -d --rm --name stocker-pg-verify -e POSTGRES_PASSWORD=test -e POSTGRES_DB=stocker -p 55432:5432 docker.io/library/postgres:16-alpine` and use port `55432`.
2. `export DATABASE_URL='postgres://postgres:test@localhost:5432/stocker?sslmode=disable'`.
3. `gofmt -l .` → no output.
4. `go vet ./...` → clean.
5. `go build ./...` → succeeds.
6. `go test ./...` → all packages pass.
7. Prove the DB-backed tests actually ran, not skipped: `go test -v -run 'TestUpdateStock' ./internal/store | tee /tmp/store-verify.log` and confirm each new subtest prints `--- PASS` with no `--- SKIP` / `DATABASE_URL not set` line. Also run the handler packages verbosely and confirm the new tests appear: `go test -v -run 'PassesOnly|PassesNil|PassesPerMessage|ScorelessMessage' ./internal/grpc ./internal/kafka`.
8. If any assertion fails, treat it as a real defect: use systematic debugging to determine whether the store SQL, a handler, or the test is wrong; fix the smallest correct thing (the expected fix is a test bug, since the SQL already merges), then re-run steps 3–7.
9. Confirm the working tree contains only the intended files (`git status --short`), then commit and push:
   ```bash
   git add internal/store/store_test.go internal/grpc/server_test.go internal/kafka/client_test.go README.md docs/design.md
   git commit -m "test(store): lock in partial score update merge semantics"
   git push
   ```

**Acceptance:** every command returns the stated result; the verbose logs show the new tests executed (not skipped); `git status` is clean after the push and the branch is up to date with `origin`.

---

## Acceptance criteria

- [ ] `TestUpdateStockPreservesUnmentionedScores` proves, for a subset update, that omitted categories keep both value and `UpdatedAt` (exact `.Equal` comparison against a pre-update read).
- [ ] The same test proves a brand-new category is added without disturbing existing categories, that a `nil` map changes nothing, and that an empty map changes nothing.
- [ ] The test proves no upsert path removes score rows (`scoreCount` is unchanged where expected) and that the returned `*Stock` hydrates the merged set.
- [ ] gRPC `UpdateStock`/`AddStocks` tests prove each request/message's scores reach the store verbatim (no unioning, no clearing) and that an absent map arrives as `nil`.
- [ ] Kafka `handle` tests prove the message's scores reach the store verbatim and a scoreless message produces a `nil` map.
- [ ] `README.md` and `docs/design.md` state the merge guarantee and name the only two deletion paths.
- [ ] `gofmt -l .` is empty; `go vet ./...`, `go build ./...`, and `go test ./...` all pass.
- [ ] DB-backed tests demonstrably **ran** with a real Postgres (`--- PASS`, not `--- SKIP`).
- [ ] No production code, proto, schema, or `go.mod` change is required; if one was made, it is minimal, justified, and covered by the tests.

---

## Risks and edge cases

1. **Scoreless upsert still refreshes the stock timestamp.** `stocks.timestamp` is refreshed by every upsert, including `nil`/empty scores (`store.go:82-87`). This is required for retention (an ingestion heartbeat) and must not be confused with score timestamps; tests assert the former moves while the latter do not.
2. **Timestamp equality vs. wall clock.** Compare stored timestamps with `.Equal`, never `==`, and capture baselines by reading rows back from Postgres. Do not infer preservation from `!After(...)` alone in a way that would also accept a *newer* timestamp; the intended assertion is equality with the baseline.
3. **`nil` vs. empty map.** Both hit the `len(scores) > 0` guard and are behaviorally identical in the store. gRPC's `req.GetScores()` returns `nil` for an absent proto map; `toMap(nil)` returns `nil`. Tests pin both nil paths so a future `len() > 0` → `!= nil` rewrite does not silently change semantics.
4. **Range validation happens before/around the store.** Kafka rejects out-of-range values before calling `UpdateStock`; gRPC has no explicit check, so the `scores.value` `CHECK` constraint fires. Because all score upserts and the stock upsert share one transaction and `defer tx.Rollback`, a constraint failure leaves **no** partial state (not even the refreshed stock timestamp). This is existing behavior, not changed here.
5. **Concurrency.** The merge is per-row `ON CONFLICT`, not a Go-level read-modify-write, so two concurrent partial updates to disjoint categories both persist; concurrent updates to the *same* category are last-writer-wins on value and timestamp. Tests are single-goroutine; this property is called out for reviewers rather than tested.
6. **Local test database is shared.** DB-backed tests must use `uniqueTag()` and `clearStock`; otherwise a pre-existing row (or leftover from a failed run) could make `scoreCount` assertions flaky. Keep the symbol prefix `TST` and defer cleanup immediately after generating the tag.
7. **`fakeStore` aliasing.** The gRPC fake currently assigns the caller's map directly (`f.stocks[k] = scores`). The new recorder must copy the map; otherwise a later mutation of the same map by the test would rewrite recorded history and mask a unioning bug. Do not change the fake's existing replacement semantics — the handler tests rely on it to observe exact pass-through.
8. **Proto map ordering.** Map iteration order is unspecified; assertions must be key-based, never index- or order-based.

---

## Verification checklist

- [ ] `podman start stocker-postgres-test` (or a disposable Postgres on a free port) is running before the store tests are claimed as passing.
- [ ] `DATABASE_URL=... go test -v ./internal/store` shows the four new subtests as `PASS`, and no `SKIP` for the new test.
- [ ] `go test ./...` passes with the database configured (DB tests executed, not skipped).
- [ ] `gofmt -l .` prints nothing; `go vet ./...` and `go build ./...` succeed.
- [ ] `git status` is clean after commit and push on the feature branch (not `main`).
