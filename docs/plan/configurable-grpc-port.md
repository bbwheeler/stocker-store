# Configurable gRPC Port — Implementation Plan

**Goal:** Make the gRPC listen port configurable via the environment variable `GRPC_PORT`, defaulting to `3500` so every existing deployment keeps working unchanged.

**Architecture:** A single pure helper in package `main` turns the raw environment value into a validated port number. `main` derives the listen address (`:<port>`) once from that value and uses the *same* string for `net.Listen`, the listen-failure log, and the "listening on" log, so the address and the log line cannot disagree. Invalid values abort startup with a fatal log instead of falling back to a default.

**Tech Stack:** Go 1.25, `net`, `strconv`, `strings`, `log` — standard library only, no new dependencies.

**Spec:** This plan is the spec for the change; it also defines the `GRPC_PORT` row to add to `docs/design.md` §12.6.

## Global constraints

- Default MUST be `3500`: no existing deployment (Quadlets, `podman run --network=host`, local `make run`) changes behaviour.
- No new dependencies; no new files outside `cmd/main_test.go` and this plan.
- The helper MUST stay inside `cmd/main.go`, **not** a new `cmd/*.go` file: `Makefile` and `Containerfile` both build the single file `./cmd/main.go`, and a second non-test file would be excluded from those builds (`undefined: parsePort`). `cmd/main_test.go` is safe because it is only compiled by `go test ./cmd`, which builds the whole directory.
- Parsing must be pure (no sockets, no env mutation) so it is unit-testable without binding a port.
- Keep the code surface minimal; do not restructure `main` beyond what the port requires.

---

## File structure (before defining steps)

| File | Change | Responsibility |
|---|---|---|
| `cmd/main.go` | Add `parsePort`; replace both `:3500` literals and the fatal message | Validate `GRPC_PORT`, derive one address string, use it everywhere. |
| `cmd/main_test.go` | **Create** | Table-driven `parsePort` tests: default, override, whitespace, invalid. |
| `docs/design.md` | Add a row to §12.6 table + one bullet | Record the variable, default, format, failure mode. |
| `README.md` | Diagram, `grpcurl` note, env table, sample env, container/quadlet sections, "stale `EXPOSE`" warnings, repo layout, troubleshooting, dev section | Describe the new reality; remove the "stale port metadata" story. |
| `Containerfile` | `EXPOSE 50051` → `EXPOSE 3500` | Image metadata matches the default listen port. |

No changes to `internal/`, `proto/`, `deploy/quadlet/*`, `go.mod`, or `Makefile`.

---

## Design decisions (summary, with rationale)

1. **Variable name — `GRPC_PORT`.** Matches the existing flat `UPPER_SNAKE` scheme (`STOCK_TTL`, `KAFKA_OUTPUT_TOPIC`) and names the thing the operator cares about (the port), not the address. Rejected: `LISTEN_ADDR`/`GRPC_ADDR` (implies host control, which is a non-goal), `PORT` (too generic, collides with platform conventions and with the container/PaaS ecosystems that inject it for HTTP).

2. **Default `3500`, applied when the variable is unset or empty.** An empty string is treated exactly like unset. This is the compatibility guarantee: no deployment changes.

3. **Accepted format — a decimal TCP port, `1`–`65535`, surrounding whitespace trimmed.** Parsing is `strings.TrimSpace` → `strconv.Atoi` → range check. The range check also rejects `0`, `-1`, `65536`, and `3.5` (which fails `Atoi`). Note `Atoi` accepts a leading `+` (`"+3500"` → `3500`); that is harmless and no extra strictness is added.

4. **Invalid input fails fast.** `main` calls `log.Fatalf("invalid GRPC_PORT %q: ...")` and the process exits non-zero. Rationale:
   - A silently-defaulted port is the worst outcome: the process is "up" but unreachable on the port the operator configured, and the operator only learns this from client `connection refused` errors much later. A boot-time fatal is loud, immediate, and leaves the failure in the logs (`journalctl`/`podman logs`) that operators already check.
   - Two replicas silently defaulting to `3500` on one host collide, and the resulting `bind: address already in use` fatal would be reported as a generic listen failure rather than as the config typo it is.
   - Precedent: a missing `DATABASE_URL` is already fatal. `STOCK_TTL` keeps its log-and-default behaviour because a bad TTL degrades only retention, whereas a bad port makes the whole service unreachable — the blast radius differs, so the handling differs.

5. **One derived address, three consumers.** `main` computes `addr := fmt.Sprintf(":%d", port)` exactly once. `net.Listen("tcp", addr)`, the fatal `listen on %s: %v`, and `stocker-store listening on %s` all use `addr`. There is no second literal anywhere, so the log can never claim a port different from the bound one.

6. **`EXPOSE 3500` in the `Containerfile` (change it, don't delete it).** `EXPOSE` is static image metadata: it does not create a listener and cannot follow `GRPC_PORT` at runtime. Declaring the default makes the metadata truthful for the common case and keeps tooling that reads it (`podman inspect`, docs generators) correct. Removing it would lose that hint for no gain. `EXPOSE` does not affect host-networked runs (the Quadlet case) and only matters for `-p`/`-P` usage — the README must say so, including that an overridden `GRPC_PORT` requires publishing that port.

7. **Quadlet units are unchanged.** They use host networking, so the host port is whatever `GRPC_PORT` resolves to; no `PublishPort` is involved. Only the README prose and the sample env file change.

### Non-goals

- Configurable bind host / interface (e.g. `GRPC_HOST`, `0.0.0.0` vs `127.0.0.1`). The listener stays `:<port>` (all interfaces).
- Any config file, flag parser, or `GRPC_ADDR`-style combined host:port value.
- TLS, mTLS, reflection, or any other gRPC server option.
- Changing the port used by anything else (Postgres/Kafka are client-side, configured by their own URLs).
- Backward compatibility shims: there is no previous port variable to honour.

---

## Configuration table row

### `docs/design.md` §12.6 — add as the first data row (service-wide rows before Kafka rows)

```markdown
| `GRPC_PORT`         | No       | `3500`           | Decimal TCP port, `1`–`65535`                       | Port the gRPC server listens on. Invalid values abort startup (new). |
```

Plus one bullet under the existing bullet list in §12.6, after the Kafka bullets (which stay untouched):

```markdown
- `GRPC_PORT` is validated before the listener is created; an unset or empty value means `3500`. An unparseable or out-of-range value is fatal at startup rather than silently defaulted.
```

### `README.md` — Environment Variables table

```markdown
| `GRPC_PORT` | No | `3500` | Decimal TCP port, `1`–`65535` | Port the gRPC server binds to (`:PORT`). Invalid values abort startup. |
```

---

## Documentation changes required (complete list)

| File / section | Change |
|---|---|
| `docs/design.md` §12.6 | Add the `GRPC_PORT` table row and the validation bullet above. No other prose in `docs/design.md` mentions the port (verified: no `3500`/`50051` anywhere in the file). |
| `README.md` line ~8 (ASCII diagram) | `gRPC :3500` → `gRPC :3500 (GRPC_PORT)` so the diagram names the knob. |
| `README.md` lines ~138–149 (`grpcurl` examples) | Keep `localhost:3500` (it is the default) and add one clause: "the default port; set `GRPC_PORT` to change it". |
| `README.md` §Configuration → Environment Variables (~line 214) | Add the `GRPC_PORT` row. |
| `README.md` §Sample `.env.podman` (~line 233) | Add a commented `# Optional — gRPC listen port (default 3500).` / `# GRPC_PORT=3500` pair. |
| `README.md` §Option B — container (~line 286) | **Replace** the "EXPOSE 50051 is stale, misleading metadata" note. New reality: `EXPOSE 3500` matches the default; `--network=host` needs no publishing; a bridge-network run needs `-p 3500:3500` (or `-p $GRPC_PORT:$GRPC_PORT` when overridden). |
| `README.md` §Option C — Quadlets (~line 304) | Say the service is reachable on the host's `GRPC_PORT` (default `3500`), that host networking means the port must be free on the host, and that setting `GRPC_PORT` in `.env.podman` moves it. |
| `README.md` §Quickstart (~line 312) | Optional: note that `GRPC_PORT` can be added to the env file; verify with `podman logs` showing the actual port. |
| `README.md` §Repository layout (~line 385) | `Containerfile ... (note the stale EXPOSE 50051)` → `Containerfile # multi-stage build (EXPOSE 3500)`; note `cmd/main.go # … gRPC listen port (GRPC_PORT) …`; add `cmd/main_test.go`. |
| `README.md` §Troubleshooting (~line 395) | **Replace** "The real port is 3500, not 50051" with: the port is `GRPC_PORT` (default `3500`), logged at startup as `stocker-store listening on :<port>`; `EXPOSE 3500` reflects the default only. |
| `README.md` §"Authoritative deployment variables" (~line 398) | Add `GRPC_PORT` to the list. |
| `README.md` §Known inconsistencies (~line 404) | **Delete** the port bullet (this feature resolves it); leave the image-name and `install.sh` bullets. |
| `README.md` §Build & test (~line 360) | Add `go test ./cmd` (or note that `make test`/`go test ./...` now covers the port parser). No separate test-suite section exists; this is the only place that documents tests. |
| `Containerfile` line 25 | `EXPOSE 50051` → `EXPOSE 3500` (justified in Design decision 6). |
| `deploy/quadlet/stocker-store.container` | **No change.** Host networking; document only. |
| `.env.podman` samples / Quadlet docs | Covered by the README sample-env edit; no file is shipped in the repo. |

---

## Testing strategy

**Unit tests (required, no sockets).** `parsePort` is a pure function, so tests never bind a listener and never need `DATABASE_URL`. Table-driven `TestParsePort` in `cmd/main_test.go`:

| Input | Expected |
|---|---|
| `""` (unset/empty) | `3500`, no error |
| `"9090"` | `9090`, no error |
| `"  8080  "` | `8080`, no error (whitespace trimmed) |
| `"1"` | `1`, no error (lower bound) |
| `"65535"` | `65535`, no error (upper bound) |
| `"abc"` | error |
| `"0"` | error (not a usable port) |
| `"65536"` | error (out of range) |
| `"-1"` | error (out of range) |
| `"3.5"` | error (not an integer) |
| `"3500 "` / `"\t3500\n"` | `3500`, no error |
| `"0x10"` | error |

Each case asserts the returned port and whether `err != nil`; the default case also asserts that the default constant is `3500` (the compatibility guarantee), ideally by referring to the named default constant rather than a magic number. Also assert an invalid value produces an error message that contains the offending raw value, so the fatal log is actionable.

`make build` (`go build ./cmd/main.go`), `go vet ./...`, `go test ./...` are the acceptance gates; they need no database.

**Integration check (manual, optional).** Because `store.NewStore` requires a reachable Postgres, a real start-up check needs `DATABASE_URL`:

1. Build, then `GRPC_PORT=19090 DATABASE_URL=... ./bin/stocker-store`.
2. Confirm the log line reads `stocker-store listening on :19090` and that `ss -ltn | grep 19090` shows the listener.
3. Repeat with `GRPC_PORT=abc` and confirm a non-zero exit with the `invalid GRPC_PORT "abc"` message and no listener.
4. With `GRPC_PORT` unset, confirm `:3500` (regression check for existing deployments).

This is not added to `go test ./...` because it depends on external infrastructure; the parser tests cover the logic and the single-derived-address rule is covered by code review plus the log/`ss` check in step 8.

---

## Implementation steps

Each step is independently verifiable; run `gofmt -l .` after any Go step (it must print nothing).

### Step 1 — Write the failing `parsePort` tests

**Files:** create `cmd/main_test.go`.
**Do:** add the table-driven `TestParsePort` with every case from the testing table above, calling `parsePort(raw)` and asserting `(port, error)`. Do not add the implementation yet.
**Acceptance:** `go test ./cmd` fails to compile with `undefined: parsePort` — a deliberate red state proving the test exercises the new helper.

### Step 2 — Implement `parsePort` in `cmd/main.go`

**Files:** modify `cmd/main.go` (helper + a named `defaultGRPCPort = 3500` constant near it).
**Do:** add `func parsePort(raw string) (int, error)` in `cmd/main.go` (**not** a new file — see Global constraints): trim, treat empty as the default, parse with `strconv.Atoi`, reject values outside `1..65535`, and return errors that quote `raw` and state the accepted range. Use `fmt.Errorf` so `main` can wrap it in `log.Fatalf`.
**Acceptance:** `go test ./cmd` passes; `go build -o /tmp/stocker-store ./cmd/main.go` succeeds (proves the single-file build still works); `gofmt -l cmd` prints nothing.

### Step 3 — Wire the port into `main`

**Files:** modify `cmd/main.go` (`main` only, around lines 55–72).
**Do:** read `os.Getenv("GRPC_PORT")`, call `parsePort`, and on error `log.Fatalf("invalid GRPC_PORT %q: %v", raw, err)`. Compute `addr := fmt.Sprintf(":%d", port)` once (or `":" + strconv.Itoa(port)`), then use `addr` in `net.Listen("tcp", addr)`, in `log.Fatalf("listen on %s: %v", addr, err)`, and in `log.Printf("stocker-store listening on %s", addr)`. Remove every `3500` literal from `main`.
**Acceptance:** `grep -n 3500 cmd/main.go` shows only the default constant; `go build ./cmd/main.go` and `go vet ./...` succeed; `go test ./cmd` still passes.

### Step 4 — Document the variable in `docs/design.md`

**Files:** modify `docs/design.md` §12.6 (around line 297).
**Do:** add the `GRPC_PORT` table row exactly as given in "Configuration table row" above, plus the validation bullet after the existing Kafka bullets. Touch nothing else in the file.
**Acceptance:** `grep -n GRPC_PORT docs/design.md` shows both the row and the bullet; the markdown table still renders (the row has five columns, same as its neighbours).

### Step 5 — Update README configuration docs

**Files:** modify `README.md` (diagram line ~8, `grpcurl` note ~138, env table ~214, sample env ~233, "Authoritative deployment variables" ~398).
**Do:** apply the configuration-doc rows from the documentation table above. Keep `localhost:3500` in commands; add the "default port, override with `GRPC_PORT`" clause.
**Acceptance:** the env table contains `GRPC_PORT` with default `3500`; the sample env file shows a commented `GRPC_PORT=3500`; no statement claims the port is hardcoded.

### Step 6 — Update README deployment and troubleshooting docs

**Files:** modify `README.md` (Option B ~286, Option C ~304, Quickstart ~312, repo layout ~385, Troubleshooting ~395, Known inconsistencies ~404, Build & test ~360).
**Do:** replace both stale-`EXPOSE` warnings with the truthful `EXPOSE 3500` description (default metadata; publish the port only for non-host networking; overrides need publishing too); describe the Quadlet host-network port as `GRPC_PORT`; add `cmd/main_test.go` and the port to the layout; add `GRPC_PORT` to the authoritative-variables bullet; delete the port bullet from "Known inconsistencies". Steps 5 and 6 edit disjoint line ranges of the same file and can be run in either order.
**Acceptance:** `grep -n "50051" README.md` returns nothing; `grep -n "stale" README.md` no longer matches any port claim.

### Step 7 — Fix the `Containerfile` metadata

**Files:** modify `Containerfile` line 25.
**Do:** change `EXPOSE 50051` to `EXPOSE 3500`. Add nothing else (no `ENV GRPC_PORT`, no comment block — the README carries the explanation).
**Acceptance:** `grep -n EXPOSE Containerfile` prints `EXPOSE 3500`; `podman build -t stocker-store .` still succeeds if a runtime is available (optional).

### Step 8 — Final verification and commit

**Files:** none (verification + commit).
**Do:**
1. `gofmt -l .` → no output.
2. `go vet ./...` → clean.
3. `go build ./cmd/main.go` → succeeds (single-file build used by `Makefile`/`Containerfile`).
4. `go test ./...` → all pass; database-backed tests skip without `DATABASE_URL`, which is expected and not a failure.
5. Optional integration check from the testing strategy (default `:3500`, override, invalid value) when a database is reachable.
6. `grep -rn "50051" .` → no matches outside `docs/plan/`.
7. Commit and push:
   ```bash
   git add cmd/main.go cmd/main_test.go docs/design.md README.md Containerfile
   git commit -m "feat(config): make the gRPC port configurable via GRPC_PORT"
   git push
   ```

**Acceptance:** every command above returns the stated result, the commit contains only the five files, and the push succeeds.

---

## Verification checklist

- [ ] Unset/empty `GRPC_PORT` still listens on `3500` (no deployment change).
- [ ] `GRPC_PORT=9090` listens on `9090`, and the log line says `:9090`.
- [ ] The log line and the bound address come from the same string (code review of `main`).
- [ ] `abc`, `0`, `65536`, `-1`, `3.5` exit non-zero with an `invalid GRPC_PORT "<value>"` message.
- [ ] No `3500` or `50051` literal remains in `cmd/main.go` except the named default constant.
- [ ] No `50051` remains anywhere outside `docs/plan/`.
- [ ] `gofmt`, `go vet`, `go build ./cmd/main.go`, `go test ./...` all clean.
