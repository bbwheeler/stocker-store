# Stocker Store — Code Review

Review date: 2026-08-10

Verified against: `go build`, `go vet`, manual reading of all tracked files.

## Build blockers (highest priority)

1. **The project does not compile.** `cmd/storage/main.go:14` imports `stocker-store/internal/grpc`, but that package does not exist anywhere in the repo (including on other branches). The gRPC server, `grpc.NewServer`, `GRPCServer()`, `HTTPServer()`, and all generated protobuf code are missing. This is the single biggest gap.

2. **Literal syntax error in `internal/data/store.go:110`:** the line `Query  return StockList` is invalid Go and aborts compilation of the `data` package.

3. **Undefined identifiers in `internal/data/store.go`:** `strings` (line 103, never imported), `rows` (line 106, never declared), `ScoreEntry` (line 106, no such type), `Stock` (lines 72/82/97, no such type), and `Stock.Created` (line 82). None of these compile even after fixing item 2.

4. **`internal/data/models.go` defines the wrong types.** It declares `ScoreValue` and `StockWithScore` but omits `Stock` and `ScoreEntry` — the exact types the store methods actually reference. Models and store are out of sync.

5. **`go.sum` is missing and uncommitted.** `go build`/`go test` fail with "missing go.sum entry" until `go mod download`/`go mod tidy` is run. It should be committed. Relatedly, `google.golang.org/grpc` in `go.mod` is currently unused (nothing imports it, so `go mod tidy` drops it) until the missing gRPC package is written.

## Schema defects (`internal/data/store.go:35-60`)

6. **Broken foreign key:** `stock_id TEXT REFERENCES stocks(symbol, exchange)` (line 45) references the composite PK `(symbol, exchange)` from a single TEXT column. PostgreSQL rejects this DDL (column-count mismatch), so `initializeTables` fails at startup and the service can never boot. The `scores` table needs `symbol` and `exchange` columns (as design.md §3.1 says it should have) referencing the composite key.

7. **No history support despite the design.** `UNIQUE (stock_id, category)` (line 49) conflicts with design.md §3.3, which calls for writing history rows while updating the current row in place. One row per `(stock, category)` makes multi-point history impossible. There is no separate history table or mechanism.

8. **Comment/code contradiction:** the comment on line 34 says the tables are "simply DROP and recreate[d]", but the SQL only runs `CREATE TABLE IF NOT EXISTS`. Nothing is ever dropped, so stale schema/constraints persist across restarts.

9. **`scores` is not joinable back to `stocks`** (no `symbol`/`exchange` columns), so score queries can't return the stock's symbol without denormalizing.

## Data-layer logic

10. **`DeleteStock` (store.go:102-112) is a non-functional stub.** Its body references undefined symbols, never issues a DELETE, and ignores the `exchange` argument even though `RemoveStockRequest` carries one. Per design.md §8, removing a stock should also delete its scores/history.

11. **Five of the eight RPCs have no backing store method.** There is no implementation for `UpdateScore`, `GetScores`, `GetRandomStocks`, `GetTopStocks`, or score-filtering. `ListStocksByCategory` (line 115) is a hardcoded stub (`return []ScoreValue{}, nil`).

12. **`AddStocksResponse.added` can't be accurate.** `InsertStock` uses `ON CONFLICT DO NOTHING` (line 64) but never reports whether a row was actually inserted, so the client-streaming handler can't count real inserts.

13. **`StockByID` is misnamed and over-clever.** It queries by *symbol* (not ID) and returns a `*Stock` type that doesn't exist. The dynamic SQL for the optional exchange filter (lines 73-79) concatenates placeholders; `AND exchange = COALESCE($2, exchange)` is simpler and equivalent.

14. **No `context` timeouts or deadlines anywhere** in the store, and no transaction use for multi-statement flows. A hung database means a hung request forever.

15. **`StocksByExchange` (line 97) swallows the query error context:** the error from `pgx.CollectRows` is returned without wrapping, unlike every other method.

16. **No input validation** (empty `symbol`/`exchange`) at either the data or API layer, despite design.md §7.2 calling for server-side validation.

## Protobuf API (`proto/v1/stock_store.proto`)

17. **`StockByExchange` (line 58) has no `exchange` field.** For `GetStockBySymbol` (which may return all exchanges), the same symbol on two exchanges is indistinguishable in the response.

18. **Proto and design.md have drifted.** design.md §4 says `GetStockBySymbol` returns a single `Stock`, but the proto returns `StockList`. `ScoreSnapshot` also differs (proto: `{symbol, exchange, scores[]}`; design: `{category, value, history[]}`). One of them should win.

19. **`GetScoresRequest.date_unix` is `optional int64` while design.md uses `google.protobuf.Timestamp`.** Pick one; if the design's intent was a `Timestamp`, use it (the `int64` loses timezone/format semantics).

20. **`ScoreHistory` (line 46) carries no `symbol`/`exchange`/`category` fields**, so a client can't attribute the returned history to a stock without prior context.

21. **No upper bound enforcement** on `GetRandomStocksRequest.count` or `GetTopStocksRequest.limit`, even though design.md §4 explicitly requires "reasonable upper limit" on both.

22. **`UpdateScore` has no server-side range validation**; invalid values surface only as a raw DB `CHECK` failure instead of an `INVALID_ARGUMENT` gRPC error (design.md §7.3).

23. **Inconsistent formatting**: some messages are one-liners, others multi-line. Run `buf lint`/`clang-format` for consistency.

24. **`optional` fields require `protoc-gen-go` ≥ v1.28**; there's no `go:generate`/protoc target documenting the toolchain (see item 37).

## `cmd/storage/main.go`

25. **The HTTP health server blocks shutdown.** `server.HTTPServer(":3501")` (line 40) is called synchronously on the main goroutine and its return value stuffed into `errServer`, which a goroutine then compares to `nil`. The HTTP server never runs as a goroutine, and the `<-ctx.Done()` shutdown path (line 56) is unreachable, so graceful shutdown never happens.

26. **`cancel()` is called twice** (defer on line 24 plus an explicit call on line 60) — harmless but sloppy.

27. **SIGTERM is not handled** even though `syscall` is imported; only `os.Interrupt` is registered (line 23). Container orchestration (Podman/Kubernetes, per design.md) sends SIGTERM, which would kill the process without graceful shutdown.

28. **Hardcoded ports `:3500`/`:3501` and DSN defaults** with no flag/env override for the ports. Design.md §6.4 says file-based config "is fine for v1"; at minimum the ports should be env-configurable like `DATABASE_URL` is.

29. **Only the gRPC server gets `GracefulStop`**; if the health server ever runs properly, it has no shutdown handling.

## Design-doc drift (`design.md`)

30. **§8 references a `stock_exchanges` table that doesn't exist** anywhere (the schema calls it `stocks`), and §6.2 mentions "company/exchange CRUD" — three different names for the same entity.

31. **"By score range" (`BETWEEN min AND max`, §5.4) has no corresponding RPC** in the proto.

32. **"Weighted scoring queries" (§2) have no representation** in schema, proto, or code — no weights table or request field.

33. **§3.3's history model contradicts the schema's `UNIQUE(stock_id, category)`** (see item 7).

34. **§10 ops/deployment is a TODO** and no container artifacts exist — the design commits to Podman Quadlet for v1, but there's no Dockerfile/quadlet file or compose file to run it.

35. **§8 history pruning ("eg 1 year") is called out but never designed** (no retention job, no config knob).

## Project / tooling

36. **There are no tests.** The Makefile has a `test` target (`go test ./...`) that has nothing to run, and the two packages don't even compile.

37. **The Makefile has no protoc/`generate` target, no `vet`/`lint` target**, and `build` only compiles the single binary rather than `./...`. Generated `*.pb.go` files are neither generated by the build nor committed.

38. **No README, LICENSE, or any user-facing documentation.**

39. **AGENTS.md workflow doesn't match reality.** It says to push to GitHub and open PRs with `gh`, with credentials in `../github.md`, but the actual remote is a self-hosted Gitea instance (`https://git.wheeli.ca/...`). Following AGENTS.md literally would push to the wrong place and fail to open PRs.

40. **Empty `internal/grpc/` and `internal/migration/` directories are not tracked** (git ignores empty dirs), so the intended structure is invisible to anyone cloning fresh — and the migration directory implies migrations that don't exist.

41. **No CI.** There is no `.github/workflows/` (or Gitea/Forgejo CI) to build, test, or vet on push.

42. **Uncommitted work in flight:** `proto/v1/stock_store.proto` has staged changes (score_filters comment, message reorder/rename `Stock`→`StockByExchange`) that are not committed, and branch `addstocks-streaming` is 1 commit ahead of origin. There are also stale branches (`feature/design-doc`, `feature/implementation`, `implement-grpc-server`) — the last one contains no gRPC server despite its name.

43. **Models `ScoreValue` and `StockWithScore` are dead code** until the score/top-stock store methods are implemented.

44. **No rate/result limits or pagination** on any list RPC; design.md §11 lists pagination as a "future extension" but it will be needed before the service is usable at the stated "thousands of stocks" scale.
