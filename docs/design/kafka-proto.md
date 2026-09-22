# Design: Replace Kafka JSON Serialization with Protobuf

## Status
Draft

> **Superseded (partially):** see [docs/design/score-entry-timestamp.md](score-entry-timestamp.md). The "why a map field?" rationale in this document (Section 2 and the "Explanation of choices" section) is **superseded**: scores now carry a per-entry `updated_at`, so `map<string,double>` is no longer sufficient — the current `StockUpdate.scores` field is `repeated ScoreEntry` in the `stockerstore.kafka.v1` namespace.

## Motivation

The kafka layer consumes stock messages over JSON. This is a well-known serialization anti-pattern: no schema evolution, large payloads, ambiguous type encoding, and no IDE support for consumers/producers. The project already has protobuf tooling set up via the gRPC service definition (`proto/v1/stock_store.proto`). We will introduce a **separate** proto file for kafka messages so that other services can use it as a shared contract without importing the full gRPC service.

## Design Decisions

### 1. Separate Proto File for Kafka Messages
- Location: `proto/v1/kafka/stock_message.proto`
- Rationale: Keeps concerns separated — gRPC service contract vs. message broker contract. External services that only publish to kafka don't need the full gRPC proto.
- Go package name: `kafkastockv1` (file path: `proto/v1/kafka/kafkastockv1.pb.go`)

### 2. Message Semantics -- One Message Per Stock
Rather than introducing request/response RPC-style terminology, kafka messages are **events**. The single type is `StockUpdate`:

| JSON field      | Proto3 field                              | Notes                          |
| --------------- | ----------------------------------------- | ------------------------------ |
| `"symbol"`      | `string symbol = 1`                       | Required (validated at runtime) |
| `"exchange"`    | `string exchange = 2`                     | Required (validated at runtime) |
| `"scores"`      | `map<string, double> scores = 3`          | Optional; empty map = no scores |

> **Superseded:** see [docs/design/score-entry-timestamp.md](score-entry-timestamp.md).

Why a map field? In proto3, `map<K,V>` defaults to nil when absent -- it never forces an empty collection the way `repeated` does. This perfectly matches the existing `omitempty` JSON behavior and means downstream code that checks `m.Scores == nil` vs `len(m.Scores) > 0` still works semantically unchanged.

**Note (superseded):** a later requirement (per-entry `updated_at` on each score) is impossible with `map<K,V>` — the current `StockUpdate.scores` field is therefore `repeated ScoreEntry`, not a map. See [docs/design/score-entry-timestamp.md](score-entry-timestamp.md).

### 3. Versioning / Importability for Other Repos
A `README.md` at `proto/v1/kafka/` covers three strategies: (a) go module dependency, (b) git submodule, (c) copy one file.

### 4. Codegen Location and Command
Generated code goes into `proto/v1/kafka/` alongside the existing gRPC generated files. Two commands in parallel for proto + grpc plugins. The existing `go.mod` already has `google.golang.org/protobuf v1.36.12` -- no new go dependencies needed beyond whatever protoc-gen-go binary is invoked at codegen time.

### 5. Migration Strategy
- **Breaking**: Producers that publish JSON must migrate too (documented, out of scope).
- **No backward-compatibility mode**: We do a clean cutover. Document the topic name and migration steps clearly. The old `Message` struct is removed entirely.

---

## Proto Definition

**File: `proto/v1/kafka/stock_message.proto`**

```protobuf
syntax = "proto3";

package stockerstore.kafka.v1;
option go_package = "github.com/wheeli-ca/stocker-store/proto/v1/kafka;kafkastockv1";

// StockUpdate carries a stock symbol, its exchange, and optional score
// entries. This is the message format published to (and consumed from)
// kafka topics by external scoring services.
message StockUpdate {
  string symbol   = 1;
  string exchange = 2;
  map<string, double> scores = 3;
}
```

### Explanation of choices

- **Package name `stockerstore.kafka.v1`**: Clear that this is the kafka schema (not gRPC), versioned at v1. Namespacing under `stockerstore` avoids collisions with other teams' message types.
- **Go package name `kafkastockv1`**: Short, readable Go import path when this repo is consumed as a module.
- **`map<string, double>` instead of `repeated ScoreEntry`**: Matches the existing `map[string]float64` Go type exactly. No need for a `ScoreEntry` wrapper type just for kafka messages -- the gRPC proto already has one for stored data.

  > **Superseded:** a later requirement (per-entry `updated_at`) cannot be carried in a map field. The current `StockUpdate.scores` field is therefore `repeated ScoreEntry` — see [docs/design/score-entry-timestamp.md](score-entry-timestamp.md).

---

## Proto Directory Layout (After)

```
proto/
├── v1/
│   ├── stock_store.proto             # (unchanged) gRPC service definition
│   ├── stockstorev1.pb.go            # (unchanged) gRPC generated code
│   ├── stockstorev1_grpc.pb.go       # (unchanged) gRPC generated code
│   └── kafka/
│       ├── README.md                 # NEW: how to consume protos from other repos
│       ├── stock_message.proto       # NEW: kafka message schema
│       ├── stock_message.pb.go                # NEW: protoc-gen-go output
│       └── stock_message_grpc.pb.go           # may be absent -- no RPC service here
```

---

## README for Proto Consumers (to live at `proto/v1/kafka/README.md`)

See the "Implementation Steps" section at the bottom of this document for the exact file content.

---

## Detailed Changes to `internal/kafka/client.go`

### Step 1: Replace imports

**Remove**: `"encoding/json"`

**Add**: `"stocker-store/proto/v1/kafka"` (aliased as `kafkastockv1`)
**Add**: `"google.golang.org/protobuf/proto"`

```go
import (
    "context"
    "errors"
    "fmt"
    "log"

    kafkastockv1 "stocker-store/proto/v1/kafka"
    "google.golang.org/protobuf/proto"

    "github.com/segmentio/kafka-go"
)
```

### Step 2: Remove `Message` struct (lines 17-21 of current file)

Delete entirely. The `Store` interface remains unchanged -- it takes a Go native `map[string]float64`, not proto types.

### Step 3: Change `Client.decoder` field type

**Before**:
```go
type Client struct {
    ...
    decoder func(raw []byte) (*Message, error)
}
```

**After**:
```go
type Client struct {
    ...
    decoder func(raw []byte) (*kafkastockv1.StockUpdate, error)
}
```

### Step 4: Change the `New` constructor to use the new decoder name

**Before**:
```go
New(cfg Config, store Store) *Client {
    return &Client{
        ...
        decoder: decodeMessage,
    }
}
```

**After**:
```go
New(cfg Config, store Store) *Client {
    return &Client{
        ...
        decoder: decodeStock,   // renamed from decodeMessage
    }
}
```

### Step 5: Replace `decodeMessage` with `decodeStock`

**Before**:
```go
func decodeMessage(raw []byte) (*Message, error) {
    var m Message
    if err := json.Unmarshal(raw, &m); err != nil {
        return nil, fmt.Errorf("decode stock message: %w", err)
    }
    if m.Scores == nil {
        m.Scores = map[string]float64{}
    }
    return &m, nil
}
```

**After**:
```go
func decodeStock(raw []byte) (*kafkastockv1.StockUpdate, error) {
    m := new(kafkastockv1.StockUpdate)
    if err := proto.Unmarshal(raw, m); err != nil {
        return nil, fmt.Errorf("decode stock message: %w", err)
    }
    if m.Scores == nil {
        m.Scores = make(map[string]float64)
    }
    return m, nil
}
```

Key differences:
- `proto.Unmarshal(raw, m)` takes a pointer to an already-allocated struct (unlike `json.Unmarshal` which accepts a pointer to the destination you create via `var`).
- `proto.Unmarshal` requires message types that implement the `proto.Message` interface. Our generated code will provide this automatically.

### Step 6: The `handle` method needs no changes beyond type inference

**Before**:
```go
func (c *Client) handle(ctx context.Context, msg kafka.Message) error {
    m, err := c.decoder(msg.Value)
    if err != nil {
        return err
    }

    if m.Symbol == "" || m.Exchange == "" {
        return errors.New("stock message missing symbol or exchange")
    }

    for cat, val := range m.Scores {
        if val < -1.0 || val > 1.0 {
            return fmt.Errorf("score %s value %v out of range [-1, 1]", cat, val)
        }
    }

    return c.store.UpdateStock(ctx, m.Symbol, m.Exchange, m.Scores)
}
```

**After**: No changes needed at all because:
- `kafkastockv1.StockUpdate` has fields named exactly `Symbol`, `Exchange`, and `Scores` (Go's protoc-gen translates proto snake_case to camelCase).
- The types are identical strings/maps.
- Comparison logic (`== ""`, comparison with -1.0/1.0) is unchanged.

### Step 7: Remove the empty line where the old struct lived

After the `Message` struct deletion, clean up extra blank lines between comments and the next type definition.

---

## Test Changes

### File to update
Likely `internal/kafka/client_test.go`. Current tests use `json.Marshal` to create test payloads.

### Adaptation pattern: before vs after

**Before (existing)**:
```go
raw, err := json.Marshal(kafka.Message{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores:   map[string]float64{"momentum": 0.7},
})
```

**After**:
```go
import kafkastockv1 "stocker-store/proto/v1/kafka"

raw, err := proto.Marshal(&kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores:   map[string]float64{"momentum": 0.7},
})
```

### Specific changes in each test case

1. Replace `json.Marshal(kafka.Message{...})` with `proto.Marshal(&kafkastockv1.StockUpdate{...})`.
2. The error handling pattern stays the same: check `err != nil`, then pass `raw` (the serialized bytes) to whatever helper the test uses.
3. Tests that validate error decoding (invalid JSON payloads) should now test invalid binary proto -- either zero-length bytes, truncated protos, or garbage bytes. The error message will change from a JSON parse error to a proto unmarshal error.
4. **No functional changes** in test expectations: the `handle` method and store interface remain identical in their signatures.

### Additions
Add one integration-style test that verifies `proto.Marshal` + `proto.Unmarshal` round-trips correctly for `StockUpdate`:
```go
func TestDecodeStockRoundtrip(t *testing.T) {
    orig := &kafkastockv1.StockUpdate{
        Symbol:   "TSLA",
        Exchange: "NYSE",
        Scores: map[string]float64{
            "momentum": 0.5,
            "value":   -0.3,
        },
    }
    raw, err := proto.Marshal(orig)
    require.NoError(t, err)

    decoded, err := decodeStock(raw)
    require.NoError(t, err)
    assert.Equal(t, orig.GetSymbol(), decoded.GetSymbol())
    assert.Equal(t, orig.Exchange, decoded.Exchange)
    assert.Equal(t, orig.Scores, decoded.Scores)
}
```

---

## README Content for `proto/v1/kafka/README.md`

```markdown
# Kafka Message Protos

Proto definitions for stock message events shared across services.

## Consuming in Another Go Service (preferred)

As a go module:

```go
// In your go.mod:
require github.com/wheeli-ca/stocker-store v...
```

Import the generated types:

```go
import kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores:   map[string]float64{"momentum": 0.7},
}
data, _ := proto.Marshal(msg)
// publish data to kafka topic...
```

## Consuming via Git Submodule

Clone this repo as a submodule or copy the entire `proto/v1/kafka/` directory into your own project's `proto/` tree.

Then run protoc against it:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
    proto/v1/kafka/stock_message.proto
```

## Consuming by Copy

Simply copy `proto/v1/kafka/stock_message.proto` into any other project's `proto/` directory and run protoc.

The proto has no external dependencies beyond standard Googletypes, so the single file is portable.

## Schema

| Field      | Type                | Required? | Notes                          |
|------------|---------------------|-----------|--------------------------------|
| `symbol`   | string              | Yes       | Stock ticker (e.g. "AAPL")    |
| `exchange` | string              | Yes       | Exchange name (e.g. "NASDAQ")  |
| `scores`   | map<string, double> | No        | Optional scoring categories    |

## Versioning

This proto lives in the `stockerstore.kafka.v1` namespace. Breaking changes
will increment the minor version suffix (e.g., `v2`). Always check for a
versioned directory if integrating from another service.
```

---

## go.mod / go.sum Impact

No changes needed to `go.mod`. The module already depends on:
- `google.golang.org/protobuf v1.36.12` -- provides `proto.Unmarshal`, `proto.Marshal`, and the protobuf runtime.
- `github.com/golang/protobuf` is NOT required; we use the new `google.golang.org/protobuf` path exclusively.

The only dependency change would be adding `bufbuild/protobuf` or similar to a build tool, which is typically in go.mod as an `exclude` or not listed at all (it's just a CLI tool).

---

## Implementation Steps (Numbered)

Each step below is small enough to be executed independently by a developer agent.

### Step 1: Create the kafka proto directory
```bash
mkdir -p /home/brian/workspace/stocker-store/proto/v1/kafka
```

### Step 2: Write `proto/v1/kafka/stock_message.proto` with the exact content from the Proto Definition section above

### Step 3: Generate Go code from the new proto file
```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/v1/kafka/stock_message.proto
```

Verify that `proto/v1/kafka/stock_message.pb.go` and `proto/v1/kafka/stock_message_grpc.pb.go` are created. The `_grpc.pb.go` file may be minimal or empty since there's no service definition -- this is fine and expected.

### Step 4: Verify proto code compiles
```bash
go build ./proto/v1/kafka/...
```

This should produce no errors. If it does, check that protoc-gen-go and protoc-gen-go-grpc are installed.

### Step 5: Write `proto/v1/kafka/README.md` using the exact content from the README Content section above

### Step 6: In `internal/kafka/client.go`, replace imports
- Delete `"encoding/json"`
- Add the protobuf import with alias: `kafkastockv1 "stocker-store/proto/v1/kafka"`
- Add `"google.golang.org/protobuf/proto"`

### Step 7: In `internal/kafka/client.go`, remove the local `Message` struct (lines 15-21)

Delete all lines defining the struct and its JSON tags.

### Step 8: In `internal/kafka/client.go`, change `Client.decoder` field type from `func(raw []byte) (*Message, error)` to `func(raw []byte) (*kafkastockv1.StockUpdate, error)`

### Step 9: In `internal/kafka/client.go`, rename the decoder reference in `New()` from `decodeMessage` to `decodeStock`

### Step 10: Rename `decodeMessage` to `decodeStock` and replace JSON unmarshaling with protobuf unmarshaling per the detailed Step 5 changes above

### Step 11: Verify `internal/kafka/client.go` compiles
```bash
go build ./internal/kafka/...
```

The existing validation in `handle()` should work unchanged because the field names and types match exactly.

### Step 12: Update test files (likely `internal/kafka/client_test.go`) -- replace `json.Marshal(kafka.Message{...})` with `proto.Marshal(&kafkastockv1.StockUpdate{...})` in every test helper that creates message payloads

### Step 13: Update or remove tests for invalid JSON decoding (error cases) -- now these should test invalid binary proto instead of malformed JSON. The error wording will change from JSON parse errors to proto unmarshal errors; adjust test assertions accordingly.

### Step 14: Add a round-trip test (`TestDecodeStockRoundtrip`) that verifies `proto.Marshal` + `decodeStock` produce an identical `StockUpdate`

### Step 15: Run the full test suite
```bash
go test ./...
```

Verify all existing tests pass.

### Step 16: Verify final code builds and passes vet
```bash
go build ./... && go vet ./...
```

---

## Migration Notes for External Services (Out of Scope)

Document this in a separate ticket or change the service's release notes to mention:

> On [date], the kafka topic `stocks` switched from JSON to protobuf-serialized `StockUpdate` messages. Producers must migrate their serialization format using the shared proto at https://github.com/wheeli-ca/stocker-store/tree/main/proto/v1/kafka/stock_message.proto.
> The schema is documented at: docs/design/kafka-proto.md

The actual migration of producing services is a separate task -- this design only covers the consumer-side code change in `stocker-store`.
