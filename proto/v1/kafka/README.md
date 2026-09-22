# Kafka Message Protos

Proto definitions for stock message events shared across services.

## Consuming in Another Go Service (preferred)

As a go module dependency:

```bash
go get github.com/wheeli-ca/stocker-store
```

Import the generated types:

```go
import (
    "time"

    kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"
    "google.golang.org/protobuf/types/known/timestamppb"
)

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores: []*kafkastockv1.ScoreEntry{
        {Category: "momentum", Value: 0.7, UpdatedAt: timestamppb.New(time.Now())},
    },
}
```

Note: the server stamps each written score with its own clock (`now()`) on write; the `updated_at` field you send is **advisory only** — the server accepts and discards it.

## Consuming via Git Submodule

Add this repo as a git submodule or copy the `proto/v1/kafka/` directory into your own project's `proto/` tree and run protoc:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
    proto/v1/kafka/stock_message.proto
```

## Consuming by Simple Copy

Copy just `stock_message.proto` into any other project's `proto/` directory and compile it. The proto has no external dependencies beyond standard google.protobuf types, so a single file is portable.

## Schema

| Field    | Type                | Required? | Notes                         |
|----------|---------------------|-----------|-------------------------------|
| `symbol`   | string              | Yes       | Stock ticker (e.g. "AAPL")  |
| `exchange` | string              | Yes       | Exchange name (e.g. "NASDAQ")|
| `scores`   | repeated ScoreEntry | No        | Optional scoring entries      |
| `ScoreEntry.category`   | string    | No        | Score category (e.g. "momentum") |
| `ScoreEntry.value`      | double    | No        | Normalized to `[-1.0, 1.0]`   |
| `ScoreEntry.updated_at` | timestamp | No        | Advisory — server stamps with its own clock on write |

## Versioning

This proto lives in the `stockerstore.kafka.v1` namespace. The **V1 shape of `scores` changed from `map<string,double>` to `repeated ScoreEntry` as a deliberate breaking cutover** — V1 has not been deployed to production, so this was done in place rather than as a `v2`. Producers must build `ScoreEntry` messages (see the example above) and note that the `updated_at` in a request is advisory: the server stamps each written score with its own clock (`now()`).

Breaking changes increment the version suffix (e.g., `v2`). Always check for a versioned directory when integrating from another service.
