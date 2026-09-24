# Protos

Proto definitions for stock info and stock message events shared across services.

## Consuming in Another Go Service (preferred)

Make sure you have git.wheeli.ca marked as private:
```bash
go env -w GOPRIVATE=git.wheeli.ca
```

Then get the import
```bash
go get git.wheeli.ca/brian/stocker-store@latest
```

Import the generated types (example for kafka types):

```go
import (
    "time"

    stockstore "git.wheeli.ca/brian/stocker-store/proto/v1"
    "google.golang.org/protobuf/types/known/timestamppb"
)

msg := &stockstore.Stock{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores: []*stockstore.ScoreEntry{
        {Category: "momentum", Value: 0.7, UpdatedAt: timestamppb.New(time.Now())},
    },
}
```

Note: the server stamps each written score with its own clock (`now()`) on write; the `updated_at` field you send is **advisory only** — the server accepts and discards it.

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

Breaking changes increment the version suffix (e.g., `v2`). Always check for a versioned directory when integrating from another service.
