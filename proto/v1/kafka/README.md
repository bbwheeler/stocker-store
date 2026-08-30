# Kafka Message Protos

Proto definitions for stock message events shared across services.

## Consuming in Another Go Service (preferred)

As a go module dependency:

```bash
go get github.com/wheeli-ca/stocker-store
```

Import the generated types:

```go
import kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores: map[string]float64{"momentum": 0.7},
}
```

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
| `scores`   | map<string, double> | No        | Optional scoring categories   |

## Versioning

This proto lives in the `stockerstore.kafka.v1` namespace. Breaking changes increment the version suffix (e.g., `v2`). Always check for a versioned directory when integrating from another service.
