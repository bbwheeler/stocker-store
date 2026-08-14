# Build stage
FROM golang:1.25-bookworm AS build
WORKDIR /src

# Install buf + protoc plugins for codegen inside the image build.
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*

COPY . .

ENV PATH="$PATH:/root/go/bin"
RUN mkdir -p gen
RUN protoc \
      --go_out=gen --go_opt=paths=source_relative \
      --go-grpc_out=gen --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
      -I /usr/include -I proto proto/tsx/v1/tsx.proto

RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/stocker-store ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-store /stocker-store
EXPOSE 50051
ENTRYPOINT ["/stocker-store"]
