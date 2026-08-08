.PHONY: build test clean run

build:
	go build -o bin/stocker-store ./cmd/storage/main.go

test:
	go test ./...

clean:
	rm -rf bin/

run: build
	./bin/stocker-store
