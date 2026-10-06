.PHONY: build test race vet check measure

build:
	mkdir -p bin
	go build -o bin/relay ./cmd/relay
	go build -o bin/relay-worker ./cmd/relay-worker
	go build -o bin/relay-bench ./cmd/relay-bench

test:
	go test -count=1 ./...

race:
	go test -race -count=1 -timeout=5m ./...

vet:
	go vet ./...

check: vet test race build
	test -z "$$(gofmt -l .)"

measure:
	./scripts/measure.sh
