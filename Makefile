BIN := scheduler
PKG := ./...

.PHONY: build run test race vet lint tidy clean

build:
	go build -o bin/$(BIN) ./cmd/scheduler

run:
	go run ./cmd/scheduler

test:
	go test -count=1 $(PKG)

race:
	go test -race -count=1 $(PKG)

vet:
	go vet $(PKG)

lint:
	golangci-lint run $(PKG)

tidy:
	go mod tidy

clean:
	rm -rf bin