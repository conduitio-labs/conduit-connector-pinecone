.PHONY: build test generate install-paramgen install-tools golangci-lint-install

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-pinecone.version=${VERSION}'" -o conduit-connector-pinecone cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race -v .

generate:
	go generate ./...

install-paramgen:
	go install github.com/conduitio/conduit-connector-sdk/cmd/paramgen@latest

install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

lint:
	golangci-lint run -v
