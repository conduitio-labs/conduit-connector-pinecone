VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-pinecone.version=${VERSION}'" -o conduit-connector-pinecone cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race -v .

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

.PHONY: lint
lint:
	golangci-lint run -v
