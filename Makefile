SHELL := /bin/bash
.DEFAULT_GOAL := help

GO ?= go
COMPOSE ?= docker compose
API_URL ?= http://127.0.0.1:$(or $(API_PORT),8080)
MOCK_PROVIDER_URL ?= http://127.0.0.1:$(or $(PROVIDER_PORT),8090)
TEST_KAFKA_BROKER ?= 127.0.0.1:$(or $(KAFKA_PORT),19092)
TEST_REDIS_ADDR ?= 127.0.0.1:$(or $(REDIS_PORT),16379)
TEST_DATABASE_URL ?= postgres://notify:notify@127.0.0.1:$(or $(POSTGRES_PORT),54329)/notify?sslmode=disable
RESULTS ?= .local/results
LOAD_FLAGS ?=

.PHONY: help fmt lint test race build check up down smoke integration ci performance burst load stress failures package openapi contract

help:
	@printf '%s\n' 'make check        Format check, vet, unit tests, race detector, binaries' \
		'make up / down    Start / stop Compose without deleting data' \
		'make integration  Start Compose, run database integration tests and HTTP smoke test' \
		'make ci           Run the local functional CI suite' \
		'make performance  Benchmark isolated code paths, report time and allocations' \
		'make burst/load/stress/failures  Run one experiment on the local stack' \
		'make package      Package committed sources and matching native binaries in dist/'

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*' -not -path './.git/*')

lint:
	@files=$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*' -not -path './.git/*')); \
		if [[ -n "$$files" ]]; then printf 'Run gofmt on:\n%s\n' "$$files"; exit 1; fi
	$(GO) vet ./...

test:
	$(GO) test -count=1 ./...

race:
	$(GO) test -race -count=1 ./...

build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/ ./cmd/...

check: lint test race build

up:
	$(COMPOSE) up --build -d --wait --wait-timeout 300

down:
	$(COMPOSE) down

smoke:
	API_URL="$(API_URL)" MOCK_PROVIDER_URL="$(MOCK_PROVIDER_URL)" RUN_SMOKE=1 $(GO) test -run '^TestWorkflow$$' -v -count=1 -timeout=2m ./tests/integration

openapi:
	./scripts/validate-openapi.sh

contract:
	python3 scripts/check-contract.py --url "$(API_URL)"

integration:
	PROVIDER_URL=http://provider:8081/send PROVIDER_IDEMPOTENT=false $(COMPOSE) up --build -d --wait --wait-timeout 300
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(GO) test -race -count=1 -v ./internal/store
	TEST_KAFKA_BROKER="$(TEST_KAFKA_BROKER)" TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" $(GO) test -race -count=1 -v ./internal/stream
	$(MAKE) smoke contract

ci: check openapi integration

performance:
	mkdir -p "$(RESULTS)"
	set -o pipefail; $(GO) test -run '^$$' -bench . -benchmem -count=5 ./... | tee "$(RESULTS)/bench.txt"

burst load stress failures:
	mkdir -p "$(RESULTS)"
	$(GO) run ./cmd/loadtest -scenario $@ -api "$(API_URL)" -provider "$(MOCK_PROVIDER_URL)" -out "$(RESULTS)/$@.json" $(LOAD_FLAGS)

package:
	./scripts/package.sh
