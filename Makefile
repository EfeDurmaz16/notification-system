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

.PHONY: help fmt lint test race build check up down smoke integration ci performance burst load stress mixed failures package openapi contract matrix recovery crash rebalance quarantine drain wave channel-isolation sustained-mixed verify-all

help:
	@printf '%s\n' 'make check        Format check, vet, unit tests, race detector, binaries' \
		'make up / down    Start / stop Compose without deleting data' \
		'make integration  Start Compose, run database integration tests and HTTP smoke test' \
		'make ci           Run the local functional CI suite' \
		'make verify-all   Run CI, benchmarks, all traffic and recovery experiments' \
		'make performance  Benchmark isolated code paths, report time and allocations' \
		'make burst/load/stress/mixed/failures  Run one experiment on the local stack' \
		'make rebalance    Kill one of three workers under load and audit every ID' \
		'make drain        Stop a worker during a provider call and audit the replacement' \
		'make quarantine   Verify a malformed Kafka record and subsequent delivery' \
		'make wave         Compare fixed, scheduled and feedback scaling on a traffic wave' \
		'make channel-isolation  Compare push latency under an SMS backlog on two worker builds' \
		'make sustained-mixed  Run 15 minutes of simultaneous SMS, email and push traffic' \
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

ci: check openapi integration quarantine drain

performance:
	mkdir -p "$(RESULTS)"
	set -o pipefail; $(GO) test -p=1 -run '^$$' -bench . -benchmem -count=5 ./... | tee "$(RESULTS)/bench.txt"

burst load stress mixed failures:
	./scripts/require-local-provider.sh
	mkdir -p "$(RESULTS)"
	$(GO) run ./cmd/loadtest -scenario $@ -api "$(API_URL)" -provider "$(MOCK_PROVIDER_URL)" -out "$(RESULTS)/$@.json" $(LOAD_FLAGS)

matrix:
	PROVIDER_URL=http://provider:8081/send PROVIDER_IDEMPOTENT=false API_URL="$(API_URL)" MOCK_PROVIDER_URL="$(MOCK_PROVIDER_URL)" RESULTS="$(RESULTS)/matrix" ./scripts/bench.sh

recovery:
	python3 scripts/recovery.py --build --report "$(RESULTS)/recovery.json"

crash:
	python3 scripts/recovery.py --build --crash --report "$(RESULTS)/crash.json"

rebalance:
	python3 -B scripts/worker-rebalance.py --out-dir "$(RESULTS)/worker-rebalance"

quarantine:
	python3 -B scripts/quarantine.py --report "$(RESULTS)/quarantine.json"

drain:
	python3 -B scripts/worker-drain.py --report "$(RESULTS)/worker-drain.json"

wave:
	python3 -B scripts/traffic-wave.py --out-dir "$(RESULTS)/traffic-wave"

channel-isolation:
	bash scripts/run-channel-isolation.sh "$(RESULTS)/channel-isolation"

sustained-mixed:
	python3 -B scripts/sustained-mixed.py --out "$(RESULTS)/sustained-mixed/$$(date -u +%Y%m%dT%H%M%SZ)"

# Sequence measurements to avoid competing load and failure injections on the same host.
verify-all:
	mkdir -p "$(RESULTS)"
	set -o pipefail; $(MAKE) ci 2>&1 | tee "$(RESULTS)/ci.txt"
	$(MAKE) performance
	$(MAKE) matrix
	$(MAKE) recovery
	$(MAKE) crash
	$(MAKE) rebalance
	$(MAKE) wave

package:
	./scripts/package.sh
