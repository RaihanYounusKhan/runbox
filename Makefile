SHELL := /bin/bash
GO ?= go
ADMIN_TOKEN ?= dev-admin-token-change-me
TEST_DB ?= postgres://runbox:runbox@localhost:5432/runbox?sslmode=disable

.PHONY: help build test test-integration lint fmt images up down logs key demo bench verify-audit clean

help: ## show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## build all binaries into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/ ./cmd/...

test: ## unit tests (no Docker or Postgres needed)
	$(GO) test -race ./...

test-integration: images ## full suite against real Docker + Postgres (start Postgres with: docker compose up -d postgres)
	RUNBOX_TEST_DATABASE_URL="$(TEST_DB)" $(GO) test -race -tags integration -count=1 -v ./internal/integration/

lint: ## vet + formatting check
	$(GO) vet ./... && $(GO) vet -tags integration ./internal/integration/
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

fmt: ## format code
	gofmt -w .

images: ## build the sandbox images on the local Docker host
	docker build -t runbox/python:3.13 sandbox-images/python
	docker build -t runbox/node:22     sandbox-images/node
	docker build -t runbox/gcc:14      sandbox-images/gcc

up: ## start the full stack
	mkdir -p $${RUNBOX_JOBS_DIR:-/tmp/runbox-jobs}
	docker compose up -d --build
	@echo "API: http://localhost:8080   Grafana: http://localhost:3000   Prometheus: http://localhost:9090"

down: ## stop the stack (keeps data)
	docker compose down

logs: ## follow API and worker logs
	docker compose logs -f api worker

key: ## create an API key
	@docker compose exec api runbox-admin keys create -name dev

demo: ## run a sample job (needs RUNBOX_API_KEY)
	@test -n "$$RUNBOX_API_KEY" || (echo "export RUNBOX_API_KEY=... (make key)"; exit 1)
	@id=$$(curl -s -X POST localhost:8080/v1/jobs -H "Authorization: Bearer $$RUNBOX_API_KEY" \
	  -d '{"language":"python","source":"import platform\nprint(\"hello from\", platform.python_version())"}' \
	  | sed -n 's/.*"id": "\([^"]*\)".*/\1/p'); \
	curl -s "localhost:8080/v1/jobs/$$id?wait=20s" -H "Authorization: Bearer $$RUNBOX_API_KEY"

bench: build ## load test (needs RUNBOX_API_KEY)
	./bin/runbox-bench -n 500 -c 32

verify-audit: ## verify the audit hash chain
	@docker compose exec api runbox-admin audit verify

clean:
	rm -rf bin coverage.out
