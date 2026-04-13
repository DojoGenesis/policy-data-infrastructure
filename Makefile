VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG         := github.com/DojoGenesis/policy-data-infrastructure/cmd/pdi
LDFLAGS     := -X '$(PKG).Version=$(VERSION)' \
               -X '$(PKG).Commit=$(COMMIT)' \
               -X '$(PKG).BuildTime=$(BUILD_TIME)'

BIN         := bin/pdi

.PHONY: build test test-short lint clean migrate-up docker-up docker-down serve

## build: compile all packages
build:
	go build ./...

## bin/pdi: build the CLI binary with version ldflags
$(BIN):
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/pdi

## test: run full test suite with verbose output
test:
	go test ./... -v

## test-short: run tests skipping slow integration tests
test-short:
	go test ./... -short

## lint: run go vet across all packages
lint:
	go vet ./...

## clean: remove compiled binary
clean:
	rm -f $(BIN)

## migrate-up: apply all pending database migrations
migrate-up:
	go run ./cmd/pdi migrate up

## docker-up: start Postgres + PostGIS via docker compose
docker-up:
	docker compose up -d

## docker-down: stop and remove docker compose containers
docker-down:
	docker compose down

## serve: run the HTTP API server
serve:
	go run ./cmd/pdi serve

## docker-build: build the production Docker image
docker-build:
	docker compose build

## deploy: start all services in production mode (detached)
deploy:
	docker compose up -d

## deploy-logs: tail pdi service logs
deploy-logs:
	docker compose logs -f pdi

## backup: dump and compress the PostgreSQL database
backup:
	bash deploy/backup.sh

# Default goal: build all packages
.DEFAULT_GOAL := build
