.PHONY: build test test-integration test-e2e test-all run docker-up

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/demo-receiver ./cmd/demo-receiver

test:
	TEST_DATABASE_URL= RUN_COMPOSE_E2E= go test -count=1 -v ./...

test-integration:
	go test -count=1 -v ./internal/store -args -require-integration-database

test-e2e:
	RUN_COMPOSE_E2E=1 go test -count=1 -v ./e2e -run TestComposeMVP

test-all:
	$(MAKE) test
	$(MAKE) test-integration
	$(MAKE) test-e2e
	go vet ./...

run: build
	./bin/api

docker-up:
	docker compose up --build
