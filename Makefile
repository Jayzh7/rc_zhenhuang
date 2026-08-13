.PHONY: build test test-integration run docker-up lint

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/demo-receiver ./cmd/demo-receiver

test:
	go test -v ./...

test-integration:
	@if [ -z "$(TEST_DATABASE_URL)" ]; then \
		echo "Usage: TEST_DATABASE_URL=postgres://notifier:notifier@localhost:5432/notifier_test?sslmode=disable make test-integration"; \
		exit 1; \
	fi
	go test -v ./internal/store -run TestStoreLifecycle

run: build
	./bin/api

docker-up:
	docker compose up --build
