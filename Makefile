.PHONY: build test test-integration run docker-up

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/demo-receiver ./cmd/demo-receiver

test:
	go test -v ./...

test-integration:
	go test -v ./internal/store -run TestStoreLifecycle

run: build
	./bin/api

docker-up:
	docker compose up --build
