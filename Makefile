-include .env

.PHONY: build otel stop clean

build:
	docker build -t log-generator .

otel:
	docker compose --profile otel up --build

stop:
	docker compose down

clean:
	docker compose down -v
	cargo clean
