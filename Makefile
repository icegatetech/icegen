SCALE ?= 1
-include .env

.PHONY: build otel stop clean

build:
	docker build -t log-generator .

otel:
	docker compose --profile otel up --build --scale log-generator-otel=$(SCALE)

stop:
	docker compose down

clean:
	docker compose down -v
	cargo clean
