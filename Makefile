-include .env

.PHONY: build otel stop clean check fmt clippy test ci

build:
	docker build -t log-generator .

otel:
	docker compose --profile otel up --build

stop:
	docker compose down

clean:
	docker compose down -v
	cargo clean

check:
	cargo check --all-targets

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test

ci: check fmt clippy test