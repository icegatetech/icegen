# OpenTelemetry Log Generator

High-performance OpenTelemetry log generator with support for HTTP (JSON/Protobuf) and gRPC transports.

## Features

- ✅ Full OTLP v1.21.0 schema compliance
- ✅ Multiple transport modes: HTTP (JSON/Protobuf) and gRPC
- ✅ Invalid message generation for testing error handling
- ✅ Message aggregation (multiple log records per message)
- ✅ Configurable invalid record percentage
- ✅ Health check support
- ✅ Continuous mode operation
- ✅ In-process concurrency with fixed worker pool
- ✅ Graceful shutdown on SIGTERM/SIGINT (Docker-friendly)
- ✅ Environment variable configuration
- ✅ Docker support
- ✅ Case-insensitive boolean parsing

## Installation

### From Source

```bash
cargo build --release
./target/release/otel-log-generator --help
```

### Docker

```bash
docker build -t otel-log-generator .
docker run otel-log-generator otel --help
```

## Usage

### Basic Usage

```bash
# Send a single log message via HTTP JSON
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs

# Send 10 messages with a 100ms interval between started messages
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --count 10 \
  --message-interval-ms 100

# Send up to 20 requests in parallel from a single container/process
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --count 10 \
  --message-interval-ms 100 \
  --concurrency 20

# Send HTTP logs in random multi-tenant mode (tenant1..tenant8)
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --tenant-count 8 \
  --count 100 \
  --concurrency 20

# Use protobuf encoding
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --use-protobuf \
  --count 5

# Use gRPC transport
otel-log-generator otel \
  --endpoint http://localhost:4317 \
  --transport grpc \
  --tenant-count 8 \
  --count 10

# Generate aggregated messages (multiple records per message)
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --records-per-message 5 \
  --count 10

# Generate invalid messages (10% invalid)
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --invalid-record-percent 10.0 \
  --count 100

# Continuous mode
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --continuous \
  --message-interval-ms 1000
```

### Environment Variables

All CLI arguments can be set via environment variables:

```bash
export OTEL_LOGS_ENDPOINT=http://localhost:4318/v1/logs
export OTEL_HEALTHCHECK_ENDPOINT=http://localhost:13133/
export OTEL_USE_PROTOBUF=false
export OTEL_TRANSPORT=http
export MESSAGE_COUNT=10
export MESSAGE_INTERVAL_MS=100
export CONCURRENCY=20
export INVALID_RECORD_PERCENT=0.0
export RECORDS_PER_MESSAGE=1
export PRINT_LOGS=false
export CONTINUOUS_MODE=false
export TENANT_ID=default
export TENANT_COUNT=1
export CLOUD_ACCOUNT_COUNT_PER_TENANT=4
export SERVICE_COUNT_PER_TENANT=6
export OTEL_LABEL_CARDINALITY_ENABLED=true
export OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT=
export OTEL_LABEL_CARDINALITY_LIMITS=k8s.pod.name=32,host.name=16,service.version=32,request.id=64,thread.id=32,user.id=64

otel-log-generator otel
```

### Using .env File

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
# Edit .env with your settings
otel-log-generator otel
```

### Graceful Shutdown

The generator handles shutdown signals gracefully, making it Docker-friendly:

- **SIGTERM**: Sent by `docker stop` - triggers graceful shutdown
- **SIGINT**: Ctrl+C - triggers graceful shutdown

In continuous mode, the generator will:
1. Stop starting new sends after `Ctrl+C` or `SIGTERM`
2. Let in-flight requests finish
3. Close all connections properly
4. Exit cleanly

**Docker Example:**
```bash
# Start in continuous mode with 20 in-process workers
docker run -d --name gen \
  -e CONCURRENCY=20 \
  otel-log-generator otel --continuous

# Gracefully stop (sends SIGTERM)
docker stop gen  # Waits up to 10s for graceful shutdown

# View logs to confirm clean shutdown
docker logs gen
```

**Boolean values:**
Boolean environment variables accept the following values:
- `true`, `1`, `yes`, `y` → true
- `false`, `0`, `no`, `n`, `` → false

## Configuration Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--endpoint` | `OTEL_LOGS_ENDPOINT` | (required) | OTEL logs ingest endpoint |
| `--healthcheck-endpoint` | `OTEL_HEALTHCHECK_ENDPOINT` | none | Health check endpoint |
| `--use-protobuf` | `OTEL_USE_PROTOBUF` | false | Use protobuf encoding |
| `--transport` | `OTEL_TRANSPORT` | http | Transport type (http/grpc) |
| `--count` | `MESSAGE_COUNT` | 1 | Number of messages to send in batch mode; ignored in continuous mode |
| `--message-interval-ms` | `MESSAGE_INTERVAL_MS` | 0 | Minimum interval between started messages in batch mode; per-worker interval in continuous mode (ms) |
| `--concurrency` | `CONCURRENCY` | 1 | Number of concurrent workers inside one process |
| `--invalid-record-percent` | `INVALID_RECORD_PERCENT` | 0.0 | % of invalid records (0-100) |
| `--records-per-message` | `RECORDS_PER_MESSAGE` | 1 | Records per message |
| `--print-logs` | `PRINT_LOGS` | false | Print detailed message logs |
| `--continuous` | `CONTINUOUS_MODE` | false | Run in continuous mode |
| `--tenant-id` | `TENANT_ID` | `default` | Single-tenant setting: fixed tenant propagated as `X-Scope-OrgID` over HTTP and `x-scope-orgid` metadata over gRPC |
| `--tenant-count` | `TENANT_COUNT` | 1 | Multi-tenant mode size. When `> 1`, each message picks a random tenant from `tenant1..tenantN` and keeps it for retries. Set to `0` to omit the `X-Scope-OrgID` header/metadata entirely; `TENANT_ID` is ignored. |
| `--cloud-account-count-per-tenant` | `CLOUD_ACCOUNT_COUNT_PER_TENANT` | 4 | Size of the tenant-local `cloud.account.id` pool. Values are generated as `tenantX-acc-YY`. Set to `0` to omit `cloud.account.id` from resource attributes. |
| `--service-count-per-tenant` | `SERVICE_COUNT_PER_TENANT` | 6 | Size of the tenant-local `service.name` pool. Values are generated as `tenantX-svc-YY`. Set to `0` to omit `service.name` from resource attributes; `scope.name` falls back to the default `io.trihub.generator`. |
| `--label-cardinality-enabled` | `OTEL_LABEL_CARDINALITY_ENABLED` | true | Enable/disable label cardinality limiting |
| `--label-cardinality-default-limit` | `OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT` | none | Default limit for unlisted keys |
| `--label-cardinality-limits` | `OTEL_LABEL_CARDINALITY_LIMITS` | `""` | CSV map `key=limit,key2=limit2` |

## Tenant Routing

Tenant routing is part of the runtime contract, not an internal detail.

- `TENANT_ID` / `--tenant-id` enables single-tenant mode. Every message uses that tenant.
- `TENANT_COUNT` / `--tenant-count` controls tenant propagation mode:
  - `0` — tenantless mode: no `X-Scope-OrgID` header or `x-scope-orgid` metadata is emitted; `TENANT_ID` is ignored. Pool value prefixes become `notenant-acc-YY` and `notenant-svc-YY`.
  - `1` — single-tenant mode: the value from `TENANT_ID` is propagated.
  - `> 1` — multi-tenant mode: a random tenant from `tenant1..tenantN` is selected per message; `TENANT_ID` is ignored.
- `CLOUD_ACCOUNT_COUNT_PER_TENANT` / `--cloud-account-count-per-tenant` controls how many stable `cloud.account.id` values are generated per tenant. Default is `4`. Set to `0` to omit `cloud.account.id` entirely.
- `SERVICE_COUNT_PER_TENANT` / `--service-count-per-tenant` controls how many stable `service.name` values are generated per tenant. Default is `6`. Set to `0` to omit `service.name` entirely.
- In multi-tenant mode, the generator ignores the configured `TENANT_ID` value for routing and uses a pool `tenant1..tenantN`.
- The tenant is selected once per message, attached immediately to the generated message, and reused on retry.
- After the tenant is selected, `service.name` and `cloud.account.id` are selected only from that tenant's local pools.
- The generated resource attributes are readable and stable: `tenant3-acc-02`, `tenant3-svc-05`, and similar values. In tenantless mode the prefix is `notenant-`, e.g. `notenant-acc-02`.
- HTTP sends the tenant through the `X-Scope-OrgID` header.
- gRPC sends the tenant through the `x-scope-orgid` metadata key.

This matters for Icegate testing because downstream partitioning and pre-WAL sorting derive `tenant_id` from that header/metadata, while `service_name` and `cloud_account_id` are now generated from tenant-local pools for realistic sorting checks.

## Label Cardinality Limiting

The generator can limit high-cardinality label values at generation time using deterministic
bucketization (`bucket_00..bucket_N-1`).

- Safe defaults are always applied for:
  - `k8s.pod.name=32`
  - `host.name=16`
  - `service.version=32`
  - `request.id=64`
  - `thread.id=32`
  - `user.id=64`
- You can override/add per-key limits using `OTEL_LABEL_CARDINALITY_LIMITS`.
- You can set a catch-all limit using `OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT`.
- Set `OTEL_LABEL_CARDINALITY_ENABLED=false` to disable normalization.

Example for Loki stress tests:

```bash
export OTEL_LABEL_CARDINALITY_ENABLED=true
export OTEL_LABEL_CARDINALITY_LIMITS=k8s.pod.name=8,host.name=8,request.id=16,thread.id=8,user.id=16
export OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT=
```

Expected cardinality behavior:

| Key | Before | After (default) |
|-----|--------|------------------|
| `k8s.pod.name` | very high | `<= 32` |
| `host.name` | very high | `<= 16` |
| `service.version` | high | `<= 32` |
| `request.id` | very high | `<= 64` |
| `thread.id` | up to ~9000 | `<= 32` |
| `user.id` | very high | `<= 64` |

## Message Types

## Concurrency Semantics

- `CONCURRENCY` controls how many long-lived workers run inside one process.
- In batch mode, total sends = `MESSAGE_COUNT`.
- Batch work is distributed across up to `CONCURRENCY` workers.
- In batch mode, `MESSAGE_INTERVAL_MS` is enforced globally between started messages.
- In continuous mode, `CONCURRENCY` independent workers run in parallel until shutdown.
- In continuous mode, `MESSAGE_COUNT` is ignored.
- In continuous mode, `MESSAGE_INTERVAL_MS` is applied independently by each worker.
- `MESSAGE_DELAY` / `--delay-ms` remain accepted as deprecated aliases for backward compatibility.
- Multi-tenant rotation happens inside the same process and worker pool. It does not require extra containers, extra workers, or shared mutable routing state.
- Recommended Docker setup: one container with `CONCURRENCY=20` instead of scaling container count for this use case.

### Valid Messages

Standard OTLP log messages with:
- Resource attributes (project_id, service.name, deployment.environment, etc.)
- Scope/instrumentation library attributes
- Log record attributes (http.method, user.id, request.id, etc.)
- Realistic log bodies based on severity level
- Proper trace/span IDs

### Invalid Messages (for testing)

Five types of invalid messages:
1. Empty resourceLogs array
2. Missing resourceLogs field
3. Null resourceLogs
4. Invalid resourceLogs type (string instead of array)
5. Malformed JSON

## Transport Modes

### HTTP JSON
Default mode. Sends JSON-encoded OTLP messages via HTTP POST.

```bash
otel-log-generator otel --endpoint http://localhost:4318/v1/logs
```

### HTTP Protobuf
Sends protobuf-encoded OTLP messages via HTTP POST.

```bash
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --use-protobuf
```

### gRPC
Uses gRPC with protobuf encoding (always uses protobuf, ignores --use-protobuf flag).

```bash
otel-log-generator otel \
  --endpoint http://localhost:4317 \
  --transport grpc
```

### HTTP Multi-Tenant Example

```bash
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --tenant-count 16 \
  --count 1000 \
  --concurrency 20
```

Each message is routed as one of `tenant1..tenant16` via `X-Scope-OrgID`.

### gRPC Multi-Tenant Example

```bash
otel-log-generator otel \
  --endpoint http://localhost:4317 \
  --transport grpc \
  --tenant-count 16 \
  --count 1000 \
  --concurrency 20
```

Each message is routed as one of `tenant1..tenant16` via gRPC metadata `x-scope-orgid`.

### Icegate Sorting Example

Use tenant-local pools when you want to validate sorting inside each tenant by `cloud_account_id`, `service_name`, `timestamp DESC`:

```bash
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --tenant-count 4 \
  --cloud-account-count-per-tenant 4 \
  --service-count-per-tenant 6 \
  --count 5000 \
  --records-per-message 1 \
  --concurrency 20
```

With this setup:

- routing still uses `tenant1..tenant4`
- each tenant gets its own `cloud.account.id` pool such as `tenant2-acc-01..tenant2-acc-04`
- each tenant gets its own `service.name` pool such as `tenant2-svc-01..tenant2-svc-06`
- OTLP resource attributes always include both `cloud.account.id` and `service.name`

That makes it easy to inspect Icegate output and verify that rows are grouped and sorted only within the current tenant.

## Development

### Prerequisites

- Rust 1.75+
- protobuf compiler (`protoc`)

### Build

```bash
cargo build
```

### Run Tests

```bash
cargo test
```

### Format Code

```bash
cargo fmt
```

### Lint

```bash
cargo clippy
```

## OTLP Schema Compliance

This generator produces logs compliant with OpenTelemetry Protocol v1.21.0:
- Schema URL: `https://opentelemetry.io/schemas/1.21.0`
- Proper resource, scope, and log record structures
- Correct attribute key-value format
- Nanosecond timestamp precision
- Valid trace/span ID formats (32/16 hex characters)

## Performance

The generator provides high-performance message generation:
- Low memory footprint
- Fast message generation
- High throughput
- Native async/await support

## License

Copyright 2026 IceGate
