# OpenTelemetry Log Generator

High-performance OpenTelemetry log generator with support for HTTP (JSON/Protobuf) and gRPC transports.

## Features

- ✅ Full OTLP v1.21.0 schema compliance
- ✅ Multiple transport modes: HTTP (JSON/Protobuf) and gRPC
- ✅ Invalid message generation for testing error handling
- ✅ Message aggregation (multiple log records per message)
- ✅ Configurable invalid record percentage
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
  --http-logs-endpoint http://localhost:4318/v1/logs

# Send 10 messages with a 100ms interval between started messages
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --count 10 \
  --message-interval-ms 100

# Send up to 20 requests in parallel from a single container/process
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --count 10 \
  --message-interval-ms 100 \
  --concurrency 20

# Send HTTP logs in random multi-tenant mode (tenant1..tenant8)
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --tenant-count 8 \
  --count 100 \
  --concurrency 20

# Use protobuf encoding
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --use-protobuf \
  --count 5

# Use gRPC transport
otel-log-generator otel \
  --grpc-endpoint http://localhost:4317 \
  --transport grpc \
  --tenant-count 8 \
  --count 10

# Generate aggregated messages (multiple records per message)
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --logs-per-message 5 \
  --count 10

# Generate invalid messages (10% invalid)
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --invalid-record-percent 10.0 \
  --count 100

# Continuous mode
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --continuous \
  --message-interval-ms 1000
```

### Environment Variables

All CLI arguments can be set via environment variables:

```bash
export OTEL_HTTP_LOGS_ENDPOINT=http://localhost:4318/v1/logs
export OTEL_HTTP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
# Read instead of the two above when OTEL_TRANSPORT=grpc:
export OTEL_GRPC_ENDPOINT=http://localhost:4317
export OTEL_USE_PROTOBUF=false
export OTEL_TRANSPORT=http
export MESSAGE_COUNT=10
export MESSAGE_INTERVAL_MS=100
export CONCURRENCY=20
export INVALID_RECORD_PERCENT=0.0
export LOGS_PER_MESSAGE=1
export TRACES_PER_MESSAGE=1
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
| `--grpc-endpoint` | `OTEL_GRPC_ENDPOINT` | none | gRPC endpoint; carries every signal over distinct OTLP services (`--transport grpc`) |
| `--http-logs-endpoint` | `OTEL_HTTP_LOGS_ENDPOINT` | none | OTLP/HTTP logs URL; required when logs are selected (`--transport http`) |
| `--http-traces-endpoint` | `OTEL_HTTP_TRACES_ENDPOINT` | none | OTLP/HTTP traces URL; required when traces are selected (`--transport http`) |
| `--signals` | `OTEL_SIGNALS` | logs | Telemetry signals to generate, comma-separated (`logs`, `traces`, `logs,traces`). Order preserved; duplicates rejected. One message per signal per generation cycle |
| `--llm-max-tool-calls` | `LLM_MAX_TOOL_CALLS` | 3 | Max `execute_tool` spans per LLM trace (traces only) |
| `--llm-capture-content` | `LLM_CAPTURE_CONTENT` | true | Capture prompt/completion content into span attributes — PII risk (traces only) |
| `--use-protobuf` | `OTEL_USE_PROTOBUF` | false | Use protobuf encoding |
| `--transport` | `OTEL_TRANSPORT` | http | Transport type (http/grpc) |
| `--count` | `MESSAGE_COUNT` | 1 | Number of generation cycles to run in batch mode; ignored in continuous mode. One cycle emits one message per signal, so network requests = `MESSAGE_COUNT × signals.len()` |
| `--message-interval-ms` | `MESSAGE_INTERVAL_MS` | 0 | Minimum interval between started generation cycles in batch mode; per-worker interval in continuous mode (ms). A cycle's per-signal messages start together, so this does not space them apart |
| `--concurrency` | `CONCURRENCY` | 1 | Number of concurrent workers inside one process |
| `--invalid-record-percent` | `INVALID_RECORD_PERCENT` | 0.0 | % of invalid records (0-100) |
| `--logs-per-message` | `LOGS_PER_MESSAGE` | 1 | Log records per message, total across all shards (logs only) |
| `--traces-per-message` | `TRACES_PER_MESSAGE` | 1 | Traces per message, total across all shards (traces only). Independent of `LOGS_PER_MESSAGE`: a shard's log records are split over that shard's traces, so each signal scales on its own knob. |
| `--service-shards-per-message` | `SERVICE_SHARDS_PER_MESSAGE` | 1 | Number of service shards packed into one request, simulating OTEL Collector batching across services/pods. Must be `>= 1`. A shard is one `ResourceLogs` group for logs and one `ResourceSpans` group for traces, holding the spans of every trace of that shard. Clamped so no shard of a configured signal comes out empty: `min(value, LOGS_PER_MESSAGE)` for logs, `min(value, TRACES_PER_MESSAGE)` for traces, and the smaller of the two when both run. Service names are sampled at random from the tenant pool; when this value exceeds `SERVICE_COUNT_PER_TENANT`, duplicate names appear across shards (intentional — models multiple pods of the same service). To get a single shard without a service name, set `SERVICE_COUNT_PER_TENANT=0` instead. |
| `--print-logs` | `PRINT_LOGS` | false | Print detailed message logs |
| `--continuous` | `CONTINUOUS_MODE` | false | Run in continuous mode |
| `--tenant-id` | `TENANT_ID` | `default` | Single-tenant setting: fixed tenant propagated as `X-Scope-OrgID` over HTTP and `x-scope-orgid` metadata over gRPC |
| `--tenant-count` | `TENANT_COUNT` | 1 | Multi-tenant mode size. When `> 1`, each message picks a random tenant from `tenant1..tenantN` and keeps it for retries. Set to `0` to omit the `X-Scope-OrgID` header/metadata entirely; `TENANT_ID` is ignored. |
| `--cloud-account-count-per-tenant` | `CLOUD_ACCOUNT_COUNT_PER_TENANT` | 4 | Size of the tenant-local `cloud.account.id` pool. Values are generated as `tenantX-acc-YY`. Set to `0` to omit `cloud.account.id` from resource attributes. |
| `--service-count-per-tenant` | `SERVICE_COUNT_PER_TENANT` | 6 | Size of the tenant-local `service.name` pool. Values are generated as `tenantX-svc-YY`. Set to `0` to omit `service.name` from resource attributes; `scope.name` falls back to the default `io.trihub.icegen`. |
| `--label-cardinality-enabled` | `OTEL_LABEL_CARDINALITY_ENABLED` | true | Enable/disable label cardinality limiting |
| `--label-cardinality-default-limit` | `OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT` | none | Default limit for unlisted keys |
| `--label-cardinality-limits` | `OTEL_LABEL_CARDINALITY_LIMITS` | `""` | CSV map `key=limit,key2=limit2` |

## Signals: Logs and Traces

The generator emits one or more telemetry signals per run, selected with `--signals` / `OTEL_SIGNALS`
(comma-separated, duplicates rejected). The list order drives the order messages are built, their
`PRINT_LOGS` blocks are printed, and the per-signal statistics are reported; the concurrent network
sends themselves complete in an unspecified order:

- `logs` (default) — flat OTLP log records.
- `traces` — synthetic OTLP traces with LLM (`gen_ai.*`) semantics following the current OpenTelemetry GenAI semantic conventions.
- `logs,traces` — both, correlated (see below).

### Generation cycles and counting

A **generation cycle** is one unit of `MESSAGE_COUNT`. It selects one tenant context and produces one
message per configured signal — so `MESSAGE_COUNT=N` with two signals yields `N` cycles and `2N`
signal messages (network requests). `CONCURRENCY` bounds the number of parallel cycles, so the upper
bound on simultaneous requests is `CONCURRENCY × signals.len()`. `MESSAGE_INTERVAL_MS` is applied
once before each cycle starts; the cycle's signal messages then start together. Retries are
independent per signal message: a successful signal is never resent because another failed. A cycle
counts as successful only when every one of its signal messages is delivered; statistics are also
kept per signal.

### Correlation (logs + traces)

When both signals run, each service shard carries `TRACES_PER_MESSAGE / shards` traces, and the
shard's log records are split evenly over those traces and then spread across each trace's spans:
every record adopts its trace's `trace_id`, the `span_id` of the span it
was assigned to, and a timestamp inside that span's window. The split gives every span an equal base
share of the records — so once a trace has at least as many records as it has spans, no span
is left without a log — and hands the remainder to spans in proportion to their duration, so
long-running spans carry more. A single-span trace pins everything to its root and otherwise takes
the identical path, so the emitted shape does not depend on the size of the span tree. Records are
merged in time order across the spans, and `RECORD_INTRA_BATCH_OVERLAP_PROBABILITY` still applies —
the backward nudge is re-applied after that merge, clamped into the window of the record's own span.
Correlation uses the
native OTLP fields (`traceId` / `spanId` on log records and spans) — no extra string attributes are
added. Both signals of a cycle share the same `tenant_id`, `cloud.account.id`, `service.name`,
`project_id`, and `generator.source`.

Every trace (a single `trace_id`; its spans live in its shard's `ResourceSpans` group) has this span tree:

```
invoke_agent {agent}        (INTERNAL)   gen_ai.operation.name, gen_ai.provider.name, gen_ai.agent.name
└─ chat {model}             (CLIENT)     gen_ai.request.*, gen_ai.response.*, gen_ai.usage.{input,output}_tokens
   └─ execute_tool {tool}*  (INTERNAL)   gen_ai.tool.name, gen_ai.tool.call.id  (0..LLM_MAX_TOOL_CALLS)
└─ embeddings {model}?      (CLIENT)     gen_ai.embeddings.dimension.count  (~40% of traces)
```

Span timing is consistent: the root span fully encloses its children, which are laid out sequentially.
Numeric attributes (token counts, temperature) use typed OTLP values (`intValue`/`doubleValue`), and
`gen_ai.response.finish_reasons` is an array value.

Prompt/completion content is captured by default (`--llm-capture-content` / `LLM_CAPTURE_CONTENT`
default `true`), attaching `gen_ai.input.messages` / `gen_ai.output.messages` — note this can carry
PII and inflate payloads. Set it to `false` to omit that content.

```bash
# Generate one LLM trace to stdout without any network transport
otel-log-generator otel --signals traces --dry-run --print-logs

# Stream traces over HTTP JSON to a collector
otel-log-generator otel --signals traces --http-traces-endpoint http://localhost:4318/v1/traces --continuous

# Logs and traces together over HTTP (both endpoints required)
otel-log-generator otel --signals logs,traces \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --http-traces-endpoint http://localhost:4318/v1/traces --continuous

# Logs and traces together over gRPC (single endpoint, distinct OTLP services; always protobuf)
otel-log-generator otel --signals logs,traces --transport grpc --grpc-endpoint http://localhost:4317
```

Tenant routing, cloud-account/service pools, concurrency, and retry behaviour apply to every
configured signal identically. Cardinality limiting is a logs feature: it bounds a log record's own
attributes, and — so a correlated log and trace never disagree — it is applied once to the shard's
whole resource attribute set whenever logs are configured, with the trace adopting the same bucketed
values. A traces-only run leaves resource attributes un-normalized.

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
- In batch mode, total generation cycles = `MESSAGE_COUNT`; total network requests = `MESSAGE_COUNT × signals.len()`.
- Batch work is distributed across up to `CONCURRENCY` workers; the upper bound on simultaneous requests is `CONCURRENCY × signals.len()`.
- In batch mode, `MESSAGE_INTERVAL_MS` is enforced globally between started generation cycles (a cycle's signal messages start together).
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
otel-log-generator otel --http-logs-endpoint http://localhost:4318/v1/logs
```

### HTTP Protobuf
Sends protobuf-encoded OTLP messages via HTTP POST.

```bash
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --use-protobuf
```

### gRPC
Uses gRPC with protobuf encoding (always uses protobuf, ignores --use-protobuf flag).

```bash
otel-log-generator otel \
  --grpc-endpoint http://localhost:4317 \
  --transport grpc
```

### HTTP Multi-Tenant Example

```bash
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --tenant-count 16 \
  --count 1000 \
  --concurrency 20
```

Each message is routed as one of `tenant1..tenant16` via `X-Scope-OrgID`.

### gRPC Multi-Tenant Example

```bash
otel-log-generator otel \
  --grpc-endpoint http://localhost:4317 \
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
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --tenant-count 4 \
  --cloud-account-count-per-tenant 4 \
  --service-count-per-tenant 6 \
  --count 5000 \
  --logs-per-message 1 \
  --concurrency 20
```

With this setup:

- routing still uses `tenant1..tenant4`
- each tenant gets its own `cloud.account.id` pool such as `tenant2-acc-01..tenant2-acc-04`
- each tenant gets its own `service.name` pool such as `tenant2-svc-01..tenant2-svc-06`
- OTLP resource attributes always include both `cloud.account.id` and `service.name`

That makes it easy to inspect Icegate output and verify that rows are grouped and sorted only within the current tenant.

### Multi-service payload (realistic OTEL Collector batching)

Real OTEL Collectors send a single `ExportLogsServiceRequest` that contains logs from multiple pods — each represented as a separate `ResourceLogs` entry with its own `service.name`, `host.name`, and `k8s.pod.name`. Use `--service-shards-per-message` to reproduce this:

```bash
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --tenant-count 4 \
  --cloud-account-count-per-tenant 4 \
  --service-count-per-tenant 6 \
  --service-shards-per-message 3 \
  --logs-per-message 30 \
  --count 1000 \
  --concurrency 10
```

With this setup each HTTP request contains exactly 3 `ResourceLogs` groups. `LOGS_PER_MESSAGE` (30) is divided evenly across the 3 groups (10 records each; the remainder, if any, is distributed one-by-one to the first shards). Timestamps within each group are monotonically non-decreasing (when `overlap_probability=0`). Across groups monotonicity is not guaranteed — they simulate independent service streams anchored to the same batch window (`now - rand(0, across_batch_jitter)..now`).

Key invariants:
- One request → one tenant (`X-Scope-OrgID`) → one `project_id` → one `cloud.account.id`
- Each `ResourceLogs` (and, for traces, each `ResourceSpans`) maps to one shard with its own `service.name`, `host.name`, `k8s.pod.name`
- `SERVICE_SHARDS_PER_MESSAGE` is clamped so no shard of a configured signal comes out empty: `min(value, LOGS_PER_MESSAGE)` for logs, `min(value, TRACES_PER_MESSAGE)` for traces, the smaller of the two when both run
- When `SERVICE_COUNT_PER_TENANT=0`, all shards fall back to a single shard without a `service.name`

### Trace volume

`TRACES_PER_MESSAGE` scales traces the way `LOGS_PER_MESSAGE` scales log records, and the two are independent:

```bash
otel-log-generator otel \
  --http-logs-endpoint http://localhost:4318/v1/logs \
  --http-traces-endpoint http://localhost:4318/v1/traces \
  --signals logs,traces \
  --service-shards-per-message 3 \
  --logs-per-message 30 \
  --traces-per-message 12 \
  --count 100
```

Each cycle sends one logs request with 3 `ResourceLogs` groups (10 records each) and one traces request with 3 `ResourceSpans` groups (one per shard) — 4 traces per shard, each with its own `trace_id` and span tree sized by `TRACE_MIN_SPANS`/`TRACE_MAX_SPANS`, their spans interleaved inside the shard's single group. A shard's 10 log records are split evenly over its 4 traces and then spread across each trace's spans, so every record still carries the `trace_id`/`span_id` of the span it belongs to.

Traces emitted per run = `MESSAGE_COUNT × TRACES_PER_MESSAGE` (the shard count only decides how they are grouped by service). When a shard holds more traces than records, the trailing traces simply carry no correlated log.

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
