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

# Send 10 messages with 100ms delay
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --count 10 \
  --delay-ms 100

# Use protobuf encoding
otel-log-generator otel \
  --endpoint http://localhost:4318/v1/logs \
  --use-protobuf \
  --count 5

# Use gRPC transport
otel-log-generator otel \
  --endpoint http://localhost:4317 \
  --transport grpc \
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
  --count 10 \
  --delay-ms 1000
```

### Environment Variables

All CLI arguments can be set via environment variables:

```bash
export OTEL_LOGS_ENDPOINT=http://localhost:4318/v1/logs
export OTEL_HEALTHCHECK_ENDPOINT=http://localhost:13133/
export OTEL_USE_PROTOBUF=false
export OTEL_TRANSPORT=http
export MESSAGE_COUNT=10
export MESSAGE_DELAY=100
export INVALID_RECORD_PERCENT=0.0
export RECORDS_PER_MESSAGE=1
export PRINT_LOGS=false
export CONTINUOUS_MODE=false

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
1. Complete the current batch of messages
2. Close all connections properly
3. Log shutdown status
4. Exit cleanly

**Docker Example:**
```bash
# Start in continuous mode
docker run -d --name gen otel-log-generator otel --continuous

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
| `--count` | `MESSAGE_COUNT` | 1 | Number of messages to send |
| `--delay-ms` | `MESSAGE_DELAY` | 0 | Delay between messages (ms) |
| `--invalid-record-percent` | `INVALID_RECORD_PERCENT` | 0.0 | % of invalid records (0-100) |
| `--records-per-message` | `RECORDS_PER_MESSAGE` | 1 | Records per message |
| `--print-logs` | `PRINT_LOGS` | false | Print detailed message logs |
| `--continuous` | `CONTINUOUS_MODE` | false | Run in continuous mode |

## Message Types

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
