# AGENTS.md

This file provides guidance to AI agents (Claude Code, Gemini Code Assist, etc.) when working with code in this repository.

## Project Overview

The project is a high-performance async OpenTelemetry log generator with:

- Multi-mode Operation: Batch send, continuous stream, dry-run
- Transport Flexibility: HTTP JSON, HTTP Protobuf, gRPC
- Advanced Tenancy: Single/multi-tenant modes with cloud account and service pools
- Timestamp Control: Sophisticated jitter at batch and record levels with overlap simulation
- Cardinality Management: Deterministic bucketing with per-key limits
- Robust Configuration: 30+ CLI flags, environment variables, validation
- Concurrency: Per-worker scheduling with global pacing and progress tracking
- Signal Handling: Graceful shutdown with current batch completion
- Testing: 11+ integration tests, extensive unit tests in each module

## Development Conventions

### Code Style

The project uses `rustfmt` for code formatting. Configuration is in `rustfmt.toml`.
@RUST.md

### Important Instructions

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- Ensure each file is finishing by new line, do not duplicate if it already exists
- It is better to give an error than to use/calculate/show invalid data.
- NEVER delete TODO comments if the changes do not fully cover the necessary edits in the comment.
- When mutating a config parameter, ALWAYS reflect the change in `.env.example`.