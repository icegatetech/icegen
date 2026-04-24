# AGENTS.md

This file provides guidance to AI agents (Claude Code, Gemini Code Assist, etc.) when working with code in this repository.

## Project Overview

High-performance OpenTelemetry log generator written in Rust. Sends OTLP logs via HTTP (JSON/Protobuf) or gRPC transports with configurable concurrency, invalid record injection, timestamp jitter, and dry-run mode. Intended for load testing and error-handling validation of OTLP collectors.

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