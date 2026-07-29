# AGENTS.md

## Project Overview

The project is a high-performance async OpenTelemetry observability signals generator.

## Development Conventions

### Code Style

The project uses `rustfmt` for code formatting. Configuration is in `rustfmt.toml`. 
For Rust development rules, see `/docs/RUST.md`.

### Important Instructions

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- Ensure each file is finishing by new line, do not duplicate if it already exists
- It is better to give an error than to use/calculate/show invalid data.
- NEVER delete TODO comments if the changes do not fully cover the necessary edits in the comment.
- When mutating a config parameter, ALWAYS reflect the change in `.env.example`.
- All code and comments should be in English only.

## Operational commands

See `Makefile`.

## Writing code

- [docs/RUST.md](docs/RUST.md) is binding for naming, errors (`thiserror`/`anyhow`), the
  type system, testing, imports, and style. Follow it; do not duplicate it here.
- **A convention is only what is documented** in this file, RUST.md, or `docs/`.
  The mere presence of a pattern in the code is **NOT** a convention — someone may
  have committed junk. Do not justify a decision with "the existing code does X";
  cite the documented rule, or propose adding one if it is missing.
- Separation of responsibility comes first; apply DRY; the lints forbid dead code.
- Schema, config fields, and service ports/URLs are referenced from their source,
  never copied as literals into working code.
- It is better to return an error than to use, calculate, or show invalid data.

## Before a change

- Determine the layer the change belongs to; signal-neutral logic goes to the shared
  module of its area (e.g. `message/resource_attrs.rs`), not into one signal's generator.
- Cover significant behaviour with tests — read [docs/tests.md](docs/tests.md) first.
- Do not break the OTLP wire contracts, nor the public API/CLI contracts.

## Before finishing

- Follow [docs/tests.md](docs/tests.md). Report which test commands were run and
  which required tests were not.
- Run targeted tests for the affected functionality; leave full `make ci` to an
  explicit request.
- Keep `TODO` comments intact unless the change fully resolves them.
- Ensure each file ends with a single trailing newline.
