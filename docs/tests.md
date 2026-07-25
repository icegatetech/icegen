# Testing

Tests MUST protect observable behavior, public contracts, and data invariants. They MUST NOT be added merely to mirror every production function or type.

## Change coverage

For every behavior change, build a coverage map before considering the work complete:

```text
changed behavior -> possible failure -> test layer -> concrete test
```

- Every reachable branch and error path introduced or changed MUST be covered, or its
  omission MUST be explicitly justified as unreachable or irrelevant.
- Every bug fix MUST include a regression test that reproduces the defect and would fail
  without the fix.
- A behavior-preserving refactor does not require tautological new tests when existing
  tests already protect the affected contract. The existing coverage MUST be identified.
- Line coverage is a gap detector, not proof of correctness. A coverage percentage does
  not replace independent assertions or the risk analysis below.

For each change, consider every applicable risk class:

- normal behavior;
- empty, missing, null, zero, one, and many values;
- immediately below, exactly at, and immediately above a boundary;
- malformed, unsupported, truncated, and overflowing input;
- duplicates, ties, ordering, and out-of-order input;
- partial failure, retry, cancellation, restart, and idempotency;
- concurrent reads, writes, and state transitions;
- mixed tenants, partitions, snapshots, files, and row groups;
- a cross-tenant negative case when tenant-scoped data is affected.

This is a risk checklist, not a requirement to add meaningless cases. Tests SHOULD use
table-driven cases or property tests when multiple inputs represent the same rule.

## Choose the relevant boundary

Use the lowest stable boundary that still includes the behavior and risk under test.

### Unit tests

Use unit tests for pure transformations, parsers, validation, state transitions,
planning rules, and deterministic algorithms. Unit tests MUST NOT perform network,
filesystem, or process I/O.

Prefer testing a stable module or crate contract. A private pure algorithm MAY be tested
directly when its behavior is independently specified and testing only through a higher
boundary would hide the failing rule. Do not expose production APIs or add production
methods solely for tests.

### Component tests

Use component tests for orchestration across several production types when the external
boundary is not relevant. Fakes and mocks are appropriate for deterministic fault
injection, call ordering that is itself a contract, and otherwise difficult failure paths.
Test doubles SHOULD implement the same production traits as real dependencies.

### Integration tests

Use real implementations when the behavior belongs to an integration boundary. In
particular, mocks are not sufficient as the only coverage for HTTP, gRPC, and OTLP wire
contracts.

A change to a public protocol MUST have a test at the real transport boundary. Building a
request or calling a handler directly is component coverage, not a substitute for an HTTP
or gRPC contract test.

New integration-style tests SHOULD live under `tests/` when the scenario can be expressed
through the public crate API. A crate-internal component test MAY live in an inline
`#[cfg(test)]` module only when access to a meaningful `pub(crate)` contract is necessary.

## Independent test oracles

- Expected results MUST come from a specification, a small independent reference model,
  or explicitly stated values.
- Expected results MUST NOT be computed with the same helper, constant, parser, planner,
  sorting routine, or conversion algorithm as the code under test.
- Arrange code MAY use canonical production schemas and builders to create valid input.
  Assertions about semantic output MUST remain independent of the implementation.
- A test MUST prove that its fixture reaches the condition under test when that condition
  is not obvious, for example a shard actually holding more records than spans, or a span
  tree actually reaching three levels.
- Do not guard important assertions with `if !result.is_empty()` or equivalent logic. If
  results are expected, assert the exact count or non-emptiness before inspecting them.
- Compare unordered results after canonicalization unless ordering is part of the public
  contract. When ordering is a contract, assert the complete relevant order.
- Do not assert against `Debug` output as the primary semantic oracle; inspect stable
  structured values instead.
- Snapshot or golden tests MAY be used for stable wire formats. Volatile values such as
  UUIDs, timestamps, paths, ports, execution times, and generated metadata locations MUST
  be normalized or asserted structurally.

For errors, assert the stable machine-readable contract: the Rust variant, error kind,
protocol code, HTTP/gRPC status, retryability, and meaningful structured fields. Do not
assert human-readable error text unless the text is explicitly part of a public protocol
contract.

## Determinism and isolation

- Use fixed timestamps and dates. Use the real clock only when current-time behavior is
  the contract under test.
- Use Tokio's paused clock for timers and backoff where possible.
- Real sleeps MUST NOT determine event ordering or correctness. Coordinate concurrency
  with barriers, notifications, channels, or explicit test gates.
- Every wait MUST have a bounded timeout. A timeout error SHOULD retain the underlying
  failure or diagnostic state.
- Randomized tests MUST use a reproducible seed. Random UUIDs MAY be used only for resource
  isolation when their value is not part of the assertion.
- Tests using shared resources MUST use unique ports, paths, and temporary directories so
  normal parallel execution is safe.
- Servers SHOULD bind to port `0` or use an already-bound listener. Harnesses MUST confirm
  readiness before sending requests.
- Harnesses MUST own temporary directories, listeners, and background tasks through
  RAII-style guards. They MUST clean up on success, error, timeout, and panic.
- A background panic, failed join, or failed shutdown MUST fail the test instead of being
  silently ignored.
- Tests MUST NOT depend on external networks, credentials, or shared remote state. A test
  that needs a collector stands one up locally on an ephemeral port.

## Test readability

- Follow Arrange-Act-Assert as a logical structure. Do not add heading comments when the
  phases are already obvious.
- Test names MUST state the condition or trigger and the observable result. Do not list a
  case in the name that the body does not exercise.
- If behavior is triggered implicitly by startup, a callback, a timer, cancellation, or a
  background task, make the trigger explicit in the name or a short comment.
- Comments explain why a case matters, an external specification, a non-obvious trigger,
  or a previous regression. Do not narrate the test body.
- Keep inline `#[cfg(test)]` modules at the end of the source file.
- Do not commit commented-out tests.
- Shared test helpers MUST reduce setup duplication without hiding the inputs and outputs
  that make the case meaningful.

## Disabled and flaky tests

- A required test MUST NOT be made green by retrying it in CI.
- Treat a flaky test as a defect. Replace timing assumptions with deterministic
  synchronization or fix resource isolation.
- `#[ignore]` requires a linked issue, an explanation of the lost coverage, and a clear
  condition for re-enabling the test.
- An ignored, skipped, or environment-gated test does not count as coverage for a change.
- If required integration infrastructure is unavailable locally, report which test command
  was not run. Do not claim the test suite passes and do not silently skip required tests.

## Test review

Review tests by mapping affected behavior to coverage by layer before reviewing individual
assertions. Two tests are not duplicates merely because their final assertions look alike:
a unit test can protect a pure rule while an integration test protects serialization or
orchestration of the same result.

Before completing a code change, verify:

- the changed behavior and failure modes are represented in the coverage map;
- the chosen test layers include every changed boundary;
- regression tests fail for the defect they claim to protect;
- expected values are independent of the implementation;
- relevant boundary, failure, tenant, ordering, and concurrency cases are covered;
- fixtures use canonical schemas and production formats where required;
- tests are deterministic, isolated, and safe under normal parallel execution;
- all applicable feature combinations were tested.

