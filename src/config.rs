use crate::error::{GeneratorError, Result};
use crate::message::traces::span_profile::{ProfileWeights, SpanBudget};
use crate::message::types::Signal;
use crate::transport::{AuthHeaders, Destination};
use rand::Rng;
use std::collections::{BTreeMap, HashMap, HashSet};

const MAX_RETRIES_UPPER_BOUND: u32 = 10;
/// Hard upper bound on `trace_max_spans`, keeping a single trace's payload bounded.
const TRACE_SPAN_BUDGET_MAX: u32 = 10_000;
const DEFAULT_CARDINALITY_LIMITS: &[(&str, usize)] = &[
    ("k8s.pod.name", 32),
    ("host.name", 16),
    ("service.version", 32),
    ("request.id", 64),
    ("thread.id", 32),
    ("user.id", 64),
];

#[derive(Debug, Clone)]
pub struct AttributesCardinalityConfig {
    pub enabled: bool,
    pub default_limit: Option<usize>,
    pub limit_by_attr: HashMap<String, usize>,
}

impl AttributesCardinalityConfig {
    pub fn limit_for(&self, key: &str) -> Option<usize> {
        self.limit_by_attr
            .get(key)
            .copied()
            .or(self.default_limit)
            .filter(|limit| *limit >= 1)
    }
}

impl Default for AttributesCardinalityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_limit: None,
            limit_by_attr: default_cardinality_limits(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl RetryConfig {
    /// Create a new RetryConfig, validating that `max_retries` does not exceed the safe upper bound.
    #[allow(clippy::result_large_err)]
    pub fn new(max_retries: u32, base_delay_ms: u64, max_delay_ms: u64) -> Result<Self> {
        if max_retries > MAX_RETRIES_UPPER_BOUND {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "max_retries must be <= {}, got {}",
                MAX_RETRIES_UPPER_BOUND, max_retries
            )));
        }
        Ok(Self {
            max_retries,
            base_delay_ms,
            max_delay_ms,
        })
    }

    /// Compute backoff delay for a given attempt.
    /// If `retry_after` is provided (from an HTTP Retry-After header, in seconds),
    /// it takes precedence over the exponential calculation, capped at max_delay_ms.
    /// Applies ±25% jitter to the result.
    pub fn compute_delay(&self, attempt: u32, retry_after: Option<u64>) -> u64 {
        let base = if let Some(retry_after_secs) = retry_after {
            (retry_after_secs * 1000).min(self.max_delay_ms)
        } else {
            let safe_shift = attempt.min(63);
            self.base_delay_ms
                .saturating_mul(1u64 << safe_shift)
                .min(self.max_delay_ms)
        };

        let jitter_range = base / 4;
        if jitter_range > 0 {
            let jitter = rand::thread_rng().gen_range(0..=jitter_range * 2);
            base.saturating_sub(jitter_range).saturating_add(jitter)
        } else {
            base
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 1000,
            max_delay_ms: 32000,
        }
    }
}

/// Timestamp jitter configuration used by [`crate::message::OTLPLogMessageGenerator`].
///
/// All fields in this struct use nanoseconds because generated OTLP payloads store
/// timestamps in `timeUnixNano` / `observedTimeUnixNano`. CLI and [`OtelConfig`]
/// accept jitter settings in milliseconds and convert them to this internal form via
/// [`OtelConfig::timestamp_jitter_config`].
///
/// Validation of user-provided ranges happens in [`OtelConfig::validate`]:
/// `record_across_batch_timestamp_jitter_ms` must be in `0..=3_600_000`,
/// `record_intra_batch_timestamp_jitter_ns` must be in `0..=60_000_000_000`, and
/// `record_intra_batch_overlap_probability` must be in `0.0..=1.0`.
#[derive(Debug, Clone, Copy)]
pub struct TimestampJitterConfig {
    /// Maximum backward shift, in nanoseconds, applied to the whole generated batch.
    ///
    /// Each batch is anchored at `now - offset`, where `offset` is sampled uniformly from
    /// `0..across_batch_timestamp_jitter_ns`. This keeps generated timestamps out of the future
    /// while allowing batches to look slightly delayed as a group.
    pub across_batch_timestamp_jitter_ns: i64,
    /// Maximum local spacing step, in nanoseconds, between neighbouring records in one batch.
    ///
    /// When this value is `0`, records inside one batch collapse to the same timestamp before any
    /// overlap logic is applied. When it is positive, each record advances the monotonic base plan
    /// by a random step from `0..intra_batch_timestamp_jitter_ns`.
    pub intra_batch_timestamp_jitter_ns: i64,
    /// Probability of forcing a local timestamp overlap between neighbouring records.
    ///
    /// `0.0` preserves the monotonic base plan, so aggregated timestamps do not decrease.
    /// Values above `0.0` occasionally move the emitted timestamp of a record backwards relative
    /// to its predecessor while still keeping the whole batch anchored by
    /// [`Self::across_batch_timestamp_jitter_ns`].
    pub intra_batch_overlap_probability: f32,
}

impl Default for TimestampJitterConfig {
    fn default() -> Self {
        Self {
            across_batch_timestamp_jitter_ns: 1_000_000_000,
            intra_batch_timestamp_jitter_ns: 5_000_000,
            intra_batch_overlap_probability: 0.05,
        }
    }
}

/// The resolved, validated subset of [`OtelConfig`] a [`crate::message::factory::SignalFactory`]
/// needs. Introduced so the `message` layer depends only on this spec, not on the whole
/// `OtelConfig`: [`OtelConfig::signal_factory_spec`] performs every config-level decision (encoding
/// choice, cardinality resolution, profile-weight parsing) once, and the factory just wires up
/// generators from it.
#[derive(Debug, Clone)]
pub struct SignalFactorySpec {
    /// Configured signals, in request order; drives which generators are built and their output order.
    pub signals: Vec<Signal>,
    /// `generator.source` stamped on every signal, so a correlated log and trace agree on it.
    pub source: String,
    /// Whether the protobuf encoders are selected (gRPC is always protobuf; HTTP honours `use_protobuf`).
    pub want_protobuf: bool,
    /// Cardinality policy for log records and the shared resource attributes. `Some` exactly when
    /// logs are among the signals (cardinality is a logs feature; a correlated trace adopts the
    /// bucketed resource). Resolved once here instead of twice as it used to be.
    pub log_cardinality: Option<AttributesCardinalityConfig>,
    /// Timestamp jitter for the log planner.
    pub timestamp_jitter: TimestampJitterConfig,
    /// LLM trace profile spec. `Some` exactly when traces are among the signals.
    pub llm: Option<LlmProfileSpec>,
}

/// The LLM span-profile inputs, present exactly when the traces signal is selected. Nested in
/// [`SignalFactorySpec`] so a logs-only spec carries no trace knobs at all.
#[derive(Debug, Clone)]
pub struct LlmProfileSpec {
    pub max_tool_calls: u32,
    pub capture_content: bool,
    pub weights: ProfileWeights,
    pub budget: Option<SpanBudget>,
}

#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Where this run sends its signals: the gRPC flow (one endpoint), the HTTP flow (one URL per
    /// signal), or a dry run (nothing). Resolved once at the CLI boundary by
    /// [`Destination::from_flags`], so no transport-conditional validation exists below this point.
    pub destination: Destination,
    pub invalid_record_percent: f32,
    /// Number of log records per message (signal=logs), divided evenly across the service shards.
    pub logs_per_message: usize,
    /// Number of traces per message (signal=traces), divided evenly across the service shards.
    /// Independent of `logs_per_message`: a shard's log records are spread over that shard's
    /// traces, so the two knobs scale each signal's volume separately.
    pub traces_per_message: usize,
    pub print_logs: bool,
    pub count: usize,
    pub message_interval_ms: u64,
    pub concurrency: usize,
    pub continuous: bool,
    pub retry_max_retries: u32,
    pub retry_base_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    pub tenant_id: String,
    pub tenant_count: usize,
    pub cloud_account_count_per_tenant: usize,
    pub service_count_per_tenant: usize,
    pub label_cardinality_enabled: bool,
    pub label_cardinality_default_limit: Option<usize>,
    pub label_cardinality_limits: String,
    pub record_across_batch_timestamp_jitter_ms: u64,
    pub record_intra_batch_timestamp_jitter_ns: u64,
    pub record_intra_batch_overlap_probability: f32,
    pub service_shards_per_message: usize,
    /// Telemetry signals to generate, in request order (e.g. `[Logs, Traces]`). Non-empty and
    /// duplicate-free (enforced by [`Self::validate`]). One message per signal is produced per
    /// generation cycle.
    pub signals: Vec<Signal>,
    /// Maximum number of `execute_tool` spans in an LLM trace (signal=traces).
    pub llm_max_tool_calls: u32,
    /// Capture prompt/completion content into span attributes (PII risk; signal=traces).
    pub llm_capture_content: bool,
    /// Relative weights of LLM call forms, e.g.
    /// `simple_chat:1,tool_loop:3,plan_execute_reflect:2,rag:1` (signal=traces).
    pub llm_profile_weights: String,
    /// Lower bound of the per-trace span-count budget (signal=traces). `0` with
    /// `trace_max_spans == 0` disables budgeting.
    pub trace_min_spans: u32,
    /// Upper bound of the per-trace span-count budget (signal=traces).
    pub trace_max_spans: u32,
    /// Raw vendor auth headers as a CSV map, e.g. `Authorization=Bearer xxx,x-bt-parent=project:foo`.
    /// Applied to every HTTP/gRPC request, independent of the tenant header.
    pub auth_headers: String,
    /// Shortcut for a Bearer token: produces `Authorization: Bearer <token>`.
    pub auth_bearer: Option<String>,
    /// Shortcut for Basic auth: `user:pass` is base64-encoded into `Authorization: Basic <b64>`.
    pub auth_basic: Option<String>,
}

impl OtelConfig {
    /// Whether `signal` is one of the configured signals.
    pub fn has_signal(&self, signal: Signal) -> bool {
        self.signals.contains(&signal)
    }

    /// Whether this run generates without sending. Derived from [`Self::destination`] so the run
    /// mode has exactly one source of truth.
    pub fn is_dry_run(&self) -> bool {
        self.destination.is_dry_run()
    }

    /// Upper bound on the number of service shards one message may be split into.
    ///
    /// A shard carries at least one unit of every configured signal — one log record for logs, one
    /// trace for traces — so the count is capped by the smallest per-message budget among them. A
    /// logs-only run is bounded by `logs_per_message` alone, a traces-only run by
    /// `traces_per_message` alone, and a `logs,traces` run by the smaller of the two. Never `0`.
    pub fn service_shard_limit(&self) -> usize {
        let mut limit = usize::MAX;
        if self.has_signal(Signal::Logs) {
            limit = limit.min(self.logs_per_message);
        }
        if self.has_signal(Signal::Traces) {
            limit = limit.min(self.traces_per_message);
        }
        limit.max(1)
    }

    #[allow(clippy::result_large_err)]
    pub fn validate(&self) -> Result<()> {
        if self.signals.is_empty() {
            return Err(GeneratorError::InvalidConfiguration(
                "at least one signal must be selected (--signals logs,traces)".to_string(),
            ));
        }
        let mut seen = HashSet::new();
        for signal in &self.signals {
            if !seen.insert(*signal) {
                return Err(GeneratorError::InvalidConfiguration(format!(
                    "duplicate signal '{}' in --signals",
                    signal.as_str()
                )));
            }
        }

        // `signals` and `destination` are resolved together at the CLI boundary, but both are
        // public fields of this struct, so nothing keeps a caller from assembling a pair that does
        // not agree. Catch it here: without this, the run starts, the banner has no URL to print
        // for the uncovered signal, and every one of its requests fails in the transport instead.
        if let Destination::Http { endpoints, .. } = &self.destination {
            for signal in &self.signals {
                if !endpoints.contains_key(signal) {
                    return Err(GeneratorError::InvalidConfiguration(format!(
                        "no HTTP endpoint resolved for signal '{}'",
                        signal.as_str()
                    )));
                }
            }
        }

        if self.invalid_record_percent < 0.0 || self.invalid_record_percent > 100.0 {
            return Err(GeneratorError::InvalidConfiguration(
                "invalid_record_percent must be between 0 and 100".to_string(),
            ));
        }

        // Invalid-record generation is implemented for logs only, and an invalid log may omit
        // `resourceLogs` entirely, so it cannot be correlated. Allow it exclusively for a run whose
        // signals are exactly `[logs]`.
        if self.invalid_record_percent > 0.0 && self.signals != [Signal::Logs] {
            return Err(GeneratorError::InvalidConfiguration(
                "invalid_record_percent > 0 is only supported for signals=[logs]".to_string(),
            ));
        }

        if self.logs_per_message < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "logs_per_message must be >= 1".to_string(),
            ));
        }

        if self.traces_per_message < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "traces_per_message must be >= 1".to_string(),
            ));
        }

        if self.concurrency < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "concurrency must be >= 1".to_string(),
            ));
        }

        if self.retry_max_retries > MAX_RETRIES_UPPER_BOUND {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "retry_max_retries must be <= {}",
                MAX_RETRIES_UPPER_BOUND
            )));
        }

        if self.retry_base_delay_ms < 100 {
            return Err(GeneratorError::InvalidConfiguration(
                "retry_base_delay_ms must be >= 100".to_string(),
            ));
        }

        if self.retry_max_delay_ms < self.retry_base_delay_ms {
            return Err(GeneratorError::InvalidConfiguration(
                "retry_max_delay_ms must be >= retry_base_delay_ms".to_string(),
            ));
        }

        if self.tenant_count == 1 {
            validate_tenant_id(&self.tenant_id)?;
        }

        if let Some(limit) = self.label_cardinality_default_limit {
            if limit < 1 {
                return Err(GeneratorError::InvalidConfiguration(
                    "label_cardinality_default_limit must be >= 1".to_string(),
                ));
            }
        }

        parse_cardinality_limits(&self.label_cardinality_limits)?;

        // Profile weights are consumed only by the traces branch (see
        // OtelGenerator::with_transport), including on the dry-run path. Run the full domain
        // validation here so a malformed trace-only setting fails fast on a traces run and never
        // blocks a logs-only run.
        if self.has_signal(Signal::Traces) {
            ProfileWeights::from_pairs(&parse_profile_weights(&self.llm_profile_weights)?)?;

            let (mn, mx) = (self.trace_min_spans, self.trace_max_spans);
            if (mn == 0) != (mx == 0) {
                return Err(GeneratorError::InvalidConfiguration(
                    "trace_min_spans and trace_max_spans must both be set (>= 1) or both be 0 (disabled)"
                        .to_string(),
                ));
            }
            if mx > 0 {
                if mx < mn {
                    return Err(GeneratorError::InvalidConfiguration(
                        "trace_max_spans must be >= trace_min_spans".to_string(),
                    ));
                }
                if mx > TRACE_SPAN_BUDGET_MAX {
                    return Err(GeneratorError::InvalidConfiguration(format!(
                        "trace_max_spans must be <= {TRACE_SPAN_BUDGET_MAX}"
                    )));
                }
            }
        }

        // Auth headers are built only on the live send path, after the dry-run early return, so
        // skip parsing them on a dry-run that will never send a request.
        if !self.is_dry_run() {
            self.auth_headers()?;
        }

        if self.record_across_batch_timestamp_jitter_ms > 3_600_000 {
            return Err(GeneratorError::InvalidConfiguration(
                "record_across_batch_timestamp_jitter_ms must be <= 3600000 (1 hour)".to_string(),
            ));
        }

        if self.record_intra_batch_timestamp_jitter_ns > 60_000_000_000 {
            return Err(GeneratorError::InvalidConfiguration(
                "record_intra_batch_timestamp_jitter_ns must be <= 60000000000 (1 minute)"
                    .to_string(),
            ));
        }

        if !(0.0f32..=1.0f32).contains(&self.record_intra_batch_overlap_probability) {
            return Err(GeneratorError::InvalidConfiguration(
                "record_intra_batch_overlap_probability must be between 0.0 and 1.0".to_string(),
            ));
        }

        // When service_shards_per_message > service_count_per_tenant, service names are picked with
        // replacement from the pool, producing duplicate names across shards. This is intentional:
        // it simulates multiple pods running the same service. No error is raised; select_service_shards
        // normalises the count to min(requested, logs_per_message) >= 1.
        if self.service_shards_per_message < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "service_shards_per_message must be >= 1".to_string(),
            ));
        }

        Ok(())
    }

    /// Convert CLI-facing jitter settings into generator-facing nanoseconds.
    ///
    /// This method does not perform validation on its own and expects [`Self::validate`] to be
    /// called first. The returned config is consumed by
    /// [`crate::message::OTLPLogMessageGenerator::new`].
    pub fn timestamp_jitter_config(&self) -> TimestampJitterConfig {
        TimestampJitterConfig {
            across_batch_timestamp_jitter_ns: self.record_across_batch_timestamp_jitter_ms as i64
                * 1_000_000,
            intra_batch_timestamp_jitter_ns: self.record_intra_batch_timestamp_jitter_ns as i64,
            intra_batch_overlap_probability: self.record_intra_batch_overlap_probability,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn retry_config(&self) -> Result<RetryConfig> {
        RetryConfig::new(
            self.retry_max_retries,
            self.retry_base_delay_ms,
            self.retry_max_delay_ms,
        )
    }

    /// Resolve and validate the [`SignalFactorySpec`] for this run: the encoding choice, the
    /// per-signal cardinality and LLM-profile inputs, all computed once. Keeps the `message` layer
    /// off the full `OtelConfig`.
    ///
    /// # Errors
    ///
    /// Propagates cardinality and profile-weight parsing/validation, which
    /// [`Self::validate`] already ran, so a valid config never fails here.
    #[allow(clippy::result_large_err)]
    pub fn signal_factory_spec(&self) -> Result<SignalFactorySpec> {
        let log_cardinality = if self.has_signal(Signal::Logs) {
            Some(self.label_cardinality_config()?)
        } else {
            None
        };

        let llm = if self.has_signal(Signal::Traces) {
            Some(LlmProfileSpec {
                max_tool_calls: self.llm_max_tool_calls,
                capture_content: self.llm_capture_content,
                weights: ProfileWeights::from_pairs(&parse_profile_weights(
                    &self.llm_profile_weights,
                )?)?,
                budget: self.span_budget(),
            })
        } else {
            None
        };

        Ok(SignalFactorySpec {
            signals: self.signals.clone(),
            // One `generator.source` for every signal so a correlated log and trace agree on it.
            source: "rust-generator".to_string(),
            want_protobuf: self.destination.want_protobuf(),
            log_cardinality,
            timestamp_jitter: self.timestamp_jitter_config(),
            llm,
        })
    }

    /// The per-trace span-count budget, or `None` when disabled (both bounds zero). Consumed by the
    /// traces branch of [`crate::message::factory::SignalFactory::from_spec`]; ignored for logs.
    pub fn span_budget(&self) -> Option<SpanBudget> {
        if self.trace_min_spans == 0 && self.trace_max_spans == 0 {
            None
        } else {
            Some(SpanBudget {
                min: self.trace_min_spans,
                max: self.trace_max_spans,
            })
        }
    }

    /// Build the vendor auth headers applied to every request. Validated eagerly in
    /// [`Self::validate`] so a misconfigured spec fails at startup, like
    /// [`parse_profile_weights`].
    #[allow(clippy::result_large_err)]
    pub fn auth_headers(&self) -> Result<AuthHeaders> {
        AuthHeaders::build(
            &self.auth_headers,
            self.auth_bearer.as_deref(),
            self.auth_basic.as_deref(),
        )
    }

    /// Build the startup banner as ordered `label -> value` rows. Pure (no I/O): the actual
    /// printing is [`crate::report::report_startup_summary`]'s job, which is what makes the banner
    /// testable. The row set adapts to the configured signals, transport, and tenant pools.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::retry_config`] validation so a bad retry setting fails at startup.
    #[allow(clippy::result_large_err)]
    pub fn startup_summary(&self) -> Result<Vec<(String, String)>> {
        let mut rows: Vec<(String, String)> = Vec::new();
        let mut push = |label: &str, value: String| rows.push((label.to_string(), value));

        let signals_str = self
            .signals
            .iter()
            .map(Signal::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        push("Signals", signals_str);

        match &self.destination {
            // The flow is still reported: it decides the encoding of the printed payload, so a
            // gRPC preview and a JSON HTTP preview are different output from the same run.
            Destination::DryRun { flow, protobuf } => {
                push(
                    "Dry-run",
                    format!(
                        "no network transport, stdout only (would use {}, protobuf={})",
                        flow.as_str(),
                        protobuf
                    ),
                );
            }
            Destination::Grpc { endpoint } => {
                push("Transport", "grpc".to_string());
                push("Endpoint", endpoint.clone());
                push("Use Protobuf", "true (gRPC wire format)".to_string());
            }
            Destination::Http {
                endpoints,
                protobuf,
            } => {
                push("Transport", "http".to_string());
                // Rows follow the configured signal order, not the map's iteration order.
                // [`Self::validate`] has already proven every configured signal has a URL here.
                for signal in &self.signals {
                    if let Some(endpoint) = endpoints.get(signal) {
                        push(&format!("Endpoint ({})", signal.as_str()), endpoint.clone());
                    }
                }
                push("Use Protobuf", protobuf.to_string());
            }
        }

        if self.has_signal(Signal::Logs) {
            push("Log records per message", self.logs_per_message.to_string());
        }
        if self.has_signal(Signal::Traces) {
            push("Traces per message", self.traces_per_message.to_string());
        }

        let effective_shards = self
            .service_shards_per_message
            .min(self.service_shard_limit())
            .max(1);
        push(
            "Services per message",
            format!(
                "{} (effective: {})",
                self.service_shards_per_message, effective_shards
            ),
        );
        push("Invalid record %", self.invalid_record_percent.to_string());
        push("Concurrency", self.concurrency.to_string());

        let tenant_routing = if self.tenant_count == 0 {
            "no tenant header".to_string()
        } else if self.tenant_count == 1 {
            format!("single tenant '{}'", self.tenant_id)
        } else {
            format!(
                "{} tenants, random tenant1..tenant{}",
                self.tenant_count, self.tenant_count
            )
        };
        push("Tenant routing", tenant_routing);

        let cloud_pool_str = if self.cloud_account_count_per_tenant == 0 {
            "omitted".to_string()
        } else {
            self.cloud_account_count_per_tenant.to_string()
        };
        let service_pool_str = if self.service_count_per_tenant == 0 {
            "omitted".to_string()
        } else {
            self.service_count_per_tenant.to_string()
        };
        let tenant_pools = if self.tenant_count == 0 {
            format!(
                "{} cloud accounts, {} services (tenantless)",
                cloud_pool_str, service_pool_str
            )
        } else {
            format!(
                "{} cloud accounts/tenant, {} services/tenant",
                cloud_pool_str, service_pool_str
            )
        };
        push("Tenant profile pools", tenant_pools);

        let retry_config = self.retry_config()?;
        push(
            "Retry",
            format!(
                "max_retries={}, base_delay={}ms, max_delay={}ms",
                retry_config.max_retries, retry_config.base_delay_ms, retry_config.max_delay_ms
            ),
        );

        // Cardinality bounds log record attributes and the shard's shared resource attributes. When
        // logs and traces run together, the trace adopts the same bucketed resource; a traces-only
        // run does not normalize anything. Report that honestly per signal.
        if self.has_signal(Signal::Logs) {
            push(
                "Label cardinality limiting (logs)",
                self.label_cardinality_enabled.to_string(),
            );
        }
        if self.has_signal(Signal::Traces) {
            let value = if self.has_signal(Signal::Logs) {
                format!(
                    "shared resource attributes only ({})",
                    self.label_cardinality_enabled
                )
            } else {
                "n/a".to_string()
            };
            push("Label cardinality limiting (traces)", value);
        }

        Ok(rows)
    }

    #[allow(clippy::result_large_err)]
    pub fn label_cardinality_config(&self) -> Result<AttributesCardinalityConfig> {
        let mut limits = default_cardinality_limits();
        let custom_limits = parse_cardinality_limits(&self.label_cardinality_limits)?;
        limits.extend(custom_limits);

        Ok(AttributesCardinalityConfig {
            enabled: self.label_cardinality_enabled,
            default_limit: self.label_cardinality_default_limit,
            limit_by_attr: limits,
        })
    }
}

fn default_cardinality_limits() -> HashMap<String, usize> {
    DEFAULT_CARDINALITY_LIMITS
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

#[allow(clippy::result_large_err)]
fn validate_tenant_id(tenant_id: &str) -> Result<()> {
    if tenant_id.is_empty() {
        return Err(GeneratorError::InvalidConfiguration(
            "tenant_id must not be empty".to_string(),
        ));
    }

    if tenant_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(GeneratorError::InvalidConfiguration(
            "tenant_id must contain only ASCII alphanumeric characters, '-' or '_'".to_string(),
        ))
    }
}

#[allow(clippy::result_large_err)]
fn parse_cardinality_limits(raw: &str) -> Result<HashMap<String, usize>> {
    let mut parsed = HashMap::new();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(parsed);
    }

    for pair in trimmed.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        let (key, value_str) = pair.split_once('=').ok_or_else(|| {
            GeneratorError::InvalidConfiguration(format!(
                "invalid label cardinality pair '{}', expected key=limit",
                pair
            ))
        })?;

        let key = key.trim();
        if key.is_empty() {
            return Err(GeneratorError::InvalidConfiguration(
                "label cardinality key must not be empty".to_string(),
            ));
        }

        let value_str = value_str.trim();
        let limit = value_str.parse::<usize>().map_err(|_| {
            GeneratorError::InvalidConfiguration(format!(
                "invalid label cardinality limit '{}' for key '{}'",
                value_str, key
            ))
        })?;

        if limit < 1 {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "label cardinality limit for key '{}' must be >= 1",
                key
            )));
        }

        parsed.insert(key.to_string(), limit);
    }

    Ok(parsed)
}

/// Tokenise the raw `LLM_PROFILE_WEIGHTS` spec (`name:weight,name:weight,…`) into neutral
/// name→weight pairs. This is the *syntactic* layer: it validates the `key:value` shape and that
/// weights are numeric, but knows nothing of the valid form names or the positive-total rule —
/// those belong to the domain ([`crate::message::traces::span_profile::ProfileWeights::from_pairs`]).
/// An empty (or whitespace) spec yields an empty map.
#[allow(clippy::result_large_err)]
pub fn parse_profile_weights(raw: &str) -> Result<HashMap<String, u32>> {
    let mut parsed = HashMap::new();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(parsed);
    }

    for pair in trimmed.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        let (name, weight_str) = pair.split_once(':').ok_or_else(|| {
            GeneratorError::InvalidConfiguration(format!(
                "invalid llm profile weight pair '{pair}', expected name:weight"
            ))
        })?;

        let name = name.trim();
        if name.is_empty() {
            return Err(GeneratorError::InvalidConfiguration(
                "llm profile weight name must not be empty".to_string(),
            ));
        }

        let weight = weight_str.trim().parse::<u32>().map_err(|_| {
            GeneratorError::InvalidConfiguration(format!(
                "invalid weight '{}' for llm profile form '{name}'",
                weight_str.trim()
            ))
        })?;

        parsed.insert(name.to_string(), weight);
    }

    Ok(parsed)
}

/// `total`/`success`/`failed` counters for one level of the statistics (generation cycles or a
/// single signal).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SignalCounters {
    /// Number of units recorded at this level (generation cycles, or requests for one signal).
    pub total: usize,
    /// Subset of `total` that succeeded.
    pub success: usize,
    /// Subset of `total` that failed; `success + failed == total`.
    pub failed: usize,
}

impl SignalCounters {
    fn record(&mut self, success: bool) {
        self.total += 1;
        if success {
            self.success += 1;
        } else {
            self.failed += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.total += other.total;
        self.success += other.success;
        self.failed += other.failed;
    }
}

/// Statistics for a run. A generation cycle emits one signal message per configured signal, so the
/// two levels are tracked separately: `cycles` counts generation cycles (a cycle succeeds only when
/// every signal message of that cycle succeeds), while `per_signal` keeps an independent
/// `total/success/failed` for each signal's network requests.
#[derive(Debug, Clone, Default)]
pub struct GenerationStats {
    /// Generation-cycle counters. A cycle is counted successful only when every signal message of
    /// that cycle was delivered.
    pub cycles: SignalCounters,
    /// Per-signal network-request counters, keyed by [`Signal`]; each tracks that signal's own
    /// `total/success/failed` independently of the others.
    pub per_signal: BTreeMap<Signal, SignalCounters>,
}

impl GenerationStats {
    /// Create an empty statistics accumulator (all counters zero, no signals recorded yet).
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the outcome of one signal message (one network request).
    pub fn record_signal(&mut self, signal: Signal, success: bool) {
        self.per_signal.entry(signal).or_default().record(success);
    }

    /// Record the completion of one generation cycle. `all_success` must be `true` only when every
    /// signal message of the cycle succeeded.
    pub fn record_cycle(&mut self, all_success: bool) {
        self.cycles.record(all_success);
    }

    /// Fold another [`GenerationStats`] into this one, summing the cycle counters and each signal's
    /// counters (matched by [`Signal`]). Used to combine per-worker results into a run total.
    pub fn merge(&mut self, other: Self) {
        self.cycles.merge(&other.cycles);
        for (signal, counters) in other.per_signal {
            self.per_signal.entry(signal).or_default().merge(&counters);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportFlow;

    fn http_logs_destination(protobuf: bool) -> Destination {
        Destination::Http {
            endpoints: HashMap::from([(Signal::Logs, "http://localhost:4318/v1/logs".to_string())]),
            protobuf,
        }
    }

    fn http_traces_destination() -> Destination {
        Destination::Http {
            endpoints: HashMap::from([(
                Signal::Traces,
                "http://localhost:4318/v1/traces".to_string(),
            )]),
            protobuf: false,
        }
    }

    /// The HTTP flow carrying both signals, each on its own URL.
    fn http_logs_and_traces_destination() -> Destination {
        Destination::Http {
            endpoints: HashMap::from([
                (Signal::Logs, "http://localhost:4318/v1/logs".to_string()),
                (
                    Signal::Traces,
                    "http://localhost:4318/v1/traces".to_string(),
                ),
            ]),
            protobuf: false,
        }
    }

    fn base_config() -> OtelConfig {
        OtelConfig {
            destination: http_logs_destination(false),
            invalid_record_percent: 0.0,
            logs_per_message: 1,
            traces_per_message: 1,
            print_logs: false,
            count: 1,
            message_interval_ms: 0,
            concurrency: 1,
            continuous: false,
            retry_max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 32000,
            tenant_id: "tenant1".to_string(),
            tenant_count: 1,
            cloud_account_count_per_tenant: 4,
            service_count_per_tenant: 6,
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
            record_across_batch_timestamp_jitter_ms: 1_000,
            record_intra_batch_timestamp_jitter_ns: 5,
            record_intra_batch_overlap_probability: 0.05,
            service_shards_per_message: 1,
            signals: vec![Signal::Logs],
            llm_max_tool_calls: 3,
            llm_capture_content: true,
            llm_profile_weights: "simple_chat:1,tool_loop:3,plan_execute_reflect:2,rag:1"
                .to_string(),
            trace_min_spans: 0,
            trace_max_spans: 0,
            auth_headers: String::new(),
            auth_bearer: None,
            auth_basic: None,
        }
    }

    #[test]
    fn validate_rejects_invalid_record_percent_for_traces() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Traces];
        cfg.destination = http_traces_destination();
        cfg.invalid_record_percent = 50.0;
        assert!(matches!(
            cfg.validate(),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_signals() {
        let mut cfg = base_config();
        cfg.signals = vec![];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_signals() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Logs];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_invalid_record_for_multi_signal() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = http_logs_and_traces_destination();
        cfg.invalid_record_percent = 10.0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_allows_invalid_record_for_logs_only() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs];
        cfg.invalid_record_percent = 10.0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn generation_stats_track_cycles_and_signals_independently() {
        let mut stats = GenerationStats::new();
        // Cycle 1: both signals succeed.
        stats.record_signal(Signal::Logs, true);
        stats.record_signal(Signal::Traces, true);
        stats.record_cycle(true);
        // Cycle 2: logs succeed, traces fail -> failed cycle, but logs stays successful.
        stats.record_signal(Signal::Logs, true);
        stats.record_signal(Signal::Traces, false);
        stats.record_cycle(false);

        assert_eq!(stats.cycles.total, 2);
        assert_eq!(stats.cycles.success, 1);
        assert_eq!(stats.cycles.failed, 1);
        assert_eq!(
            stats.per_signal[&Signal::Logs],
            SignalCounters {
                total: 2,
                success: 2,
                failed: 0
            }
        );
        assert_eq!(
            stats.per_signal[&Signal::Traces],
            SignalCounters {
                total: 2,
                success: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn validate_allows_zero_invalid_record_percent_for_traces() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Traces];
        cfg.destination = http_traces_destination();
        cfg.invalid_record_percent = 0.0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn span_budget_disabled_when_both_zero() {
        let mut c = base_config();
        c.trace_min_spans = 0;
        c.trace_max_spans = 0;
        assert!(c.span_budget().is_none());
    }

    #[test]
    fn span_budget_some_when_set() {
        let mut c = base_config();
        c.trace_min_spans = 5;
        c.trace_max_spans = 10;
        let b = c.span_budget().expect("budget present");
        assert_eq!((b.min, b.max), (5, 10));
    }

    #[test]
    fn validate_rejects_half_set_budget() {
        let mut c = base_config();
        c.signals = vec![Signal::Traces];
        c.destination = http_traces_destination();
        c.trace_min_spans = 5;
        c.trace_max_spans = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_max_below_min() {
        let mut c = base_config();
        c.signals = vec![Signal::Traces];
        c.destination = http_traces_destination();
        c.trace_min_spans = 10;
        c.trace_max_spans = 5;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_budget_over_cap() {
        let mut c = base_config();
        c.signals = vec![Signal::Traces];
        c.destination = http_traces_destination();
        c.trace_min_spans = 1;
        c.trace_max_spans = TRACE_SPAN_BUDGET_MAX + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_valid_budget() {
        let mut c = base_config();
        c.signals = vec![Signal::Traces];
        c.destination = http_traces_destination();
        c.trace_min_spans = 5;
        c.trace_max_spans = 40;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_ignores_budget_for_logs() {
        // Budget is a traces-only knob; a half-set budget must not fail a logs run.
        let mut c = base_config();
        c.signals = vec![Signal::Logs];
        c.trace_min_spans = 5;
        c.trace_max_spans = 0;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn test_parse_cardinality_limits_ok() {
        let parsed = parse_cardinality_limits("k8s.pod.name=32, request.id=64,user.id=10").unwrap();
        assert_eq!(parsed.get("k8s.pod.name"), Some(&32));
        assert_eq!(parsed.get("request.id"), Some(&64));
        assert_eq!(parsed.get("user.id"), Some(&10));
    }

    #[test]
    fn test_parse_cardinality_limits_invalid_pairs() {
        assert!(parse_cardinality_limits("k=").is_err());
        assert!(parse_cardinality_limits("=1").is_err());
        assert!(parse_cardinality_limits("k=abc").is_err());
        assert!(parse_cardinality_limits("k=-1").is_err());
    }

    #[test]
    fn test_parse_profile_weights_ok() {
        let parsed = parse_profile_weights("simple_chat:1, tool_loop:3,rag:0").unwrap();
        assert_eq!(parsed.get("simple_chat"), Some(&1));
        assert_eq!(parsed.get("tool_loop"), Some(&3));
        assert_eq!(parsed.get("rag"), Some(&0));
        // an empty spec is syntactically valid (the domain layer rejects an empty total)
        assert!(parse_profile_weights("").unwrap().is_empty());
    }

    #[test]
    fn test_parse_profile_weights_invalid_syntax() {
        assert!(parse_profile_weights("rag").is_err()); // no colon
        assert!(parse_profile_weights("rag:x").is_err()); // non-numeric weight
        assert!(parse_profile_weights(":1").is_err()); // empty name
        assert!(parse_profile_weights("rag:-1").is_err()); // negative weight
    }

    #[test]
    fn test_label_cardinality_config_merges_defaults_and_overrides() {
        let mut cfg = base_config();
        cfg.label_cardinality_limits = "k8s.pod.name=7,my.key=3".to_string();
        cfg.label_cardinality_default_limit = Some(11);

        let cardinality = cfg.label_cardinality_config().unwrap();
        assert_eq!(cardinality.limit_for("k8s.pod.name"), Some(7));
        assert_eq!(cardinality.limit_for("request.id"), Some(64));
        assert_eq!(cardinality.limit_for("my.key"), Some(3));
        assert_eq!(cardinality.limit_for("unlisted.key"), Some(11));
    }

    #[test]
    fn test_concurrency_validation() {
        let mut cfg = base_config();
        cfg.concurrency = 0;
        assert!(cfg.validate().is_err());

        cfg.concurrency = 1;
        assert!(cfg.validate().is_ok());

        cfg.concurrency = 20;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_tenant_count_validation() {
        let mut cfg = base_config();
        cfg.tenant_count = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_cloud_account_count_per_tenant_validation() {
        let mut cfg = base_config();
        cfg.cloud_account_count_per_tenant = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_service_count_per_tenant_validation() {
        let mut cfg = base_config();
        cfg.service_count_per_tenant = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn zero_tenant_count_ignores_invalid_tenant_id() {
        let mut cfg = base_config();
        cfg.tenant_count = 0;
        cfg.tenant_id = "invalid tenant id!".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_single_tenant_id_validation() {
        let mut cfg = base_config();
        cfg.tenant_id = "tenant with spaces".to_string();
        assert!(cfg.validate().is_err());

        cfg.tenant_id = "tenant_1-ok".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_multi_tenant_mode_ignores_legacy_single_tenant_value_validation() {
        let mut cfg = base_config();
        cfg.tenant_count = 3;
        cfg.tenant_id = "tenant with spaces".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_intra_batch_jitter_validation() {
        let mut cfg = base_config();
        cfg.record_intra_batch_timestamp_jitter_ns = 60_000_000_001;
        assert!(cfg.validate().is_err());

        cfg.record_intra_batch_timestamp_jitter_ns = 60_000_000_000;
        assert!(cfg.validate().is_ok());

        cfg.record_intra_batch_timestamp_jitter_ns = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_overlap_probability_validation() {
        let mut cfg = base_config();
        cfg.record_intra_batch_overlap_probability = -0.1;
        assert!(cfg.validate().is_err());

        cfg.record_intra_batch_overlap_probability = 1.1;
        assert!(cfg.validate().is_err());

        cfg.record_intra_batch_overlap_probability = 0.0;
        assert!(cfg.validate().is_ok());

        cfg.record_intra_batch_overlap_probability = 1.0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_service_shards_per_message_validation() {
        let mut cfg = base_config();
        cfg.service_shards_per_message = 0;
        assert!(cfg.validate().is_err());

        cfg.service_shards_per_message = 1;
        assert!(cfg.validate().is_ok());

        cfg.service_shards_per_message = 10;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_traces_per_message_must_be_at_least_one() {
        let mut cfg = base_config();
        cfg.traces_per_message = 0;
        assert!(matches!(
            cfg.validate(),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn test_service_shard_limit_uses_only_configured_signals() {
        // Logs-only: the traces budget must not cap the shard count, and vice versa; a run with
        // both signals is bounded by the smaller of the two.
        let mut cfg = base_config();
        cfg.logs_per_message = 10;
        cfg.traces_per_message = 3;

        cfg.signals = vec![Signal::Logs];
        assert_eq!(cfg.service_shard_limit(), 10);

        cfg.signals = vec![Signal::Traces];
        assert_eq!(cfg.service_shard_limit(), 3);

        cfg.signals = vec![Signal::Logs, Signal::Traces];
        assert_eq!(cfg.service_shard_limit(), 3);
    }

    #[test]
    fn test_service_shards_per_message_has_no_upper_bound() {
        // No upper bound is intentional: select_service_shards clamps to the per-signal budgets at
        // runtime, and large values simulate "every record is its own pod". This test pins down
        // the missing upper-bound contract so a future regression that adds one fails loudly.
        let mut cfg = base_config();
        cfg.service_shards_per_message = usize::MAX;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_service_shards_per_message_accepts_typical_value() {
        let mut cfg = base_config();
        cfg.service_shards_per_message = 4;
        assert!(cfg.validate().is_ok());
    }

    /// Row lookups against the ordered `label -> value` startup summary.
    fn summary_has_label(rows: &[(String, String)], label: &str) -> bool {
        rows.iter().any(|(l, _)| l == label)
    }

    fn summary_value<'a>(rows: &'a [(String, String)], label: &str) -> Option<&'a str> {
        rows.iter()
            .find(|(l, _)| l == label)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn startup_summary_logs_only_omits_traces_per_message() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs];
        let rows = cfg.startup_summary().unwrap();
        assert!(summary_has_label(&rows, "Log records per message"));
        assert!(!summary_has_label(&rows, "Traces per message"));
        // Cardinality is a logs feature; the traces row must not appear for a logs-only run.
        assert!(summary_has_label(
            &rows,
            "Label cardinality limiting (logs)"
        ));
        assert!(!summary_has_label(
            &rows,
            "Label cardinality limiting (traces)"
        ));
    }

    #[test]
    fn startup_summary_traces_only_omits_log_records_per_message() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Traces];
        cfg.destination = http_traces_destination();
        let rows = cfg.startup_summary().unwrap();
        assert!(summary_has_label(&rows, "Traces per message"));
        assert!(!summary_has_label(&rows, "Log records per message"));
        // Traces-only normalizes nothing, so the traces cardinality row reports n/a.
        assert_eq!(
            summary_value(&rows, "Label cardinality limiting (traces)"),
            Some("n/a")
        );
    }

    #[test]
    fn startup_summary_reports_one_endpoint_row_per_signal_for_the_http_flow() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = http_logs_and_traces_destination();
        let rows = cfg.startup_summary().unwrap();
        assert_eq!(summary_value(&rows, "Transport"), Some("http"));
        assert_eq!(
            summary_value(&rows, "Endpoint (logs)"),
            Some("http://localhost:4318/v1/logs")
        );
        assert_eq!(
            summary_value(&rows, "Endpoint (traces)"),
            Some("http://localhost:4318/v1/traces")
        );
        // http keeps per-signal endpoints, so the single-endpoint grpc row is absent.
        assert!(!summary_has_label(&rows, "Endpoint"));
    }

    #[test]
    fn startup_summary_reports_a_single_endpoint_row_for_the_grpc_flow() {
        let mut cfg = base_config();
        cfg.destination = Destination::Grpc {
            endpoint: "http://localhost:4317".to_string(),
        };
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        let rows = cfg.startup_summary().unwrap();
        assert_eq!(summary_value(&rows, "Transport"), Some("grpc"));
        assert_eq!(
            summary_value(&rows, "Endpoint"),
            Some("http://localhost:4317")
        );
        assert!(!summary_has_label(&rows, "Endpoint (logs)"));
        assert!(!summary_has_label(&rows, "Endpoint (traces)"));
    }

    #[test]
    fn signal_factory_spec_logs_only_has_cardinality_and_no_llm() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs];
        let spec = cfg.signal_factory_spec().unwrap();
        assert_eq!(spec.signals, vec![Signal::Logs]);
        assert!(spec.log_cardinality.is_some());
        assert!(spec.llm.is_none());
    }

    #[test]
    fn signal_factory_spec_traces_only_has_llm_and_no_cardinality() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Traces];
        cfg.trace_min_spans = 5;
        cfg.trace_max_spans = 10;
        let spec = cfg.signal_factory_spec().unwrap();
        assert!(spec.log_cardinality.is_none());
        let llm = spec.llm.expect("traces spec carries an llm profile");
        assert_eq!(llm.budget.map(|b| (b.min, b.max)), Some((5, 10)));
    }

    #[test]
    fn signal_factory_spec_both_signals_carry_both_inputs() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = http_logs_and_traces_destination();
        let spec = cfg.signal_factory_spec().unwrap();
        assert!(spec.log_cardinality.is_some());
        assert!(spec.llm.is_some());
    }

    #[test]
    fn signal_factory_spec_wants_protobuf_for_grpc_regardless_of_flag() {
        // The gRPC flow has no protobuf flag to unset: its wire format is protobuf either way.
        let mut cfg = base_config();
        cfg.destination = Destination::Grpc {
            endpoint: "http://localhost:4317".to_string(),
        };
        assert!(cfg.signal_factory_spec().unwrap().want_protobuf);
    }

    #[test]
    fn dry_run_config_reports_no_transport_and_is_dry_run() {
        let mut cfg = base_config();
        cfg.destination = Destination::DryRun {
            flow: TransportFlow::Http,
            protobuf: false,
        };

        assert!(cfg.is_dry_run());
        assert!(cfg.validate().is_ok());
        let rows = cfg.startup_summary().unwrap();
        assert!(summary_has_label(&rows, "Dry-run"));
        assert!(!summary_has_label(&rows, "Transport"));
        assert!(!summary_has_label(&rows, "Use Protobuf"));
    }

    /// A gRPC dry run and an HTTP dry run print different payloads, so the banner must say which
    /// one is being previewed instead of the single opaque "dry-run" line it used to print.
    #[test]
    fn dry_run_banner_names_the_previewed_flow_and_encoding() {
        let mut cfg = base_config();
        cfg.destination = Destination::DryRun {
            flow: TransportFlow::Grpc,
            protobuf: true,
        };
        let grpc_row = cfg.startup_summary().unwrap();
        let grpc_row = summary_value(&grpc_row, "Dry-run").expect("the dry-run row is present");
        assert!(grpc_row.contains("grpc"), "got: {grpc_row}");
        assert!(grpc_row.contains("protobuf=true"), "got: {grpc_row}");

        cfg.destination = Destination::DryRun {
            flow: TransportFlow::Http,
            protobuf: false,
        };
        let http_row = cfg.startup_summary().unwrap();
        let http_row = summary_value(&http_row, "Dry-run").expect("the dry-run row is present");
        assert!(http_row.contains("http"), "got: {http_row}");
        assert!(http_row.contains("protobuf=false"), "got: {http_row}");
    }

    /// The two fields are resolved together at the CLI boundary but are both public, so an
    /// assembled-by-hand config can disagree; the run must fail at startup rather than per request.
    #[test]
    fn validate_rejects_http_destination_missing_a_selected_signal() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = http_logs_destination(false);
        assert!(matches!(
            cfg.validate(),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn validate_accepts_http_destination_covering_every_signal() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = http_logs_and_traces_destination();
        assert!(cfg.validate().is_ok());
    }

    /// The gRPC flow carries every signal over one channel, so signal coverage is not its concern.
    #[test]
    fn validate_accepts_grpc_destination_for_every_signal() {
        let mut cfg = base_config();
        cfg.signals = vec![Signal::Logs, Signal::Traces];
        cfg.destination = Destination::Grpc {
            endpoint: "http://localhost:4317".to_string(),
        };
        assert!(cfg.validate().is_ok());
    }
}
