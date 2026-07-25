use crate::config::OtelConfig;
use crate::error::GeneratorError;
use crate::message::types::Signal;
use crate::transport::{Destination, DestinationFlags};
use clap::{Args, Parser, Subcommand};

/// Environment variables that no longer exist: the retired name, the variables that replace it
/// (empty when a setting was dropped outright, several when one name forked), and the guidance
/// shown when it is still set. clap silently ignores an env var it does not bind, so a stale name
/// in a shell or `.env` would change the run without a word; [`reject_retired_env_vars`] turns each
/// into a startup error.
///
/// The replacements are a structured field rather than prose because a test walks them to prove
/// every recommended name is really bound to an argument.
///
/// Deprecated *aliases* do not belong here: they still work (see `MESSAGE_DELAY`).
const RETIRED_ENV_VARS: &[(&str, &[&str], &str)] = &[
    (
        "OTEL_SIGNAL",
        &["OTEL_SIGNALS"],
        "use OTEL_SIGNALS (e.g. OTEL_SIGNALS=logs,traces)",
    ),
    (
        "RECORDS_PER_MESSAGE",
        &["LOGS_PER_MESSAGE"],
        "use LOGS_PER_MESSAGE (e.g. LOGS_PER_MESSAGE=10)",
    ),
    (
        "OTEL_LOGS_ENDPOINT",
        &["OTEL_HTTP_LOGS_ENDPOINT", "OTEL_GRPC_ENDPOINT"],
        "endpoints are per transport now: OTEL_HTTP_LOGS_ENDPOINT for OTEL_TRANSPORT=http, \
         OTEL_GRPC_ENDPOINT for OTEL_TRANSPORT=grpc",
    ),
    (
        "OTEL_TRACES_ENDPOINT",
        &["OTEL_HTTP_TRACES_ENDPOINT", "OTEL_GRPC_ENDPOINT"],
        "use OTEL_HTTP_TRACES_ENDPOINT for OTEL_TRANSPORT=http; OTEL_TRANSPORT=grpc carries traces \
         over OTEL_GRPC_ENDPOINT",
    ),
    (
        "OTEL_HEALTHCHECK_ENDPOINT",
        &[],
        "health checking was removed; unset it",
    ),
];

/// Fail the run when a retired environment variable is still set. Called once at startup, after the
/// `.env` file is loaded and before the CLI is parsed, so a `.env` left over from an older build
/// cannot silently change what the run does.
#[allow(clippy::result_large_err)]
pub fn reject_retired_env_vars() -> crate::error::Result<()> {
    for (name, _replacements, guidance) in RETIRED_ENV_VARS {
        if std::env::var_os(name).is_some() {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "{name} is no longer used: {guidance}"
            )));
        }
    }
    Ok(())
}

/// Parse boolean values in a case-insensitive way
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" | "" => Ok(false),
        _ => Err(format!("invalid boolean value: '{}'", s)),
    }
}

/// Parse a single telemetry signal token in a case-insensitive way. Applied per element of the
/// comma-separated `--signals` list.
fn parse_signal(s: &str) -> Result<Signal, String> {
    match s.trim().to_lowercase().as_str() {
        "logs" => Ok(Signal::Logs),
        "traces" => Ok(Signal::Traces),
        other => Err(format!(
            "invalid signal '{other}', must be 'logs' or 'traces'"
        )),
    }
}

#[derive(Parser)]
#[command(name = "otel-log-generator")]
#[command(about = "OpenTelemetry log generator with HTTP and gRPC support", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub generator: GeneratorType,
}

#[derive(Subcommand)]
pub enum GeneratorType {
    Otel(OtelArgs),
}

#[derive(Args)]
pub struct OtelArgs {
    /// OTLP gRPC endpoint (--transport grpc). One channel carries every signal, separated by its
    /// OTLP service; there is no second endpoint to set.
    #[arg(long = "grpc-endpoint", env = "OTEL_GRPC_ENDPOINT")]
    pub grpc_endpoint: Option<String>,

    /// OTLP/HTTP logs URL (--transport http). Required when logs are among --signals.
    #[arg(long = "http-logs-endpoint", env = "OTEL_HTTP_LOGS_ENDPOINT")]
    pub http_logs_endpoint: Option<String>,

    /// OTLP/HTTP traces URL (--transport http). Required when traces are among --signals; there is
    /// no fallback to the logs URL.
    #[arg(long = "http-traces-endpoint", env = "OTEL_HTTP_TRACES_ENDPOINT")]
    pub http_traces_endpoint: Option<String>,

    /// Use protobuf encoding instead of JSON
    #[arg(long, env = "OTEL_USE_PROTOBUF", default_value = "false", value_parser = parse_bool)]
    pub use_protobuf: bool,

    /// Transport type: http or grpc
    #[arg(long, env = "OTEL_TRANSPORT", default_value = "http")]
    pub transport: String,

    /// Number of messages to send in batch mode; ignored in continuous mode
    #[arg(long, env = "MESSAGE_COUNT", default_value = "1")]
    pub count: usize,

    /// Minimum interval between started messages in milliseconds; global in batch mode, per worker in continuous mode
    #[arg(long = "message-interval-ms", env = "MESSAGE_INTERVAL_MS")]
    pub message_interval_ms: Option<u64>,

    /// Deprecated alias for --message-interval-ms / MESSAGE_INTERVAL_MS
    #[arg(long = "delay-ms", env = "MESSAGE_DELAY", hide = true)]
    pub delay_ms_legacy: Option<u64>,

    /// Number of concurrent workers
    #[arg(long, env = "CONCURRENCY", default_value = "1")]
    pub concurrency: usize,

    /// Percentage of invalid records to generate (0-100)
    #[arg(long, env = "INVALID_RECORD_PERCENT", default_value = "0.0")]
    pub invalid_record_percent: f32,

    /// Number of log records per message (signal=logs)
    #[arg(long, env = "LOGS_PER_MESSAGE", default_value = "1")]
    pub logs_per_message: usize,

    /// Number of traces per message (signal=traces), divided evenly across the service shards
    #[arg(long, env = "TRACES_PER_MESSAGE", default_value = "1")]
    pub traces_per_message: usize,

    /// Print detailed logs for each message
    #[arg(long, env = "PRINT_LOGS", default_value = "false", value_parser = parse_bool)]
    pub print_logs: bool,

    /// Generate messages and print to stdout only; do not open any network transport
    #[arg(long, env = "DRY_RUN", default_value = "false", value_parser = parse_bool)]
    pub dry_run: bool,

    /// Run in continuous mode
    #[arg(long, env = "CONTINUOUS_MODE", default_value = "false", value_parser = parse_bool)]
    pub continuous: bool,

    /// Maximum number of retries on rate limiting (0-10)
    #[arg(long, env = "RETRY_MAX_RETRIES", default_value = "3")]
    pub retry_max_retries: u32,

    /// Base delay in milliseconds for retry backoff
    #[arg(long, env = "RETRY_BASE_DELAY_MS", default_value = "1000")]
    pub retry_base_delay_ms: u64,

    /// Maximum delay in milliseconds for retry backoff
    #[arg(long, env = "RETRY_MAX_DELAY_MS", default_value = "32000")]
    pub retry_max_delay_ms: u64,

    /// Tenant ID for X-Scope-OrgID in single-tenant mode
    #[arg(long, env = "TENANT_ID")]
    pub tenant_id: Option<String>,

    /// Number of tenants for random routing; when > 1 uses tenant1..tenantN and ignores TENANT_ID.
    /// Set to 0 to omit the X-Scope-OrgID header/metadata entirely; TENANT_ID is ignored.
    #[arg(long, env = "TENANT_COUNT", default_value = "1")]
    pub tenant_count: usize,

    /// Number of cloud.account.id values generated per tenant.
    /// Set to 0 to omit cloud.account.id from resource attributes.
    #[arg(long, env = "CLOUD_ACCOUNT_COUNT_PER_TENANT", default_value = "4")]
    pub cloud_account_count_per_tenant: usize,

    /// Number of service.name values generated per tenant.
    /// Set to 0 to omit service.name from resource attributes; scope.name uses default 'io.trihub.icegen'.
    #[arg(long, env = "SERVICE_COUNT_PER_TENANT", default_value = "6")]
    pub service_count_per_tenant: usize,

    /// Enable label cardinality limiting
    #[arg(
        long,
        env = "OTEL_LABEL_CARDINALITY_ENABLED",
        default_value = "true",
        value_parser = parse_bool
    )]
    pub label_cardinality_enabled: bool,

    /// Default label cardinality limit for keys not listed in OTEL_LABEL_CARDINALITY_LIMITS
    #[arg(long, env = "OTEL_LABEL_CARDINALITY_DEFAULT_LIMIT")]
    pub label_cardinality_default_limit: Option<usize>,

    /// Per-key cardinality limits as CSV map, e.g. key1=32,key2=64
    #[arg(long, env = "OTEL_LABEL_CARDINALITY_LIMITS", default_value = "")]
    pub label_cardinality_limits: String,

    /// Per-batch jitter for log record timestamps in milliseconds; whole request shifts back by
    /// rand(0, value). Applied once per batch, not per record. (0 to disable, max 3600000)
    #[arg(
        long,
        env = "RECORD_ACROSS_BATCH_TIMESTAMP_JITTER_MS",
        default_value = "1000"
    )]
    pub record_across_batch_timestamp_jitter_ms: u64,

    /// Intra-batch jitter in nanoseconds: forward step between adjacent records and size of
    /// rare backward nudge. (0 to disable, max 60000000000)
    #[arg(
        long,
        env = "RECORD_INTRA_BATCH_TIMESTAMP_JITTER_NS",
        default_value = "5"
    )]
    pub record_intra_batch_timestamp_jitter_ns: u64,

    /// Probability [0.0, 1.0] that a record (i > 0) steps backward instead of forward
    #[arg(
        long,
        env = "RECORD_INTRA_BATCH_OVERLAP_PROBABILITY",
        default_value = "0.05"
    )]
    pub record_intra_batch_overlap_probability: f32,

    /// Number of service shards (distinct service.name pods) packed into one request,
    /// simulating OTEL Collector batching across services/pods.
    /// LOGS_PER_MESSAGE and TRACES_PER_MESSAGE are each divided evenly across the shards; the
    /// shard count is clamped to the smaller of the two over the configured signals.
    #[arg(long, env = "SERVICE_SHARDS_PER_MESSAGE", default_value = "1")]
    pub service_shards_per_message: usize,

    /// Telemetry signals to generate, comma-separated (e.g. `logs`, `traces`, `logs,traces`). Order
    /// is preserved and drives the order messages are built and their `PRINT_LOGS` blocks are
    /// printed (and the per-signal statistics order); the concurrent network sends complete in an
    /// unspecified order. Duplicates are rejected. One message per signal is produced per generation
    /// cycle.
    #[arg(
        long = "signals",
        env = "OTEL_SIGNALS",
        default_value = "logs",
        value_delimiter = ',',
        value_parser = parse_signal
    )]
    pub signals: Vec<Signal>,

    /// Maximum number of tool-call spans in an LLM trace (signal=traces)
    #[arg(long, env = "LLM_MAX_TOOL_CALLS", default_value = "3")]
    pub llm_max_tool_calls: u32,

    /// Capture prompt/completion content into span attributes (PII!); signal=traces
    #[arg(long, env = "LLM_CAPTURE_CONTENT", default_value = "true", value_parser = parse_bool)]
    pub llm_capture_content: bool,

    /// Relative weights of LLM call forms; signal=traces
    #[arg(
        long,
        env = "LLM_PROFILE_WEIGHTS",
        default_value = "simple_chat:1,tool_loop:3,plan_execute_reflect:2,rag:1"
    )]
    pub llm_profile_weights: String,

    /// Lower bound of the per-trace span-count budget (signal=traces). 0 with TRACE_MAX_SPANS=0
    /// disables budgeting (natural per-form shape). Set min == max for a fixed span count.
    #[arg(long, env = "TRACE_MIN_SPANS", default_value = "0")]
    pub trace_min_spans: u32,

    /// Upper bound of the per-trace span-count budget (signal=traces). Must be >= TRACE_MIN_SPANS
    /// when enabled; capped to keep payloads bounded.
    #[arg(long, env = "TRACE_MAX_SPANS", default_value = "0")]
    pub trace_max_spans: u32,

    /// Raw vendor auth headers as a CSV map (key=value,key2=value2), applied to every request
    #[arg(long, env = "OTEL_EXPORTER_OTLP_HEADERS", default_value = "")]
    pub auth_headers: String,

    /// Bearer token shortcut -> Authorization: Bearer <token>
    #[arg(long, env = "OTEL_AUTH_BEARER")]
    pub auth_bearer: Option<String>,

    /// Basic auth shortcut: user:pass -> base64 -> Authorization: Basic <b64>
    #[arg(long, env = "OTEL_AUTH_BASIC")]
    pub auth_basic: Option<String>,
}

impl TryFrom<OtelArgs> for OtelConfig {
    type Error = GeneratorError;

    #[allow(clippy::result_large_err)]
    fn try_from(args: OtelArgs) -> std::result::Result<Self, Self::Error> {
        let tenant_id = args.tenant_id.unwrap_or_else(|| "default".to_string());
        let destination = Destination::from_flags(DestinationFlags {
            transport: args.transport.trim(),
            dry_run: args.dry_run,
            protobuf: args.use_protobuf,
            grpc_endpoint: args.grpc_endpoint.as_deref(),
            http_logs_endpoint: args.http_logs_endpoint.as_deref(),
            http_traces_endpoint: args.http_traces_endpoint.as_deref(),
            signals: &args.signals,
        })?;

        Ok(Self {
            destination,
            invalid_record_percent: args.invalid_record_percent,
            logs_per_message: args.logs_per_message,
            traces_per_message: args.traces_per_message,
            print_logs: args.print_logs || args.dry_run,
            count: args.count,
            message_interval_ms: args
                .message_interval_ms
                .or(args.delay_ms_legacy)
                .unwrap_or(0),
            concurrency: args.concurrency,
            continuous: args.continuous,
            retry_max_retries: args.retry_max_retries,
            retry_base_delay_ms: args.retry_base_delay_ms,
            retry_max_delay_ms: args.retry_max_delay_ms,
            tenant_id,
            tenant_count: args.tenant_count,
            cloud_account_count_per_tenant: args.cloud_account_count_per_tenant,
            service_count_per_tenant: args.service_count_per_tenant,
            label_cardinality_enabled: args.label_cardinality_enabled,
            label_cardinality_default_limit: args.label_cardinality_default_limit,
            label_cardinality_limits: args.label_cardinality_limits,
            record_across_batch_timestamp_jitter_ms: args.record_across_batch_timestamp_jitter_ms,
            record_intra_batch_timestamp_jitter_ns: args.record_intra_batch_timestamp_jitter_ns,
            record_intra_batch_overlap_probability: args.record_intra_batch_overlap_probability,
            service_shards_per_message: args.service_shards_per_message,
            signals: args.signals,
            llm_max_tool_calls: args.llm_max_tool_calls,
            llm_capture_content: args.llm_capture_content,
            llm_profile_weights: args.llm_profile_weights,
            trace_min_spans: args.trace_min_spans,
            trace_max_spans: args.trace_max_spans,
            auth_headers: args.auth_headers,
            auth_bearer: args.auth_bearer,
            auth_basic: args.auth_basic,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::ffi::OsString;
    use std::sync::Mutex;

    /// Serializes every test that touches process-wide env vars — including the ones that only
    /// *read* them through clap's `env` bindings.
    ///
    /// That reading happens while the `Command` is **built**, not while argv is parsed: clap's
    /// `Arg::env` snapshots `env::var_os` as the derive wires each binding up. So `Cli::command()`
    /// needs this lock exactly as much as `Cli::parse_from` does.
    static TEST_ENV_MUTEX: Mutex<()> = Mutex::new(());

    /// Take [`TEST_ENV_MUTEX`], ignoring poisoning. The mutex serializes access to the process
    /// environment rather than guarding an invariant, and [`EnvGuard`] restores every variable on
    /// unwind, so a panicking test leaves nothing behind: propagating the poison would only turn
    /// one real failure into a cascade of `PoisonError` panics in every later env test.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_ENV_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Parse an `otel` invocation with the environment held still: a parse builds the `Command` and
    /// so snapshots every `env` binding (see [`TEST_ENV_MUTEX`]), which running alongside another
    /// test's mutation would pick up. A test that already holds the lock (to install an [`EnvGuard`]) must
    /// call [`parse_otel_args_under_env_lock`] instead — the lock is not reentrant.
    fn parse_otel_args<'a>(argv: impl IntoIterator<Item = &'a str>) -> OtelArgs {
        let _lock = env_lock();
        parse_otel_args_under_env_lock(argv)
    }

    /// [`parse_otel_args`] for a caller that already holds [`TEST_ENV_MUTEX`].
    ///
    /// Every variable that steers destination resolution is unset for the duration of the parse:
    /// each test states the transport and the endpoints it cares about in `argv`, and a flag beats
    /// an env binding — but an *absent* flag does not, so an exported `OTEL_TRANSPORT` or endpoint
    /// from the developer's shell would otherwise resolve a flow the test never asked for.
    fn parse_otel_args_under_env_lock<'a>(argv: impl IntoIterator<Item = &'a str>) -> OtelArgs {
        let _destination_env = clear_destination_env();
        let cli = Cli::parse_from(argv);
        let GeneratorType::Otel(args) = cli.generator;
        args
    }

    /// [`parse_otel_args`] for an invocation clap is expected to reject. A rejected parse resolves
    /// the same `env` bindings on its way to the error, so it belongs under the lock too.
    fn try_parse_otel_args<'a>(
        argv: impl IntoIterator<Item = &'a str>,
    ) -> std::result::Result<Cli, clap::Error> {
        let _lock = env_lock();
        let _destination_env = clear_destination_env();
        Cli::try_parse_from(argv)
    }

    /// Unset every variable [`Destination::from_flags`] reads, until the returned guards drop.
    fn clear_destination_env() -> Vec<EnvGuard> {
        [
            "OTEL_TRANSPORT",
            "OTEL_GRPC_ENDPOINT",
            "OTEL_HTTP_LOGS_ENDPOINT",
            "OTEL_HTTP_TRACES_ENDPOINT",
        ]
        .into_iter()
        .map(EnvGuard::remove)
        .collect()
    }

    /// Unset every retired variable, until the returned guards drop. [`reject_retired_env_vars`]
    /// reports the first name it finds, so a test about one of them must clear the rest.
    fn clear_retired_env_vars() -> Vec<EnvGuard> {
        RETIRED_ENV_VARS
            .iter()
            .map(|(name, ..)| EnvGuard::remove(name))
            .collect()
    }

    /// Resolve args a test expects to be valid; the destination-resolution failures have their own
    /// tests.
    fn config_from(args: OtelArgs) -> OtelConfig {
        OtelConfig::try_from(args).expect("args must resolve to a config")
    }

    /// RAII guard: restores an env var to its prior value on drop.
    struct EnvGuard {
        key: &'static str,
        prior: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prior = std::env::var_os(key);
            // SAFETY: single-threaded access guaranteed by TEST_ENV_MUTEX.
            #[allow(deprecated)]
            std::env::set_var(key, value);
            Self { key, prior }
        }

        fn remove(key: &'static str) -> Self {
            let prior = std::env::var_os(key);
            // SAFETY: single-threaded access guaranteed by TEST_ENV_MUTEX.
            #[allow(deprecated)]
            std::env::remove_var(key);
            Self { key, prior }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            #[allow(deprecated)]
            match &self.prior {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn cli_accepts_new_message_interval_flag() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--message-interval-ms",
            "250",
        ]);
        let config = config_from(args);
        assert_eq!(config.message_interval_ms, 250);
    }

    #[test]
    fn cli_keeps_legacy_delay_flag_as_alias() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--delay-ms",
            "125",
        ]);
        let config = config_from(args);
        assert_eq!(config.message_interval_ms, 125);
    }

    #[test]
    fn cli_prefers_new_message_interval_over_legacy_alias() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--delay-ms",
            "125",
            "--message-interval-ms",
            "250",
        ]);
        let config = config_from(args);
        assert_eq!(config.message_interval_ms, 250);
    }

    #[test]
    fn cli_reads_tenant_count_and_tenant_id() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--tenant-id",
            "tenant_custom",
            "--tenant-count",
            "3",
        ]);
        let config = config_from(args);
        assert_eq!(config.tenant_id, "tenant_custom");
        assert_eq!(config.tenant_count, 3);
    }

    #[test]
    fn cli_reads_tenant_profile_pool_sizes() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--cloud-account-count-per-tenant",
            "5",
            "--service-count-per-tenant",
            "7",
        ]);
        let config = config_from(args);
        assert_eq!(config.cloud_account_count_per_tenant, 5);
        assert_eq!(config.service_count_per_tenant, 7);
    }

    #[test]
    fn cli_reads_intra_batch_jitter_options() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--record-intra-batch-timestamp-jitter-ns",
            "10",
            "--record-intra-batch-overlap-probability",
            "0.2",
        ]);
        let config = config_from(args);
        assert_eq!(config.record_intra_batch_timestamp_jitter_ns, 10);
        assert!((config.record_intra_batch_overlap_probability - 0.2).abs() < 1e-6);
    }

    #[test]
    fn cli_accepts_dry_run_without_endpoint() {
        // The absence of an endpoint is the condition under test; `parse_otel_args` unsets the
        // endpoint variables so an inherited one cannot supply it behind clap's back.
        let args = parse_otel_args(["otel-log-generator", "otel", "--dry-run"]);
        assert!(args.dry_run);
        assert!(args.http_logs_endpoint.is_none());
        let config = config_from(args);
        assert!(config.is_dry_run());
        assert!(config.print_logs);
    }

    #[test]
    fn cli_service_shards_per_message_default_env_and_flag_precedence() {
        // Serializes against any other test touching SERVICE_SHARDS_PER_MESSAGE.
        let _lock = env_lock();

        // 1. No flag, no env -> default_value = "1".
        let _guard = EnvGuard::remove("SERVICE_SHARDS_PER_MESSAGE");
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        let config = config_from(args);
        assert_eq!(
            config.service_shards_per_message, 1,
            "default when no flag and no env"
        );

        // 2. Env SERVICE_SHARDS_PER_MESSAGE=2 is read via clap's `env = "..."` binding.
        let _guard = EnvGuard::set("SERVICE_SHARDS_PER_MESSAGE", "2");
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        let config = config_from(args);
        assert_eq!(
            config.service_shards_per_message, 2,
            "env binding must be honoured"
        );

        // 3. --service-shards-per-message flag overrides the env value.
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--service-shards-per-message",
            "5",
        ]);
        let config = config_from(args);
        assert_eq!(config.service_shards_per_message, 5, "flag must beat env");
    }

    #[test]
    fn cli_reads_service_shards_per_message() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--service-shards-per-message",
            "3",
        ]);
        let config = config_from(args);
        assert_eq!(config.service_shards_per_message, 3);
    }

    #[test]
    fn cli_parses_single_signal_traces() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
            "--signals",
            "traces",
        ]);
        let config = config_from(args);
        assert_eq!(config.signals, vec![Signal::Traces]);
    }

    #[test]
    fn cli_signals_default_to_logs() {
        // Serialize against the OTEL_SIGNALS env test and clear the var so the default applies.
        let _lock = env_lock();
        let _guard = EnvGuard::remove("OTEL_SIGNALS");
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        let config = config_from(args);
        assert_eq!(config.signals, vec![Signal::Logs]);
    }

    #[test]
    fn cli_parses_multi_signal_list_preserving_order() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
            "--signals",
            "logs,traces",
        ]);
        let config = config_from(args);
        assert_eq!(config.signals, vec![Signal::Logs, Signal::Traces]);
    }

    #[test]
    fn cli_preserves_reverse_signal_order() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
            "--signals",
            "traces,logs",
        ]);
        let config = config_from(args);
        assert_eq!(config.signals, vec![Signal::Traces, Signal::Logs]);
    }

    #[test]
    fn cli_reads_signals_from_env() {
        let _lock = env_lock();
        let _guard = EnvGuard::set("OTEL_SIGNALS", "logs,traces");
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
        ]);
        let config = config_from(args);
        assert_eq!(config.signals, vec![Signal::Logs, Signal::Traces]);
    }

    #[test]
    fn cli_rejects_unknown_signal() {
        let result = try_parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--signals",
            "metrics",
        ]);
        assert!(result.is_err(), "unknown signal must be rejected");
    }

    #[test]
    fn cli_rejects_legacy_signal_flag() {
        let result = try_parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--signal",
            "traces",
        ]);
        assert!(result.is_err(), "old --signal flag must no longer parse");
    }

    #[test]
    fn env_rejects_legacy_signal_var() {
        // The CLI half of this migration is covered by `cli_rejects_legacy_signal_flag`; clap
        // ignores an unbound env var instead, so the rename needs its own guard.
        let _lock = env_lock();
        let _retired = clear_retired_env_vars();
        let _guard = EnvGuard::set("OTEL_SIGNAL", "traces");
        assert!(matches!(
            reject_retired_env_vars(),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn env_accepts_current_signals_var() {
        let _lock = env_lock();
        let _retired = clear_retired_env_vars();
        let _current = EnvGuard::set("OTEL_SIGNALS", "logs,traces");
        assert!(reject_retired_env_vars().is_ok());
    }

    #[test]
    fn retired_endpoint_env_vars_fail_the_run() {
        let _lock = env_lock();
        // Only the variable under test may be set: the check reports the first retired name it
        // finds, so an inherited one would make every iteration pass without ever reaching it.
        let _retired = clear_retired_env_vars();
        for name in [
            "OTEL_LOGS_ENDPOINT",
            "OTEL_TRACES_ENDPOINT",
            "OTEL_HEALTHCHECK_ENDPOINT",
        ] {
            let _guard = EnvGuard::set(name, "http://localhost:4318/v1/logs");
            assert!(
                matches!(
                    reject_retired_env_vars(),
                    Err(GeneratorError::InvalidConfiguration(_))
                ),
                "{name} must be rejected at startup"
            );
        }
    }

    #[test]
    fn env_rejects_legacy_records_per_message_var() {
        // RECORDS_PER_MESSAGE became LOGS_PER_MESSAGE when traces got their own budget; a stale
        // `.env` would otherwise silently fall back to the default of one record per message.
        let _lock = env_lock();
        let _retired = clear_retired_env_vars();
        let _guard = EnvGuard::set("RECORDS_PER_MESSAGE", "10");
        assert!(matches!(
            reject_retired_env_vars(),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn cli_reads_logs_and_traces_per_message() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--logs-per-message",
            "30",
            "--traces-per-message",
            "12",
        ]);
        let config = config_from(args);
        assert_eq!(config.logs_per_message, 30);
        assert_eq!(config.traces_per_message, 12);
    }

    #[test]
    fn cli_http_flow_reads_both_signal_endpoints() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--signals",
            "logs,traces",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
        ]);
        let config = config_from(args);
        let Destination::Http { endpoints, .. } = &config.destination else {
            panic!("expected the HTTP flow");
        };
        assert_eq!(
            endpoints.get(&Signal::Traces).map(String::as_str),
            Some("http://localhost:4318/v1/traces")
        );
    }

    /// Regression: one `.env` carrying the HTTP URLs must not break a gRPC run.
    #[test]
    fn cli_grpc_flow_resolves_with_http_endpoint_flags_present() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--transport",
            "grpc",
            "--signals",
            "logs,traces",
            "--grpc-endpoint",
            "http://localhost:4317",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
        ]);
        let config = config_from(args);
        assert!(matches!(
            &config.destination,
            Destination::Grpc { endpoint } if endpoint == "http://localhost:4317"
        ));
    }

    #[test]
    fn cli_http_flow_without_traces_endpoint_fails_to_build_a_config() {
        // The absence of the traces URL is the condition under test; `parse_otel_args` unsets the
        // endpoint variables so an inherited environment cannot supply one behind clap's back.
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--signals",
            "logs,traces",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        assert!(matches!(
            OtelConfig::try_from(args),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn cli_reads_llm_trace_knobs() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
            "--signals",
            "traces",
            "--llm-max-tool-calls",
            "5",
            "--llm-capture-content",
            "--llm-profile-weights",
            "simple_chat:2,rag:1",
        ]);
        let config = config_from(args);
        assert_eq!(config.llm_max_tool_calls, 5);
        assert!(config.llm_capture_content);
        assert_eq!(config.llm_profile_weights, "simple_chat:2,rag:1");
    }

    #[test]
    fn cli_reads_auth_flags() {
        // Each half asserts that the shortcut it did *not* pass stays unset, so an exported
        // credential must not reach clap's `env` bindings.
        let _lock = env_lock();
        let _bearer = EnvGuard::remove("OTEL_AUTH_BEARER");
        let _basic = EnvGuard::remove("OTEL_AUTH_BASIC");
        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--auth-headers",
            "x-api-key=secret,x-bt-parent=project:foo",
            "--auth-bearer",
            "token123",
        ]);
        let config = config_from(args);
        assert_eq!(
            config.auth_headers,
            "x-api-key=secret,x-bt-parent=project:foo"
        );
        assert_eq!(config.auth_bearer.as_deref(), Some("token123"));
        assert_eq!(config.auth_basic, None);

        let args = parse_otel_args_under_env_lock([
            "otel-log-generator",
            "otel",
            "--http-logs-endpoint",
            "http://localhost:4318/v1/logs",
            "--auth-basic",
            "user:pass",
        ]);
        let config = config_from(args);
        assert_eq!(config.auth_basic.as_deref(), Some("user:pass"));
        assert_eq!(config.auth_bearer, None);
    }

    #[test]
    fn cli_reads_trace_span_budget() {
        let args = parse_otel_args([
            "otel-log-generator",
            "otel",
            "--http-traces-endpoint",
            "http://localhost:4318/v1/traces",
            "--signals",
            "traces",
            "--trace-min-spans",
            "10",
            "--trace-max-spans",
            "50",
        ]);
        let config = config_from(args);
        assert_eq!(config.trace_min_spans, 10);
        assert_eq!(config.trace_max_spans, 50);
    }

    #[test]
    fn retirement_replacements_are_bound_in_clap() {
        // Every replacement a retirement points at must be a live clap `env` binding on `OtelArgs`.
        // Without this, a retirement could reject the old name while nothing reads the name it
        // recommends — a silent config break. Walk the actual clap bindings so the invariant tracks
        // the declaration, not a hand-kept copy. `OTEL_HEALTHCHECK_ENDPOINT` names no replacement.
        let _lock = env_lock();
        let mut command = Cli::command();
        let otel = command.find_subcommand_mut("otel").unwrap();
        let bound: std::collections::HashSet<String> = otel
            .get_arguments()
            .filter_map(|arg| arg.get_env())
            .map(|env| env.to_string_lossy().into_owned())
            .collect();

        for (retired, replacements, _guidance) in RETIRED_ENV_VARS {
            for name in *replacements {
                assert!(
                    bound.contains(*name),
                    "{name}, the replacement for {retired}, is not bound to any otel arg via clap `env`"
                );
            }
        }
    }

    #[test]
    fn cli_help_documents_tenant_routing_inputs() {
        let _lock = env_lock();
        let mut command = Cli::command();
        let otel = command.find_subcommand_mut("otel").unwrap();
        let mut help = Vec::new();
        otel.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("--tenant-id"));
        assert!(help.contains("TENANT_ID"));
        assert!(help.contains("--tenant-count"));
        assert!(help.contains("TENANT_COUNT"));
        assert!(help.contains("--cloud-account-count-per-tenant"));
        assert!(help.contains("CLOUD_ACCOUNT_COUNT_PER_TENANT"));
        assert!(help.contains("--service-count-per-tenant"));
        assert!(help.contains("SERVICE_COUNT_PER_TENANT"));
    }
}
