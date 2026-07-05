use crate::config::OtelConfig;
use crate::message::types::Signal;
use clap::{Args, Parser, Subcommand};

/// Parse boolean values in a case-insensitive way
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" | "" => Ok(false),
        _ => Err(format!("invalid boolean value: '{}'", s)),
    }
}

/// Parse the telemetry signal selector in a case-insensitive way.
fn parse_signal(s: &str) -> Result<Signal, String> {
    match s.to_lowercase().as_str() {
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
    /// OTEL logs ingest endpoint (not required with --dry-run)
    #[arg(long, env = "OTEL_LOGS_ENDPOINT")]
    pub endpoint: Option<String>,

    /// Health check endpoint (optional)
    #[arg(long, env = "OTEL_HEALTHCHECK_ENDPOINT")]
    pub healthcheck_endpoint: Option<String>,

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

    /// Number of records per message
    #[arg(long, env = "RECORDS_PER_MESSAGE", default_value = "1")]
    pub records_per_message: usize,

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

    /// Number of ResourceLogs groups (distinct service.name pods) packed into one request,
    /// simulating OTEL Collector batching across services/pods.
    /// RECORDS_PER_MESSAGE is divided evenly across groups (clamped to <= RECORDS_PER_MESSAGE).
    #[arg(long, env = "SERVICE_SHARDS_PER_MESSAGE", default_value = "1")]
    pub service_shards_per_message: usize,

    /// Telemetry signal to generate: logs or traces (one signal per run)
    #[arg(long, env = "OTEL_SIGNAL", default_value = "logs", value_parser = parse_signal)]
    pub signal: Signal,

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

impl From<OtelArgs> for OtelConfig {
    fn from(args: OtelArgs) -> Self {
        let tenant_id = args.tenant_id.unwrap_or_else(|| "default".to_string());

        Self {
            ingest_endpoint: args.endpoint.unwrap_or_default(),
            healthcheck_endpoint: args.healthcheck_endpoint,
            use_protobuf: args.use_protobuf,
            transport: args.transport,
            invalid_record_percent: args.invalid_record_percent,
            records_per_message: args.records_per_message,
            print_logs: args.print_logs || args.dry_run,
            dry_run: args.dry_run,
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
            signal: args.signal,
            llm_max_tool_calls: args.llm_max_tool_calls,
            llm_capture_content: args.llm_capture_content,
            llm_profile_weights: args.llm_profile_weights,
            trace_min_spans: args.trace_min_spans,
            trace_max_spans: args.trace_max_spans,
            auth_headers: args.auth_headers,
            auth_bearer: args.auth_bearer,
            auth_basic: args.auth_basic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::ffi::OsString;
    use std::sync::Mutex;

    /// Serializes all tests that touch process-wide env vars.
    static TEST_ENV_MUTEX: Mutex<()> = Mutex::new(());

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
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--message-interval-ms",
            "250",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.message_interval_ms, 250);
    }

    #[test]
    fn cli_keeps_legacy_delay_flag_as_alias() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--delay-ms",
            "125",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.message_interval_ms, 125);
    }

    #[test]
    fn cli_prefers_new_message_interval_over_legacy_alias() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--delay-ms",
            "125",
            "--message-interval-ms",
            "250",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.message_interval_ms, 250);
    }

    #[test]
    fn cli_reads_tenant_count_and_tenant_id() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--tenant-id",
            "tenant_custom",
            "--tenant-count",
            "3",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.tenant_id, "tenant_custom");
        assert_eq!(config.tenant_count, 3);
    }

    #[test]
    fn cli_reads_tenant_profile_pool_sizes() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--cloud-account-count-per-tenant",
            "5",
            "--service-count-per-tenant",
            "7",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.cloud_account_count_per_tenant, 5);
        assert_eq!(config.service_count_per_tenant, 7);
    }

    #[test]
    fn cli_reads_intra_batch_jitter_options() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--record-intra-batch-timestamp-jitter-ns",
            "10",
            "--record-intra-batch-overlap-probability",
            "0.2",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.record_intra_batch_timestamp_jitter_ns, 10);
        assert!((config.record_intra_batch_overlap_probability - 0.2).abs() < 1e-6);
    }

    #[test]
    fn cli_accepts_dry_run_without_endpoint() {
        let cli = Cli::parse_from(["otel-log-generator", "otel", "--dry-run"]);
        let GeneratorType::Otel(args) = cli.generator;
        assert!(args.dry_run);
        assert!(args.endpoint.is_none());
        let config: OtelConfig = args.into();
        assert!(config.dry_run);
        assert!(config.ingest_endpoint.is_empty());
        assert!(config.print_logs);
    }

    #[test]
    fn cli_service_shards_per_message_default_env_and_flag_precedence() {
        // Serializes against any other test touching SERVICE_SHARDS_PER_MESSAGE.
        let _lock = TEST_ENV_MUTEX.lock().unwrap();

        // 1. No flag, no env -> default_value = "1".
        let _guard = EnvGuard::remove("SERVICE_SHARDS_PER_MESSAGE");
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(
            config.service_shards_per_message, 1,
            "default when no flag and no env"
        );

        // 2. Env SERVICE_SHARDS_PER_MESSAGE=2 is read via clap's `env = "..."` binding.
        let _guard = EnvGuard::set("SERVICE_SHARDS_PER_MESSAGE", "2");
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
        ]);
        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(
            config.service_shards_per_message, 2,
            "env binding must be honoured"
        );

        // 3. --service-shards-per-message flag overrides the env value.
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--service-shards-per-message",
            "5",
        ]);
        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.service_shards_per_message, 5, "flag must beat env");
    }

    #[test]
    fn cli_reads_service_shards_per_message() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--service-shards-per-message",
            "3",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.service_shards_per_message, 3);
    }

    #[test]
    fn cli_parses_signal_traces() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/traces",
            "--signal",
            "traces",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.signal, Signal::Traces);
    }

    #[test]
    fn cli_signal_defaults_to_logs() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.signal, Signal::Logs);
    }

    #[test]
    fn cli_reads_llm_trace_knobs() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/traces",
            "--signal",
            "traces",
            "--llm-max-tool-calls",
            "5",
            "--llm-capture-content",
            "--llm-profile-weights",
            "simple_chat:2,rag:1",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.llm_max_tool_calls, 5);
        assert!(config.llm_capture_content);
        assert_eq!(config.llm_profile_weights, "simple_chat:2,rag:1");
    }

    #[test]
    fn cli_reads_auth_flags() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--auth-headers",
            "x-api-key=secret,x-bt-parent=project:foo",
            "--auth-bearer",
            "token123",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(
            config.auth_headers,
            "x-api-key=secret,x-bt-parent=project:foo"
        );
        assert_eq!(config.auth_bearer.as_deref(), Some("token123"));
        assert_eq!(config.auth_basic, None);

        let cli = Cli::parse_from(["otel-log-generator", "otel", "--auth-basic", "user:pass"]);
        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.auth_basic.as_deref(), Some("user:pass"));
        assert_eq!(config.auth_bearer, None);
    }

    #[test]
    fn cli_reads_trace_span_budget() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/traces",
            "--signal",
            "traces",
            "--trace-min-spans",
            "10",
            "--trace-max-spans",
            "50",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.trace_min_spans, 10);
        assert_eq!(config.trace_max_spans, 50);
    }

    #[test]
    fn cli_help_documents_tenant_routing_inputs() {
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
