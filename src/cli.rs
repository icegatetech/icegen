use crate::config::OtelConfig;
use clap::{Args, Parser, Subcommand};

/// Parse boolean values in a case-insensitive way
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" | "" => Ok(false),
        _ => Err(format!("invalid boolean value: '{}'", s)),
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
    /// Set to 0 to omit service.name from resource attributes; scope.name uses default 'generator'.
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
        env = "RECORD_ACCROSS_BATCH_TIMESTAMP_JITTER_MS",
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

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
