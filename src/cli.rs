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
    /// OTEL logs ingest endpoint
    #[arg(long, env = "OTEL_LOGS_ENDPOINT")]
    pub endpoint: String,

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

    /// Tenant ID for X-Scope-OrgID in single-tenant mode; preferred over deprecated ORG_ID
    #[arg(long, env = "TENANT_ID")]
    pub tenant_id: Option<String>,

    /// Number of tenants for random routing; when > 1 uses tenant1..tenantN and ignores TENANT_ID / ORG_ID
    #[arg(long, env = "TENANT_COUNT", default_value = "1")]
    pub tenant_count: usize,

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
}

impl From<OtelArgs> for OtelConfig {
    fn from(args: OtelArgs) -> Self {
        let tenant_id = args
            .tenant_id
            .unwrap_or_else(|| "default".to_string());

        Self {
            ingest_endpoint: args.endpoint,
            healthcheck_endpoint: args.healthcheck_endpoint,
            use_protobuf: args.use_protobuf,
            transport: args.transport,
            invalid_record_percent: args.invalid_record_percent,
            records_per_message: args.records_per_message,
            print_logs: args.print_logs,
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
            label_cardinality_enabled: args.label_cardinality_enabled,
            label_cardinality_default_limit: args.label_cardinality_default_limit,
            label_cardinality_limits: args.label_cardinality_limits,
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
    fn cli_reads_legacy_org_id_when_tenant_id_is_missing() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--org-id",
            "legacy_tenant",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.tenant_id, "legacy_tenant");
    }

    #[test]
    fn cli_prefers_tenant_id_over_legacy_org_id() {
        let cli = Cli::parse_from([
            "otel-log-generator",
            "otel",
            "--endpoint",
            "http://localhost:4318/v1/logs",
            "--org-id",
            "legacy_tenant",
            "--tenant-id",
            "preferred_tenant",
        ]);

        let GeneratorType::Otel(args) = cli.generator;
        let config: OtelConfig = args.into();
        assert_eq!(config.tenant_id, "preferred_tenant");
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
    }
}
