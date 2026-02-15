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

    /// Number of messages to send
    #[arg(long, env = "MESSAGE_COUNT", default_value = "1")]
    pub count: usize,

    /// Delay between messages in milliseconds
    #[arg(long, env = "MESSAGE_DELAY", default_value = "0")]
    pub delay_ms: u64,

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

    /// Maximum number of retry attempts on rate limiting (0-10)
    #[arg(long, env = "RETRY_MAX_ATTEMPTS", default_value = "3")]
    pub retry_max_attempts: u32,

    /// Base delay in milliseconds for retry backoff
    #[arg(long, env = "RETRY_BASE_DELAY_MS", default_value = "1000")]
    pub retry_base_delay_ms: u64,

    /// Maximum delay in milliseconds for retry backoff
    #[arg(long, env = "RETRY_MAX_DELAY_MS", default_value = "32000")]
    pub retry_max_delay_ms: u64,
}

impl From<OtelArgs> for OtelConfig {
    fn from(args: OtelArgs) -> Self {
        Self {
            ingest_endpoint: args.endpoint,
            healthcheck_endpoint: args.healthcheck_endpoint,
            use_protobuf: args.use_protobuf,
            transport: args.transport,
            invalid_record_percent: args.invalid_record_percent,
            records_per_message: args.records_per_message,
            print_logs: args.print_logs,
            count: args.count,
            delay_ms: args.delay_ms,
            continuous: args.continuous,
            retry_max_attempts: args.retry_max_attempts,
            retry_base_delay_ms: args.retry_base_delay_ms,
            retry_max_delay_ms: args.retry_max_delay_ms,
        }
    }
}
