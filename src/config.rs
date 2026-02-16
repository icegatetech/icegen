use crate::error::{GeneratorError, Result};
use rand::Rng;

const MAX_RETRIES_UPPER_BOUND: u32 = 10;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl RetryConfig {
    /// Create a new RetryConfig, validating that `max_retries` does not exceed the safe upper bound.
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

#[derive(Debug, Clone)]
pub struct OtelConfig {
    pub ingest_endpoint: String,
    pub healthcheck_endpoint: Option<String>,
    pub use_protobuf: bool,
    pub transport: String,
    pub invalid_record_percent: f32,
    pub records_per_message: usize,
    pub print_logs: bool,
    pub count: usize,
    pub delay_ms: u64,
    pub continuous: bool,
    pub retry_max_retries: u32,
    pub retry_base_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    pub org_id: String,
}

impl OtelConfig {
    pub fn validate(&self) -> Result<()> {
        if self.invalid_record_percent < 0.0 || self.invalid_record_percent > 100.0 {
            return Err(GeneratorError::InvalidConfiguration(
                "invalid_record_percent must be between 0 and 100".to_string(),
            ));
        }

        if self.records_per_message < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "records_per_message must be >= 1".to_string(),
            ));
        }

        if self.transport != "http" && self.transport != "grpc" {
            return Err(GeneratorError::InvalidTransport(format!(
                "Invalid transport '{}', must be 'http' or 'grpc'",
                self.transport
            )));
        }

        if self.retry_max_retries > MAX_RETRIES_UPPER_BOUND {
            return Err(GeneratorError::InvalidConfiguration(
                format!("retry_max_retries must be <= {}", MAX_RETRIES_UPPER_BOUND),
            ));
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

        Ok(())
    }

    pub fn retry_config(&self) -> Result<RetryConfig> {
        RetryConfig::new(
            self.retry_max_retries,
            self.retry_base_delay_ms,
            self.retry_max_delay_ms,
        )
    }
}

#[derive(Debug, Clone)]
pub struct BatchResult {
    pub total: usize,
    pub success: usize,
    pub failed: usize,
}

impl BatchResult {
    pub fn new() -> Self {
        Self {
            total: 0,
            success: 0,
            failed: 0,
        }
    }

    pub fn add_success(&mut self) {
        self.total += 1;
        self.success += 1;
    }

    pub fn add_failure(&mut self) {
        self.total += 1;
        self.failed += 1;
    }
}

impl Default for BatchResult {
    fn default() -> Self {
        Self::new()
    }
}
