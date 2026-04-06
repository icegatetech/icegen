use crate::error::{GeneratorError, Result};
use rand::Rng;
use std::collections::HashMap;

const MAX_RETRIES_UPPER_BOUND: u32 = 10;
const DEFAULT_CARDINALITY_LIMITS: &[(&str, usize)] = &[
    ("k8s.pod.name", 32),
    ("host.name", 16),
    ("service.version", 32),
    ("request.id", 64),
    ("thread.id", 32),
    ("user.id", 64),
];

#[derive(Debug, Clone)]
pub struct LabelCardinalityConfig {
    pub enabled: bool,
    pub default_limit: Option<usize>,
    pub limits: HashMap<String, usize>,
}

impl LabelCardinalityConfig {
    pub fn limit_for(&self, key: &str) -> Option<usize> {
        self.limits
            .get(key)
            .copied()
            .or(self.default_limit)
            .filter(|limit| *limit >= 1)
    }
}

impl Default for LabelCardinalityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_limit: None,
            limits: default_cardinality_limits(),
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
    pub enable_logs: bool,
    pub enable_metrics: bool,
}

impl OtelConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enable_logs && !self.enable_metrics {
            return Err(GeneratorError::InvalidConfiguration(
                "at least one signal must be enabled (--enable-logs or --enable-metrics)".to_string(),
            ));
        }

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

        if self.concurrency < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "concurrency must be >= 1".to_string(),
            ));
        }

        if self.transport != "http" && self.transport != "grpc" {
            return Err(GeneratorError::InvalidTransport(format!(
                "Invalid transport '{}', must be 'http' or 'grpc'",
                self.transport
            )));
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

        if self.tenant_count < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "tenant_count must be >= 1".to_string(),
            ));
        }

        if self.cloud_account_count_per_tenant < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "cloud_account_count_per_tenant must be >= 1".to_string(),
            ));
        }

        if self.service_count_per_tenant < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "service_count_per_tenant must be >= 1".to_string(),
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

        Ok(())
    }

    pub fn retry_config(&self) -> Result<RetryConfig> {
        RetryConfig::new(
            self.retry_max_retries,
            self.retry_base_delay_ms,
            self.retry_max_delay_ms,
        )
    }

    pub fn label_cardinality_config(&self) -> Result<LabelCardinalityConfig> {
        let mut limits = default_cardinality_limits();
        let custom_limits = parse_cardinality_limits(&self.label_cardinality_limits)?;
        limits.extend(custom_limits);

        Ok(LabelCardinalityConfig {
            enabled: self.label_cardinality_enabled,
            default_limit: self.label_cardinality_default_limit,
            limits,
        })
    }
}

fn default_cardinality_limits() -> HashMap<String, usize> {
    DEFAULT_CARDINALITY_LIMITS
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

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

    pub fn merge(&mut self, other: Self) {
        self.total += other.total;
        self.success += other.success;
        self.failed += other.failed;
    }
}

impl Default for BatchResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> OtelConfig {
        OtelConfig {
            ingest_endpoint: "http://localhost:4318/v1/logs".to_string(),
            healthcheck_endpoint: None,
            use_protobuf: false,
            transport: "http".to_string(),
            invalid_record_percent: 0.0,
            records_per_message: 1,
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
            enable_logs: true,
            enable_metrics: false,
        }
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
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_cloud_account_count_per_tenant_validation() {
        let mut cfg = base_config();
        cfg.cloud_account_count_per_tenant = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_service_count_per_tenant_validation() {
        let mut cfg = base_config();
        cfg.service_count_per_tenant = 0;
        assert!(cfg.validate().is_err());
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
    fn test_at_least_one_signal_must_be_enabled() {
        let mut cfg = base_config();
        cfg.enable_logs = false;
        cfg.enable_metrics = false;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_logs_only_is_valid() {
        let mut cfg = base_config();
        cfg.enable_logs = true;
        cfg.enable_metrics = false;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_metrics_only_is_valid() {
        let mut cfg = base_config();
        cfg.enable_logs = false;
        cfg.enable_metrics = true;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_both_signals_is_valid() {
        let mut cfg = base_config();
        cfg.enable_logs = true;
        cfg.enable_metrics = true;
        assert!(cfg.validate().is_ok());
    }
}
