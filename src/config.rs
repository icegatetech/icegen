use crate::error::{GeneratorError, Result};

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

        Ok(())
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
