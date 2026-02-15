use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::transport::Transport;
use async_trait::async_trait;
use rand::Rng;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

pub struct HttpTransport {
    client: Client,
    endpoint: String,
    #[allow(dead_code)]
    use_protobuf: bool,
    retry_config: RetryConfig,
}

impl HttpTransport {
    pub fn new(endpoint: String, use_protobuf: bool, retry_config: RetryConfig) -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

        Ok(Self {
            client,
            endpoint,
            use_protobuf,
            retry_config,
        })
    }

    pub async fn health_check(&self, health_endpoint: &str) -> Result<()> {
        let response = self
            .client
            .get(health_endpoint)
            .timeout(Duration::from_secs(3))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(GeneratorError::HealthCheckFailed(
                response.status().as_u16(),
            ));
        }

        Ok(())
    }

    fn build_request(&self, message: &OTLPLogMessage) -> reqwest::RequestBuilder {
        match &message.message {
            MessagePayload::Json(json_value) => self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/json")
                .header("User-Agent", "trihub-log-generator/1.0")
                .header("X-Scope-OrgID", "tenant1")
                .json(json_value),
            MessagePayload::Protobuf(bytes) => self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/x-protobuf")
                .header("User-Agent", "trihub-log-generator/1.0")
                .header("X-Scope-OrgID", "tenant1")
                .body(bytes.clone()),
            MessagePayload::MalformedJson(malformed_string) => self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/json")
                .header("User-Agent", "trihub-log-generator/1.0")
                .body(malformed_string.clone()),
        }
    }

    fn compute_delay(&self, attempt: u32, retry_after: Option<u64>) -> u64 {
        let base = if let Some(retry_after_secs) = retry_after {
            let retry_after_ms = retry_after_secs * 1000;
            retry_after_ms.min(self.retry_config.max_delay_ms)
        } else {
            let exponential = self.retry_config.base_delay_ms.saturating_mul(1u64 << attempt);
            exponential.min(self.retry_config.max_delay_ms)
        };

        // Apply ±25% jitter
        let jitter_range = base / 4;
        if jitter_range > 0 {
            let jitter = rand::thread_rng().gen_range(0..=jitter_range * 2);
            base.saturating_sub(jitter_range).saturating_add(jitter)
        } else {
            base
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()> {
        let max_attempts = self.retry_config.max_attempts;

        for attempt in 0..=max_attempts {
            let request = self.build_request(message);
            let response = request.send().await?;
            let status = response.status();

            if status.is_success() {
                if attempt > 0 {
                    eprintln!("  \u{2713} Request succeeded after {} retries", attempt);
                }
                return Ok(());
            }

            if status.as_u16() == 429 {
                if attempt == max_attempts {
                    return Err(GeneratorError::RateLimitExceeded(max_attempts));
                }

                // Parse Retry-After header (integer seconds)
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());

                let delay = self.compute_delay(attempt, retry_after);

                eprintln!(
                    "  \u{26a0} HTTP 429 Too Many Requests (attempt {}/{})",
                    attempt + 1,
                    max_attempts + 1
                );
                eprintln!("  Waiting {}ms before retry...", delay);

                sleep(Duration::from_millis(delay)).await;
                continue;
            }

            // Non-429 error: fail immediately
            let error_text = response.text().await.unwrap_or_default();
            return Err(GeneratorError::HttpError(status.as_u16(), error_text));
        }

        Err(GeneratorError::RateLimitExceeded(max_attempts))
    }
}
