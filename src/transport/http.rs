use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::transport::Transport;
use async_trait::async_trait;
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

}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()> {
        let max_retries = self.retry_config.max_retries;
        let mut last_error: Option<GeneratorError> = None;

        for attempt in 0..=max_retries {
            let request = self.build_request(message);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) if attempt < max_retries && is_transient_reqwest_error(&e) => {
                    let delay = self.retry_config.compute_delay(attempt, None);
                    eprintln!(
                        "  \u{26a0} Transient network error (attempt {}/{}): {}",
                        attempt + 1,
                        max_retries + 1,
                        e
                    );
                    eprintln!("  Waiting {}ms before retry...", delay);
                    last_error = Some(GeneratorError::RequestError(e));
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                Err(e) => return Err(GeneratorError::RequestError(e)),
            };

            let status = response.status();

            if status.is_success() {
                if attempt > 0 {
                    eprintln!("  \u{2713} Request succeeded after {} retries", attempt);
                }
                return Ok(());
            }

            if status.as_u16() == 429 {
                if attempt == max_retries {
                    return Err(GeneratorError::RateLimitExceeded(max_retries));
                }

                // Parse Retry-After header (integer seconds)
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());

                let delay = self.retry_config.compute_delay(attempt, retry_after);

                eprintln!(
                    "  \u{26a0} HTTP 429 Too Many Requests (attempt {}/{})",
                    attempt + 1,
                    max_retries + 1
                );
                eprintln!("  Waiting {}ms before retry...", delay);

                sleep(Duration::from_millis(delay)).await;
                continue;
            }

            // Non-retryable HTTP error: fail immediately
            let error_text = response.text().await.unwrap_or_default();
            return Err(GeneratorError::HttpError(status.as_u16(), error_text));
        }

        // Retries exhausted — return the last network error if that's what consumed them,
        // otherwise it was all 429s
        match last_error {
            Some(e) => Err(e),
            None => Err(GeneratorError::RateLimitExceeded(max_retries)),
        }
    }
}

fn is_transient_reqwest_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}
