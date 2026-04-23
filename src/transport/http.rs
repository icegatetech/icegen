use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::transport::{SendOutcome, Transport};
use async_trait::async_trait;
use reqwest::Client;
use std::time::Duration;
use tokio::sync::watch;
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
            MessagePayload::Json(json_value) => {
                let mut req = self
                    .client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "trihub-log-generator/1.0");
                if let Some(tid) = &message.tenant_id {
                    req = req.header("X-Scope-OrgID", tid);
                }
                req.json(json_value)
            }
            MessagePayload::Protobuf(bytes) => {
                let mut req = self
                    .client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/x-protobuf")
                    .header("User-Agent", "trihub-log-generator/1.0");
                if let Some(tid) = &message.tenant_id {
                    req = req.header("X-Scope-OrgID", tid);
                }
                req.body(bytes.clone())
            }
            MessagePayload::MalformedJson(malformed_string) => {
                let mut req = self
                    .client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "trihub-log-generator/1.0");
                if let Some(tid) = &message.tenant_id {
                    req = req.header("X-Scope-OrgID", tid);
                }
                req.body(malformed_string.clone())
            }
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        let max_retries = self.retry_config.max_retries;
        let mut shutdown_rx = shutdown_rx.clone();

        for attempt in 0..=max_retries {
            let request = self.build_request(message);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) if attempt < max_retries && is_transient_reqwest_error(&e) => {
                    let delay = self.retry_config.compute_delay(attempt, None);
                    eprintln!(
                        "  \u{26a0} Retry[http]: transient network error, attempt {}/{}, waiting {}ms, error: {}",
                        attempt + 1,
                        max_retries + 1,
                        delay,
                        e
                    );
                    if *shutdown_rx.borrow() {
                        return SendOutcome::Failure {
                            retries: attempt as usize,
                            error: GeneratorError::Interrupted,
                        };
                    }
                    tokio::select! {
                        _ = sleep(Duration::from_millis(delay)) => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_ok() && *shutdown_rx.borrow() {
                                return SendOutcome::Failure {
                                    retries: attempt as usize,
                                    error: GeneratorError::Interrupted,
                                };
                            }
                        }
                    }
                    continue;
                }
                Err(e) => {
                    return SendOutcome::Failure {
                        retries: attempt as usize,
                        error: GeneratorError::RequestError(e),
                    };
                }
            };

            let status = response.status();

            if status.is_success() {
                if attempt > 0 {
                    eprintln!("  \u{2713} Retry[http]: request succeeded after {} retries", attempt);
                }
                return SendOutcome::Success { retries: attempt as usize };
            }

            if status.as_u16() == 429 {
                if attempt == max_retries {
                    return SendOutcome::Failure {
                        retries: attempt as usize,
                        error: GeneratorError::RateLimitExceeded(attempt),
                    };
                }

                // Parse Retry-After header (integer seconds)
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());

                let delay = self.retry_config.compute_delay(attempt, retry_after);

                eprintln!(
                    "  \u{26a0} Retry[http]: 429 Too Many Requests, attempt {}/{}, waiting {}ms",
                    attempt + 1,
                    max_retries + 1,
                    delay
                );

                if *shutdown_rx.borrow() {
                    return SendOutcome::Failure {
                        retries: attempt as usize,
                        error: GeneratorError::Interrupted,
                    };
                }
                tokio::select! {
                    _ = sleep(Duration::from_millis(delay)) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            return SendOutcome::Failure {
                                retries: attempt as usize,
                                error: GeneratorError::Interrupted,
                            };
                        }
                    }
                }
                continue;
            }

            // Non-retryable HTTP error: fail immediately
            let error_text = response.text().await.unwrap_or_default();
            return SendOutcome::Failure {
                retries: attempt as usize,
                error: GeneratorError::HttpError(status.as_u16(), error_text),
            };
        }

        unreachable!("all loop paths return explicitly")
    }
}

fn is_transient_reqwest_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn retry_config() -> RetryConfig {
        RetryConfig::new(1, 1000, 2000).unwrap()
    }

    fn message(tenant_id: &str) -> OTLPLogMessage {
        OTLPLogMessage::new(
            MessagePayload::Json(json!({"resourceLogs": []})),
            Some(tenant_id.to_string()),
            "project1".to_string(),
            "source1".to_string(),
            crate::message::OTLPLogMessageType::Valid,
        )
    }

    #[test]
    fn http_header_uses_message_tenant_id() {
        let transport = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
        )
        .unwrap();

        let request = transport
            .build_request(&message("tenant2"))
            .build()
            .unwrap();
        assert_eq!(request.headers().get("X-Scope-OrgID").unwrap(), "tenant2");
    }

    #[test]
    fn consecutive_requests_can_use_different_tenants() {
        let transport = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
        )
        .unwrap();

        let first = transport
            .build_request(&message("tenant1"))
            .build()
            .unwrap();
        let second = transport
            .build_request(&message("tenant3"))
            .build()
            .unwrap();

        assert_eq!(first.headers().get("X-Scope-OrgID").unwrap(), "tenant1");
        assert_eq!(second.headers().get("X-Scope-OrgID").unwrap(), "tenant3");
    }

    #[test]
    fn http_omits_scope_header_when_tenant_id_none() {
        let transport = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
        )
        .unwrap();

        let msg = OTLPLogMessage::new(
            MessagePayload::Json(json!({"resourceLogs": []})),
            None,
            "project1".to_string(),
            "source1".to_string(),
            crate::message::OTLPLogMessageType::Valid,
        );
        let request = transport.build_request(&msg).build().unwrap();
        assert!(request.headers().get("X-Scope-OrgID").is_none());
    }
}
