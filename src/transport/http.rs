use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPMessage};
use crate::transport::{AuthHeaders, SendOutcome, Transport};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
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
    #[allow(clippy::result_large_err)]
    pub fn new(
        endpoint: String,
        use_protobuf: bool,
        retry_config: RetryConfig,
        auth: AuthHeaders,
    ) -> Result<Self> {
        // Vendor auth headers go in as client default headers, so reqwest attaches them to every
        // request without touching build_request. An invalid header name/value fails at startup.
        let mut default_headers = HeaderMap::new();
        for (name, value) in auth.iter() {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                GeneratorError::InvalidConfiguration(format!(
                    "invalid auth header name '{name}': {e}"
                ))
            })?;
            let header_value = HeaderValue::from_str(value).map_err(|e| {
                GeneratorError::InvalidConfiguration(format!(
                    "invalid auth header value for '{name}': {e}"
                ))
            })?;
            default_headers.insert(header_name, header_value);
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .default_headers(default_headers)
            .build()?;

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

    fn build_request(&self, message: &OTLPMessage) -> reqwest::RequestBuilder {
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
        message: &OTLPMessage,
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
                    let sleep_fut = sleep(Duration::from_millis(delay));
                    tokio::pin!(sleep_fut);
                    loop {
                        tokio::select! {
                            _ = &mut sleep_fut => break,
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() {
                                    // Sender dropped: no shutdown signal will arrive.
                                    // Preserve the remaining backoff delay instead of
                                    // spinning hot and hammering the collector.
                                    (&mut sleep_fut).await;
                                    break;
                                }
                                if *shutdown_rx.borrow() {
                                    return SendOutcome::Failure {
                                        retries: attempt as usize,
                                        error: GeneratorError::Interrupted,
                                    };
                                }
                            }
                        }
                    }
                    continue;
                }
                Err(e) => {
                    // Normalize a terminal request timeout into the dedicated Timeout variant
                    // so SendOutcome::is_timeout() and the timeout counter recognize it.
                    let error = if e.is_timeout() {
                        GeneratorError::Timeout
                    } else {
                        GeneratorError::RequestError(e)
                    };
                    return SendOutcome::Failure {
                        retries: attempt as usize,
                        error,
                    };
                }
            };

            let status = response.status();

            if status.is_success() {
                if attempt > 0 {
                    eprintln!(
                        "  \u{2713} Retry[http]: request succeeded after {} retries",
                        attempt
                    );
                }
                return SendOutcome::Success {
                    retries: attempt as usize,
                };
            }

            if status.as_u16() == 429 {
                if attempt == max_retries {
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let body = response.text().await.unwrap_or_default();
                    eprintln!(
                        "  \u{26a0} Retry[http]: 429 Too Many Requests, attempt {}/{}, retry-after: {:?}, body: {}",
                        attempt + 1,
                        max_retries + 1,
                        retry_after,
                        body
                    );
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
                let sleep_fut = sleep(Duration::from_millis(delay));
                tokio::pin!(sleep_fut);
                loop {
                    tokio::select! {
                        _ = &mut sleep_fut => break,
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() {
                                // Sender dropped: no shutdown signal will arrive.
                                // Preserve the remaining backoff delay instead of
                                // spinning hot and hammering the collector.
                                (&mut sleep_fut).await;
                                break;
                            }
                            if *shutdown_rx.borrow() {
                                return SendOutcome::Failure {
                                    retries: attempt as usize,
                                    error: GeneratorError::Interrupted,
                                };
                            }
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
    use crate::message::Signal;
    use serde_json::json;

    fn retry_config() -> RetryConfig {
        RetryConfig::new(1, 1000, 2000).unwrap()
    }

    fn message(tenant_id: &str) -> OTLPMessage {
        OTLPMessage::new(
            MessagePayload::Json(json!({"resourceLogs": []})),
            Signal::Logs,
            Some(tenant_id.to_string()),
            "project1".to_string(),
            "source1".to_string(),
            crate::message::OTLPMessageType::Valid,
        )
    }

    #[test]
    fn http_header_uses_message_tenant_id() {
        let transport = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
            AuthHeaders::default(),
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
            AuthHeaders::default(),
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
            AuthHeaders::default(),
        )
        .unwrap();

        let msg = OTLPMessage::new(
            MessagePayload::Json(json!({"resourceLogs": []})),
            Signal::Logs,
            None,
            "project1".to_string(),
            "source1".to_string(),
            crate::message::OTLPMessageType::Valid,
        );
        let request = transport.build_request(&msg).build().unwrap();
        assert!(request.headers().get("X-Scope-OrgID").is_none());
    }

    #[test]
    fn http_accepts_valid_vendor_auth_headers() {
        // reqwest applies client default_headers at send time (see Client::execute_request), so they
        // are not observable on a `.build()`-only request. Here we assert the construction path: a
        // valid auth set builds a transport without error (the headers ride on every real request).
        let auth = AuthHeaders::build("x-api-key=secret", Some("xyz"), None).unwrap();
        assert!(HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
            auth,
        )
        .is_ok());
    }

    #[tokio::test]
    async fn http_send_carries_auth_headers_on_the_wire() {
        // Proves vendor auth actually rides on a real request (parity with the gRPC metadata test):
        // a throwaway TCP listener captures the raw request bytes and we assert the headers landed.
        // Deleting `.default_headers(...)` in `HttpTransport::new` makes this red.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let mut data = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&buf[..n]);
                if data.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&data).to_string()
        });

        let auth = AuthHeaders::build("x-api-key=secret", Some("xyz"), None).unwrap();
        let transport = HttpTransport::new(
            format!("http://{addr}/v1/logs"),
            false,
            retry_config(),
            auth,
        )
        .unwrap();

        let (_tx, rx) = tokio::sync::watch::channel(false);
        let outcome = transport.send(&message("tenant1"), &rx).await;
        assert!(matches!(outcome, SendOutcome::Success { .. }));

        let raw = server.await.unwrap().to_lowercase();
        assert!(
            raw.contains("x-api-key: secret"),
            "missing x-api-key: {raw}"
        );
        assert!(
            raw.contains("authorization: bearer xyz"),
            "missing authorization: {raw}"
        );
    }

    #[test]
    fn http_rejects_invalid_auth_header_name() {
        let auth = AuthHeaders::build("bad key=value", None, None).unwrap();
        let result = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
            auth,
        );
        assert!(matches!(
            result,
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn http_rejects_invalid_auth_header_value() {
        let auth = AuthHeaders::build("x-token=bad\u{1}val", None, None).unwrap();
        let result = HttpTransport::new(
            "http://localhost:4318/v1/logs".to_string(),
            false,
            retry_config(),
            auth,
        );
        assert!(matches!(
            result,
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }
}
