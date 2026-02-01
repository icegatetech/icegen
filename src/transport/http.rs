use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::transport::Transport;
use async_trait::async_trait;
use reqwest::Client;
use std::time::Duration;

pub struct HttpTransport {
    client: Client,
    endpoint: String,
    #[allow(dead_code)]
    use_protobuf: bool,
}

impl HttpTransport {
    pub fn new(endpoint: String, use_protobuf: bool) -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

        Ok(Self {
            client,
            endpoint,
            use_protobuf,
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
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()> {
        let request = match &message.message {
            MessagePayload::Json(json_value) => {
                // JSON mode
                self.client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "trihub-log-generator/1.0")
                    .json(json_value)
            }
            MessagePayload::Protobuf(bytes) => {
                // Protobuf mode
                self.client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/x-protobuf")
                    .header("User-Agent", "trihub-log-generator/1.0")
                    .body(bytes.clone())
            }
            MessagePayload::MalformedJson(malformed_string) => {
                // Malformed JSON for testing
                self.client
                    .post(&self.endpoint)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "trihub-log-generator/1.0")
                    .body(malformed_string.clone())
            }
        };

        let response = request.send().await?;
        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(GeneratorError::HttpError(status.as_u16(), error_text));
        }

        Ok(())
    }
}
