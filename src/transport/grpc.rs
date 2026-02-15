use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::pb::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use crate::transport::Transport;
use async_trait::async_trait;
use prost::Message;
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;

pub struct GrpcTransport {
    client: LogsServiceClient<Channel>,
    retry_config: RetryConfig,
}

impl GrpcTransport {
    pub async fn new(endpoint: String, retry_config: RetryConfig) -> Result<Self> {
        // Strip http:// or https:// prefix if present
        let normalized_endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string();

        // Add http:// prefix for tonic
        let full_endpoint = if normalized_endpoint.starts_with("http://")
            || normalized_endpoint.starts_with("https://")
        {
            normalized_endpoint
        } else {
            format!("http://{}", normalized_endpoint)
        };

        let channel = Channel::from_shared(full_endpoint)
            .map_err(|e| GeneratorError::InvalidConfiguration(e.to_string()))?
            .timeout(Duration::from_secs(5))
            .connect()
            .await?;

        let client = LogsServiceClient::new(channel);

        Ok(Self {
            client,
            retry_config,
        })
    }

    fn compute_delay(&self, attempt: u32) -> u64 {
        let exponential = self
            .retry_config
            .base_delay_ms
            .saturating_mul(1u64 << attempt);
        let base = exponential.min(self.retry_config.max_delay_ms);

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
impl Transport for GrpcTransport {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()> {
        let proto_request = match &message.message {
            MessagePayload::Protobuf(bytes) => ExportLogsServiceRequest::decode(&bytes[..])?,
            MessagePayload::Json(_) | MessagePayload::MalformedJson(_) => {
                return Err(GeneratorError::InvalidMessageType(
                    "gRPC transport only supports protobuf messages".to_string(),
                ));
            }
        };

        let max_attempts = self.retry_config.max_attempts;

        for attempt in 0..=max_attempts {
            let mut client = self.client.clone();
            match client.export(proto_request.clone()).await {
                Ok(_) => {
                    if attempt > 0 {
                        eprintln!("  \u{2713} Request succeeded after {} retries", attempt);
                    }
                    return Ok(());
                }
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    if attempt == max_attempts {
                        return Err(GeneratorError::RateLimitExceeded(max_attempts));
                    }

                    let delay = self.compute_delay(attempt);

                    eprintln!(
                        "  \u{26a0} gRPC ResourceExhausted (attempt {}/{})",
                        attempt + 1,
                        max_attempts + 1
                    );
                    eprintln!("  Waiting {}ms before retry...", delay);

                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                Err(status) => {
                    return Err(GeneratorError::GrpcError(status));
                }
            }
        }

        Err(GeneratorError::RateLimitExceeded(max_attempts))
    }
}
