use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::pb::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use crate::transport::Transport;
use async_trait::async_trait;
use prost::Message;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;

pub struct GrpcTransport {
    client: LogsServiceClient<Channel>,
    retry_config: RetryConfig,
}

impl GrpcTransport {
    pub async fn new(endpoint: String, retry_config: RetryConfig) -> Result<Self> {
        // Detect the original scheme and preserve it
        let (scheme, host) = if endpoint.starts_with("https://") {
            eprintln!("  ⚠ gRPC endpoint uses HTTPS scheme: {}", endpoint);
            ("https", endpoint.trim_start_matches("https://"))
        } else if endpoint.starts_with("http://") {
            ("http", endpoint.trim_start_matches("http://"))
        } else {
            ("http", endpoint.as_str())
        };
        let full_endpoint = format!("{}://{}", scheme, host);

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

        let max_retries = self.retry_config.max_retries;
        let mut last_error: Option<GeneratorError> = None;

        for attempt in 0..=max_retries {
            let mut client = self.client.clone();
            match client.export(proto_request.clone()).await {
                Ok(_) => {
                    if attempt > 0 {
                        eprintln!("  \u{2713} Request succeeded after {} retries", attempt);
                    }
                    return Ok(());
                }
                Err(status) if is_retryable_grpc_code(status.code()) => {
                    if attempt == max_retries {
                        return Err(GeneratorError::GrpcError(status));
                    }

                    let label = match status.code() {
                        tonic::Code::ResourceExhausted => "ResourceExhausted",
                        tonic::Code::Unavailable => "Unavailable",
                        tonic::Code::Aborted => "Aborted",
                        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
                        _ => "transient error",
                    };

                    let delay = self.retry_config.compute_delay(attempt, None);

                    eprintln!(
                        "  \u{26a0} gRPC {} (attempt {}/{}): {}",
                        label,
                        attempt + 1,
                        max_retries + 1,
                        status.message()
                    );
                    eprintln!("  Waiting {}ms before retry...", delay);

                    last_error = Some(GeneratorError::GrpcError(status));
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                Err(status) => {
                    return Err(GeneratorError::GrpcError(status));
                }
            }
        }

        match last_error {
            Some(e) => Err(e),
            None => Err(GeneratorError::RateLimitExceeded(max_retries)),
        }
    }
}

fn is_retryable_grpc_code(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::ResourceExhausted
            | tonic::Code::Unavailable
            | tonic::Code::Aborted
            | tonic::Code::DeadlineExceeded
    )
}
