use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::pb::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use crate::transport::Transport;
use async_trait::async_trait;
use prost::Message;
use std::time::Duration;
use tonic::transport::Channel;

pub struct GrpcTransport {
    client: LogsServiceClient<Channel>,
}

impl GrpcTransport {
    pub async fn new(endpoint: String) -> Result<Self> {
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

        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()> {
        let request = match &message.message {
            MessagePayload::Protobuf(bytes) => {
                // Decode protobuf bytes back to ExportLogsServiceRequest
                ExportLogsServiceRequest::decode(&bytes[..])?
            }
            MessagePayload::Json(_) | MessagePayload::MalformedJson(_) => {
                return Err(GeneratorError::InvalidMessageType(
                    "gRPC transport only supports protobuf messages".to_string(),
                ));
            }
        };

        let mut client = self.client.clone();
        client.export(request).await?;

        Ok(())
    }
}
