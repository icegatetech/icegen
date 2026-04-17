use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPLogMessage};
use crate::pb::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use crate::transport::{SendReport, Transport};
use async_trait::async_trait;
use prost::Message;
use std::time::Duration;
use tokio::sync::watch;
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

    fn prepare_export_parts(
        message: &OTLPLogMessage,
    ) -> Result<(
        ExportLogsServiceRequest,
        tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    )> {
        let proto_request = match &message.message {
            MessagePayload::Protobuf(bytes) => ExportLogsServiceRequest::decode(&bytes[..])?,
            MessagePayload::Json(_) | MessagePayload::MalformedJson(_) => {
                return Err(GeneratorError::InvalidMessageType(
                    "gRPC transport only supports protobuf messages".to_string(),
                ));
            }
        };

        let tenant =
            tonic::metadata::MetadataValue::try_from(message.tenant_id.as_str()).map_err(|_| {
                GeneratorError::InvalidConfiguration(format!(
                    "invalid tenant_id for gRPC metadata: {}",
                    message.tenant_id
                ))
            })?;

        Ok((proto_request, tenant))
    }

    fn build_export_request(
        proto_request: ExportLogsServiceRequest,
        tenant: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    ) -> tonic::Request<ExportLogsServiceRequest> {
        let mut request = tonic::Request::new(proto_request);
        request.metadata_mut().insert("x-scope-orgid", tenant);
        request
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn send(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendReport {
        let max_retries = self.retry_config.max_retries;
        let mut shutdown_rx = shutdown_rx.clone();
        let (proto_request, tenant) = match Self::prepare_export_parts(message) {
            Ok(parts) => parts,
            Err(e) => return SendReport::failure(0, e.to_string()),
        };

        for attempt in 0..=max_retries {
            let mut client = self.client.clone();
            let request = Self::build_export_request(proto_request.clone(), tenant.clone());
            match client.export(request).await {
                Ok(_) => {
                    if attempt > 0 {
                        eprintln!("  \u{2713} Request succeeded after {} retries", attempt);
                    }
                    return SendReport::success(attempt as usize);
                }
                Err(status) if is_retryable_grpc_code(status.code()) => {
                    if attempt == max_retries {
                        return SendReport::failure(
                            attempt as usize,
                            format!("grpc retryable error exhausted: {}", status),
                        );
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
                        " \u{26a0} Retry[grpc]: {}, attempt {}/{}, waiting {}ms, error: {}",
                        label,
                        attempt + 1,
                        max_retries + 1,
                        delay,
                        status.message()
                    );

                    if *shutdown_rx.borrow() {
                        return SendReport::failure(
                            attempt as usize,
                            format!("grpc request interrupted by shutdown: {}", status),
                        );
                    }
                    tokio::select! {
                        _ = sleep(Duration::from_millis(delay)) => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_ok() && *shutdown_rx.borrow() {
                                return SendReport::failure(
                                    attempt as usize,
                                    format!("grpc request interrupted during retry wait: {}", status),
                                );
                            }
                        }
                    }
                    continue;
                }
                Err(status) => {
                    return SendReport::failure(
                        attempt as usize,
                        format!("grpc error: {}", status),
                    );
                }
            }
        }

        SendReport::failure(
            max_retries as usize,
            format!("grpc retries exhausted after {} attempts", max_retries + 1),
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::OTLPLogMessageType;
    use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    fn protobuf_message(tenant_id: &str) -> OTLPLogMessage {
        let request = ExportLogsServiceRequest {
            resource_logs: Vec::new(),
        };
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        OTLPLogMessage::new(
            MessagePayload::Protobuf(buf),
            tenant_id.to_string(),
            "project1".to_string(),
            "source1".to_string(),
            OTLPLogMessageType::Valid,
        )
    }

    #[test]
    fn grpc_metadata_uses_message_tenant_id() {
        let (proto_request, tenant) =
            GrpcTransport::prepare_export_parts(&protobuf_message("tenant2")).unwrap();
        let request = GrpcTransport::build_export_request(proto_request, tenant);
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn grpc_rejects_non_protobuf_payload() {
        let message = OTLPLogMessage::new(
            MessagePayload::Json(serde_json::json!({"resourceLogs": []})),
            "tenant2".to_string(),
            "project1".to_string(),
            "source1".to_string(),
            OTLPLogMessageType::Valid,
        );

        let error =
            GrpcTransport::prepare_export_parts(&message).expect_err("expected invalid payload");
        assert!(matches!(error, GeneratorError::InvalidMessageType(_)));
    }

    #[test]
    fn grpc_prepared_parts_can_build_multiple_requests_without_redecode() {
        let message = protobuf_message("tenant2");
        let (proto_request, tenant) = GrpcTransport::prepare_export_parts(&message).unwrap();

        let request1 = GrpcTransport::build_export_request(proto_request.clone(), tenant.clone());
        let request2 = GrpcTransport::build_export_request(proto_request, tenant);

        assert_eq!(request1.metadata().get("x-scope-orgid").unwrap(), "tenant2");
        assert_eq!(request2.metadata().get("x-scope-orgid").unwrap(), "tenant2");
        assert_eq!(
            request1.get_ref().resource_logs.len(),
            request2.get_ref().resource_logs.len()
        );
    }
}
