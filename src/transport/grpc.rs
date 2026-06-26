use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPMessage};
use crate::pb::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use crate::pb::opentelemetry::proto::collector::trace::v1::trace_service_client::TraceServiceClient;
use crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use crate::transport::grpc_retry::run_with_retry;
use crate::transport::{AuthHeaders, SendOutcome, Transport};
use async_trait::async_trait;
use prost::Message;
use std::time::Duration;
use tokio::sync::watch;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};

/// A vendor auth header precompiled into a gRPC metadata key/value pair.
type AuthMetadata = Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>;

/// Precompile vendor auth headers into ASCII gRPC metadata pairs. Non-ASCII or otherwise invalid
/// keys/values fail here (at transport startup) rather than being silently dropped per request.
#[allow(clippy::result_large_err)]
fn compile_grpc_auth_metadata(auth: &AuthHeaders) -> Result<AuthMetadata> {
    auth.iter()
        .map(|(name, value)| {
            let key = MetadataKey::from_bytes(name.as_bytes()).map_err(|_| {
                GeneratorError::InvalidConfiguration(format!(
                    "invalid auth header name for gRPC metadata: {name}"
                ))
            })?;
            let value = MetadataValue::try_from(value).map_err(|_| {
                GeneratorError::InvalidConfiguration(format!(
                    "invalid auth header value for gRPC metadata (must be ASCII): {name}"
                ))
            })?;
            Ok((key, value))
        })
        .collect()
}

/// Insert precompiled auth metadata pairs into a gRPC request.
fn insert_auth_metadata<T>(request: &mut tonic::Request<T>, auth_meta: &AuthMetadata) {
    for (key, value) in auth_meta {
        request.metadata_mut().insert(key.clone(), value.clone());
    }
}

/// Establish a gRPC channel to `endpoint`, preserving the original http/https scheme and
/// applying a 5s connect/request timeout. Shared by the logs and traces transports.
async fn connect_channel(endpoint: &str) -> Result<Channel> {
    // Detect the original scheme and preserve it
    let (scheme, host) = if let Some(rest) = endpoint.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        ("http", rest)
    } else {
        ("http", endpoint)
    };
    let full_endpoint = format!("{}://{}", scheme, host);
    let mut builder = Channel::from_shared(full_endpoint)
        .map_err(|e| GeneratorError::InvalidConfiguration(e.to_string()))?
        .timeout(Duration::from_secs(5));

    // An https endpoint terminates TLS; tonic does not negotiate it implicitly, so a plaintext
    // channel to a TLS-only port stalls until the timeout fires. Enable TLS for https endpoints.
    // ClientTlsConfig::new() loads the platform's native root certificates via the `tls-roots`
    // feature.
    if scheme == "https" {
        builder = builder
            .tls_config(ClientTlsConfig::new())
            .map_err(|e| GeneratorError::InvalidConfiguration(e.to_string()))?;
    }

    let channel = builder.connect().await?;

    Ok(channel)
}

/// gRPC transport for the logs signal.
pub struct LogGrpcTransport {
    client: LogsServiceClient<Channel>,
    retry_config: RetryConfig,
    auth_meta: AuthMetadata,
}

impl LogGrpcTransport {
    pub async fn new(
        endpoint: String,
        retry_config: RetryConfig,
        auth: AuthHeaders,
    ) -> Result<Self> {
        let auth_meta = compile_grpc_auth_metadata(&auth)?;
        let channel = connect_channel(&endpoint).await?;
        let client = LogsServiceClient::new(channel);

        Ok(Self {
            client,
            retry_config,
            auth_meta,
        })
    }

    #[allow(clippy::result_large_err)]
    fn prepare_export_parts(
        message: &OTLPMessage,
    ) -> Result<(
        ExportLogsServiceRequest,
        Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    )> {
        let proto_request = match &message.message {
            MessagePayload::Protobuf(bytes) => ExportLogsServiceRequest::decode(&bytes[..])?,
            MessagePayload::Json(_) | MessagePayload::MalformedJson(_) => {
                return Err(GeneratorError::InvalidMessageType(
                    "gRPC transport only supports protobuf messages".to_string(),
                ));
            }
        };

        let tenant = message
            .tenant_id
            .as_deref()
            .map(|tid| {
                tonic::metadata::MetadataValue::try_from(tid).map_err(|_| {
                    GeneratorError::InvalidConfiguration(format!(
                        "invalid tenant_id for gRPC metadata: {}",
                        tid
                    ))
                })
            })
            .transpose()?;

        Ok((proto_request, tenant))
    }

    fn build_export_request(
        proto_request: ExportLogsServiceRequest,
        tenant: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
        auth_meta: &AuthMetadata,
    ) -> tonic::Request<ExportLogsServiceRequest> {
        let mut request = tonic::Request::new(proto_request);
        if let Some(tenant) = tenant {
            request.metadata_mut().insert("x-scope-orgid", tenant);
        }
        insert_auth_metadata(&mut request, auth_meta);
        request
    }
}

#[async_trait]
impl Transport for LogGrpcTransport {
    async fn send(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        let (proto_request, tenant) = match Self::prepare_export_parts(message) {
            Ok(parts) => parts,
            Err(e) => {
                return SendOutcome::Failure {
                    retries: 0,
                    error: e,
                }
            }
        };
        let client = self.client.clone();
        let auth_meta = &self.auth_meta;
        run_with_retry(&self.retry_config, shutdown_rx, || {
            let mut client = client.clone();
            let request =
                Self::build_export_request(proto_request.clone(), tenant.clone(), auth_meta);
            async move { client.export(request).await.map(|_| ()) }
        })
        .await
    }
}

/// gRPC transport for the traces signal.
pub struct TraceGrpcTransport {
    client: TraceServiceClient<Channel>,
    retry_config: RetryConfig,
    auth_meta: AuthMetadata,
}

impl TraceGrpcTransport {
    pub async fn new(
        endpoint: String,
        retry_config: RetryConfig,
        auth: AuthHeaders,
    ) -> Result<Self> {
        let auth_meta = compile_grpc_auth_metadata(&auth)?;
        let channel = connect_channel(&endpoint).await?;
        Ok(Self {
            client: TraceServiceClient::new(channel),
            retry_config,
            auth_meta,
        })
    }

    #[allow(clippy::result_large_err)]
    fn prepare_export_parts(
        message: &OTLPMessage,
    ) -> Result<(
        ExportTraceServiceRequest,
        Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    )> {
        let proto_request = match &message.message {
            MessagePayload::Protobuf(bytes) => ExportTraceServiceRequest::decode(&bytes[..])?,
            MessagePayload::Json(_) | MessagePayload::MalformedJson(_) => {
                return Err(GeneratorError::InvalidMessageType(
                    "gRPC trace transport only supports protobuf messages".to_string(),
                ));
            }
        };

        let tenant = message
            .tenant_id
            .as_deref()
            .map(|tid| {
                tonic::metadata::MetadataValue::try_from(tid).map_err(|_| {
                    GeneratorError::InvalidConfiguration(format!(
                        "invalid tenant_id for gRPC metadata: {}",
                        tid
                    ))
                })
            })
            .transpose()?;

        Ok((proto_request, tenant))
    }

    fn build_export_request(
        proto_request: ExportTraceServiceRequest,
        tenant: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
        auth_meta: &AuthMetadata,
    ) -> tonic::Request<ExportTraceServiceRequest> {
        let mut request = tonic::Request::new(proto_request);
        if let Some(tenant) = tenant {
            request.metadata_mut().insert("x-scope-orgid", tenant);
        }
        insert_auth_metadata(&mut request, auth_meta);
        request
    }
}

#[async_trait]
impl Transport for TraceGrpcTransport {
    async fn send(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        let (proto_request, tenant) = match Self::prepare_export_parts(message) {
            Ok(parts) => parts,
            Err(e) => {
                return SendOutcome::Failure {
                    retries: 0,
                    error: e,
                }
            }
        };
        let client = self.client.clone();
        let auth_meta = &self.auth_meta;
        run_with_retry(&self.retry_config, shutdown_rx, || {
            let mut client = client.clone();
            let request =
                Self::build_export_request(proto_request.clone(), tenant.clone(), auth_meta);
            async move { client.export(request).await.map(|_| ()) }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{OTLPMessageType, Signal};
    use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    fn protobuf_message(tenant_id: Option<&str>) -> OTLPMessage {
        let request = ExportLogsServiceRequest {
            resource_logs: Vec::new(),
        };
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        OTLPMessage::new(
            MessagePayload::Protobuf(buf),
            Signal::Logs,
            tenant_id.map(ToString::to_string),
            "project1".to_string(),
            "source1".to_string(),
            OTLPMessageType::Valid,
        )
    }

    #[test]
    fn grpc_metadata_uses_message_tenant_id() {
        let (proto_request, tenant) =
            LogGrpcTransport::prepare_export_parts(&protobuf_message(Some("tenant2"))).unwrap();
        let request =
            LogGrpcTransport::build_export_request(proto_request, tenant, &AuthMetadata::new());
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn grpc_omits_scope_metadata_when_tenant_id_none() {
        let (proto_request, tenant) =
            LogGrpcTransport::prepare_export_parts(&protobuf_message(None)).unwrap();
        let request =
            LogGrpcTransport::build_export_request(proto_request, tenant, &AuthMetadata::new());
        assert!(request.metadata().get("x-scope-orgid").is_none());
    }

    #[test]
    fn grpc_rejects_non_protobuf_payload() {
        let message = OTLPMessage::new(
            MessagePayload::Json(serde_json::json!({"resourceLogs": []})),
            Signal::Logs,
            Some("tenant2".to_string()),
            "project1".to_string(),
            "source1".to_string(),
            OTLPMessageType::Valid,
        );

        let error =
            LogGrpcTransport::prepare_export_parts(&message).expect_err("expected invalid payload");
        assert!(matches!(error, GeneratorError::InvalidMessageType(_)));
    }

    #[test]
    fn grpc_prepared_parts_can_build_multiple_requests_without_redecode() {
        let message = protobuf_message(Some("tenant2"));
        let (proto_request, tenant) = LogGrpcTransport::prepare_export_parts(&message).unwrap();

        let request1 = LogGrpcTransport::build_export_request(
            proto_request.clone(),
            tenant.clone(),
            &AuthMetadata::new(),
        );
        let request2 =
            LogGrpcTransport::build_export_request(proto_request, tenant, &AuthMetadata::new());

        assert_eq!(request1.metadata().get("x-scope-orgid").unwrap(), "tenant2");
        assert_eq!(request2.metadata().get("x-scope-orgid").unwrap(), "tenant2");
        assert_eq!(
            request1.get_ref().resource_logs.len(),
            request2.get_ref().resource_logs.len()
        );
    }

    fn trace_protobuf_message(tenant_id: Option<&str>) -> OTLPMessage {
        let request = ExportTraceServiceRequest {
            resource_spans: Vec::new(),
        };
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        OTLPMessage::new(
            MessagePayload::Protobuf(buf),
            Signal::Traces,
            tenant_id.map(ToString::to_string),
            "project1".to_string(),
            "source1".to_string(),
            OTLPMessageType::Valid,
        )
    }

    #[test]
    fn trace_grpc_metadata_uses_message_tenant_id() {
        let (proto_request, tenant) =
            TraceGrpcTransport::prepare_export_parts(&trace_protobuf_message(Some("tenant2")))
                .unwrap();
        let request =
            TraceGrpcTransport::build_export_request(proto_request, tenant, &AuthMetadata::new());
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn trace_grpc_omits_scope_metadata_when_tenant_id_none() {
        let (proto_request, tenant) =
            TraceGrpcTransport::prepare_export_parts(&trace_protobuf_message(None)).unwrap();
        let request =
            TraceGrpcTransport::build_export_request(proto_request, tenant, &AuthMetadata::new());
        assert!(request.metadata().get("x-scope-orgid").is_none());
    }

    #[test]
    fn trace_grpc_rejects_non_protobuf_payload() {
        let message = OTLPMessage::new(
            MessagePayload::Json(serde_json::json!({"resourceSpans": []})),
            Signal::Traces,
            Some("tenant2".to_string()),
            "project1".to_string(),
            "source1".to_string(),
            OTLPMessageType::Valid,
        );

        let error = TraceGrpcTransport::prepare_export_parts(&message)
            .expect_err("expected invalid payload");
        assert!(matches!(error, GeneratorError::InvalidMessageType(_)));
    }

    #[test]
    fn grpc_build_request_carries_auth_metadata() {
        let auth = AuthHeaders::build("x-api-key=secret", Some("xyz"), None).unwrap();
        let auth_meta = compile_grpc_auth_metadata(&auth).unwrap();
        let (proto_request, tenant) =
            LogGrpcTransport::prepare_export_parts(&protobuf_message(Some("tenant2"))).unwrap();
        let request = LogGrpcTransport::build_export_request(proto_request, tenant, &auth_meta);

        assert_eq!(request.metadata().get("x-api-key").unwrap(), "secret");
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer xyz"
        );
        // Auth metadata coexists with the per-message tenant metadata.
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn grpc_auth_metadata_rejects_invalid_value() {
        // gRPC Ascii metadata permits obs-text bytes (0x80..=0xFF), so a non-ASCII value like
        // "héllo" is accepted; only structurally invalid values (control chars) are rejected.
        let auth = AuthHeaders::build("x-token=bad\u{1}val", None, None).unwrap();
        let error = compile_grpc_auth_metadata(&auth).expect_err("control char must fail");
        assert!(matches!(error, GeneratorError::InvalidConfiguration(_)));
    }

    #[test]
    fn grpc_auth_metadata_rejects_invalid_key() {
        let auth = AuthHeaders::build("bad key=value", None, None).unwrap();
        let error = compile_grpc_auth_metadata(&auth).expect_err("invalid key must fail");
        assert!(matches!(error, GeneratorError::InvalidConfiguration(_)));
    }
}
