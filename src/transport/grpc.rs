use crate::config::RetryConfig;
use crate::error::{GeneratorError, Result};
use crate::message::{MessagePayload, OTLPMessage, Signal};
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

/// Extract the per-message tenant into gRPC `x-scope-orgid` metadata, if present.
#[allow(clippy::result_large_err)]
fn tenant_metadata(
    message: &OTLPMessage,
) -> Result<Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>> {
    message
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
        .transpose()
}

/// Unified gRPC transport. A single `Channel` carries both OTLP services; the logs and/or traces
/// client is created depending on the configured signals, and [`Transport::send`] routes by
/// `message.signal`. Retries of the two clients are independent.
pub struct GrpcTransport {
    logs_client: Option<LogsServiceClient<Channel>>,
    trace_client: Option<TraceServiceClient<Channel>>,
    retry_config: RetryConfig,
    auth_meta: AuthMetadata,
}

impl GrpcTransport {
    /// Connect one channel to `endpoint` and instantiate the OTLP service clients required by
    /// `signals` on top of it.
    ///
    /// # Arguments
    ///
    /// * `endpoint` — gRPC target URL shared by both OTLP services.
    /// * `signals` — signals to serve; a client is created only for [`Signal::Logs`] and/or
    ///   [`Signal::Traces`] present here.
    /// * `retry_config` — backoff/retry policy applied per send.
    /// * `auth` — vendor auth headers compiled into gRPC metadata attached to every request.
    ///
    /// # Errors
    ///
    /// Returns [`GeneratorError::InvalidConfiguration`] if an auth header is not valid gRPC
    /// metadata, or a connection error if the channel to `endpoint` cannot be established.
    pub async fn new(
        endpoint: String,
        signals: &[Signal],
        retry_config: RetryConfig,
        auth: AuthHeaders,
    ) -> Result<Self> {
        let auth_meta = compile_grpc_auth_metadata(&auth)?;
        let channel = connect_channel(&endpoint).await?;
        let logs_client = signals
            .contains(&Signal::Logs)
            .then(|| LogsServiceClient::new(channel.clone()));
        let trace_client = signals
            .contains(&Signal::Traces)
            .then(|| TraceServiceClient::new(channel.clone()));

        Ok(Self {
            logs_client,
            trace_client,
            retry_config,
            auth_meta,
        })
    }

    #[allow(clippy::result_large_err)]
    fn prepare_log_export_parts(
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
        Ok((proto_request, tenant_metadata(message)?))
    }

    fn build_log_export_request(
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

    #[allow(clippy::result_large_err)]
    fn prepare_trace_export_parts(
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
        Ok((proto_request, tenant_metadata(message)?))
    }

    fn build_trace_export_request(
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

    async fn send_logs(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        let Some(client) = self.logs_client.as_ref() else {
            return SendOutcome::Failure {
                retries: 0,
                error: GeneratorError::InvalidConfiguration(
                    "gRPC logs client not configured for this run".to_string(),
                ),
            };
        };
        let (proto_request, tenant) = match Self::prepare_log_export_parts(message) {
            Ok(parts) => parts,
            Err(e) => {
                return SendOutcome::Failure {
                    retries: 0,
                    error: e,
                }
            }
        };
        let auth_meta = &self.auth_meta;
        run_with_retry(&self.retry_config, shutdown_rx, || {
            let mut client = client.clone();
            let request =
                Self::build_log_export_request(proto_request.clone(), tenant.clone(), auth_meta);
            async move { client.export(request).await.map(|_| ()) }
        })
        .await
    }

    async fn send_traces(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        let Some(client) = self.trace_client.as_ref() else {
            return SendOutcome::Failure {
                retries: 0,
                error: GeneratorError::InvalidConfiguration(
                    "gRPC traces client not configured for this run".to_string(),
                ),
            };
        };
        let (proto_request, tenant) = match Self::prepare_trace_export_parts(message) {
            Ok(parts) => parts,
            Err(e) => {
                return SendOutcome::Failure {
                    retries: 0,
                    error: e,
                }
            }
        };
        let auth_meta = &self.auth_meta;
        run_with_retry(&self.retry_config, shutdown_rx, || {
            let mut client = client.clone();
            let request =
                Self::build_trace_export_request(proto_request.clone(), tenant.clone(), auth_meta);
            async move { client.export(request).await.map(|_| ()) }
        })
        .await
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn send(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        match message.signal {
            Signal::Logs => self.send_logs(message, shutdown_rx).await,
            Signal::Traces => self.send_traces(message, shutdown_rx).await,
        }
    }
}

/// OTLP server stubs generated into `OUT_DIR` by `build.rs`, used to stand up a real collector for
/// the transport contract test. The message types come from [`crate::pb`]; only the service
/// scaffolding lives here, and only in test builds.
#[cfg(test)]
mod pb_server {
    pub mod logs {
        include!(concat!(
            env!("OUT_DIR"),
            "/opentelemetry.proto.collector.logs.v1.rs"
        ));
    }
    pub mod trace {
        include!(concat!(
            env!("OUT_DIR"),
            "/opentelemetry.proto.collector.trace.v1.rs"
        ));
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
            GrpcTransport::prepare_log_export_parts(&protobuf_message(Some("tenant2"))).unwrap();
        let request =
            GrpcTransport::build_log_export_request(proto_request, tenant, &AuthMetadata::new());
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn grpc_omits_scope_metadata_when_tenant_id_none() {
        let (proto_request, tenant) =
            GrpcTransport::prepare_log_export_parts(&protobuf_message(None)).unwrap();
        let request =
            GrpcTransport::build_log_export_request(proto_request, tenant, &AuthMetadata::new());
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

        let error = GrpcTransport::prepare_log_export_parts(&message)
            .expect_err("expected invalid payload");
        assert!(matches!(error, GeneratorError::InvalidMessageType(_)));
    }

    #[test]
    fn grpc_prepared_parts_can_build_multiple_requests_without_redecode() {
        let message = protobuf_message(Some("tenant2"));
        let (proto_request, tenant) = GrpcTransport::prepare_log_export_parts(&message).unwrap();

        let request1 = GrpcTransport::build_log_export_request(
            proto_request.clone(),
            tenant.clone(),
            &AuthMetadata::new(),
        );
        let request2 =
            GrpcTransport::build_log_export_request(proto_request, tenant, &AuthMetadata::new());

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
            GrpcTransport::prepare_trace_export_parts(&trace_protobuf_message(Some("tenant2")))
                .unwrap();
        let request =
            GrpcTransport::build_trace_export_request(proto_request, tenant, &AuthMetadata::new());
        assert_eq!(request.metadata().get("x-scope-orgid").unwrap(), "tenant2");
    }

    #[test]
    fn trace_grpc_omits_scope_metadata_when_tenant_id_none() {
        let (proto_request, tenant) =
            GrpcTransport::prepare_trace_export_parts(&trace_protobuf_message(None)).unwrap();
        let request =
            GrpcTransport::build_trace_export_request(proto_request, tenant, &AuthMetadata::new());
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

        let error = GrpcTransport::prepare_trace_export_parts(&message)
            .expect_err("expected invalid payload");
        assert!(matches!(error, GeneratorError::InvalidMessageType(_)));
    }

    #[test]
    fn grpc_build_request_carries_auth_metadata() {
        let auth = AuthHeaders::build("x-api-key=secret", Some("xyz"), None).unwrap();
        let auth_meta = compile_grpc_auth_metadata(&auth).unwrap();
        let (proto_request, tenant) =
            GrpcTransport::prepare_log_export_parts(&protobuf_message(Some("tenant2"))).unwrap();
        let request = GrpcTransport::build_log_export_request(proto_request, tenant, &auth_meta);

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

    /// The OTLP service a request landed on, plus the tenant metadata it carried.
    type ExportCall = (&'static str, Option<String>);

    /// A real OTLP collector serving both services on one port, recording which service each
    /// export reached.
    #[derive(Clone)]
    struct RecordingCollector {
        calls: tokio::sync::mpsc::UnboundedSender<ExportCall>,
    }

    impl RecordingCollector {
        fn record<T>(&self, service: &'static str, request: &tonic::Request<T>) {
            let tenant = request
                .metadata()
                .get("x-scope-orgid")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);
            self.calls.send((service, tenant)).expect("record export");
        }
    }

    #[async_trait]
    impl pb_server::logs::logs_service_server::LogsService for RecordingCollector {
        async fn export(
            &self,
            request: tonic::Request<ExportLogsServiceRequest>,
        ) -> std::result::Result<
            tonic::Response<
                crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse,
            >,
            tonic::Status,
        > {
            self.record("logs", &request);
            Ok(tonic::Response::new(Default::default()))
        }
    }

    #[async_trait]
    impl pb_server::trace::trace_service_server::TraceService for RecordingCollector {
        async fn export(
            &self,
            request: tonic::Request<ExportTraceServiceRequest>,
        ) -> std::result::Result<
            tonic::Response<
                crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse,
            >,
            tonic::Status,
        > {
            self.record("traces", &request);
            Ok(tonic::Response::new(Default::default()))
        }
    }

    /// A running [`RecordingCollector`] on an ephemeral port, owning its server task and aborting it
    /// on drop so a failing or panicking test cannot leak it.
    struct CollectorHarness {
        endpoint: String,
        calls: tokio::sync::mpsc::UnboundedReceiver<ExportCall>,
        server: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
    }

    impl CollectorHarness {
        async fn start() -> Self {
            use pb_server::logs::logs_service_server::LogsServiceServer;
            use pb_server::trace::trace_service_server::TraceServiceServer;
            use tonic::transport::server::TcpIncoming;

            let (calls_tx, calls_rx) = tokio::sync::mpsc::unbounded_channel();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            // The listener is already bound, so the endpoint is reachable before the first send.
            let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
            let collector = RecordingCollector { calls: calls_tx };
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(LogsServiceServer::new(collector.clone()))
                    .add_service(TraceServiceServer::new(collector))
                    .serve_with_incoming(incoming),
            );

            Self {
                endpoint: format!("http://{addr}"),
                calls: calls_rx,
                server,
            }
        }

        /// Next export the collector received, bounded so a lost request fails instead of hanging.
        async fn next_call(&mut self) -> ExportCall {
            tokio::time::timeout(Duration::from_secs(5), self.calls.recv())
                .await
                .expect("collector received an export")
                .expect("collector channel open")
        }

        fn received_nothing(&mut self) -> bool {
            matches!(
                self.calls.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            )
        }
    }

    impl Drop for CollectorHarness {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    /// `Transport::send` must reach the OTLP service that matches the message's signal, on the real
    /// gRPC boundary: building a `tonic::Request` proves nothing about which method is invoked.
    #[tokio::test]
    async fn send_routes_each_signal_to_its_own_otlp_service() {
        let mut collector = CollectorHarness::start().await;

        let transport = GrpcTransport::new(
            collector.endpoint.clone(),
            &[Signal::Logs, Signal::Traces],
            RetryConfig::new(0, 100, 100).unwrap(),
            AuthHeaders::default(),
        )
        .await
        .unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let traces_outcome = transport
            .send(&trace_protobuf_message(Some("tenant2")), &shutdown_rx)
            .await;
        assert!(matches!(traces_outcome, SendOutcome::Success { .. }));
        let logs_outcome = transport
            .send(&protobuf_message(Some("tenant2")), &shutdown_rx)
            .await;
        assert!(matches!(logs_outcome, SendOutcome::Success { .. }));

        // Order is the send order, not the signal order: a misrouted traces message would surface
        // here as ("logs", _) first.
        let first = collector.next_call().await;
        let second = collector.next_call().await;
        assert_eq!(first, ("traces", Some("tenant2".to_string())));
        assert_eq!(second, ("logs", Some("tenant2".to_string())));
    }

    #[tokio::test]
    async fn send_fails_when_traces_client_not_configured() {
        // A traces message on a logs-only run must fail explicitly instead of being misrouted to
        // the logs service; the guard fires before any network call, so nothing reaches the
        // collector even though the channel is connected.
        let mut collector = CollectorHarness::start().await;
        let transport = GrpcTransport::new(
            collector.endpoint.clone(),
            &[Signal::Logs],
            RetryConfig::new(0, 100, 100).unwrap(),
            AuthHeaders::default(),
        )
        .await
        .unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let outcome = transport
            .send(&trace_protobuf_message(Some("tenant2")), &shutdown_rx)
            .await;

        assert!(matches!(
            outcome,
            SendOutcome::Failure {
                error: GeneratorError::InvalidConfiguration(_),
                ..
            }
        ));
        assert!(collector.received_nothing());
    }

    #[tokio::test]
    async fn send_fails_when_logs_client_not_configured() {
        // Symmetric guard: a logs message on a traces-only run must fail rather than reach the
        // trace service.
        let mut collector = CollectorHarness::start().await;
        let transport = GrpcTransport::new(
            collector.endpoint.clone(),
            &[Signal::Traces],
            RetryConfig::new(0, 100, 100).unwrap(),
            AuthHeaders::default(),
        )
        .await
        .unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let outcome = transport
            .send(&protobuf_message(Some("tenant2")), &shutdown_rx)
            .await;

        assert!(matches!(
            outcome,
            SendOutcome::Failure {
                error: GeneratorError::InvalidConfiguration(_),
                ..
            }
        ));
        assert!(collector.received_nothing());
    }
}
