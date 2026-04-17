pub mod grpc;
pub mod http;
pub mod protobuf;

use crate::message::OTLPLogMessage;
use async_trait::async_trait;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct SendReport {
    pub success: bool,
    /// Retry attempt index for the final send result (0-based).
    /// Examples: `0` means no retry; `1` means one retry before success/failure.
    pub retries: usize,
    pub error: Option<String>,
}

impl SendReport {
    pub fn success(retries: usize) -> Self {
        Self {
            success: true,
            retries,
            error: None,
        }
    }

    pub fn failure(retries: usize, error: impl Into<String>) -> Self {
        Self {
            success: false,
            retries,
            error: Some(error.into()),
        }
    }
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendReport;
}

pub use grpc::GrpcTransport;
pub use http::HttpTransport;
