pub mod grpc;
pub mod http;
pub mod noop;
pub mod protobuf;

use crate::error::GeneratorError;
use crate::message::OTLPLogMessage;
use async_trait::async_trait;
use tokio::sync::watch;

#[derive(Debug)]
pub enum SendOutcome {
    Success {
        retries: usize,
    },
    Failure {
        retries: usize,
        error: GeneratorError,
    },
}

impl SendOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub fn retries(&self) -> usize {
        match self {
            Self::Success { retries } | Self::Failure { retries, .. } => *retries,
        }
    }
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome;
}

pub use grpc::GrpcTransport;
pub use http::HttpTransport;
pub use noop::NoopTransport;
