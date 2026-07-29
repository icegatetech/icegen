pub mod auth;
pub mod destination;
pub mod grpc;
pub mod grpc_retry;
pub mod http;
pub mod noop;
pub mod protobuf;

use crate::error::GeneratorError;
use crate::message::OTLPMessage;
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

    pub fn is_timeout(&self) -> bool {
        matches!(
            self,
            Self::Failure {
                error: crate::error::GeneratorError::Timeout,
                ..
            }
        )
    }
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, message: &OTLPMessage, shutdown_rx: &watch::Receiver<bool>)
        -> SendOutcome;
}

pub use auth::AuthHeaders;
pub use destination::{Destination, DestinationFlags, TransportFlow};
pub use grpc::GrpcTransport;
pub use http::HttpTransport;
pub use noop::NoopTransport;
