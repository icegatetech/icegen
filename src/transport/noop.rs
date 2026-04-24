use crate::message::OTLPLogMessage;
use crate::transport::{SendOutcome, Transport};
use async_trait::async_trait;
use tokio::sync::watch;

pub struct NoopTransport;

#[async_trait]
impl Transport for NoopTransport {
    async fn send(
        &self,
        _message: &OTLPLogMessage,
        _shutdown_rx: &watch::Receiver<bool>,
    ) -> SendOutcome {
        SendOutcome::Success { retries: 0 }
    }
}
