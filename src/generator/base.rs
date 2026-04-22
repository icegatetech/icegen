use crate::config::BatchResult;
use crate::error::Result;
use crate::message::OTLPLogMessage;
use crate::transport::SendOutcome;
use async_trait::async_trait;
use tokio::sync::watch;

#[async_trait]
pub trait LogGenerator: Send + Sync {
    fn generate_message(&self) -> Result<OTLPLogMessage>;
    async fn send_message(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<SendOutcome>;
    async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<BatchResult>;
    async fn close(&self) -> Result<()>;
}
