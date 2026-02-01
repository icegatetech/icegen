use crate::config::BatchResult;
use crate::error::Result;
use crate::message::OTLPLogMessage;
use async_trait::async_trait;

#[async_trait]
pub trait LogGenerator: Send + Sync {
    fn generate_message(&self) -> Result<OTLPLogMessage>;
    async fn send_message(&self, message: &OTLPLogMessage) -> Result<bool>;
    async fn send_messages_batch(&self, count: usize, delay_ms: u64) -> Result<BatchResult>;
    async fn close(&self) -> Result<()>;
}
