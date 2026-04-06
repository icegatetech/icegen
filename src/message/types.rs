use crate::transport::types::{MessagePayload, MessageType, OTLPMessage, SignalPath};

pub type OTLPLogMessageType = MessageType;

#[derive(Debug, Clone)]
pub struct OTLPLogMessage {
    pub message: OTLPMessage,
}

impl OTLPLogMessage {
    pub fn new(
        payload: MessagePayload,
        tenant_id: String,
        project_id: String,
        source: String,
        message_type: OTLPLogMessageType,
    ) -> Self {
        Self {
            message: OTLPMessage {
                payload,
                tenant_id,
                project_id,
                source,
                message_type,
                signal_path: SignalPath::Logs,
            },
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.message.tenant_id
    }

    pub fn project_id(&self) -> &str {
        &self.message.project_id
    }

    pub fn source(&self) -> &str {
        &self.message.source
    }

    pub fn message_type(&self) -> MessageType {
        self.message.message_type
    }

    pub fn payload(&self) -> &MessagePayload {
        &self.message.payload
    }

    pub fn payload_size_bytes(&self) -> usize {
        self.message.payload_size_bytes()
    }

    pub fn as_otlp_message(&self) -> &OTLPMessage {
        &self.message
    }
}
