pub mod fake_data;
pub mod generator;
pub mod types;

pub use fake_data::FakeDataGenerator;
pub use generator::OTLPLogMessageGenerator;
pub use types::{MessagePayload, OTLPLogMessage, OTLPLogMessageType};
