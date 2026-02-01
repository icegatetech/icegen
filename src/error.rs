use thiserror::Error;

#[derive(Error, Debug)]
pub enum GeneratorError {
    #[error("HTTP error {0}: {1}")]
    HttpError(u16, String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Health check failed with status {0}")]
    HealthCheckFailed(u16),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Protobuf encode error: {0}")]
    ProtobufEncodeError(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),

    #[error("Request timeout")]
    Timeout,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Invalid transport: {0}")]
    InvalidTransport(String),

    #[error("Invalid message type for transport: {0}")]
    InvalidMessageType(String),

    #[error("Hex decode error: {0}")]
    HexDecodeError(#[from] hex::FromHexError),

    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[allow(clippy::result_large_err)]
pub type Result<T> = std::result::Result<T, GeneratorError>;
