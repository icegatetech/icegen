use crate::config::{BatchResult, OtelConfig};
use crate::error::Result;
use crate::generator::base::LogGenerator;
use crate::message::{MessagePayload, OTLPLogMessage, OTLPLogMessageGenerator};
use crate::transport::{GrpcTransport, HttpTransport, Transport};
use async_trait::async_trait;
use rand::Rng;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub struct OtelLogGenerator {
    config: OtelConfig,
    message_generator: OTLPLogMessageGenerator,
    transport: Arc<dyn Transport>,
}

impl OtelLogGenerator {
    pub async fn new(config: OtelConfig) -> Result<Self> {
        config.validate()?;

        println!("Initializing OTEL Log Generator...");
        println!("  Endpoint: {}", config.ingest_endpoint);
        println!("  Transport: {}", config.transport);
        println!("  Use Protobuf: {}", config.use_protobuf);
        println!("  Records per message: {}", config.records_per_message);
        println!("  Invalid record %: {}", config.invalid_record_percent);

        let retry_config = config.retry_config();
        println!("  Retry: max_attempts={}, base_delay={}ms, max_delay={}ms",
            retry_config.max_attempts, retry_config.base_delay_ms, retry_config.max_delay_ms);

        // Create transport
        let transport: Arc<dyn Transport> = match config.transport.as_str() {
            "http" => {
                let http_transport =
                    HttpTransport::new(config.ingest_endpoint.clone(), config.use_protobuf, retry_config)?;

                // Perform health check if configured
                if let Some(ref health_endpoint) = config.healthcheck_endpoint {
                    println!("Performing health check: {}", health_endpoint);
                    match http_transport.health_check(health_endpoint).await {
                        Ok(_) => println!("✓ Health check passed"),
                        Err(e) => {
                            eprintln!("✗ Health check failed: {}", e);
                            return Err(e);
                        }
                    }
                }

                Arc::new(http_transport)
            }
            "grpc" => {
                let grpc_transport = GrpcTransport::new(config.ingest_endpoint.clone(), retry_config).await?;
                Arc::new(grpc_transport)
            }
            _ => unreachable!(), // Already validated in config
        };

        let message_generator = OTLPLogMessageGenerator::new("rust-generator".to_string());

        println!("✓ Generator initialized successfully\n");

        Ok(Self {
            config,
            message_generator,
            transport,
        })
    }
}

#[async_trait]
impl LogGenerator for OtelLogGenerator {
    fn generate_message(&self) -> Result<OTLPLogMessage> {
        let mut rng = rand::thread_rng();

        // Decide if this should be an invalid message
        let should_be_invalid = rng.gen::<f32>() * 100.0 < self.config.invalid_record_percent;

        let message = if should_be_invalid {
            self.message_generator.generate_invalid_message()?
        } else {
            // Generate based on transport and format
            if self.config.transport == "grpc" || self.config.use_protobuf {
                self.message_generator
                    .generate_protobuf_message(self.config.records_per_message)?
            } else {
                self.message_generator
                    .generate_aggregated_message(self.config.records_per_message)?
            }
        };

        Ok(message)
    }

    async fn send_message(&self, message: &OTLPLogMessage) -> Result<bool> {
        if self.config.print_logs {
            println!("Sending message:");
            println!("  Project ID: {}", message.project_id);
            println!("  Source: {}", message.source);
            println!("  Type: {:?}", message.message_type);
            match &message.message {
                MessagePayload::Json(json) => {
                    println!(
                        "  Payload: {}",
                        serde_json::to_string_pretty(json).unwrap_or_default()
                    );
                }
                MessagePayload::Protobuf(bytes) => {
                    println!("  Payload: <protobuf {} bytes>", bytes.len());
                }
                MessagePayload::MalformedJson(s) => {
                    println!("  Payload: {}", s);
                }
            }
        }

        match self.transport.send(message).await {
            Ok(_) => {
                if self.config.print_logs {
                    println!("✓ Message sent successfully\n");
                }
                Ok(true)
            }
            Err(e) => {
                eprintln!("✗ Failed to send message: {}", e);
                Ok(false)
            }
        }
    }

    async fn send_messages_batch(&self, count: usize, delay_ms: u64) -> Result<BatchResult> {
        let mut result = BatchResult::new();

        for i in 0..count {
            let message = self.generate_message()?;
            let success = self.send_message(&message).await?;

            if success {
                result.add_success();
            } else {
                result.add_failure();
            }

            if !self.config.print_logs && (i + 1) % 10 == 0 {
                println!("Progress: {}/{} messages sent", i + 1, count);
            }

            if delay_ms > 0 && i < count - 1 {
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        Ok(result)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
