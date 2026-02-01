use clap::Parser;
use otel_log_generator::{Cli, GeneratorType, LogGenerator, OtelConfig, OtelLogGenerator};
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables from .env file if present
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    let cli = Cli::parse();

    match cli.generator {
        GeneratorType::Otel(args) => {
            let config: OtelConfig = args.into();
            let generator = OtelLogGenerator::new(config.clone()).await?;

            if config.continuous {
                println!("Running in continuous mode. Press Ctrl+C or send SIGTERM to stop.");

                // Setup signal handlers for graceful shutdown
                let ctrl_c = signal::ctrl_c();
                #[cfg(unix)]
                let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

                #[cfg(unix)]
                let shutdown = async {
                    tokio::select! {
                        _ = ctrl_c => {
                            println!("\nReceived Ctrl+C, shutting down gracefully...");
                        }
                        _ = sigterm.recv() => {
                            println!("\nReceived SIGTERM, shutting down gracefully...");
                        }
                    }
                };

                #[cfg(not(unix))]
                let shutdown = async {
                    ctrl_c.await.ok();
                    println!("\nReceived Ctrl+C, shutting down gracefully...");
                };

                tokio::select! {
                    _ = async {
                        loop {
                            let result = generator
                                .send_messages_batch(config.count, config.delay_ms)
                                .await.unwrap_or_else(|e| {
                                    eprintln!("Error sending batch: {}", e);
                                    Default::default()
                                });
                            if !config.print_logs {
                                println!(
                                    "Batch complete: {}/{} messages sent successfully",
                                    result.success, result.total
                                );
                            }
                        }
                    } => {}
                    _ = shutdown => {
                        generator.close().await?;
                        println!("Shutdown complete.");
                    }
                }
            } else {
                let result = generator
                    .send_messages_batch(config.count, config.delay_ms)
                    .await?;
                println!(
                    "\nSummary: {}/{} messages sent successfully",
                    result.success, result.total
                );
                if result.failed > 0 {
                    println!("Failed: {} messages", result.failed);
                }
            }
        }
    }

    Ok(())
}
