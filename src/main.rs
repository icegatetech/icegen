use clap::Parser;
use otel_log_generator::{Cli, GeneratorType, LogGenerator, OtelConfig, OtelLogGenerator};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;

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
            let generator = Arc::new(OtelLogGenerator::new(config.clone()).await?);

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

                let mut generator_task = {
                    let generator = Arc::clone(&generator);
                    let (shutdown_tx, shutdown_rx) = watch::channel(false);
                    let generator_task =
                        tokio::spawn(async move { generator.run_continuous(shutdown_rx).await });
                    (shutdown_tx, generator_task)
                };

                tokio::select! {
                    result = &mut generator_task.1 => {
                        let result = result??;
                        if !config.print_logs {
                            if config.dry_run {
                                println!(
                                    "\nSummary: {} messages generated (dry-run, nothing sent)",
                                    result.total
                                );
                            } else {
                                println!(
                                    "\nSummary: {}/{} messages sent successfully",
                                    result.success, result.total
                                );
                            }
                            if result.failed > 0 {
                                println!("Failed: {} messages", result.failed);
                            }
                        }
                    }
                    _ = shutdown => {
                        let _ = generator_task.0.send(true);
                        let result = generator_task.1.await??;
                        generator.close().await?;
                        if config.dry_run {
                            println!(
                                "Shutdown complete. Final summary: {} messages generated (dry-run, nothing sent)",
                                result.total
                            );
                        } else {
                            println!(
                                "Shutdown complete. Final summary: {}/{} messages sent successfully",
                                result.success, result.total
                            );
                        }
                        if result.failed > 0 {
                            println!("Failed: {} messages", result.failed);
                        }
                    }
                }
            } else {
                let result = generator
                    .send_messages_batch(config.count, config.message_interval_ms)
                    .await?;
                if config.dry_run {
                    println!(
                        "\nSummary: {} messages generated (dry-run, nothing sent)",
                        result.total
                    );
                } else {
                    println!(
                        "\nSummary: {}/{} messages sent successfully",
                        result.success, result.total
                    );
                }
                if result.failed > 0 {
                    println!("Failed: {} messages", result.failed);
                }
            }
        }
    }

    Ok(())
}
