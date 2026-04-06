use crate::config::{BatchResult, OtelConfig};
use crate::error::Result;
use crate::message::metrics_types::OTLPMetricsMessage;
use crate::message::OTLPMetricsMessageGenerator;
use crate::transport::Transport;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
struct TenantProfile {
    tenant_id: String,
    cloud_account_ids: Arc<[String]>,
    service_names: Arc<[String]>,
}

impl TenantProfile {
    fn select_cloud_account_id(&self) -> String {
        let mut rng = rand::thread_rng();
        self.cloud_account_ids[rng.gen_range(0..self.cloud_account_ids.len())].clone()
    }

    fn select_service_name(&self) -> String {
        let mut rng = rand::thread_rng();
        self.service_names[rng.gen_range(0..self.service_names.len())].clone()
    }
}

#[derive(Clone)]
pub struct OtelMetricsGenerator {
    config: OtelConfig,
    message_generator: OTLPMetricsMessageGenerator,
    transport: Arc<dyn Transport>,
    tenant_profiles: Arc<[TenantProfile]>,
}

impl OtelMetricsGenerator {
    pub fn with_transport(config: OtelConfig, transport: Arc<dyn Transport>) -> Result<Self> {
        config.validate()?;
        let cardinality_config = config.label_cardinality_config()?;
        let message_generator = OTLPMetricsMessageGenerator::new_with_cardinality(
            "rust-generator".to_string(),
            cardinality_config,
        );
        let tenant_profiles = build_tenant_profiles(&config);
        Ok(Self {
            config,
            message_generator,
            transport,
            tenant_profiles,
        })
    }

    fn select_tenant_profile(&self) -> &TenantProfile {
        let mut rng = rand::thread_rng();
        &self.tenant_profiles[rng.gen_range(0..self.tenant_profiles.len())]
    }

    pub fn generate_message(&self) -> Result<OTLPMetricsMessage> {
        let tp = self.select_tenant_profile();
        let tenant_id = tp.tenant_id.clone();
        let cloud_account_id = tp.select_cloud_account_id();
        let service_name = tp.select_service_name();

        if self.config.transport == "grpc" || self.config.use_protobuf {
            self.message_generator.generate_protobuf_metric_message(
                tenant_id,
                cloud_account_id,
                service_name,
            )
        } else {
            self.message_generator.generate_random_metric_message(
                tenant_id,
                cloud_account_id,
                service_name,
            )
        }
    }

    pub async fn send_message(
        &self,
        message: &OTLPMetricsMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<bool> {
        match self
            .transport
            .send(message.as_otlp_message(), shutdown_rx)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                eprintln!("Failed to send metrics message: {}", e);
                Ok(false)
            }
        }
    }

    pub async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<BatchResult> {
        let mut result = BatchResult::new();
        let (shutdown_tx, _) = watch::channel(false);
        let shutdown_rx = shutdown_tx.subscribe();

        for _ in 0..count {
            let message = self.generate_message()?;
            let success = self.send_message(&message, &shutdown_rx).await?;
            if success {
                result.add_success();
            } else {
                result.add_failure();
            }
            if message_interval_ms > 0 {
                sleep(Duration::from_millis(message_interval_ms)).await;
            }
        }
        Ok(result)
    }

    pub async fn send_messages_batch_continuous(
        &self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<BatchResult> {
        let mut result = BatchResult::new();
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            let message = self.generate_message()?;
            let success = self.send_message(&message, &shutdown_rx).await?;
            if success {
                result.add_success();
            } else {
                result.add_failure();
            }
            if self.config.message_interval_ms > 0 {
                tokio::select! {
                    _ = sleep(Duration::from_millis(self.config.message_interval_ms)) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(result)
    }
}

fn build_tenant_profiles(config: &OtelConfig) -> Arc<[TenantProfile]> {
    let tenant_ids = if config.tenant_count == 1 {
        vec![config.tenant_id.clone()]
    } else {
        (1..=config.tenant_count)
            .map(|i| format!("tenant{}", i))
            .collect()
    };
    Arc::from(
        tenant_ids
            .into_iter()
            .map(|tenant_id| TenantProfile {
                cloud_account_ids: build_tenant_value_pool(
                    &tenant_id,
                    "acc",
                    config.cloud_account_count_per_tenant,
                ),
                service_names: build_tenant_value_pool(
                    &tenant_id,
                    "svc",
                    config.service_count_per_tenant,
                ),
                tenant_id,
            })
            .collect::<Vec<_>>(),
    )
}

fn build_tenant_value_pool(tenant_id: &str, suffix: &str, count: usize) -> Arc<[String]> {
    let width = count.to_string().len().max(2);
    Arc::from(
        (1..=count)
            .map(|i| format!("{tenant_id}-{suffix}-{i:0width$}"))
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::types::OTLPMessage;
    use async_trait::async_trait;

    struct NoopTransport;

    #[async_trait]
    impl Transport for NoopTransport {
        async fn send(&self, _: &OTLPMessage, _: &watch::Receiver<bool>) -> Result<()> {
            Ok(())
        }
    }

    fn test_config(count: usize, concurrency: usize) -> OtelConfig {
        OtelConfig {
            ingest_endpoint: "http://localhost:4318".to_string(),
            healthcheck_endpoint: None,
            use_protobuf: false,
            transport: "http".to_string(),
            invalid_record_percent: 0.0,
            records_per_message: 1,
            print_logs: false,
            count,
            message_interval_ms: 0,
            concurrency,
            continuous: false,
            retry_max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 32000,
            tenant_id: "tenant1".to_string(),
            tenant_count: 1,
            cloud_account_count_per_tenant: 4,
            service_count_per_tenant: 6,
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
            enable_logs: true,
            enable_metrics: true,
        }
    }

    #[tokio::test]
    async fn metrics_batch_sends_correct_count() {
        let config = test_config(5, 1);
        let gen =
            OtelMetricsGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let result = gen.send_messages_batch(5, 0).await.unwrap();
        assert_eq!(result.total, 5);
        assert_eq!(result.success, 5);
    }

    #[test]
    fn metrics_generator_uses_tenant_profiles() {
        let mut config = test_config(1, 1);
        config.tenant_count = 3;
        let gen =
            OtelMetricsGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..50 {
            let msg = gen.generate_message().unwrap();
            seen.insert(msg.tenant_id().to_string());
        }
        assert!(seen.len() > 1, "expected multiple tenants, got {:?}", seen);
    }
}
