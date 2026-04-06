use crate::config::LabelCardinalityConfig;
use crate::message::fake_data::FakeDataGenerator;
use rand::Rng;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct AttributeGenerator {
    source: String,
    label_cardinality: LabelCardinalityConfig,
}

impl AttributeGenerator {
    pub fn new(source: String, label_cardinality: LabelCardinalityConfig) -> Self {
        Self {
            source,
            label_cardinality,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn attributes_pairs_to_dict_list(pairs: &[(String, String)]) -> Vec<Value> {
        pairs
            .iter()
            .map(|(key, value)| {
                json!({
                    "key": key,
                    "value": {
                        "stringValue": value
                    }
                })
            })
            .collect()
    }

    pub fn generate_resource_attributes_pairs(
        &self,
        project_id: &str,
        cloud_account_id: &str,
        service_name: &str,
    ) -> Vec<(String, String)> {
        let attributes = vec![
            ("project_id".to_string(), project_id.to_string()),
            (
                "cloud.account.id".to_string(),
                cloud_account_id.to_string(),
            ),
            ("service.name".to_string(), service_name.to_string()),
            (
                "service.version".to_string(),
                FakeDataGenerator::generate_service_version(),
            ),
            (
                "deployment.environment".to_string(),
                FakeDataGenerator::generate_deployment_environment(),
            ),
            (
                "host.name".to_string(),
                FakeDataGenerator::generate_host_name(),
            ),
            (
                "k8s.pod.name".to_string(),
                FakeDataGenerator::generate_k8s_pod_name(),
            ),
            (
                "k8s.namespace.name".to_string(),
                FakeDataGenerator::generate_k8s_namespace(),
            ),
            ("generator.source".to_string(), self.source.clone()),
        ];

        self.normalize_attribute_pairs(attributes)
    }

    pub fn generate_scope_attributes_pairs(&self, service_name: &str) -> Vec<(String, String)> {
        let mut rng = rand::thread_rng();
        vec![
            (
                "library.name".to_string(),
                format!("trihub-{}", service_name),
            ),
            (
                "library.version".to_string(),
                format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
            ),
        ]
    }

    pub fn normalize_attribute_pairs(&self, pairs: Vec<(String, String)>) -> Vec<(String, String)> {
        pairs
            .into_iter()
            .map(|(key, value)| {
                let normalized = self.normalize_by_cardinality(&key, &value);
                (key, normalized)
            })
            .collect()
    }

    pub fn normalize_by_cardinality(&self, key: &str, value: &str) -> String {
        if !self.label_cardinality.enabled {
            return value.to_string();
        }

        let Some(limit) = self.label_cardinality.limit_for(key) else {
            return value.to_string();
        };

        if limit <= 1 {
            return "bucket_00".to_string();
        }

        let index = stable_bucket_index(key, value, limit);
        let width = num_digits(limit.saturating_sub(1));
        format!("bucket_{index:0width$}")
    }
}

pub fn stable_bucket_index(key: &str, value: &str, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }

    // FNV-1a 64-bit for deterministic, stable bucket assignment across runs.
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for byte in key
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0xff))
        .chain(value.as_bytes().iter())
    {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }

    (hash as usize) % limit
}

pub fn num_digits(mut number: usize) -> usize {
    if number == 0 {
        return 1;
    }

    let mut digits = 0;
    while number > 0 {
        number /= 10;
        digits += 1;
    }
    digits
}
