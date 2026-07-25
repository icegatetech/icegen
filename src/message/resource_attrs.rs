//! Shared OTLP resource-attribute builder plus the signal-neutral home of the cardinality
//! normalizer and the per-shard [`ShardResourceAttrs`].
//!
//! A shard's whole resource-attribute set — the identity keys (`project_id`, `cloud.account.id`,
//! `service.name`, `generator.source`) and the deliberately variable ones (`host.name`, `k8s.*`,
//! `service.version`, `deployment.environment`) — is sampled and normalized here exactly once per
//! generation cycle. Every signal then reuses that set verbatim, so no two generators derive a
//! shard's pod apart.

use crate::config::AttributesCardinalityConfig;
use crate::message::fake_data::FakeDataGenerator;
use crate::message::ServiceShard;

/// Single default service name for the log and trace paths (service.name, scope.name, k8s.pod.name).
pub const DEFAULT_SERVICE_NAME: &str = "icegen";

/// Resource-attribute keys that make up the shared identity of a shard. They are normalized once
/// while building the [`ResourceIdentityPlan`] and must therefore be skipped when the variable
/// keys are normalized (re-bucketing a bucket label would desynchronize the signals).
const IDENTITY_KEYS: [&str; 4] = [
    "project_id",
    "cloud.account.id",
    "service.name",
    "generator.source",
];

/// Whether `key` is one of the shared [`IDENTITY_KEYS`] normalized inside [`ResourceIdentityPlan`].
fn is_identity_key(key: &str) -> bool {
    IDENTITY_KEYS.contains(&key)
}

/// The identity attribute values of one service shard, normalized once and shared by every signal
/// of a generation cycle.
///
/// A correlated log record and its trace must carry the same `project_id`, `cloud.account.id`,
/// `service.name`, and `generator.source`. Because cardinality bucketing is deterministic but not
/// idempotent, these values are normalized a single time here and then handed to both planners
/// verbatim, rather than each generator normalizing independently.
#[derive(Debug, Clone)]
struct ResourceIdentityPlan {
    /// `project_id`, cardinality-normalized.
    project_id: String,
    /// `cloud.account.id`, cardinality-normalized; `None` when no cloud account is configured.
    cloud_account_id: Option<String>,
    /// `service.name`, cardinality-normalized; `None` for a tenantless/service-less shard.
    service_name: Option<String>,
    /// `generator.source`, cardinality-normalized.
    generator_source: String,
}

impl ResourceIdentityPlan {
    /// Build one shard's identity, applying `cardinality` to each identity key. `cardinality` is
    /// `None` when the run must not normalize identity (e.g. a traces-only run, where cardinality is
    /// a logs-only feature); each value is then carried through unchanged.
    fn new(
        project_id: &str,
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
        generator_source: &str,
        cardinality: Option<&AttributesCardinalityConfig>,
    ) -> Self {
        let norm = |key: &str, value: &str| match cardinality {
            Some(cfg) => normalize_by_cardinality(cfg, key, value),
            None => value.to_string(),
        };
        Self {
            project_id: norm("project_id", project_id),
            cloud_account_id: cloud_account_id.map(|v| norm("cloud.account.id", v)),
            service_name: service_name.map(|v| norm("service.name", v)),
            generator_source: norm("generator.source", generator_source),
        }
    }
}

/// The complete resource-attribute set of one service shard, sampled and normalized once per
/// generation cycle and reused verbatim by every signal.
///
/// Sharing the whole set — not only the identity keys — is what makes a shard one pod: a correlated
/// log record and its trace report the same `host.name`, `k8s.pod.name`, `service.version`,
/// `deployment.environment`, and `k8s.namespace.name`. Sampling per signal instead would describe
/// the same `service.name` as several different hosts.
#[derive(Debug, Clone)]
pub(crate) struct ShardResourceAttrs {
    pairs: Vec<(String, String)>,
}

impl ShardResourceAttrs {
    /// Build one shard's attribute set: identity keys normalized inside [`ResourceIdentityPlan`],
    /// variable keys freshly sampled and then normalized here. `cardinality` is `None` when the run
    /// must not normalize at all (a traces-only run — cardinality is a logs-only knob); every value
    /// is then carried through raw.
    pub(crate) fn new(
        project_id: &str,
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
        generator_source: &str,
        cardinality: Option<&AttributesCardinalityConfig>,
    ) -> Self {
        let identity = ResourceIdentityPlan::new(
            project_id,
            cloud_account_id,
            service_name,
            generator_source,
            cardinality,
        );
        let pairs = build_resource_attribute_pairs(&identity, service_name)
            .into_iter()
            .map(|(key, value)| match cardinality {
                // Identity keys are already bucketed; bucketing a bucket label would move the
                // signals apart from each other.
                Some(cfg) if !is_identity_key(&key) => {
                    let normalized = normalize_by_cardinality(cfg, &key, &value);
                    (key, normalized)
                }
                _ => (key, value),
            })
            .collect();
        Self { pairs }
    }

    /// Build one attribute set per shard (in shard order), sharing `project_id`,
    /// `cloud_account_id`, and `generator_source` and taking each shard's own `service.name`.
    pub(crate) fn for_shards(
        project_id: &str,
        cloud_account_id: Option<&str>,
        shards: &[ServiceShard],
        generator_source: &str,
        cardinality: Option<&AttributesCardinalityConfig>,
    ) -> Vec<Self> {
        shards
            .iter()
            .map(|shard| {
                Self::new(
                    project_id,
                    cloud_account_id,
                    shard.service_name.as_deref(),
                    generator_source,
                    cardinality,
                )
            })
            .collect()
    }

    /// The shard's attribute pairs, in emission order.
    pub(crate) fn pairs(&self) -> &[(String, String)] {
        &self.pairs
    }
}

/// Collect resource-attribute pairs for one shard: the shared identity values from `identity`
/// (already cardinality-normalized) plus the deliberately variable attributes (`service.version`,
/// `deployment.environment`, `host.name`, `k8s.*`), which are freshly randomized here and left for
/// [`ShardResourceAttrs::new`] to normalize. `service_name_for_variable` seeds `k8s.pod.name` from
/// the shard's *raw* service name and is unrelated to the possibly-bucketed identity `service.name`.
fn build_resource_attribute_pairs(
    identity: &ResourceIdentityPlan,
    service_name_for_variable: Option<&str>,
) -> Vec<(String, String)> {
    let mut attributes = vec![("project_id".to_string(), identity.project_id.clone())];
    if let Some(acc) = &identity.cloud_account_id {
        attributes.push(("cloud.account.id".to_string(), acc.clone()));
    }
    if let Some(svc) = &identity.service_name {
        attributes.push(("service.name".to_string(), svc.clone()));
    }
    attributes.push((
        "service.version".to_string(),
        FakeDataGenerator::generate_service_version(),
    ));
    attributes.push((
        "deployment.environment".to_string(),
        FakeDataGenerator::generate_deployment_environment(),
    ));
    attributes.push((
        "host.name".to_string(),
        FakeDataGenerator::generate_host_name(),
    ));
    attributes.push((
        "k8s.pod.name".to_string(),
        FakeDataGenerator::generate_k8s_pod_name(
            service_name_for_variable.unwrap_or(DEFAULT_SERVICE_NAME),
        ),
    ));
    attributes.push((
        "k8s.namespace.name".to_string(),
        FakeDataGenerator::generate_k8s_namespace(),
    ));
    attributes.push((
        "generator.source".to_string(),
        identity.generator_source.clone(),
    ));
    attributes
}

/// Map `value` to a stable `bucket_NN` label when `key` has a cardinality limit under `cfg`,
/// bounding the number of distinct values that key can emit. Values are returned unchanged when
/// limiting is disabled or `key` has no applicable limit.
pub(crate) fn normalize_by_cardinality(
    cfg: &AttributesCardinalityConfig,
    key: &str,
    value: &str,
) -> String {
    if !cfg.enabled {
        return value.to_string();
    }

    let Some(limit) = cfg.limit_for(key) else {
        return value.to_string();
    };

    if limit <= 1 {
        return "bucket_00".to_string();
    }

    let index = stable_bucket_index(key, value, limit);
    let width = num_digits(limit.saturating_sub(1));
    format!("bucket_{index:0width$}")
}

fn stable_bucket_index(key: &str, value: &str, limit: usize) -> usize {
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

fn num_digits(mut number: usize) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn attr_map(cardinality: Option<&AttributesCardinalityConfig>) -> HashMap<String, String> {
        ShardResourceAttrs::new(
            "proj-1",
            Some("acc-1"),
            Some("svc-a"),
            "gen-src",
            cardinality,
        )
        .pairs()
        .iter()
        .cloned()
        .collect()
    }

    #[test]
    fn builds_core_resource_attrs() {
        let map = attr_map(None);
        assert_eq!(map.get("project_id").map(String::as_str), Some("proj-1"));
        assert_eq!(
            map.get("cloud.account.id").map(String::as_str),
            Some("acc-1")
        );
        assert_eq!(map.get("service.name").map(String::as_str), Some("svc-a"));
        assert_eq!(
            map.get("generator.source").map(String::as_str),
            Some("gen-src")
        );
        assert!(map.contains_key("deployment.environment"));
        assert!(map.contains_key("host.name"));
    }

    #[test]
    fn omits_optional_when_none() {
        let attrs = ShardResourceAttrs::new("proj-1", None, None, "gen-src", None);
        let keys: Vec<&str> = attrs.pairs().iter().map(|(k, _)| k.as_str()).collect();
        assert!(!keys.contains(&"cloud.account.id"));
        assert!(!keys.contains(&"service.name"));
    }

    #[test]
    fn identity_normalization_is_deterministic_and_shared() {
        // default_limit=1 collapses every identity key to a single bucket; two independent builds
        // must produce byte-identical identity values so logs and traces stay in lock-step.
        let cfg = AttributesCardinalityConfig {
            enabled: true,
            default_limit: Some(1),
            limit_by_attr: HashMap::new(),
        };
        let ma = attr_map(Some(&cfg));
        let mb = attr_map(Some(&cfg));
        for key in IDENTITY_KEYS {
            assert_eq!(ma.get(key), mb.get(key), "identity key {key} diverged");
            assert_eq!(ma.get(key).map(String::as_str), Some("bucket_00"));
        }
    }

    /// The variable keys are bucketed too — sharing the set across signals is only useful if the
    /// same policy the log path used to apply is still applied.
    #[test]
    fn variable_keys_are_normalized() {
        let cfg = AttributesCardinalityConfig {
            enabled: true,
            default_limit: Some(2),
            limit_by_attr: HashMap::new(),
        };
        let map = attr_map(Some(&cfg));
        for key in ["host.name", "k8s.pod.name", "deployment.environment"] {
            let value = map.get(key).expect("variable key present");
            assert!(
                value == "bucket_0" || value == "bucket_1",
                "{key} not bucketed into limit=2: {value}"
            );
        }
        // Identity keys keep their own single bucketing pass, not a second one over a bucket label.
        assert_eq!(
            map.get("service.name").map(String::as_str),
            Some(normalize_by_cardinality(&cfg, "service.name", "svc-a").as_str())
        );
    }

    #[test]
    fn identity_keys_are_recognized() {
        assert!(is_identity_key("project_id"));
        assert!(is_identity_key("generator.source"));
        assert!(!is_identity_key("host.name"));
        assert!(!is_identity_key("k8s.pod.name"));
    }
}
