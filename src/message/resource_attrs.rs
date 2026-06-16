//! Shared OTLP resource-attribute builder, used by the log and trace generators.
//! Cardinality normalization stays in the calling generator.

use crate::message::fake_data::FakeDataGenerator;

/// Single default service name for the log and trace paths (service.name, scope.name, k8s.pod.name).
pub const DEFAULT_SERVICE_NAME: &str = "icegen";

/// Collect resource-attribute pairs (project_id, cloud.account.id?, service.name?, version,
/// environment, host, k8s.*, generator.source). Values are pre-cardinality-normalization.
pub fn build_resource_attribute_pairs(
    project_id: &str,
    cloud_account_id: Option<&str>,
    service_name: Option<&str>,
    generator_source: &str,
) -> Vec<(String, String)> {
    let mut attributes = vec![("project_id".to_string(), project_id.to_string())];
    if let Some(acc) = cloud_account_id {
        attributes.push(("cloud.account.id".to_string(), acc.to_string()));
    }
    if let Some(svc) = service_name {
        attributes.push(("service.name".to_string(), svc.to_string()));
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
        FakeDataGenerator::generate_k8s_pod_name(service_name.unwrap_or(DEFAULT_SERVICE_NAME)),
    ));
    attributes.push((
        "k8s.namespace.name".to_string(),
        FakeDataGenerator::generate_k8s_namespace(),
    ));
    attributes.push(("generator.source".to_string(), generator_source.to_string()));
    attributes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_core_resource_attrs() {
        let pairs =
            build_resource_attribute_pairs("proj-1", Some("acc-1"), Some("svc-a"), "gen-src");
        let map: std::collections::HashMap<_, _> = pairs.into_iter().collect();
        assert_eq!(map.get("project_id").map(String::as_str), Some("proj-1"));
        assert_eq!(
            map.get("cloud.account.id").map(String::as_str),
            Some("acc-1")
        );
        assert_eq!(map.get("service.name").map(String::as_str), Some("svc-a"));
        assert!(map.contains_key("deployment.environment"));
        assert!(map.contains_key("host.name"));
    }

    #[test]
    fn omits_optional_when_none() {
        let pairs = build_resource_attribute_pairs("proj-1", None, None, "gen-src");
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert!(!keys.contains(&"cloud.account.id"));
        assert!(!keys.contains(&"service.name"));
    }
}
