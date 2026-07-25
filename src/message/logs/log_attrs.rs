//! Log-record content generation: scope attributes, per-record attributes (with cardinality
//! normalization), and record bodies. Signal-specific fake-content helpers kept out of the
//! generator's orchestration, mirroring how the trace side keeps span population in its profile.

use crate::config::AttributesCardinalityConfig;
use crate::message::fake_data::FakeDataGenerator;
use crate::message::resource_attrs::{normalize_by_cardinality, DEFAULT_SERVICE_NAME};
use rand::seq::SliceRandom;
use rand::Rng;

/// Instrumentation-scope attributes for a shard's `ScopeLogs`.
pub(crate) fn generate_scope_attributes_pairs(service_name: Option<&str>) -> Vec<(String, String)> {
    let mut rng = rand::thread_rng();
    let mut attrs = Vec::new();
    if let Some(svc) = service_name {
        attrs.push(("library.name".to_string(), format!("trihub-{}", svc)));
    }
    attrs.push((
        "library.version".to_string(),
        format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
    ));
    attrs
}

/// Per-record log attributes, with this run's cardinality policy already applied.
pub(crate) fn generate_log_attributes_pairs(
    cardinality: &AttributesCardinalityConfig,
    request_id: &str,
    thread_id: &str,
) -> Vec<(String, String)> {
    let mut rng = rand::thread_rng();
    let mut log_attributes = Vec::new();

    if rng.gen::<f32>() > 0.5 {
        log_attributes.push((
            "http.method".to_string(),
            FakeDataGenerator::generate_http_method(),
        ));
    }

    if rng.gen::<f32>() > 0.6 {
        log_attributes.push((
            "http.status_code".to_string(),
            FakeDataGenerator::generate_http_status_code().to_string(),
        ));
    }

    if rng.gen::<f32>() > 0.7 {
        log_attributes.push(("user.id".to_string(), FakeDataGenerator::generate_uuid()));
    }

    log_attributes.push(("request.id".to_string(), request_id.to_string()));
    log_attributes.push(("thread.id".to_string(), thread_id.to_string()));

    normalize_attribute_pairs(cardinality, log_attributes)
}

/// Apply `cardinality` to each log-specific attribute pair.
fn normalize_attribute_pairs(
    cardinality: &AttributesCardinalityConfig,
    pairs: Vec<(String, String)>,
) -> Vec<(String, String)> {
    pairs
        .into_iter()
        .map(|(key, value)| {
            let normalized = normalize_by_cardinality(cardinality, &key, &value);
            (key, normalized)
        })
        .collect()
}

/// A severity-appropriate record body for `service_name` (falls back to the default service name).
pub(crate) fn generate_log_body(severity_text: &str, service_name: Option<&str>) -> String {
    let mut rng = rand::thread_rng();

    let bodies = match severity_text {
        "INFO" => vec![
            format!(
                "Request processed successfully in {}ms",
                rng.gen_range(10..500)
            ),
            format!(
                "User {} authenticated successfully",
                FakeDataGenerator::generate_uuid()
            ),
            format!(
                "Database connection established to {}",
                FakeDataGenerator::generate_host_name()
            ),
            format!(
                "Cache hit for key {}",
                &FakeDataGenerator::generate_uuid()[..8]
            ),
            format!(
                "Health check passed for service {}",
                service_name.unwrap_or(DEFAULT_SERVICE_NAME)
            ),
        ],
        "WARN" => vec![
            format!("High memory usage detected: {}%", rng.gen_range(70..96)),
            format!("Slow query detected: {}ms", rng.gen_range(1000..5000)),
            format!(
                "Connection pool near capacity: {}/100",
                rng.gen_range(80..100)
            ),
            format!(
                "Rate limit approaching for user {}",
                FakeDataGenerator::generate_uuid()
            ),
            format!(
                "Deprecated API endpoint accessed: /api/v1/{}",
                FakeDataGenerator::generate_sentence()
                    .split_whitespace()
                    .next()
                    .unwrap_or("endpoint")
            ),
        ],
        "ERROR" => vec![
            format!(
                "Database connection failed: {}",
                FakeDataGenerator::generate_sentence()
            ),
            format!(
                "Failed to process request: {}",
                FakeDataGenerator::generate_sentence()
            ),
            format!(
                "Authentication failed for user {}",
                FakeDataGenerator::generate_email()
            ),
            format!(
                "External API call failed: HTTP {}",
                [500, 502, 503, 504].choose(&mut rng).unwrap()
            ),
            format!(
                "Queue processing error: {}",
                FakeDataGenerator::generate_sentence()
            ),
        ],
        _ => vec!["Generic log message".to_string()],
    };

    bodies.choose(&mut rng).unwrap().clone()
}
