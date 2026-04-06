use crate::config::LabelCardinalityConfig;
use crate::error::Result;
use crate::message::attributes::AttributeGenerator;
use crate::message::fake_data::FakeDataGenerator;
use crate::message::metrics_types::{OTLPMetricsMessage, GAUGE_METRIC_NAMES, SUM_METRIC_NAMES};
use crate::transport::types::{MessagePayload, MessageType};
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct OTLPMetricsMessageGenerator {
    attributes: AttributeGenerator,
}

impl OTLPMetricsMessageGenerator {
    pub fn new(source: String) -> Self {
        Self {
            attributes: AttributeGenerator::new(source, LabelCardinalityConfig::default()),
        }
    }

    pub fn new_with_cardinality(source: String, label_cardinality: LabelCardinalityConfig) -> Self {
        Self {
            attributes: AttributeGenerator::new(source, label_cardinality),
        }
    }

    fn build_message(
        &self,
        payload: MessagePayload,
        tenant_id: String,
        project_id: String,
        message_type: MessageType,
    ) -> OTLPMetricsMessage {
        OTLPMetricsMessage::new(
            payload,
            tenant_id,
            project_id,
            self.attributes.source().to_string(),
            message_type,
        )
    }

    fn wrap_metric_in_otlp(
        &self,
        project_id: &str,
        cloud_account_id: &str,
        service_name: &str,
        metric: Value,
    ) -> Value {
        let mut rng = rand::thread_rng();
        let resource_attributes =
            self.attributes
                .generate_resource_attributes_pairs(project_id, cloud_account_id, service_name);
        let scope_attributes = self.attributes.generate_scope_attributes_pairs(service_name);

        json!({
            "resource": {
                "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&resource_attributes),
                "droppedAttributesCount": rng.gen_range(0..4)
            },
            "scopeMetrics": [{
                "scope": {
                    "name": format!("io.trihub.{}", service_name.replace('-', ".")),
                    "version": format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&scope_attributes),
                    "droppedAttributesCount": rng.gen_range(0..3)
                },
                "metrics": [metric],
                "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
            }],
            "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
        })
    }

    fn generate_metric_attributes(&self) -> Vec<(String, String)> {
        let mut rng = rand::thread_rng();
        let mut attrs = Vec::new();
        if rng.gen::<f32>() > 0.5 {
            attrs.push((
                "http.method".to_string(),
                FakeDataGenerator::generate_http_method(),
            ));
        }
        if rng.gen::<f32>() > 0.6 {
            attrs.push((
                "http.status_code".to_string(),
                FakeDataGenerator::generate_http_status_code().to_string(),
            ));
        }
        self.attributes.normalize_attribute_pairs(attrs)
    }

    pub fn generate_gauge_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let name = GAUGE_METRIC_NAMES.choose(&mut rng).unwrap();
        let value: f64 = rng.gen_range(0.0..100.0);
        let attrs = self.generate_metric_attributes();

        let metric = json!({
            "name": name,
            "unit": "1",
            "gauge": {
                "dataPoints": [{
                    "timeUnixNano": timestamp_ns.to_string(),
                    "asDouble": value,
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&attrs)
                }]
            }
        });

        let resource_metric =
            self.wrap_metric_in_otlp(&project_id, &cloud_account_id, &service_name, metric);
        let otlp_message = json!({ "resourceMetrics": [resource_metric] });
        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }

    pub fn generate_sum_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let start_timestamp_ns = timestamp_ns - rng.gen_range(1_000_000_000..60_000_000_000i64);
        let name = SUM_METRIC_NAMES.choose(&mut rng).unwrap();
        let value: f64 = rng.gen_range(100.0..100_000.0);
        let attrs = self.generate_metric_attributes();

        let metric = json!({
            "name": name,
            "unit": "1",
            "sum": {
                "dataPoints": [{
                    "startTimeUnixNano": start_timestamp_ns.to_string(),
                    "timeUnixNano": timestamp_ns.to_string(),
                    "asDouble": value,
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&attrs)
                }],
                "aggregationTemporality": 2,
                "isMonotonic": true
            }
        });

        let resource_metric =
            self.wrap_metric_in_otlp(&project_id, &cloud_account_id, &service_name, metric);
        let otlp_message = json!({ "resourceMetrics": [resource_metric] });
        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }

    pub fn generate_histogram_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        use crate::message::metrics_types::{HISTOGRAM_BOUNDARIES, HISTOGRAM_METRIC_NAMES};

        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let start_timestamp_ns = timestamp_ns - rng.gen_range(1_000_000_000..60_000_000_000i64);
        let name = HISTOGRAM_METRIC_NAMES.choose(&mut rng).unwrap();
        let attrs = self.generate_metric_attributes();

        let num_buckets = HISTOGRAM_BOUNDARIES.len() + 1;
        let bucket_counts: Vec<u64> = (0..num_buckets)
            .map(|i| {
                let weight = if i < 3 {
                    5
                } else if i < 6 {
                    20
                } else if i < 8 {
                    10
                } else {
                    2
                };
                rng.gen_range(0..weight)
            })
            .collect();

        let count: u64 = bucket_counts.iter().sum();
        let min_val: f64 = rng.gen_range(0.0..5.0);
        let max_val: f64 = rng.gen_range(500.0..1500.0);
        let sum_val: f64 = count as f64 * rng.gen_range(50.0..200.0);

        let metric = json!({
            "name": name, "unit": "ms",
            "histogram": {
                "dataPoints": [{
                    "startTimeUnixNano": start_timestamp_ns.to_string(),
                    "timeUnixNano": timestamp_ns.to_string(),
                    "count": count, "sum": sum_val, "min": min_val, "max": max_val,
                    "bucketCounts": bucket_counts, "explicitBounds": HISTOGRAM_BOUNDARIES,
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&attrs),
                }],
                "aggregationTemporality": 2
            }
        });

        let resource_metric =
            self.wrap_metric_in_otlp(&project_id, &cloud_account_id, &service_name, metric);
        let otlp_message = json!({ "resourceMetrics": [resource_metric] });
        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }

    pub fn generate_exponential_histogram_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        use crate::message::metrics_types::EXPONENTIAL_HISTOGRAM_METRIC_NAMES;

        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let start_timestamp_ns = timestamp_ns - rng.gen_range(1_000_000_000..60_000_000_000i64);
        let name = EXPONENTIAL_HISTOGRAM_METRIC_NAMES.choose(&mut rng).unwrap();
        let attrs = self.generate_metric_attributes();

        let scale: i32 = rng.gen_range(1..8);
        let num_positive_buckets = rng.gen_range(5..15);
        let positive_bucket_counts: Vec<u64> =
            (0..num_positive_buckets).map(|_| rng.gen_range(0..20)).collect();
        let zero_count: u64 = rng.gen_range(0..5);
        let positive_offset: i32 = rng.gen_range(0..5);

        let count: u64 = positive_bucket_counts.iter().sum::<u64>() + zero_count;
        let min_val: f64 = rng.gen_range(0.0..5.0);
        let max_val: f64 = rng.gen_range(500.0..1500.0);
        let sum_val: f64 = count as f64 * rng.gen_range(50.0..200.0);

        let empty_counts: &[u64] = &[];
        let metric = json!({
            "name": name, "unit": "ms",
            "exponentialHistogram": {
                "dataPoints": [{
                    "startTimeUnixNano": start_timestamp_ns.to_string(),
                    "timeUnixNano": timestamp_ns.to_string(),
                    "count": count, "sum": sum_val, "min": min_val, "max": max_val,
                    "scale": scale, "zeroCount": zero_count,
                    "positive": { "offset": positive_offset, "bucketCounts": positive_bucket_counts },
                    "negative": { "offset": 0, "bucketCounts": empty_counts },
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&attrs),
                }],
                "aggregationTemporality": 2
            }
        });

        let resource_metric =
            self.wrap_metric_in_otlp(&project_id, &cloud_account_id, &service_name, metric);
        let otlp_message = json!({ "resourceMetrics": [resource_metric] });
        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }

    pub fn generate_summary_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        use crate::message::metrics_types::{SUMMARY_METRIC_NAMES, SUMMARY_QUANTILES};

        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let start_timestamp_ns = timestamp_ns - rng.gen_range(1_000_000_000..60_000_000_000i64);
        let name = SUMMARY_METRIC_NAMES.choose(&mut rng).unwrap();
        let attrs = self.generate_metric_attributes();

        let base_latency: f64 = rng.gen_range(10.0..100.0);
        let mut current = base_latency;
        let quantile_values: Vec<Value> = SUMMARY_QUANTILES
            .iter()
            .map(|&quantile| {
                current += rng.gen_range(5.0..50.0);
                json!({ "quantile": quantile, "value": current })
            })
            .collect();

        let count: u64 = rng.gen_range(100..10_000);
        let sum_val: f64 = count as f64 * rng.gen_range(50.0..200.0);

        let metric = json!({
            "name": name, "unit": "ms",
            "summary": {
                "dataPoints": [{
                    "startTimeUnixNano": start_timestamp_ns.to_string(),
                    "timeUnixNano": timestamp_ns.to_string(),
                    "count": count, "sum": sum_val,
                    "quantileValues": quantile_values,
                    "attributes": AttributeGenerator::attributes_pairs_to_dict_list(&attrs),
                }]
            }
        });

        let resource_metric =
            self.wrap_metric_in_otlp(&project_id, &cloud_account_id, &service_name, metric);
        let otlp_message = json!({ "resourceMetrics": [resource_metric] });
        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }

    pub fn generate_random_metric_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        let mut rng = rand::thread_rng();
        let roll: f32 = rng.gen();
        if roll < 0.30 {
            self.generate_gauge_message(tenant_id, cloud_account_id, service_name)
        } else if roll < 0.60 {
            self.generate_sum_message(tenant_id, cloud_account_id, service_name)
        } else if roll < 0.85 {
            self.generate_histogram_message(tenant_id, cloud_account_id, service_name)
        } else if roll < 0.95 {
            self.generate_exponential_histogram_message(tenant_id, cloud_account_id, service_name)
        } else {
            self.generate_summary_message(tenant_id, cloud_account_id, service_name)
        }
    }

    pub fn generate_protobuf_metric_message(
        &self,
        tenant_id: String,
        cloud_account_id: String,
        service_name: String,
    ) -> Result<OTLPMetricsMessage> {
        use crate::message::metrics_types::GAUGE_METRIC_NAMES;
        use crate::pb::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
        use crate::pb::opentelemetry::proto::common::v1::{
            any_value, AnyValue, InstrumentationScope, KeyValue,
        };
        use crate::pb::opentelemetry::proto::metrics::v1::{
            metric, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            number_data_point,
        };
        use crate::pb::opentelemetry::proto::resource::v1::Resource;
        use prost::Message;

        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let now = Utc::now();
        let timestamp_ns = now.timestamp_nanos_opt().unwrap_or(0) as u64;
        let start_timestamp_ns = timestamp_ns - rng.gen_range(1_000_000_000..60_000_000_000u64);

        let resource_attrs = self.attributes.generate_resource_attributes_pairs(
            &project_id,
            &cloud_account_id,
            &service_name,
        );
        let resource_kv: Vec<KeyValue> = resource_attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(v.clone())),
                }),
            })
            .collect();

        let scope_attrs = self.attributes.generate_scope_attributes_pairs(&service_name);
        let scope_kv: Vec<KeyValue> = scope_attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(v.clone())),
                }),
            })
            .collect();

        let name = GAUGE_METRIC_NAMES.choose(&mut rng).unwrap().to_string();
        let value: f64 = rng.gen_range(0.0..100.0);

        let data_point = NumberDataPoint {
            time_unix_nano: timestamp_ns,
            start_time_unix_nano: start_timestamp_ns,
            value: Some(number_data_point::Value::AsDouble(value)),
            attributes: vec![],
            exemplars: vec![],
            flags: 0,
        };

        let proto_metric = Metric {
            name,
            description: String::new(),
            unit: "1".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![data_point],
            })),
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_kv,
                    dropped_attributes_count: rng.gen_range(0..4),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: format!("io.trihub.{}", service_name.replace('-', ".")),
                        version: format!(
                            "1.{}.{}",
                            rng.gen_range(0..10),
                            rng.gen_range(0..10)
                        ),
                        attributes: scope_kv,
                        dropped_attributes_count: rng.gen_range(0..3),
                    }),
                    metrics: vec![proto_metric],
                    schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                }],
                schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
            }],
        };

        let mut buf = Vec::new();
        request.encode(&mut buf)?;
        Ok(self.build_message(
            MessagePayload::Protobuf(buf),
            tenant_id,
            project_id,
            MessageType::Valid,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::types::MessagePayload;

    fn test_generator() -> OTLPMetricsMessageGenerator {
        OTLPMetricsMessageGenerator::new("test-source".to_string())
    }

    #[test]
    fn gauge_json_has_valid_otlp_structure() {
        let gen = test_generator();
        let msg = gen
            .generate_gauge_message(
                "tenant1".to_string(),
                "tenant1-acc-01".to_string(),
                "tenant1-svc-01".to_string(),
            )
            .unwrap();
        let MessagePayload::Json(json) = msg.payload() else {
            panic!("Expected JSON payload")
        };
        let resource_metrics = json.get("resourceMetrics").unwrap().as_array().unwrap();
        assert_eq!(resource_metrics.len(), 1);
        let scope_metrics = resource_metrics[0]
            .get("scopeMetrics")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(scope_metrics.len(), 1);
        let metrics = scope_metrics[0].get("metrics").unwrap().as_array().unwrap();
        assert_eq!(metrics.len(), 1);
        let metric = &metrics[0];
        assert!(metric.get("name").unwrap().as_str().unwrap().contains('.'));
        assert!(metric.get("gauge").is_some());
        let data_points = metric["gauge"]["dataPoints"].as_array().unwrap();
        assert_eq!(data_points.len(), 1);
        assert!(data_points[0].get("asDouble").is_some());
        assert!(data_points[0].get("timeUnixNano").is_some());
    }

    #[test]
    fn sum_json_has_valid_otlp_structure() {
        let gen = test_generator();
        let msg = gen
            .generate_sum_message(
                "tenant1".to_string(),
                "tenant1-acc-01".to_string(),
                "tenant1-svc-01".to_string(),
            )
            .unwrap();
        let MessagePayload::Json(json) = msg.payload() else {
            panic!("Expected JSON payload")
        };
        let metrics = json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap();
        let metric = &metrics[0];
        assert!(metric.get("sum").is_some());
        let sum = &metric["sum"];
        assert_eq!(sum["isMonotonic"].as_bool().unwrap(), true);
        assert_eq!(sum["aggregationTemporality"].as_u64().unwrap(), 2);
        let data_points = sum["dataPoints"].as_array().unwrap();
        assert_eq!(data_points.len(), 1);
        assert!(data_points[0].get("asDouble").is_some());
        assert!(data_points[0]["asDouble"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn gauge_value_is_within_realistic_bounds() {
        let gen = test_generator();
        for _ in 0..50 {
            let msg = gen
                .generate_gauge_message(
                    "t1".to_string(),
                    "a1".to_string(),
                    "s1".to_string(),
                )
                .unwrap();
            let MessagePayload::Json(json) = msg.payload() else {
                panic!()
            };
            let value = json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]
                ["dataPoints"][0]["asDouble"]
                .as_f64()
                .unwrap();
            assert!(
                value >= 0.0 && value <= 100.0,
                "gauge value {} out of bounds",
                value
            );
        }
    }

    #[test]
    fn histogram_json_has_valid_otlp_structure() {
        let gen = test_generator();
        let msg = gen
            .generate_histogram_message("t1".to_string(), "a1".to_string(), "s1".to_string())
            .unwrap();
        let MessagePayload::Json(json) = msg.payload() else {
            panic!()
        };
        let metric = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert!(metric.get("histogram").is_some());
        let histogram = &metric["histogram"];
        assert_eq!(histogram["aggregationTemporality"].as_u64().unwrap(), 2);
        let dp = &histogram["dataPoints"][0];
        let bucket_counts = dp["bucketCounts"].as_array().unwrap();
        let boundaries = dp["explicitBounds"].as_array().unwrap();
        assert_eq!(bucket_counts.len(), boundaries.len() + 1);
        let count = dp["count"].as_u64().unwrap();
        let sum: u64 = bucket_counts.iter().map(|c| c.as_u64().unwrap()).sum();
        assert_eq!(count, sum);
        assert!(dp.get("sum").is_some());
        assert!(dp.get("min").is_some());
        assert!(dp.get("max").is_some());
        assert!(dp["min"].as_f64().unwrap() <= dp["max"].as_f64().unwrap());
    }

    #[test]
    fn exponential_histogram_json_has_valid_structure() {
        let gen = test_generator();
        let msg = gen
            .generate_exponential_histogram_message(
                "t1".to_string(),
                "a1".to_string(),
                "s1".to_string(),
            )
            .unwrap();
        let MessagePayload::Json(json) = msg.payload() else {
            panic!()
        };
        let metric = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert!(metric.get("exponentialHistogram").is_some());
        let eh = &metric["exponentialHistogram"];
        assert_eq!(eh["aggregationTemporality"].as_u64().unwrap(), 2);
        let dp = &eh["dataPoints"][0];
        assert!(dp.get("scale").is_some());
        assert!(dp.get("zeroCount").is_some());
        assert!(dp.get("positive").is_some());
        assert!(dp["positive"].get("bucketCounts").is_some());
        assert!(dp.get("count").is_some());
        assert!(dp.get("sum").is_some());
        assert!(dp.get("min").is_some());
        assert!(dp.get("max").is_some());
        assert!(dp["min"].as_f64().unwrap() <= dp["max"].as_f64().unwrap());
    }

    #[test]
    fn summary_json_has_monotonically_ordered_quantiles() {
        let gen = test_generator();
        for _ in 0..20 {
            let msg = gen
                .generate_summary_message("t1".to_string(), "a1".to_string(), "s1".to_string())
                .unwrap();
            let MessagePayload::Json(json) = msg.payload() else {
                panic!()
            };
            let metric = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
            assert!(metric.get("summary").is_some());
            let dp = &metric["summary"]["dataPoints"][0];
            let quantiles = dp["quantileValues"].as_array().unwrap();
            assert_eq!(quantiles.len(), 4);
            let values: Vec<f64> = quantiles
                .iter()
                .map(|q| q["value"].as_f64().unwrap())
                .collect();
            for window in values.windows(2) {
                assert!(
                    window[0] <= window[1],
                    "quantiles not monotonic: {:?}",
                    values
                );
            }
        }
    }

    #[test]
    fn random_metric_produces_all_types_over_many_iterations() {
        let gen = test_generator();
        let mut seen_gauge = false;
        let mut seen_sum = false;
        let mut seen_histogram = false;
        let mut seen_exp_histogram = false;
        let mut seen_summary = false;

        for _ in 0..200 {
            let msg = gen
                .generate_random_metric_message(
                    "t1".to_string(),
                    "a1".to_string(),
                    "s1".to_string(),
                )
                .unwrap();
            let MessagePayload::Json(json) = msg.payload() else {
                panic!()
            };
            let metric = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
            if metric.get("gauge").is_some() {
                seen_gauge = true;
            }
            if metric.get("sum").is_some() {
                seen_sum = true;
            }
            if metric.get("histogram").is_some() {
                seen_histogram = true;
            }
            if metric.get("exponentialHistogram").is_some() {
                seen_exp_histogram = true;
            }
            if metric.get("summary").is_some() {
                seen_summary = true;
            }
        }

        assert!(seen_gauge, "never saw gauge");
        assert!(seen_sum, "never saw sum");
        assert!(seen_histogram, "never saw histogram");
        assert!(seen_exp_histogram, "never saw exponential histogram");
        assert!(seen_summary, "never saw summary");
    }

    #[test]
    fn protobuf_metric_message_is_decodable() {
        use crate::pb::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
        use prost::Message;

        let gen = test_generator();
        let msg = gen
            .generate_protobuf_metric_message(
                "t1".to_string(),
                "a1".to_string(),
                "s1".to_string(),
            )
            .unwrap();
        let MessagePayload::Protobuf(bytes) = msg.payload() else {
            panic!("Expected Protobuf payload")
        };
        let decoded = ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.resource_metrics.len(), 1);
        let scope_metrics = &decoded.resource_metrics[0].scope_metrics;
        assert_eq!(scope_metrics.len(), 1);
        assert_eq!(scope_metrics[0].metrics.len(), 1);
    }
}
