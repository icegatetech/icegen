//! Shared OTLP attribute converters for JSON and protobuf, used by the log and trace encoders.

use crate::message::traces::trace_plan::AttrValue;
use crate::pb::opentelemetry::proto::common::v1::{any_value, AnyValue, ArrayValue, KeyValue};
use serde_json::{json, Value};

/// String pairs → OTLP/JSON attributes (`{"key","value":{"stringValue"}}`).
pub fn pairs_to_json_attrs(pairs: &[(String, String)]) -> Vec<Value> {
    pairs
        .iter()
        .map(|(key, value)| json!({ "key": key, "value": { "stringValue": value } }))
        .collect()
}

/// String pairs → protobuf `KeyValue` with `StringValue`.
pub fn pairs_to_proto_kv(pairs: &[(String, String)]) -> Vec<KeyValue> {
    pairs
        .iter()
        .map(|(key, value)| KeyValue {
            key: key.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.clone())),
            }),
        })
        .collect()
}

/// Typed value → OTLP/JSON value object.
pub fn attr_value_to_json(value: &AttrValue) -> Value {
    match value {
        AttrValue::Str(s) => json!({ "stringValue": s }),
        // uint64/int64-like fields are serialized as strings in OTLP/JSON.
        AttrValue::Int(i) => json!({ "intValue": i.to_string() }),
        AttrValue::Double(d) => json!({ "doubleValue": d }),
        AttrValue::Bool(b) => json!({ "boolValue": b }),
        AttrValue::StrArray(items) => json!({
            "arrayValue": {
                "values": items.iter().map(|s| json!({ "stringValue": s })).collect::<Vec<_>>()
            }
        }),
    }
}

/// Typed value → protobuf `AnyValue`.
pub fn attr_value_to_proto(value: &AttrValue) -> AnyValue {
    let inner = match value {
        AttrValue::Str(s) => any_value::Value::StringValue(s.clone()),
        AttrValue::Int(i) => any_value::Value::IntValue(*i),
        AttrValue::Double(d) => any_value::Value::DoubleValue(*d),
        AttrValue::Bool(b) => any_value::Value::BoolValue(*b),
        AttrValue::StrArray(items) => any_value::Value::ArrayValue(ArrayValue {
            values: items
                .iter()
                .map(|s| AnyValue {
                    value: Some(any_value::Value::StringValue(s.clone())),
                })
                .collect(),
        }),
    };
    AnyValue { value: Some(inner) }
}

/// Typed pairs → JSON attributes.
pub fn typed_pairs_to_json_attrs(pairs: &[(String, AttrValue)]) -> Vec<Value> {
    pairs
        .iter()
        .map(|(key, value)| json!({ "key": key, "value": attr_value_to_json(value) }))
        .collect()
}

/// Typed pairs → protobuf `KeyValue`.
pub fn typed_pairs_to_proto_kv(pairs: &[(String, AttrValue)]) -> Vec<KeyValue> {
    pairs
        .iter()
        .map(|(key, value)| KeyValue {
            key: key.clone(),
            value: Some(attr_value_to_proto(value)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::trace_plan::AttrValue;
    use crate::pb::opentelemetry::proto::common::v1::any_value;

    #[test]
    fn typed_attr_to_json() {
        assert_eq!(
            attr_value_to_json(&AttrValue::Int(5)),
            serde_json::json!({"intValue": "5"})
        );
        assert_eq!(
            attr_value_to_json(&AttrValue::Double(0.5)),
            serde_json::json!({"doubleValue": 0.5})
        );
        assert_eq!(
            attr_value_to_json(&AttrValue::Bool(true)),
            serde_json::json!({"boolValue": true})
        );
        assert_eq!(
            attr_value_to_json(&AttrValue::StrArray(vec!["a".into()])),
            serde_json::json!({"arrayValue": {"values": [{"stringValue": "a"}]}})
        );
    }

    #[test]
    fn typed_attr_to_proto_int() {
        let v = attr_value_to_proto(&AttrValue::Int(7));
        assert!(matches!(v.value, Some(any_value::Value::IntValue(7))));
    }
}
