// Generated protobuf code will be included here
pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!("opentelemetry.proto.collector.logs.v1.rs");
                }
            }
            pub mod trace {
                pub mod v1 {
                    include!("opentelemetry.proto.collector.trace.v1.rs");
                }
            }
        }
        pub mod common {
            pub mod v1 {
                include!("opentelemetry.proto.common.v1.rs");
            }
        }
        pub mod logs {
            pub mod v1 {
                include!("opentelemetry.proto.logs.v1.rs");
            }
        }
        pub mod resource {
            pub mod v1 {
                include!("opentelemetry.proto.resource.v1.rs");
            }
        }
        pub mod trace {
            pub mod v1 {
                include!("opentelemetry.proto.trace.v1.rs");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    use super::opentelemetry::proto::trace::v1::{ResourceSpans, Span};
    use prost::Message;

    #[test]
    fn trace_bindings_compile_and_round_trip() {
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let decoded = ExportTraceServiceRequest::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.resource_spans.len(), 1);
        let _span_name_field = Span::default().name; // type exists
    }
}
