use crate::error::{GeneratorError, Result};
use crate::message::types::Signal;
use std::collections::HashMap;

/// The raw endpoint inputs as they arrive from the CLI and the environment, before a transport flow
/// is chosen. Exists so [`Destination::from_flags`] stays a pure function over the flags and can be
/// tested without building a whole [`crate::config::OtelConfig`].
#[derive(Debug, Clone, Copy)]
pub struct DestinationFlags<'a> {
    /// `--transport` / `OTEL_TRANSPORT`: selects the flow. Anything but `http`/`grpc` is an error.
    pub transport: &'a str,
    pub dry_run: bool,
    /// `--use-protobuf`: honoured by the HTTP flow; the gRPC flow is protobuf-only regardless. A dry
    /// run previews its flow's encoding, so it follows whichever of the two applies.
    pub protobuf: bool,
    pub grpc_endpoint: Option<&'a str>,
    pub http_logs_endpoint: Option<&'a str>,
    pub http_traces_endpoint: Option<&'a str>,
    /// Configured signals; the HTTP flow needs one URL per entry.
    pub signals: &'a [Signal],
}

/// The transport flow named by `--transport`, parsed before any endpoint is read so an unknown
/// value fails the same way for every run, a dry run included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFlow {
    Grpc,
    Http,
}

impl TransportFlow {
    /// Parse the `--transport` / `OTEL_TRANSPORT` value.
    ///
    /// # Errors
    ///
    /// [`GeneratorError::InvalidTransport`] for anything but `http` / `grpc`.
    #[allow(clippy::result_large_err)]
    fn from_flag(raw: &str) -> Result<Self> {
        match raw {
            "grpc" => Ok(Self::Grpc),
            "http" => Ok(Self::Http),
            other => Err(GeneratorError::InvalidTransport(format!(
                "Invalid transport '{other}', must be 'http' or 'grpc'"
            ))),
        }
    }

    /// The encoding this flow puts on the wire: gRPC is protobuf-only, HTTP honours
    /// `--use-protobuf`. A dry run previews what the flow would have sent, so it resolves the
    /// encoding through the same rule instead of reading the flag directly.
    fn want_protobuf(self, protobuf_flag: bool) -> bool {
        matches!(self, Self::Grpc) || protobuf_flag
    }

    /// The flow's name as it is spelled in `--transport` and reported in the startup banner.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http",
        }
    }
}

/// Where a run sends its signals, resolved once by [`Self::from_flags`].
///
/// One variant per transport flow, so a field exists only for the flow that has a route for it: the
/// gRPC flow cannot read an HTTP URL, and the HTTP flow cannot read the gRPC endpoint. That is what
/// removes the transport-conditional validation this type replaced — one `.env` can hold both flows'
/// variables and neither can leak into the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// No network at all: messages are generated and printed. `flow` is the transport the run would
    /// have opened; it is kept because it fixes the encoding of the preview, so a gRPC dry run
    /// prints the protobuf payload that a live gRPC run would send.
    DryRun { flow: TransportFlow, protobuf: bool },
    /// One channel to one endpoint; signals are separated by their OTLP service
    /// (`LogsService` / `TraceService`), which is why a second endpoint has nowhere to go.
    Grpc { endpoint: String },
    /// One URL per signal. `endpoints` holds exactly the configured signals — no fallback between
    /// them, so a trace request can never reach a `/v1/logs` path.
    Http {
        endpoints: HashMap<Signal, String>,
        protobuf: bool,
    },
}

impl Destination {
    /// Resolve the flow for this run.
    ///
    /// # Errors
    ///
    /// [`GeneratorError::InvalidTransport`] for an unknown transport;
    /// [`GeneratorError::InvalidConfiguration`] when the chosen flow is missing an endpoint it
    /// needs. A dry run needs none.
    #[allow(clippy::result_large_err)]
    pub fn from_flags(flags: DestinationFlags<'_>) -> Result<Self> {
        let flow = TransportFlow::from_flag(flags.transport)?;
        if flags.dry_run {
            return Ok(Self::DryRun {
                flow,
                protobuf: flow.want_protobuf(flags.protobuf),
            });
        }

        match flow {
            TransportFlow::Grpc => {
                let endpoint = parse_flag_value(flags.grpc_endpoint).ok_or_else(|| {
                    GeneratorError::InvalidConfiguration(
                        "--grpc-endpoint / OTEL_GRPC_ENDPOINT is required for --transport grpc"
                            .to_string(),
                    )
                })?;
                Ok(Self::Grpc {
                    endpoint: endpoint.to_string(),
                })
            }
            TransportFlow::Http => {
                let mut endpoints = HashMap::new();
                for signal in flags.signals {
                    let (raw, flag) = match signal {
                        Signal::Logs => (
                            flags.http_logs_endpoint,
                            "--http-logs-endpoint / OTEL_HTTP_LOGS_ENDPOINT",
                        ),
                        Signal::Traces => (
                            flags.http_traces_endpoint,
                            "--http-traces-endpoint / OTEL_HTTP_TRACES_ENDPOINT",
                        ),
                    };
                    let endpoint = parse_flag_value(raw).ok_or_else(|| {
                        GeneratorError::InvalidConfiguration(format!(
                            "{flag} is required for --transport http with signal '{}'",
                            signal.as_str()
                        ))
                    })?;
                    endpoints.insert(*signal, endpoint.to_string());
                }
                Ok(Self::Http {
                    endpoints,
                    protobuf: flags.protobuf,
                })
            }
        }
    }

    /// Whether this run generates without opening any transport.
    pub fn is_dry_run(&self) -> bool {
        matches!(self, Self::DryRun { .. })
    }

    /// Whether the protobuf encoders are selected. gRPC is protobuf-only on the wire, the HTTP flow
    /// honours `--use-protobuf`, and a dry run reports the choice its previewed [`TransportFlow`]
    /// already made in [`Self::from_flags`] — so a gRPC dry run wants protobuf whatever the flag.
    pub fn want_protobuf(&self) -> bool {
        match self {
            Self::Grpc { .. } => true,
            Self::DryRun { protobuf, .. } | Self::Http { protobuf, .. } => *protobuf,
        }
    }
}

/// Read one endpoint flag: a value set to a blank string carries no intent, so it counts as unset.
fn parse_flag_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags<'a>(transport: &'a str, signals: &'a [Signal]) -> DestinationFlags<'a> {
        DestinationFlags {
            transport,
            dry_run: false,
            protobuf: false,
            grpc_endpoint: None,
            http_logs_endpoint: None,
            http_traces_endpoint: None,
            signals,
        }
    }

    /// Regression: one `.env` serving both flows carries the HTTP URLs while OTEL_TRANSPORT=grpc.
    /// The gRPC flow has no field for them, so the run must resolve instead of failing.
    #[test]
    fn grpc_flow_resolves_while_http_endpoints_are_set() {
        let signals = [Signal::Logs, Signal::Traces];
        let mut raw = flags("grpc", &signals);
        raw.grpc_endpoint = Some("http://localhost:4317");
        raw.http_logs_endpoint = Some("http://localhost:4318/v1/logs");
        raw.http_traces_endpoint = Some("http://localhost:4318/v1/traces");

        let destination = Destination::from_flags(raw).expect("grpc flow must ignore HTTP URLs");
        assert!(matches!(
            &destination,
            Destination::Grpc { endpoint } if endpoint == "http://localhost:4317"
        ));
    }

    #[test]
    fn grpc_flow_without_its_endpoint_is_rejected() {
        let signals = [Signal::Logs];
        let mut raw = flags("grpc", &signals);
        raw.http_logs_endpoint = Some("http://localhost:4318/v1/logs");
        assert!(matches!(
            Destination::from_flags(raw),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    /// A variable set to an empty string carries no intent and counts as unset.
    #[test]
    fn grpc_flow_treats_blank_endpoint_as_unset() {
        let signals = [Signal::Logs];
        let mut raw = flags("grpc", &signals);
        raw.grpc_endpoint = Some("   ");
        assert!(matches!(
            Destination::from_flags(raw),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn http_flow_maps_each_selected_signal_to_its_own_url() {
        let signals = [Signal::Logs, Signal::Traces];
        let mut raw = flags("http", &signals);
        raw.http_logs_endpoint = Some("http://logs.example/v1/logs");
        raw.http_traces_endpoint = Some("http://traces.example/v1/traces");

        let Destination::Http {
            endpoints,
            protobuf,
        } = Destination::from_flags(raw).expect("both URLs are present")
        else {
            panic!("expected the HTTP flow");
        };
        assert_eq!(endpoints.len(), 2);
        assert_eq!(
            endpoints.get(&Signal::Logs).map(String::as_str),
            Some("http://logs.example/v1/logs")
        );
        assert_eq!(
            endpoints.get(&Signal::Traces).map(String::as_str),
            Some("http://traces.example/v1/traces")
        );
        assert!(!protobuf);
    }

    /// No fallback to the logs URL: a selected signal needs its own URL.
    #[test]
    fn http_flow_without_traces_url_is_rejected_when_traces_selected() {
        let signals = [Signal::Traces];
        let mut raw = flags("http", &signals);
        raw.http_logs_endpoint = Some("http://logs.example/v1/logs");
        assert!(matches!(
            Destination::from_flags(raw),
            Err(GeneratorError::InvalidConfiguration(_))
        ));
    }

    /// Signal selection, not transport crosstalk: a URL for an unselected signal is ignored.
    #[test]
    fn http_flow_ignores_url_of_unselected_signal() {
        let signals = [Signal::Logs];
        let mut raw = flags("http", &signals);
        raw.http_logs_endpoint = Some("http://logs.example/v1/logs");
        raw.http_traces_endpoint = Some("http://traces.example/v1/traces");
        raw.grpc_endpoint = Some("http://localhost:4317");

        let Destination::Http { endpoints, .. } =
            Destination::from_flags(raw).expect("logs-only HTTP run")
        else {
            panic!("expected the HTTP flow");
        };
        assert_eq!(endpoints.len(), 1);
        assert!(endpoints.contains_key(&Signal::Logs));
    }

    #[test]
    fn dry_run_with_http_transport_honours_the_protobuf_flag() {
        let signals = [Signal::Logs, Signal::Traces];
        for protobuf in [false, true] {
            let mut raw = flags("http", &signals);
            raw.dry_run = true;
            raw.protobuf = protobuf;

            let destination = Destination::from_flags(raw).expect("a dry run needs no endpoint");
            assert_eq!(
                destination,
                Destination::DryRun {
                    flow: TransportFlow::Http,
                    protobuf
                }
            );
            assert_eq!(destination.want_protobuf(), protobuf);
        }
    }

    /// A dry run previews what would go on the wire, and the gRPC wire is protobuf-only. Showing a
    /// JSON payload for `--transport grpc --dry-run` would preview something no run ever sends.
    #[test]
    fn dry_run_with_grpc_transport_keeps_the_protobuf_encoder() {
        let signals = [Signal::Logs, Signal::Traces];
        let mut raw = flags("grpc", &signals);
        raw.dry_run = true;

        let destination = Destination::from_flags(raw).expect("a dry run needs no endpoint");
        assert!(destination.is_dry_run());
        assert!(
            destination.want_protobuf(),
            "the gRPC flow is protobuf-only, dry run included"
        );
    }

    #[test]
    fn unknown_transport_is_rejected_as_invalid_transport() {
        let signals = [Signal::Logs];
        let raw = flags("kafka", &signals);
        assert!(matches!(
            Destination::from_flags(raw),
            Err(GeneratorError::InvalidTransport(_))
        ));
    }

    /// The transport names a flow even when nothing is opened, so an unknown one is still a typo
    /// worth failing on rather than a silently ignored setting.
    #[test]
    fn dry_run_rejects_unknown_transport() {
        let signals = [Signal::Logs];
        let mut raw = flags("kafka", &signals);
        raw.dry_run = true;
        assert!(matches!(
            Destination::from_flags(raw),
            Err(GeneratorError::InvalidTransport(_))
        ));
    }

    /// gRPC is protobuf-only on the wire; HTTP honours the flag.
    #[test]
    fn want_protobuf_is_forced_for_grpc_and_flag_driven_for_http() {
        let grpc = Destination::Grpc {
            endpoint: "http://localhost:4317".to_string(),
        };
        assert!(grpc.want_protobuf());

        let json_http = Destination::Http {
            endpoints: HashMap::from([(Signal::Logs, "http://logs.example".to_string())]),
            protobuf: false,
        };
        assert!(!json_http.want_protobuf());

        let proto_http = Destination::Http {
            endpoints: HashMap::from([(Signal::Logs, "http://logs.example".to_string())]),
            protobuf: true,
        };
        assert!(proto_http.want_protobuf());
    }
}
