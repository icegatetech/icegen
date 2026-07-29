const PROTOS: [&str; 2] = [
    "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
    "proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/pb")
        .compile(&PROTOS, &["proto/"])?;

    // OTLP *server* stubs, used only by the gRPC transport contract test to stand up a real
    // collector. They land in OUT_DIR (not `src/pb`) so the library never carries a server it does
    // not implement, and `extern_path` points them at the checked-in message types so every OTLP
    // struct still exists exactly once.
    let mut server = tonic_build::configure()
        .build_server(true)
        .build_client(false);
    for package in [
        "common",
        "resource",
        "logs",
        "trace",
        "collector.logs",
        "collector.trace",
    ] {
        server = server.extern_path(
            format!(".opentelemetry.proto.{package}.v1"),
            format!(
                "crate::pb::opentelemetry::proto::{}::v1",
                package.replace('.', "::")
            ),
        );
    }
    server.compile(&PROTOS, &["proto/"])?;

    Ok(())
}
