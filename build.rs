fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/pb")
        .compile(
            &["proto/opentelemetry/proto/collector/logs/v1/logs_service.proto"],
            &["proto/"],
        )?;

    Ok(())
}
