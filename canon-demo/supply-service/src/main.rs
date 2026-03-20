use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("supply-service ready");

    // ServiceBuilder::new()
    //     .for_aggregate::<supply_service::aggregate::Inventory>()
    //     .build()
    //
    // Runtime wiring will be added once infrastructure crates
    // (Kafka, YugabyteDB) are integrated.
}
