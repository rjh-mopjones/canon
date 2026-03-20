use tracing::info;

/// Station definitions for startup registration.
///
/// Supply chain ring: Alpha←Delta, Beta←Alpha, Gamma←Beta, Delta←Gamma.
/// Each station is supplied by exactly one other station.
struct StationDef {
    name: &'static str,
    capacity_kg: f32,
    drain_rate_kg_per_s: f32,
}

const STATIONS: [StationDef; 4] = [
    StationDef {
        name: "Alpha Depot",
        capacity_kg: 5000.0,
        drain_rate_kg_per_s: 2.0,
    },
    StationDef {
        name: "Beta Relay",
        capacity_kg: 3000.0,
        drain_rate_kg_per_s: 1.5,
    },
    StationDef {
        name: "Gamma Outpost",
        capacity_kg: 2000.0,
        drain_rate_kg_per_s: 3.0,
    },
    StationDef {
        name: "Delta Prime",
        capacity_kg: 4000.0,
        drain_rate_kg_per_s: 1.0,
    },
];

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    for def in &STATIONS {
        info!(
            station = def.name,
            capacity_kg = def.capacity_kg,
            drain_rate = def.drain_rate_kg_per_s,
            "registering station"
        );
    }

    // ServiceBuilder::new().for_aggregate::<Station>().build()
    // will be wired once the runtime framework is complete.
    // Station aggregates are registered via commands submitted through the gateway.
    // The 4 stations (Alpha Depot, Beta Relay, Gamma Outpost, Delta Prime) are
    // created on startup by the gateway or an init script issuing RegisterStation
    // commands with the definitions above.

    info!("station-service ready");
}
