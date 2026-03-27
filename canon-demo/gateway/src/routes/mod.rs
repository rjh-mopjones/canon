mod admin;
mod cargo;
mod debug;
mod fleet;
mod game;
mod health;
mod navigation;
mod replay;
mod session;
mod station;
mod supply;
mod ws;

use axum::{Extension, Router};

use crate::middleware::auth::auth_gate;
use crate::middleware::rate_limit::{rate_limit, RateLimiterState};
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let rate_limiter = RateLimiterState::new(30);

    Router::new()
        .merge(health::router())
        .merge(fleet::router())
        .merge(cargo::router())
        .merge(navigation::router())
        .merge(supply::router())
        .merge(station::router())
        .merge(admin::router())
        .merge(debug::router())
        .merge(game::router())
        .merge(replay::router())
        .merge(session::router())
        .merge(ws::router())
        .with_state(state)
        .layer(axum::middleware::from_fn(auth_gate))
        .layer(axum::middleware::from_fn(rate_limit))
        .layer(Extension(rate_limiter))
}
