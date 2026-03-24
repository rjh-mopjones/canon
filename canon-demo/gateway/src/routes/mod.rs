mod admin;
mod cargo;
mod debug;
mod fleet;
mod health;
mod navigation;
mod replay;
mod session;
mod station;
mod supply;
mod ws;

use axum::Router;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(health::router())
        .merge(fleet::router())
        .merge(cargo::router())
        .merge(navigation::router())
        .merge(supply::router())
        .merge(station::router())
        .merge(admin::router())
        .merge(debug::router())
        .merge(replay::router())
        .merge(session::router())
        .merge(ws::router())
        .with_state(state)
}
