mod admin;
mod cargo;
mod fleet;
mod navigation;
mod replay;
mod station;
mod supply;
mod ws;

use axum::Router;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(fleet::router())
        .merge(cargo::router())
        .merge(navigation::router())
        .merge(supply::router())
        .merge(station::router())
        .merge(admin::router())
        .merge(replay::router())
        .merge(ws::router())
        .with_state(state)
}
