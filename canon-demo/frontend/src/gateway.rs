//! Gateway URL resolution and HTTP helpers.
//!
//! The base URL is determined in order of priority:
//! 1. Build-time `GATEWAY_URL` env var (baked into the WASM binary).
//! 2. Runtime `window.CANON_GATEWAY_URL` JS global (set by `config.js`).
//! 3. Current origin (when served by the gateway itself).

use wasm_bindgen::prelude::*;

/// Returns the gateway base URL without a trailing slash.
///
/// Resolution order:
/// 1. Compile-time `GATEWAY_URL` env var.
/// 2. `window.CANON_GATEWAY_URL` JS global.
/// 3. Current `window.location.origin`.
/// 4. Fallback `"http://localhost:3000"`.
pub fn gateway_base_url() -> String {
    // 1. Compile-time override
    if let Some(url) = option_env!("GATEWAY_URL") {
        if !url.is_empty() {
            return url.trim_end_matches('/').to_owned();
        }
    }

    // 2. Runtime JS global
    if let Some(url) = js_global_gateway_url() {
        if !url.is_empty() {
            return url.trim_end_matches('/').to_owned();
        }
    }

    // 3. Current origin
    if let Some(window) = web_sys::window() {
        if let Ok(origin) = window.location().origin() {
            if !origin.is_empty() && origin != "null" {
                return origin;
            }
        }
    }

    // 4. Fallback
    "http://localhost:3000".to_owned()
}

/// Read `window.CANON_GATEWAY_URL` from JS.
fn js_global_gateway_url() -> Option<String> {
    let window = web_sys::window()?;
    let val = js_sys::Reflect::get(&window, &JsValue::from_str("CANON_GATEWAY_URL")).ok()?;
    val.as_string()
}
