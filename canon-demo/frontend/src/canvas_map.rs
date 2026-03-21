//! Canvas-based map renderer for the Live Fleet page.
//!
//! Replaces all DOM/SVG station markers, ship markers, route lines, and the CSS grid
//! background with a single `<canvas>` element driven by `requestAnimationFrame`.
//!
//! Draws: grid, stars (dark mode), planets (with ring/highlight/warning), ship hull
//! with thrust flame, dashed route lines, labels, and stock percentages.

use std::f64::consts::PI;

use crate::state::{ShipState, ShipStatus, StationDef};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GRID_SPACING: f64 = 70.0;
const STAR_COUNT: usize = 80;
const FLIGHT_DURATION_MS: f64 = 4200.0;

// ---------------------------------------------------------------------------
// Public drawing entry point
// ---------------------------------------------------------------------------

/// Draw a single frame of the map onto the given canvas context.
///
/// * `ctx`       - 2D rendering context
/// * `w`, `h`    - canvas pixel dimensions
/// * `stations`  - station definitions
/// * `ships`     - mutable ship states (canvas_x/y updated for animated ships)
/// * `tick`      - monotonically increasing frame counter (for animations)
/// * `light`     - true when the page is in light mode
/// * `now_ms`    - current `performance.now()` value in milliseconds
pub fn draw_map(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    stations: &[StationDef],
    ships: &mut [ShipState],
    tick: u32,
    light: bool,
    now_ms: f64,
) {
    ctx.clear_rect(0.0, 0.0, w, h);

    // Background
    let bg = if light { "#eef3f9" } else { "#070f1c" };
    ctx.set_fill_style_str(bg);
    ctx.fill_rect(0.0, 0.0, w, h);

    draw_grid(ctx, w, h, light);

    if !light {
        draw_stars(ctx, w, h, tick);
    }

    draw_route_lines(ctx, w, h, ships, stations, light);

    draw_planets(ctx, w, h, stations, tick, light);

    draw_ships(ctx, w, h, ships, stations, tick, now_ms, light);
}

/// Detect a click at canvas pixel coordinates `(cx, cy)`.
/// Returns `CanvasHit::Ship(idx)` if the click hit a ship, `CanvasHit::Station(idx)`
/// if it hit a station, or `CanvasHit::None`.
pub fn hit_test(
    cx: f64,
    cy: f64,
    w: f64,
    h: f64,
    stations: &[StationDef],
    ships: &[ShipState],
) -> CanvasHit {
    // Check ships first (they render on top)
    for (i, ship) in ships.iter().enumerate() {
        let (sx, sy) = ship_pixel_pos(ship, w, h, stations);
        let dist = ((cx - sx).powi(2) + (cy - sy).powi(2)).sqrt();
        if dist < 22.0 {
            return CanvasHit::Ship(i);
        }
    }

    // Check planets
    for (i, st) in stations.iter().enumerate() {
        let (px, py) = station_xy(st, w, h);
        let r = st.planet_radius;
        let dist = ((cx - px).powi(2) + (cy - py).powi(2)).sqrt();
        if dist < r + 12.0 {
            return CanvasHit::Station(i);
        }
    }

    CanvasHit::None
}

/// Compute the pixel position of a ship for popup placement and hit testing.
pub fn ship_pixel_pos(ship: &ShipState, w: f64, h: f64, stations: &[StationDef]) -> (f64, f64) {
    if let Some(cx) = ship.canvas_x {
        if let Some(cy) = ship.canvas_y {
            return (cx, cy);
        }
    }
    // Docked ships sit above their station planet
    if ship.status == ShipStatus::Docked {
        if let Some(si) = ship.current_station_idx {
            if let Some(st) = stations.get(si) {
                let (px, py) = station_xy(st, w, h);
                return (px, py - st.planet_radius - 18.0);
            }
        }
    }
    // Fallback: percentage position
    (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CanvasHit {
    None,
    Ship(usize),
    Station(usize),
}

// ---------------------------------------------------------------------------
// Grid
// ---------------------------------------------------------------------------

fn draw_grid(ctx: &web_sys::CanvasRenderingContext2d, w: f64, h: f64, light: bool) {
    let color = if light {
        "rgba(0,120,180,0.06)"
    } else {
        "rgba(0,160,230,0.04)"
    };
    ctx.set_stroke_style_str(color);
    ctx.set_line_width(1.0);

    let mut x = 0.0;
    while x < w {
        ctx.begin_path();
        ctx.move_to(x, 0.0);
        ctx.line_to(x, h);
        ctx.stroke();
        x += GRID_SPACING;
    }

    let mut y = 0.0;
    while y < h {
        ctx.begin_path();
        ctx.move_to(0.0, y);
        ctx.line_to(w, y);
        ctx.stroke();
        y += GRID_SPACING;
    }
}

// ---------------------------------------------------------------------------
// Stars (dark mode only)
// ---------------------------------------------------------------------------

fn draw_stars(ctx: &web_sys::CanvasRenderingContext2d, w: f64, h: f64, tick: u32) {
    ctx.set_fill_style_str("rgba(255,255,255,0.55)");
    for i in 0..STAR_COUNT {
        let ix = i as f64;
        let sx = ((ix * 137.0 + 31.0) % w + w) % w;
        let sy = ((ix * 97.0 + 53.0) % h + h) % h;
        let blink = (tick as f64 * 0.02 + ix).sin() * 0.3 + 0.7;
        ctx.set_global_alpha(blink * 0.4);
        ctx.begin_path();
        let _ = ctx.arc(sx, sy, 0.8, 0.0, PI * 2.0);
        ctx.fill();
    }
    ctx.set_global_alpha(1.0);
}

// ---------------------------------------------------------------------------
// Route lines (dashed, during transit)
// ---------------------------------------------------------------------------

fn draw_route_lines(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    ships: &[ShipState],
    stations: &[StationDef],
    light: bool,
) {
    for ship in ships.iter() {
        if ship.status != ShipStatus::Transit {
            continue;
        }
        let dest_idx = match ship.destination_station_idx {
            Some(d) => d,
            None => continue,
        };
        let dest = match stations.get(dest_idx) {
            Some(d) => d,
            None => continue,
        };

        let from_x = ship.from_pct_x.unwrap_or(ship.left_pct) / 100.0 * w;
        let from_y = ship.from_pct_y.unwrap_or(ship.top_pct) / 100.0 * h;
        let (to_x, to_y) = station_xy(dest, w, h);

        ctx.save();
        ctx.set_line_dash(&js_sys::Array::of2(
            &wasm_bindgen::JsValue::from_f64(5.0),
            &wasm_bindgen::JsValue::from_f64(8.0),
        ))
        .unwrap_or(());
        let route_color = if light {
            "rgba(0,120,180,0.25)"
        } else {
            "rgba(0,160,230,0.22)"
        };
        ctx.set_stroke_style_str(route_color);
        ctx.set_line_width(1.0);
        ctx.begin_path();
        ctx.move_to(from_x, from_y);
        ctx.line_to(to_x, to_y);
        ctx.stroke();
        ctx.restore();
    }
}

// ---------------------------------------------------------------------------
// Planets
// ---------------------------------------------------------------------------

fn draw_planets(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    stations: &[StationDef],
    tick: u32,
    light: bool,
) {
    for st in stations.iter() {
        let (x, y) = station_xy(st, w, h);
        let r = st.planet_radius;

        // Planet body
        ctx.set_fill_style_str(&st.planet_color);
        ctx.begin_path();
        let _ = ctx.arc(x, y, r, 0.0, PI * 2.0);
        ctx.fill();

        // Ring (Beta Relay)
        if st.has_ring {
            ctx.save();
            ctx.translate(x, y).unwrap_or(());
            ctx.scale(1.0, 0.28).unwrap_or(());
            ctx.set_stroke_style_str(&st.planet_color);
            ctx.set_line_width(5.0);
            ctx.set_global_alpha(0.5);
            ctx.begin_path();
            let _ = ctx.arc(0.0, 0.0, r + 12.0, 0.0, PI * 2.0);
            ctx.stroke();
            ctx.restore();
            ctx.set_global_alpha(1.0);
        }

        // Subtle highlight sheen
        ctx.set_fill_style_str("rgba(255,255,255,0.12)");
        ctx.begin_path();
        let _ = ctx.arc(x - r * 0.25, y - r * 0.28, r * 0.45, 0.0, PI * 2.0);
        ctx.fill();

        // Stock-low pulsing amber ring
        if st.stock_pct < 25.0 {
            ctx.save();
            ctx.set_stroke_style_str("#f5a623");
            ctx.set_line_width(2.0);
            let pulse = 0.7 + (tick as f64 * 0.08).sin() * 0.3;
            ctx.set_global_alpha(pulse);
            ctx.set_line_dash(&js_sys::Array::of2(
                &wasm_bindgen::JsValue::from_f64(4.0),
                &wasm_bindgen::JsValue::from_f64(4.0),
            ))
            .unwrap_or(());
            ctx.begin_path();
            let _ = ctx.arc(x, y, r + 8.0, 0.0, PI * 2.0);
            ctx.stroke();
            ctx.restore();
            ctx.set_global_alpha(1.0);
        }

        // Station name label
        ctx.set_font("600 13px Inter, sans-serif");
        ctx.set_text_align("center");
        let label_color = if light {
            "rgba(10,30,60,0.75)"
        } else {
            "rgba(200,220,240,0.85)"
        };
        ctx.set_fill_style_str(label_color);
        ctx.fill_text(&st.name, x, y + r + 18.0).unwrap_or(());

        // Stock percentage below label
        let pct = st.stock_pct.round() as i32;
        ctx.set_font("11px Inter, sans-serif");
        let stock_color = if pct > 50 {
            "#00a86b"
        } else if pct > 25 {
            "#d4820a"
        } else {
            "#d42e55"
        };
        ctx.set_fill_style_str(stock_color);
        ctx.fill_text(&format!("{pct}%"), x, y + r + 32.0)
            .unwrap_or(());
    }
}

// ---------------------------------------------------------------------------
// Ships
// ---------------------------------------------------------------------------

fn draw_ships(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    ships: &mut [ShipState],
    stations: &[StationDef],
    tick: u32,
    now_ms: f64,
    light: bool,
) {
    for ship in ships.iter_mut() {
        let is_transit = ship.status == ShipStatus::Transit;
        let is_dead = ship.status == ShipStatus::Dead;

        // Compute current pixel position
        let (sx, sy) = if is_transit {
            compute_flight_pos(ship, w, h, stations, now_ms)
        } else if ship.status == ShipStatus::Docked {
            if let Some(si) = ship.current_station_idx {
                if let Some(st) = stations.get(si) {
                    let (px, py) = station_xy(st, w, h);
                    (px, py - st.planet_radius - 18.0)
                } else {
                    (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
                }
            } else {
                (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
            }
        } else {
            // Dead or other
            (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
        };

        // Update cached canvas position for hit testing and popup placement
        ship.canvas_x = Some(sx);
        ship.canvas_y = Some(sy);

        if is_dead {
            draw_dead_ship(ctx, sx, sy, &ship.name, tick, light);
        } else if is_transit {
            // Compute heading angle toward destination
            let angle = if let Some(di) = ship.destination_station_idx {
                if let Some(dst) = stations.get(di) {
                    let (dx, dy) = station_xy(dst, w, h);
                    (dy - sy).atan2(dx - sx) + PI / 2.0
                } else {
                    0.0
                }
            } else {
                0.0
            };
            draw_transit_ship(ctx, sx, sy, angle, &ship.name, tick, light);
        } else {
            draw_docked_ship(ctx, sx, sy, &ship.name, light);
        }
    }
}

/// Compute interpolated flight position using quadratic ease-in-out.
fn compute_flight_pos(
    ship: &ShipState,
    w: f64,
    h: f64,
    stations: &[StationDef],
    now_ms: f64,
) -> (f64, f64) {
    let start = ship.flight_start_ms.unwrap_or(now_ms);
    let dur = ship.flight_duration_ms.unwrap_or(FLIGHT_DURATION_MS);
    let elapsed = now_ms - start;
    let t = (elapsed / dur).clamp(0.0, 1.0);
    // Quadratic ease-in-out
    let ease = if t < 0.5 {
        2.0 * t * t
    } else {
        -1.0 + (4.0 - 2.0 * t) * t
    };

    let from_x = ship.from_pct_x.unwrap_or(ship.left_pct) / 100.0 * w;
    let from_y = ship.from_pct_y.unwrap_or(ship.top_pct) / 100.0 * h;

    let (to_x, to_y) = if let Some(di) = ship.destination_station_idx {
        if let Some(dst) = stations.get(di) {
            station_xy(dst, w, h)
        } else {
            (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
        }
    } else {
        (ship.left_pct / 100.0 * w, ship.top_pct / 100.0 * h)
    };

    let cx = from_x + (to_x - from_x) * ease;
    let cy = from_y + (to_y - from_y) * ease;
    (cx, cy)
}

fn draw_transit_ship(
    ctx: &web_sys::CanvasRenderingContext2d,
    x: f64,
    y: f64,
    angle: f64,
    name: &str,
    tick: u32,
    light: bool,
) {
    ctx.save();
    ctx.translate(x, y).unwrap_or(());
    ctx.rotate(angle).unwrap_or(());

    // Thrust flame
    let flame_alpha = 0.6 + (tick as f64 * 0.25).sin() * 0.3;
    ctx.set_global_alpha(flame_alpha);
    ctx.set_fill_style_str("#EF9F27");
    ctx.begin_path();
    ctx.move_to(-4.0, 9.0);
    ctx.line_to(4.0, 9.0);
    let flame_len = 17.0 + (tick as f64 * 0.35).sin() * 3.0;
    ctx.line_to(0.0, flame_len);
    ctx.close_path();
    ctx.fill();
    ctx.set_global_alpha(1.0);

    // Hull
    ctx.set_fill_style_str("#4a90d9");
    ctx.begin_path();
    ctx.move_to(0.0, -14.0);
    ctx.line_to(8.0, 9.0);
    ctx.line_to(0.0, 5.0);
    ctx.line_to(-8.0, 9.0);
    ctx.close_path();
    ctx.fill();

    // Cockpit
    ctx.set_fill_style_str("#fff");
    ctx.begin_path();
    let _ = ctx.arc(0.0, -5.0, 4.0, 0.0, PI * 2.0);
    ctx.fill();

    ctx.restore();

    // Ship name label
    ctx.set_font("600 12px Inter, sans-serif");
    ctx.set_text_align("center");
    let name_color = if light { "#1a5fa8" } else { "#85b7eb" };
    ctx.set_fill_style_str(name_color);
    ctx.fill_text(&format!("VSS {}", name.to_uppercase()), x, y + 26.0)
        .unwrap_or(());
}

fn draw_docked_ship(
    ctx: &web_sys::CanvasRenderingContext2d,
    x: f64,
    y: f64,
    name: &str,
    light: bool,
) {
    ctx.save();
    ctx.translate(x, y).unwrap_or(());

    // Hull (pointing up)
    ctx.set_fill_style_str("#4a90d9");
    ctx.begin_path();
    ctx.move_to(0.0, -14.0);
    ctx.line_to(8.0, 9.0);
    ctx.line_to(0.0, 5.0);
    ctx.line_to(-8.0, 9.0);
    ctx.close_path();
    ctx.fill();

    // Cockpit
    ctx.set_fill_style_str("#fff");
    ctx.begin_path();
    let _ = ctx.arc(0.0, -5.0, 4.0, 0.0, PI * 2.0);
    ctx.fill();

    ctx.restore();

    // Ship name label
    ctx.set_font("600 12px Inter, sans-serif");
    ctx.set_text_align("center");
    let name_color = if light { "#1a5fa8" } else { "#85b7eb" };
    ctx.set_fill_style_str(name_color);
    ctx.fill_text(&format!("VSS {}", name.to_uppercase()), x, y + 26.0)
        .unwrap_or(());
}

fn draw_dead_ship(
    ctx: &web_sys::CanvasRenderingContext2d,
    x: f64,
    y: f64,
    name: &str,
    _tick: u32,
    light: bool,
) {
    ctx.save();
    ctx.set_global_alpha(0.4);
    ctx.translate(x, y).unwrap_or(());

    // Hull (dimmed)
    ctx.set_fill_style_str("#8b4040");
    ctx.begin_path();
    ctx.move_to(0.0, -14.0);
    ctx.line_to(8.0, 9.0);
    ctx.line_to(0.0, 5.0);
    ctx.line_to(-8.0, 9.0);
    ctx.close_path();
    ctx.fill();

    // Skull indicator
    ctx.set_font("14px sans-serif");
    ctx.set_text_align("center");
    ctx.fill_text("\u{1F480}", 0.0, -2.0).unwrap_or(());

    ctx.restore();

    // Name label
    ctx.set_font("600 12px Inter, sans-serif");
    ctx.set_text_align("center");
    ctx.set_global_alpha(0.4);
    let dead_name_color = if light { "#994444" } else { "#cc6666" };
    ctx.set_fill_style_str(dead_name_color);
    ctx.fill_text(&format!("VSS {}", name.to_uppercase()), x, y + 26.0)
        .unwrap_or(());
    ctx.set_global_alpha(1.0);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert station percentage position to canvas pixel coordinates.
fn station_xy(st: &StationDef, w: f64, h: f64) -> (f64, f64) {
    (st.left_pct / 100.0 * w, st.top_pct / 100.0 * h)
}
