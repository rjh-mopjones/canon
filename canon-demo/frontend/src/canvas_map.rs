//! Canvas-based map renderer for the Live Fleet page.
//!
//! Replaces all DOM/SVG station markers, ship markers, route lines, and the CSS grid
//! background with a single `<canvas>` element driven by `requestAnimationFrame`.
//!
//! Draws: grid, stars (dark mode), planets (with ring/highlight/warning), ship hull
//! with thrust flame, dashed route lines, labels, and stock percentages.
//!
//! All colours are read from CSS custom properties at the start of each frame via
//! `getComputedStyle()`, so the canvas respects the active theme.

use std::f64::consts::PI;

use crate::state::{ShipState, ShipStatus, StationDef};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GRID_SPACING: f64 = 70.0;
const STAR_COUNT: usize = 80;
const FLIGHT_DURATION_MS: f64 = 4200.0;

// ---------------------------------------------------------------------------
// Theme colours read from CSS custom properties
// ---------------------------------------------------------------------------

/// Holds all colour values read from CSS custom properties via `getComputedStyle()`.
/// Created once per frame and threaded through all draw functions so the canvas
/// never uses hardcoded hex values.
pub struct ThemeColors {
    pub bg: String,
    pub grid: String,
    pub cyan: String,
    pub green: String,
    pub amber: String,
    pub red: String,
    pub purple: String,
    pub txt: String,
    pub txthi: String,
    pub txtlo: String,
    pub planet_green: String,
    pub planet_purple: String,
    pub planet_coral: String,
    pub planet_blue: String,
    pub ship_hull: String,
    pub ship_label: String,
    pub ship_dead: String,
    pub ship_dead_label: String,
    pub route_line: String,
    pub star: String,
    pub highlight_sheen: String,
    pub label_text: String,
    pub cockpit: String,
}

/// Dark-mode fallback defaults, used when the DOM is unavailable.
impl Default for ThemeColors {
    fn default() -> Self {
        Self {
            bg: "#070f1c".into(),
            grid: "rgba(0,160,230,0.032)".into(),
            cyan: "#00b4ff".into(),
            green: "#00e58a".into(),
            amber: "#f5a623".into(),
            red: "#ff4069".into(),
            purple: "#a78bfa".into(),
            txt: "#9db8d2".into(),
            txthi: "#daeaf8".into(),
            txtlo: "#4e6a82".into(),
            planet_green: "#3B6D11".into(),
            planet_purple: "#534AB7".into(),
            planet_coral: "#993C1D".into(),
            planet_blue: "#185FA5".into(),
            ship_hull: "#4a90d9".into(),
            ship_label: "#85b7eb".into(),
            ship_dead: "#8b4040".into(),
            ship_dead_label: "#cc6666".into(),
            route_line: "rgba(0,160,230,0.22)".into(),
            star: "rgba(255,255,255,0.55)".into(),
            highlight_sheen: "rgba(255,255,255,0.12)".into(),
            label_text: "rgba(200,220,240,0.85)".into(),
            cockpit: "#ffffff".into(),
        }
    }
}

/// Read a single CSS custom property, trimming whitespace. Returns the fallback
/// if the property is missing or the DOM APIs are unavailable.
fn read_css_var(style: &web_sys::CssStyleDeclaration, name: &str, fallback: &str) -> String {
    style
        .get_property_value(name)
        .ok()
        .map(|v| {
            let trimmed = v.trim().to_string();
            if trimmed.is_empty() {
                fallback.to_string()
            } else {
                trimmed
            }
        })
        .unwrap_or_else(|| fallback.to_string())
}

/// Read all theme colours from CSS custom properties on the document element.
///
/// Falls back to sensible defaults if the DOM is not available (e.g. during SSR,
/// though this frontend is CSR-only). The fallbacks match the dark-mode values
/// defined in `style/main.css`.
pub fn read_theme_colors() -> ThemeColors {
    let style = (|| -> Option<web_sys::CssStyleDeclaration> {
        let window = web_sys::window()?;
        let document = window.document()?;
        let el = document.document_element()?;
        window.get_computed_style(&el).ok().flatten()
    })();

    match style {
        Some(s) => {
            let defaults = ThemeColors::default();
            ThemeColors {
                bg: read_css_var(&s, "--bg", &defaults.bg),
                grid: read_css_var(&s, "--grid", &defaults.grid),
                cyan: read_css_var(&s, "--cyan", &defaults.cyan),
                green: read_css_var(&s, "--green", &defaults.green),
                amber: read_css_var(&s, "--amber", &defaults.amber),
                red: read_css_var(&s, "--red", &defaults.red),
                purple: read_css_var(&s, "--purple", &defaults.purple),
                txt: read_css_var(&s, "--txt", &defaults.txt),
                txthi: read_css_var(&s, "--txthi", &defaults.txthi),
                txtlo: read_css_var(&s, "--txtlo", &defaults.txtlo),
                planet_green: read_css_var(&s, "--planet-green", &defaults.planet_green),
                planet_purple: read_css_var(&s, "--planet-purple", &defaults.planet_purple),
                planet_coral: read_css_var(&s, "--planet-coral", &defaults.planet_coral),
                planet_blue: read_css_var(&s, "--planet-blue", &defaults.planet_blue),
                ship_hull: read_css_var(&s, "--ship-hull", &defaults.ship_hull),
                ship_label: read_css_var(&s, "--ship-label", &defaults.ship_label),
                ship_dead: read_css_var(&s, "--ship-dead", &defaults.ship_dead),
                ship_dead_label: read_css_var(&s, "--ship-dead-label", &defaults.ship_dead_label),
                route_line: read_css_var(&s, "--route-line", &defaults.route_line),
                star: read_css_var(&s, "--star", &defaults.star),
                highlight_sheen: read_css_var(&s, "--highlight-sheen", &defaults.highlight_sheen),
                label_text: read_css_var(&s, "--label-text", &defaults.label_text),
                cockpit: read_css_var(&s, "--cockpit", &defaults.cockpit),
            }
        }
        None => ThemeColors::default(),
    }
}

/// Resolve a station's planet colour from the theme, keyed by `planet_color_var`.
/// Returns a reference into `colors` to avoid cloning on every frame.
pub fn resolve_planet_color<'a>(station: &StationDef, colors: &'a ThemeColors) -> &'a str {
    match station.planet_color_var.as_str() {
        "--planet-green" => &colors.planet_green,
        "--planet-purple" => &colors.planet_purple,
        "--planet-coral" => &colors.planet_coral,
        "--planet-blue" => &colors.planet_blue,
        other => {
            web_sys::console::warn_1(
                &format!("Unknown planet_color_var: {other}, falling back to --cyan").into(),
            );
            &colors.cyan
        }
    }
}

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
#[allow(clippy::too_many_arguments)]
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
    let colors = read_theme_colors();

    ctx.clear_rect(0.0, 0.0, w, h);

    // Background
    ctx.set_fill_style_str(&colors.bg);
    ctx.fill_rect(0.0, 0.0, w, h);

    draw_grid(ctx, w, h, &colors);

    if !light {
        draw_stars(ctx, w, h, tick, &colors);
    }

    draw_route_lines(ctx, w, h, ships, stations, &colors);

    draw_planets(ctx, w, h, stations, tick, &colors);

    draw_ships(ctx, w, h, ships, stations, tick, now_ms, &colors);
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

fn draw_grid(ctx: &web_sys::CanvasRenderingContext2d, w: f64, h: f64, colors: &ThemeColors) {
    ctx.set_stroke_style_str(&colors.grid);
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

fn draw_stars(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    tick: u32,
    colors: &ThemeColors,
) {
    ctx.set_fill_style_str(&colors.star);
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
    colors: &ThemeColors,
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
        ctx.set_stroke_style_str(&colors.route_line);
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
    colors: &ThemeColors,
) {
    for st in stations.iter() {
        let (x, y) = station_xy(st, w, h);
        let r = st.planet_radius;

        // Resolve planet colour from CSS variable
        let planet_col = resolve_planet_color(st, colors);

        // Planet body
        ctx.set_fill_style_str(planet_col);
        ctx.begin_path();
        let _ = ctx.arc(x, y, r, 0.0, PI * 2.0);
        ctx.fill();

        // Ring (Beta Relay)
        if st.has_ring {
            ctx.save();
            ctx.translate(x, y).unwrap_or(());
            ctx.scale(1.0, 0.28).unwrap_or(());
            ctx.set_stroke_style_str(planet_col);
            ctx.set_line_width(5.0);
            ctx.set_global_alpha(0.5);
            ctx.begin_path();
            let _ = ctx.arc(0.0, 0.0, r + 12.0, 0.0, PI * 2.0);
            ctx.stroke();
            ctx.restore();
            ctx.set_global_alpha(1.0);
        }

        // Subtle highlight sheen
        ctx.set_fill_style_str(&colors.highlight_sheen);
        ctx.begin_path();
        let _ = ctx.arc(x - r * 0.25, y - r * 0.28, r * 0.45, 0.0, PI * 2.0);
        ctx.fill();

        // Stock-low pulsing amber ring
        if st.stock_pct < 25.0 {
            ctx.save();
            ctx.set_stroke_style_str(&colors.amber);
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
        ctx.set_fill_style_str(&colors.label_text);
        ctx.fill_text(&st.name, x, y + r + 18.0).unwrap_or(());

        // Stock percentage below label
        let pct = st.stock_pct.round() as i32;
        ctx.set_font("11px Inter, sans-serif");
        let stock_color = if pct > 50 {
            &colors.green
        } else if pct > 25 {
            &colors.amber
        } else {
            &colors.red
        };
        ctx.set_fill_style_str(stock_color);
        ctx.fill_text(&format!("{pct}%"), x, y + r + 32.0)
            .unwrap_or(());
    }
}

// ---------------------------------------------------------------------------
// Ships
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn draw_ships(
    ctx: &web_sys::CanvasRenderingContext2d,
    w: f64,
    h: f64,
    ships: &mut [ShipState],
    stations: &[StationDef],
    tick: u32,
    now_ms: f64,
    colors: &ThemeColors,
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
            draw_dead_ship(ctx, sx, sy, &ship.name, tick, colors);
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
            draw_transit_ship(ctx, sx, sy, angle, &ship.name, tick, colors);
        } else {
            draw_docked_ship(ctx, sx, sy, &ship.name, colors);
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
    colors: &ThemeColors,
) {
    ctx.save();
    ctx.translate(x, y).unwrap_or(());
    ctx.rotate(angle).unwrap_or(());

    // Thrust flame
    let flame_alpha = 0.6 + (tick as f64 * 0.25).sin() * 0.3;
    ctx.set_global_alpha(flame_alpha);
    ctx.set_fill_style_str(&colors.amber);
    ctx.begin_path();
    ctx.move_to(-4.0, 9.0);
    ctx.line_to(4.0, 9.0);
    let flame_len = 17.0 + (tick as f64 * 0.35).sin() * 3.0;
    ctx.line_to(0.0, flame_len);
    ctx.close_path();
    ctx.fill();
    ctx.set_global_alpha(1.0);

    // Hull
    ctx.set_fill_style_str(&colors.ship_hull);
    ctx.begin_path();
    ctx.move_to(0.0, -14.0);
    ctx.line_to(8.0, 9.0);
    ctx.line_to(0.0, 5.0);
    ctx.line_to(-8.0, 9.0);
    ctx.close_path();
    ctx.fill();

    // Cockpit
    ctx.set_fill_style_str(&colors.cockpit);
    ctx.begin_path();
    let _ = ctx.arc(0.0, -5.0, 4.0, 0.0, PI * 2.0);
    ctx.fill();

    ctx.restore();

    // Ship name label
    ctx.set_font("600 12px Inter, sans-serif");
    ctx.set_text_align("center");
    ctx.set_fill_style_str(&colors.ship_label);
    ctx.fill_text(&name.to_uppercase(), x, y + 26.0)
        .unwrap_or(());
}

fn draw_docked_ship(
    ctx: &web_sys::CanvasRenderingContext2d,
    x: f64,
    y: f64,
    name: &str,
    colors: &ThemeColors,
) {
    ctx.save();
    ctx.translate(x, y).unwrap_or(());

    // Hull (pointing up)
    ctx.set_fill_style_str(&colors.ship_hull);
    ctx.begin_path();
    ctx.move_to(0.0, -14.0);
    ctx.line_to(8.0, 9.0);
    ctx.line_to(0.0, 5.0);
    ctx.line_to(-8.0, 9.0);
    ctx.close_path();
    ctx.fill();

    // Cockpit
    ctx.set_fill_style_str(&colors.cockpit);
    ctx.begin_path();
    let _ = ctx.arc(0.0, -5.0, 4.0, 0.0, PI * 2.0);
    ctx.fill();

    ctx.restore();

    // Ship name label
    ctx.set_font("600 12px Inter, sans-serif");
    ctx.set_text_align("center");
    ctx.set_fill_style_str(&colors.ship_label);
    ctx.fill_text(&name.to_uppercase(), x, y + 26.0)
        .unwrap_or(());
}

fn draw_dead_ship(
    ctx: &web_sys::CanvasRenderingContext2d,
    x: f64,
    y: f64,
    name: &str,
    _tick: u32,
    colors: &ThemeColors,
) {
    ctx.save();
    ctx.set_global_alpha(0.4);
    ctx.translate(x, y).unwrap_or(());

    // Hull (dimmed)
    ctx.set_fill_style_str(&colors.ship_dead);
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
    ctx.set_fill_style_str(&colors.ship_dead_label);
    ctx.fill_text(&name.to_uppercase(), x, y + 26.0)
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
