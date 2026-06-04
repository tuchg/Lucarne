//! adapter — the sole boundary that touches `rmux_sdk` value types.
//!
//! Converts a captured `rmux_sdk::PaneSnapshot` into the stable terminal
//! [`crate::term::PaneGrid`] vocabulary. Field shapes are verbatim from the
//! rmux-sdk 0.3.1 probe (verified against the installed crate source):
//! - `PaneSnapshot { cols, rows, cells, cursor, revision }`
//! - `PaneCell { glyph, attributes, foreground, background, underline }`
//! - `PaneGlyph { text: String, width: u8, padding: bool }`
//! - `PaneColor` — 8 variants (`#[non_exhaustive]`)
//! - `PaneAttributes { bits: u16 }`
//! - `PaneCursor { row, col, visible, style: u32 }`
//!
//! Keeping this the only place that names `rmux_*` value types means preview-API
//! churn never leaks into the gateway or web client.

use rmux_sdk::{PaneAttributes, PaneColor, PaneCursor, PaneSnapshot};

use crate::term::{Cell, Color, Cursor, PaneGrid, Style};

/// Maps one `rmux_sdk::PaneColor` to a terminal [`Color`] (1:1).
///
/// `PaneColor` is `#[non_exhaustive]`, so the trailing arm keeps this compiling
/// against future SDK variants. Rather than dropping an unknown color
/// (fix-don't-hide), it is preserved via the raw `encoded()` round-trip.
pub fn map_color(color: PaneColor) -> Color {
    match color {
        PaneColor::Default => Color::Default,
        PaneColor::None => Color::None,
        PaneColor::Terminal => Color::Terminal,
        PaneColor::Ansi { index } => Color::Ansi(index),
        PaneColor::BrightAnsi { index } => Color::BrightAnsi(index),
        PaneColor::Indexed { index } => Color::Indexed(index),
        PaneColor::Rgb { red, green, blue } => Color::Rgb(red, green, blue),
        PaneColor::Encoded { value } => Color::Encoded(value),
        // Forward-compat: a future #[non_exhaustive] variant is preserved as its
        // raw encoding instead of being silently lost.
        other => Color::Encoded(other.encoded()),
    }
}

/// Maps `rmux_sdk::PaneAttributes` (raw `u16` bitset) to terminal [`Style`].
/// Uses `from_bits_retain` so unknown/future bits survive (fix-don't-hide) — the
/// bit layout is identical between the two types.
pub fn map_style(attrs: PaneAttributes) -> Style {
    Style::from_bits_retain(attrs.bits())
}

/// Maps `rmux_sdk::PaneCursor` to terminal [`Cursor`]. rmux uses `row`/`col`
/// (not `x`/`y`) and carries an undecoded DECSCUSR `style: u32`.
pub fn map_cursor(cursor: PaneCursor) -> Cursor {
    Cursor {
        row: cursor.row,
        col: cursor.col,
        visible: cursor.visible,
        style_raw: cursor.style,
    }
}

/// Maps one `rmux_sdk::PaneCell` to terminal [`Cell`].
fn map_cell(cell: &rmux_sdk::PaneCell) -> Cell {
    Cell {
        text: cell.glyph.text.clone(),
        width: cell.glyph.width,
        padding: cell.glyph.padding,
        fg: map_color(cell.foreground),
        bg: map_color(cell.background),
        underline_color: map_color(cell.underline),
        style: map_style(cell.attributes),
    }
}

/// Converts a captured `PaneSnapshot` into the rmux-free `PaneGrid` wire type.
/// Cells stay row-major (`index = row * cols + col`), so `cells.len() == cols *
/// rows` is preserved verbatim; `revision` becomes the `rev` delta baseline.
pub fn snapshot_to_grid(snap: &PaneSnapshot) -> PaneGrid {
    PaneGrid {
        cols: snap.cols,
        rows: snap.rows,
        cells: snap.cells.iter().map(map_cell).collect(),
        rev: snap.revision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmux_sdk::{PaneCell, PaneGlyph};

    /// Builds a tiny 2x2 snapshot: a wide CJK glyph + its padding cell on row 0,
    /// an Rgb-fg / BOLD|UNDERLINE styled cell and a blank on row 1.
    fn fixture() -> PaneSnapshot {
        let wide = PaneCell {
            glyph: PaneGlyph::new("你", 2),
            ..PaneCell::default()
        };
        let pad = PaneCell::padding();
        let styled = PaneCell {
            glyph: PaneGlyph::new("p", 1),
            attributes: PaneAttributes::BOLD | PaneAttributes::UNDERLINE,
            foreground: PaneColor::rgb(10, 20, 30),
            background: PaneColor::indexed(200),
            underline: PaneColor::ansi(3),
        };
        let blank = PaneCell::blank();

        PaneSnapshot::new(
            2,
            2,
            vec![wide, pad, styled, blank],
            PaneCursor::new(1, 0, true, 7),
        )
        .expect("2x2 snapshot shape is valid")
        .with_revision(42)
    }

    #[test]
    fn grid_shape_matches_dims() {
        let grid = snapshot_to_grid(&fixture());
        assert_eq!(grid.cols, 2);
        assert_eq!(grid.rows, 2);
        assert_eq!(grid.rev, 42);
        assert_eq!(
            grid.cells.len(),
            usize::from(grid.cols) * usize::from(grid.rows)
        );
    }

    #[test]
    fn wide_glyph_and_padding_carry_width() {
        let grid = snapshot_to_grid(&fixture());
        let wide = &grid.cells[0];
        assert_eq!(wide.text, "你");
        assert_eq!(wide.width, 2);
        assert!(!wide.padding);
        let pad = &grid.cells[1];
        assert_eq!(pad.width, 0);
        assert!(pad.padding);
    }

    #[test]
    fn colors_and_style_map_per_variant() {
        let grid = snapshot_to_grid(&fixture());
        let styled = &grid.cells[2];
        assert_eq!(styled.fg, Color::Rgb(10, 20, 30));
        assert_eq!(styled.bg, Color::Indexed(200));
        assert_eq!(styled.underline_color, Color::Ansi(3));
        assert!(styled.style.contains(Style::BOLD));
        assert!(styled.style.contains(Style::UNDERLINE));
        assert!(!styled.style.contains(Style::ITALIC));
    }

    #[test]
    fn cursor_maps_row_col_and_raw_style() {
        let cursor = map_cursor(PaneCursor::new(5, 9, false, 6));
        assert_eq!(cursor.row, 5);
        assert_eq!(cursor.col, 9);
        assert!(!cursor.visible);
        assert_eq!(cursor.style_raw, 6);
    }

    #[test]
    fn all_eight_color_variants_round_trip() {
        assert_eq!(map_color(PaneColor::Default), Color::Default);
        assert_eq!(map_color(PaneColor::None), Color::None);
        assert_eq!(map_color(PaneColor::Terminal), Color::Terminal);
        assert_eq!(map_color(PaneColor::ansi(7)), Color::Ansi(7));
        assert_eq!(map_color(PaneColor::bright_ansi(4)), Color::BrightAnsi(4));
        assert_eq!(map_color(PaneColor::indexed(123)), Color::Indexed(123));
        assert_eq!(map_color(PaneColor::rgb(1, 2, 3)), Color::Rgb(1, 2, 3));
        assert_eq!(
            map_color(PaneColor::Encoded { value: -5 }),
            Color::Encoded(-5)
        );
    }

    #[test]
    fn unknown_bits_are_retained() {
        let attrs = PaneAttributes::from_bits(0x8000 | PaneAttributes::BOLD.bits());
        let style = map_style(attrs);
        assert_eq!(style.bits(), 0x8000 | Style::BOLD.bits());
    }
}
