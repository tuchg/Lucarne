//! Terminal grid value types — the single vocabulary for the mirror wire.
//!
//! Field shapes are verbatim from the rmux-sdk 0.3.1 probe:
//! - `Cell.text: String` (a grapheme cluster), not `ch: char` — preserves CJK /
//!   emoji / non-BMP. `width: u8` + `padding: bool` carried from rmux so the
//!   client NEVER recomputes Unicode width.
//! - `Cell.underline_color: Color` — rmux has an independent underline color.
//! - `Color` has 8 variants (1:1 with `rmux_sdk::PaneColor`).
//! - `Style` is a `u16` bitflags set with the 15 named rmux attribute bits.
//! - `Cursor` uses `row`/`col` (not `x`/`y`) plus `style_raw: u32`.
//! - `PaneGrid` carries `rev: u64` (mapped from `PaneSnapshot.revision`).

use serde::{Deserialize, Serialize};

/// Visible grid dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dims {
    pub cols: u16,
    pub rows: u16,
}

/// A full visible pane grid. `cells.len() == cols * rows`, row-major,
/// `index = row * cols + col`. `rev` mirrors `PaneSnapshot.revision` and is the
/// delta-resync baseline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaneGrid {
    pub cols: u16,
    pub rows: u16,
    /// len == cols*rows, row-major; index = row*cols + col.
    pub cells: Vec<Cell>,
    /// Maps from `PaneSnapshot.revision` — the delta baseline counter.
    pub rev: u64,
}

/// One terminal cell. Mirrors `rmux_sdk::PaneCell` (glyph flattened) plus its
/// independent underline color. The display `width` and `padding` are carried
/// from the snapshot and MUST be trusted by the renderer (no JS-side wcwidth).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    /// Glyph text — a grapheme cluster (may be multiple chars), not a bare char.
    pub text: String,
    /// Display width: CJK/emoji = 2, normal = 1, trailing padding = 0.
    pub width: u8,
    /// True when this is the trailing placeholder of a preceding wide glyph.
    pub padding: bool,
    pub fg: Color,
    pub bg: Color,
    /// rmux carries an independent underline color (distinct from fg/bg).
    pub underline_color: Color,
    pub style: Style,
}

/// Terminal color — a 1:1 mapping of `rmux_sdk::PaneColor`. Serialized as an
/// externally-tagged enum (`snake_case`) so unit and data variants round-trip
/// cleanly in JSON (an `untagged` enum cannot distinguish the unit variants).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Color {
    /// Terminal default color (raw encoding 8).
    #[default]
    Default,
    /// Explicit no-color (raw -1).
    None,
    /// Terminal color sentinel (raw 9).
    Terminal,
    /// Standard ANSI 0..=7.
    Ansi(u8),
    /// Bright ANSI (90..=97 index).
    BrightAnsi(u8),
    /// 256-color palette index.
    Indexed(u8),
    /// 24-bit truecolor.
    Rgb(u8, u8, u8),
    /// Forward-compat fallback for unknown/future raw encodings.
    Encoded(i32),
}

bitflags::bitflags! {
    /// Cell style attributes. Bit values are aligned to `rmux_sdk::PaneAttributes`
    /// (u16). Note the tmux aliases collapse onto the same bit:
    /// BOLD==BRIGHT, UNDERLINE==UNDERSCORE, ITALIC==ITALICS.
    ///
    /// Serde: serialized as the raw `u16` bits (the `bitflags` dep does not enable
    /// its own `serde` feature, so we implement it explicitly below — this keeps
    /// the JSON compact and stable: `"style": 5` == BOLD|UNDERLINE).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Style: u16 {
        const BOLD              = 0x0001; // == BRIGHT
        const DIM               = 0x0002;
        const UNDERLINE         = 0x0004; // == UNDERSCORE
        const BLINK             = 0x0008;
        const REVERSE           = 0x0010;
        const HIDDEN            = 0x0020;
        const ITALIC            = 0x0040; // == ITALICS
        const CHARSET           = 0x0080; // ACS line drawing
        const STRIKETHROUGH     = 0x0100;
        const DOUBLE_UNDERLINE  = 0x0200;
        const CURLY_UNDERLINE   = 0x0400;
        const DOTTED_UNDERLINE  = 0x0800;
        const DASHED_UNDERLINE  = 0x1000;
        const OVERLINE          = 0x2000;
        const NO_ATTRIBUTES     = 0x4000;
    }
}

impl Serialize for Style {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(self.bits())
    }
}

impl<'de> Deserialize<'de> for Style {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bits = u16::deserialize(deserializer)?;
        // Preserve unknown bits for forward-compat (fix-don't-hide: surface, not drop).
        Ok(Style::from_bits_retain(bits))
    }
}

/// Cursor position and state. rmux uses `row`/`col` (not `x`/`y`) and carries a
/// raw DECSCUSR `style_raw` value (shape/blink, undecoded). MVP renders block
/// only; `style_raw` is preserved for backlog use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub row: u16,
    pub col: u16,
    pub visible: bool,
    /// Raw DECSCUSR value from `PaneCursor.style` (undecoded).
    pub style_raw: u32,
}

// ---- Delta frame shapes (the differ in `diff.rs` produces these) ----

/// Only the changed rows of a grid. An identical grid yields `rows: []`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GridDelta {
    pub rows: Vec<RowDelta>,
}

/// Contiguous changed runs within one row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDelta {
    pub y: u16,
    pub spans: Vec<CellSpan>,
}

/// A contiguous run of changed cells starting at column `x`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellSpan {
    pub x: u16,
    pub cells: Vec<Cell>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn style_serializes_as_raw_bits_and_round_trips() {
        let s = Style::BOLD | Style::UNDERLINE;
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "5"); // 0x1 | 0x4
        let back: Style = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn style_retains_unknown_bits() {
        // A bit not named in Style must survive a round-trip (fix-don't-hide).
        let raw = 0x8000u16 | Style::BOLD.bits();
        let back: Style = serde_json::from_str(&raw.to_string()).unwrap();
        assert_eq!(back.bits(), raw);
    }

    #[test]
    fn color_variants_round_trip_as_tagged_json() {
        for c in [
            Color::Default,
            Color::None,
            Color::Terminal,
            Color::Ansi(7),
            Color::BrightAnsi(4),
            Color::Indexed(200),
            Color::Rgb(10, 20, 30),
            Color::Encoded(-5),
        ] {
            let json = serde_json::to_string(&c).unwrap();
            let back: Color = serde_json::from_str(&json).unwrap();
            assert_eq!(back, c, "round-trip failed for {c:?} ({json})");
        }
    }
}
