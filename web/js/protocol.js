// Style bit values aligned to the Rust `lucarne_rmux::term::Style` (u16 bitflags,
// serialized as raw bits). Mirror of the server-side names.
export const StyleBits = {
  BOLD: 0x0001,
  DIM: 0x0002,
  UNDERLINE: 0x0004,
  BLINK: 0x0008,
  REVERSE: 0x0010,
  HIDDEN: 0x0020,
  ITALIC: 0x0040,
  CHARSET: 0x0080,
  STRIKETHROUGH: 0x0100,
  DOUBLE_UNDERLINE: 0x0200,
  CURLY_UNDERLINE: 0x0400,
  DOTTED_UNDERLINE: 0x0800,
  DASHED_UNDERLINE: 0x1000,
  OVERLINE: 0x2000,
  NO_ATTRIBUTES: 0x4000,
};

// Any underline variant — collapsed to a single underline by the renderer.
export const ANY_UNDERLINE =
  StyleBits.UNDERLINE |
  StyleBits.DOUBLE_UNDERLINE |
  StyleBits.CURLY_UNDERLINE |
  StyleBits.DOTTED_UNDERLINE |
  StyleBits.DASHED_UNDERLINE;
