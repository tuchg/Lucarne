// 2D Canvas dirty-cell renderer — zoom-aware snapshot viewer (spike3 fidelity
// M1–M5). Trusts server-provided cell.width/padding (no JS wcwidth recompute).

import { StyleBits, ANY_UNDERLINE } from "./protocol.js";

const BASE_FONT_PX = 14;
const FONT_FAMILY = '"SF Mono", Menlo, Consolas, "DejaVu Sans Mono", monospace';
const DEFAULT_FG = "#d8d8d8";
const DEFAULT_BG = "#101014";
const CURSOR_COLOR = "#d8d8d8";

const ANSI_BASE = ["#000000", "#cc0000", "#4e9a06", "#c4a000", "#3465a4", "#75507b", "#06989a", "#d3d7cf"];
const ANSI_BRIGHT = ["#555753", "#ef2929", "#8ae234", "#fce94f", "#729fcf", "#ad7fa8", "#34e2e2", "#eeeeec"];

export function colorToCss(c, fallback) {
  if (typeof c === "string") return fallback;
  if ("ansi" in c) return ANSI_BASE[c.ansi & 7] ?? fallback;
  if ("bright_ansi" in c) return ANSI_BRIGHT[c.bright_ansi & 7] ?? fallback;
  if ("indexed" in c) return indexed256ToCss(c.indexed);
  if ("rgb" in c) { const [r, g, b] = c.rgb; return `rgb(${r},${g},${b})`; }
  return fallback;
}

function indexed256ToCss(i) {
  if (i < 8) return ANSI_BASE[i];
  if (i < 16) return ANSI_BRIGHT[i - 8];
  if (i < 232) {
    const n = i - 16;
    const r = Math.floor(n / 36), g = Math.floor((n % 36) / 6), b = n % 6;
    const conv = (v) => (v === 0 ? 0 : 55 + v * 40);
    return `rgb(${conv(r)},${conv(g)},${conv(b)})`;
  }
  const level = 8 + (i - 232) * 10;
  return `rgb(${level},${level},${level})`;
}

function styleToAttrs(style) {
  return {
    bold: (style & StyleBits.BOLD) !== 0,
    italic: (style & StyleBits.ITALIC) !== 0,
    underline: (style & ANY_UNDERLINE) !== 0,
    reverse: (style & StyleBits.REVERSE) !== 0,
    dim: (style & StyleBits.DIM) !== 0,
    strikethrough: (style & StyleBits.STRIKETHROUGH) !== 0,
  };
}

export class CanvasRenderer {
  constructor(canvas) {
    this.canvas = canvas;
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("2d canvas context unavailable");
    this.ctx = ctx;
    this.grid = null;
    this.cursor = null;
    this.dpr = 1;
    this.zoom = 1;
    this.recomputeMetrics();
  }

  recomputeMetrics() {
    this.fontPx = Math.max(6, Math.round(BASE_FONT_PX * this.zoom));
    this.cellH = Math.round(this.fontPx * 1.4);
    this.baseFont = `${this.fontPx}px ${FONT_FAMILY}`;
    this.ctx.font = this.baseFont;
    const advance = this.ctx.measureText("M").width;
    this.cellW = Math.max(1, Math.ceil(advance));
  }

  /** Set zoom (0.5–3×), recompute metrics, repaint. Returns the applied zoom. */
  setZoom(z) {
    this.zoom = Math.max(0.5, Math.min(3, z));
    this.recomputeMetrics();
    if (this.grid) this.renderSnapshot(this.grid, this.cursor);
    return this.zoom;
  }

  resizeBacking(cols, rows) {
    const cssW = cols * this.cellW;
    const cssH = rows * this.cellH;
    this.dpr = window.devicePixelRatio || 1;
    this.canvas.width = Math.round(cssW * this.dpr);
    this.canvas.height = Math.round(cssH * this.dpr);
    this.canvas.style.width = `${cssW}px`;
    this.canvas.style.height = `${cssH}px`;
    this.ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    this.ctx.textBaseline = "top";
    this.ctx.font = this.baseFont;
  }

  rev() { return this.grid?.rev ?? 0; }

  renderSnapshot(grid, cursor) {
    this.grid = grid;
    this.cursor = cursor;
    this.resizeBacking(grid.cols, grid.rows);
    this.ctx.fillStyle = DEFAULT_BG;
    this.ctx.fillRect(0, 0, grid.cols * this.cellW, grid.rows * this.cellH);
    for (let row = 0; row < grid.rows; row++) this.paintRow(row, 0, grid.cols);
    this.drawCursor();
  }

  renderDelta(delta, rev, cursor) {
    const grid = this.grid;
    if (!grid) return;
    for (const rowDelta of delta.rows) {
      const y = rowDelta.y;
      if (y >= grid.rows) continue;
      for (const span of rowDelta.spans) {
        for (let i = 0; i < span.cells.length; i++) {
          const col = span.x + i;
          if (col >= grid.cols) break;
          grid.cells[y * grid.cols + col] = span.cells[i];
        }
        const endCol = Math.min(span.x + span.cells.length, grid.cols);
        this.paintRow(y, span.x, endCol);
      }
    }
    grid.rev = rev;
    this.cursor = cursor;
    this.drawCursor();
  }

  paintRow(row, startCol, endCol) {
    const grid = this.grid;
    if (!grid) return;
    let col = startCol;
    while (col < endCol) {
      const cell = grid.cells[row * grid.cols + col];
      if (!cell) { col += 1; continue; }
      if (cell.padding) { col += 1; continue; }
      const span = cell.width >= 2 ? 2 : 1;
      this.paintCell(cell, row, col, span);
      col += span;
    }
  }

  paintCell(cell, row, col, span) {
    const ctx = this.ctx;
    const attrs = styleToAttrs(cell.style);
    let fg = colorToCss(cell.fg, DEFAULT_FG);
    let bg = colorToCss(cell.bg, DEFAULT_BG);
    if (attrs.reverse) { const t = fg; fg = bg; bg = t; }

    const x = col * this.cellW;
    const y = row * this.cellH;
    const w = this.cellW * span;

    ctx.fillStyle = bg;
    ctx.fillRect(x, y, w, this.cellH);

    if ((cell.style & StyleBits.HIDDEN) !== 0) return;
    if (cell.text.length === 0) return;

    const weight = attrs.bold ? "bold" : "normal";
    const fontStyle = attrs.italic ? "italic" : "normal";
    ctx.font = `${fontStyle} ${weight} ${this.fontPx}px ${FONT_FAMILY}`;
    ctx.globalAlpha = attrs.dim ? 0.6 : 1.0;
    ctx.fillStyle = fg;
    ctx.fillText(cell.text, x, y + 2);
    ctx.globalAlpha = 1.0;

    ctx.strokeStyle = fg;
    ctx.lineWidth = 1;
    if (attrs.underline) {
      const uy = y + this.cellH - 2;
      ctx.beginPath(); ctx.moveTo(x, uy); ctx.lineTo(x + w, uy); ctx.stroke();
    }
    if (attrs.strikethrough) {
      const sy = y + Math.floor(this.cellH / 2);
      ctx.beginPath(); ctx.moveTo(x, sy); ctx.lineTo(x + w, sy); ctx.stroke();
    }
  }

  drawCursor() {
    const cursor = this.cursor;
    const grid = this.grid;
    if (!cursor || !grid || !cursor.visible) return;
    if (cursor.row >= grid.rows || cursor.col >= grid.cols) return;
    const x = cursor.col * this.cellW;
    const y = cursor.row * this.cellH;
    this.ctx.save();
    this.ctx.fillStyle = CURSOR_COLOR;
    this.ctx.fillRect(x, y, this.cellW, this.cellH);
    const cell = grid.cells[cursor.row * grid.cols + cursor.col];
    if (cell && cell.text.length > 0 && !cell.padding) {
      this.ctx.font = `${this.fontPx}px ${FONT_FAMILY}`;
      this.ctx.fillStyle = DEFAULT_BG;
      this.ctx.fillText(cell.text, x, y + 2);
    }
    this.ctx.restore();
  }
}
