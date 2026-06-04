//! diff — self-authored snapshot differ.
//!
//! rmux-sdk 0.3.1 exposes NO native cell/row delta (spike1, three-way proven):
//! `render_stream()` re-emits a full `PaneSnapshot` on every change and
//! `revision` is only a "did anything change" counter. To keep the mirror hot
//! path cheap (`<50ms`, minimal bytes), the monitor keeps the previous
//! [`PaneGrid`] and diffs each full snapshot cell-by-cell into a [`GridDelta`] of
//! dirty-row runs.

use crate::term::grid::{Cell, CellSpan, GridDelta, PaneGrid, RowDelta};

/// Outcome of feeding a fresh full snapshot to the [`Differ`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffResult {
    /// Send a full snapshot: first frame after subscribe, or a resize.
    Full(PaneGrid),
    /// Send an incremental update over the hot path. `base_rev` is the rev the
    /// delta is computed *against*; `rev` is the new rev.
    Delta {
        base_rev: u64,
        rev: u64,
        delta: GridDelta,
    },
    /// rev gap detected — the client must ask for a fresh full snapshot.
    /// `have_rev` is the rev the differ last held (the client's stale baseline).
    Resync { have_rev: u64 },
}

/// Self-authored snapshot differ. Holds the last grid for one pane/session;
/// instantiate one `Differ` per subscribed session.
#[derive(Clone, Debug, Default)]
pub struct Differ {
    last_grid: Option<PaneGrid>,
}

impl Differ {
    /// New differ with no baseline — the next frame is always a `Full`.
    pub fn new() -> Self {
        Self { last_grid: None }
    }

    /// The rev of the currently held baseline grid, if any.
    pub fn current_rev(&self) -> Option<u64> {
        self.last_grid.as_ref().map(|g| g.rev)
    }

    /// Feed a fresh full snapshot, trusting rev continuity (no gap check).
    pub fn feed(&mut self, new: PaneGrid) -> DiffResult {
        self.feed_checked(new, None)
    }

    /// Feed a fresh full snapshot.
    pub fn feed_checked(&mut self, new: PaneGrid, expected_base_rev: Option<u64>) -> DiffResult {
        match self.last_grid.take() {
            None => {
                self.last_grid = Some(new.clone());
                DiffResult::Full(new)
            }
            Some(last) => {
                if last.cols != new.cols || last.rows != new.rows {
                    self.last_grid = Some(new.clone());
                    return DiffResult::Full(new);
                }

                if let Some(expected) = expected_base_rev {
                    if expected != last.rev {
                        let have_rev = last.rev;
                        self.last_grid = Some(new);
                        return DiffResult::Resync { have_rev };
                    }
                }

                let base_rev = last.rev;
                let rev = new.rev;
                let delta = diff(&last, &new);
                self.last_grid = Some(new);
                DiffResult::Delta {
                    base_rev,
                    rev,
                    delta,
                }
            }
        }
    }
}

/// Pure cell-by-cell diff of two equally-dimensioned grids into dirty-row runs.
pub fn diff(last: &PaneGrid, new: &PaneGrid) -> GridDelta {
    let cols = new.cols.min(last.cols) as usize;
    let rows = new.rows.min(last.rows) as usize;
    let new_stride = new.cols as usize;
    let last_stride = last.cols as usize;

    let mut row_deltas: Vec<RowDelta> = Vec::new();

    for y in 0..rows {
        let new_row = &new.cells[y * new_stride..y * new_stride + cols];
        let last_row = &last.cells[y * last_stride..y * last_stride + cols];

        let spans = row_spans(last_row, new_row);
        if !spans.is_empty() {
            row_deltas.push(RowDelta { y: y as u16, spans });
        }
    }

    GridDelta { rows: row_deltas }
}

/// Coalesce contiguous changed cells of one row into [`CellSpan`]s.
fn row_spans(last_row: &[Cell], new_row: &[Cell]) -> Vec<CellSpan> {
    let mut spans: Vec<CellSpan> = Vec::new();
    let mut run_start: Option<usize> = None;
    let mut run_cells: Vec<Cell> = Vec::new();

    for (x, (old, cur)) in last_row.iter().zip(new_row.iter()).enumerate() {
        if old != cur {
            if run_start.is_none() {
                run_start = Some(x);
            }
            run_cells.push(cur.clone());
        } else if let Some(start) = run_start.take() {
            spans.push(CellSpan {
                x: start as u16,
                cells: std::mem::take(&mut run_cells),
            });
        }
    }

    if let Some(start) = run_start {
        spans.push(CellSpan {
            x: start as u16,
            cells: run_cells,
        });
    }

    spans
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::grid::{Color, Style};

    fn cell(text: &str) -> Cell {
        Cell {
            text: text.to_string(),
            width: 1,
            padding: false,
            fg: Color::Default,
            bg: Color::Default,
            underline_color: Color::Default,
            style: Style::empty(),
        }
    }

    fn grid(cols: u16, rows: u16, rev: u64, fill: &str, edits: &[(usize, &str)]) -> PaneGrid {
        let mut cells = vec![cell(fill); (cols as usize) * (rows as usize)];
        for (idx, text) in edits {
            cells[*idx] = cell(text);
        }
        PaneGrid {
            cols,
            rows,
            cells,
            rev,
        }
    }

    #[test]
    fn identical_grids_produce_empty_delta() {
        let a = grid(4, 2, 1, ".", &[]);
        let b = grid(4, 2, 2, ".", &[]);
        assert!(diff(&a, &b).rows.is_empty());
    }

    #[test]
    fn single_cell_change_emits_one_row_one_span() {
        let a = grid(4, 3, 1, ".", &[]);
        let b = grid(4, 3, 2, ".", &[(6, "X")]);
        let d = diff(&a, &b);
        assert_eq!(d.rows.len(), 1);
        assert_eq!(d.rows[0].y, 1);
        assert_eq!(d.rows[0].spans.len(), 1);
        assert_eq!(d.rows[0].spans[0].x, 2);
        assert_eq!(d.rows[0].spans[0].cells.len(), 1);
        assert_eq!(d.rows[0].spans[0].cells[0].text, "X");
    }

    #[test]
    fn adjacent_changes_coalesce_into_one_span() {
        let a = grid(5, 1, 1, ".", &[]);
        let b = grid(5, 1, 2, ".", &[(1, "A"), (2, "B")]);
        let d = diff(&a, &b);
        assert_eq!(d.rows[0].spans.len(), 1);
        assert_eq!(d.rows[0].spans[0].x, 1);
        assert_eq!(d.rows[0].spans[0].cells.len(), 2);
    }

    #[test]
    fn non_adjacent_changes_produce_two_spans() {
        let a = grid(5, 1, 1, ".", &[]);
        let b = grid(5, 1, 2, ".", &[(0, "A"), (3, "B")]);
        let d = diff(&a, &b);
        assert_eq!(d.rows[0].spans.len(), 2);
        assert_eq!(d.rows[0].spans[0].x, 0);
        assert_eq!(d.rows[0].spans[1].x, 3);
    }

    #[test]
    fn first_frame_returns_full() {
        let mut differ = Differ::new();
        let g = grid(4, 2, 1, ".", &[]);
        assert!(matches!(differ.feed(g.clone()), DiffResult::Full(full) if full == g));
        assert_eq!(differ.current_rev(), Some(1));
    }

    #[test]
    fn dimension_change_returns_full() {
        let mut differ = Differ::new();
        differ.feed(grid(4, 2, 1, ".", &[]));
        let resized = grid(6, 3, 2, ".", &[]);
        assert!(matches!(differ.feed(resized.clone()), DiffResult::Full(full) if full == resized));
    }

    #[test]
    fn rev_gap_returns_resync() {
        let mut differ = Differ::new();
        differ.feed(grid(4, 2, 5, ".", &[]));
        let next = grid(4, 2, 10, ".", &[(0, "X")]);
        assert!(matches!(
            differ.feed_checked(next, Some(4)),
            DiffResult::Resync { have_rev: 5 }
        ));
        assert_eq!(differ.current_rev(), Some(10));
    }
}
