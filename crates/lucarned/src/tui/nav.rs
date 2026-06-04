//! Shared list-navigation helpers (MNT-007).
//!
//! Every panel that drives a `ratatui` [`ListState`] over a `Vec` needs the same
//! two operations: STEP the selection one row (clamped to the list bounds) and
//! CLAMP a possibly-stale selection back into range after the list changes. This
//! logic was duplicated in `SessionsPanel` (`select_next`/`select_previous`/
//! `clamp_selection`), `ConfigPanel` (`select_next`/`select_previous`/
//! `reload_rows`), and the `ui.rs` render guard; collecting it here removes the
//! drift between those copies and gives one tested seam.
//!
//! Panels differ only in what an EMPTY list means for the selection — Sessions
//! drops to `None` (no row to act on), Config clamps to index 0 when non-empty and
//! `None` when empty. That single difference is captured by [`EmptyPolicy`], so
//! both panels share the same step/clamp body and only pass their policy.

use ratatui::widgets::ListState;

/// What the selection becomes when the list is EMPTY.
///
/// The only behavioral difference between the panels' navigation: an empty list
/// always yields `None` (there is nothing to select) — this enum exists for the
/// NON-empty default that [`clamp`] applies when there is currently no selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyPolicy {
    /// A non-empty list with no current selection stays unselected until the user
    /// moves (Sessions: navigation seeds index 0 itself, but a bare clamp leaves
    /// `None` as `None`). Used by the Sessions panel.
    KeepNone,
    /// A non-empty list with no current selection clamps to index 0 (Config: the
    /// form always has a focused row when it has any rows).
    SelectFirst,
}

/// Move `state`'s selection one row within `[0, len)`, clamped at both ends.
///
/// `forward` steps down (toward higher indices); `!forward` steps up. An empty
/// list (`len == 0`) clears the selection (`None`). With a selection already set,
/// the step saturates at the first/last row (never wraps, never goes
/// out of bounds). With no selection on a non-empty list, the first step lands on
/// index 0 (so the first arrow press always selects the top row).
pub fn step(state: &mut ListState, len: usize, forward: bool) {
    if len == 0 {
        state.select(None);
        return;
    }
    let next = match state.selected() {
        Some(i) if forward => (i + 1).min(len - 1),
        Some(i) => i.saturating_sub(1),
        None => 0,
    };
    state.select(Some(next));
}

/// Clamp `state`'s selection into `[0, len)` after the backing list may have
/// changed length, applying `policy` for the no-selection case.
///
/// - `len == 0` → always `None` (no row to select).
/// - a selection `>= len` (a stale index after a shrink) → the last valid row.
/// - an in-range selection → unchanged.
/// - no selection on a non-empty list → `policy` decides: [`EmptyPolicy::KeepNone`]
///   leaves it `None`; [`EmptyPolicy::SelectFirst`] selects index 0.
pub fn clamp(state: &mut ListState, len: usize, policy: EmptyPolicy) {
    match state.selected() {
        _ if len == 0 => state.select(None),
        Some(i) if i >= len => state.select(Some(len - 1)),
        Some(_) => {}
        None => match policy {
            EmptyPolicy::KeepNone => {}
            EmptyPolicy::SelectFirst => state.select(Some(0)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_on_empty_list_clears_selection() {
        let mut state = ListState::default();
        state.select(Some(3)); // a stale selection from a previous, longer list
        step(&mut state, 0, true);
        assert_eq!(state.selected(), None);
        step(&mut state, 0, false);
        assert_eq!(state.selected(), None);
    }

    #[test]
    fn step_seeds_index_zero_when_unselected() {
        let mut state = ListState::default();
        // No selection + a non-empty list: the first step (either direction) lands
        // on the top row.
        step(&mut state, 3, true);
        assert_eq!(state.selected(), Some(0));
        let mut state2 = ListState::default();
        step(&mut state2, 3, false);
        assert_eq!(state2.selected(), Some(0));
    }

    #[test]
    fn step_clamps_at_both_ends() {
        let mut state = ListState::default();
        state.select(Some(0));
        // Up at the top stays at 0.
        step(&mut state, 3, false);
        assert_eq!(state.selected(), Some(0));
        // Down stops at the last index, never past it.
        for _ in 0..10 {
            step(&mut state, 3, true);
        }
        assert_eq!(state.selected(), Some(2));
    }

    #[test]
    fn clamp_empty_drops_selection_under_both_policies() {
        for policy in [EmptyPolicy::KeepNone, EmptyPolicy::SelectFirst] {
            let mut state = ListState::default();
            state.select(Some(2));
            clamp(&mut state, 0, policy);
            assert_eq!(state.selected(), None, "empty list always clears selection");
        }
    }

    #[test]
    fn clamp_stale_index_snaps_to_last_row() {
        let mut state = ListState::default();
        state.select(Some(5)); // stale index past the new (shorter) list
        clamp(&mut state, 3, EmptyPolicy::KeepNone);
        assert_eq!(state.selected(), Some(2));
    }

    #[test]
    fn clamp_no_selection_follows_policy() {
        // KeepNone: a non-empty list with no selection stays None.
        let mut keep = ListState::default();
        clamp(&mut keep, 3, EmptyPolicy::KeepNone);
        assert_eq!(keep.selected(), None);
        // SelectFirst: a non-empty list with no selection clamps to 0.
        let mut first = ListState::default();
        clamp(&mut first, 3, EmptyPolicy::SelectFirst);
        assert_eq!(first.selected(), Some(0));
    }

    #[test]
    fn clamp_in_range_selection_is_unchanged() {
        let mut state = ListState::default();
        state.select(Some(1));
        clamp(&mut state, 3, EmptyPolicy::SelectFirst);
        assert_eq!(state.selected(), Some(1));
    }
}
