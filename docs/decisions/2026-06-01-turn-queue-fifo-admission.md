# Turn Queue FIFO Admission

## Context

WeChat can receive a second reply for the same routed workspace while the older
submitted turn is still waiting for provider output. `LucarneCore::submit_turn`
previously recorded submitted turns only for event attribution. After the
provider accepted input, the API returned and a later caller could submit another
input to the same live session before the older turn emitted completion or
failure.

The old scheduler also counted queued turns separately from the mutex waiter
that actually owned order. A later admission could acquire the mutex during the
gap between an older queued turn being counted and that waiter's task being
polled.

## Decision

Per-workspace FIFO order is owned by `TurnScheduler` admission and represented
as an explicit in-memory queue. A permit owns the active turn; releasing it
grants exactly the next queued waiter before any later admission can become
ready. A granted-but-not-yet-awaited waiter still counts as ahead of later
turns.

`LucarneCore::submit_turn` is the single user-message live submission entry.
It acquires a per-workspace permit, opens the durable control-plane turn, records
the inbound user timeline item, submits to the live session, and stores the
permit with that turn lifecycle. The permit is released only when the submitted
turn completes, fails, times out, or is explicitly removed with the live
workspace. Provider and channel integrations do not own this ordering policy.

`start_turn` remains a control-plane lifecycle primitive for command workflows
and tests, but it no longer registers a submitted turn or participates in live
admission. Channels must not pre-open a user turn and then submit directly to the
provider session.

Channel projections may still record channel-side artifacts while draining
events. Telegram re-applies turn completion after its drain finishes so
permission callbacks registered from earlier events cannot outlive a completed
turn when the core event pump observes `TurnCompleted` before the channel
projection consumes all queued events. This is lifecycle cleanup, not a second
provider submission path.

## Consequences

- Queue order no longer depends on Tokio task polling order for spawned mutex
  waiters.
- A newer same-workspace inbound message cannot reach the provider until the
  older submitted turn has left the current submitted-turn slot.
- There is no compatibility path that reuses a previously opened user turn.
  Mixing `start_turn` with `submit_turn` is rejected by the control-plane live
  state instead of being silently accepted.
- Queue semantics remain provider-agnostic; WeChat and any other caller that
  enters through `submit_turn` use the same core lifecycle.
- Telegram normal user turns enter through `submit_turn`; command workflows keep
  using the provider command API because that is a distinct provider capability,
  not a user-message submit.
- Bot state and the daemon core must refer to the same live runtime registry.
  A cached live handle that is absent from the core registry is discarded and
  reopened/resumed before the next user-message turn instead of submitting
  directly to the stale handle.
- Providers still own provider parsing, resume, discovery, and session behavior.
