//! Atomic lifecycle state for a task with a CAS loop that resolves the
//! wake-vs-terminal TOCTOU race.
//!
//! `TaskState` is `repr(u8)`; the `AtomicU8` discriminant is the only
//! source of state truth. `AtomicTaskState::transition` rejects terminal
//! `expected` states without a CAS attempt and re-inspects every CAS failure
//! inside the loop body, so a concurrent terminal transition cannot be
//! mistaken for a spurious failure and retried indefinitely.
//!
//! ```text
//! Sleeping --wake()--> Woken --poll--> Running --Ready--> Done --join--> Taken
//!                                              \--> Cancelled
//!                                              \--> Failed
//! ```
#![allow(
    dead_code,
    reason = "task state is consumed by worker and scheduler, ported in a later PR"
)]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) satisfies unreachable_pub on this private module"
)]

#[cfg(not(loom))]
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU8, Ordering};

/// Lifecycle state of a task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum TaskState {
    /// Initial state. No waker pending and the future is not running.
    Sleeping = 0,
    /// `wake()` set this state. A worker will poll the task soon.
    Woken = 1,
    /// Currently being polled on a worker.
    Running = 2,
    /// Output written to the cell, awaiting [`TaskState::Taken`] by the
    /// join handle. Not terminal - a join is expected to follow.
    Done = 3,
    /// Terminal - cancelled before completion.
    Cancelled = 4,
    /// Terminal - panicked or returned an unrecoverable error.
    Failed = 5,
    /// Terminal - the join handle has read and consumed the output.
    Taken = 6,
    /// Terminal - the slot's task relocated to another worker's slab.
    ///
    /// Marks the *source* slot of a completed move. The slot's generation
    /// rolls immediately after the retire, so no live handle resolves it;
    /// only a cancel that raced the move can still observe this value.
    /// The task itself lives on at its destination slot.
    Retired = 7,
}

impl TaskState {
    /// Returns `true` for [`TaskState::Cancelled`], [`TaskState::Failed`],
    /// [`TaskState::Taken`], and [`TaskState::Retired`].
    ///
    /// [`TaskState::Done`] is *not* terminal: the output is sitting in
    /// the cell waiting for the join handle to consume it via the
    /// `Done -> Taken` transition. `Retired` is terminal for the *slot*:
    /// the task continues at its destination, but this slot is spent.
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::Failed | Self::Taken | Self::Retired
        )
    }

    /// Recovers a [`TaskState`] from a raw `u8` discriminant.
    ///
    /// # Panics
    ///
    /// Panics on values outside the valid range (0..=7). The atomic only
    /// stores values written from a `TaskState` cast, so the panic arm
    /// is unreachable in correct code.
    const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Sleeping,
            1 => Self::Woken,
            2 => Self::Running,
            3 => Self::Done,
            4 => Self::Cancelled,
            5 => Self::Failed,
            6 => Self::Taken,
            7 => Self::Retired,
            _ => panic!("invalid TaskState discriminant"),
        }
    }
}

/// Atomic [`TaskState`] container backed by an [`AtomicU8`], paired with
/// the relocation claim bit.
#[derive(Debug)]
pub(crate) struct AtomicTaskState {
    /// Lifecycle discriminant; the single source of state truth.
    state: AtomicU8,
    /// Relocation claim. A thief holds it across a slot move so the move
    /// has exclusive access to the slot body. Kept beside the state rather
    /// than inside it so the existing transition CAS paths stay untouched.
    claim: AtomicBool,
}

impl AtomicTaskState {
    /// New state initialized to [`TaskState::Sleeping`], unclaimed.
    #[cfg(not(loom))]
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU8::new(TaskState::Sleeping as u8),
            claim: AtomicBool::new(false),
        }
    }

    /// New state initialized to [`TaskState::Sleeping`], unclaimed.
    ///
    /// Non-`const` under loom because `loom::sync::atomic::AtomicU8::new`
    /// is instrumented and not available in const context.
    #[cfg(loom)]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(TaskState::Sleeping as u8),
            claim: AtomicBool::new(false),
        }
    }

    /// Loads the current state with `Acquire` ordering.
    #[inline]
    pub(crate) fn load(&self) -> TaskState {
        TaskState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Attempts to claim the slot for relocation.
    ///
    /// Returns `false` when another claimer already holds it. The claim
    /// gives the holder exclusive access to the slot body for the move
    /// window; it does not block the state CAS paths, which stay sound
    /// against the move through [`AtomicTaskState::try_retire`]'s
    /// `Sleeping -> Retired` compare-exchange.
    #[cfg_attr(
        not(any(test, feature = "steal")),
        expect(
            dead_code,
            reason = "the consumer is the slot relocation path, compiled only under the steal feature"
        )
    )]
    pub(crate) fn try_claim(&self) -> bool {
        self.claim
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Releases a held claim.
    #[cfg_attr(
        not(any(test, feature = "steal")),
        expect(
            dead_code,
            reason = "the consumer is the slot relocation path, compiled only under the steal feature"
        )
    )]
    pub(crate) fn release_claim(&self) {
        self.claim.store(false, Ordering::Release);
    }

    /// Retires a relocated source slot via a `Sleeping -> Retired` CAS.
    ///
    /// The single compare-exchange reconciles the move with concurrent
    /// actors: success means the move completed cleanly, while failure
    /// returns the state of whichever actor won the window -- a `Cancelled`
    /// or `Woken` winner stays intact and the thief aborts the move, so the
    /// displaced state is never overwritten and the slot drops correctly.
    ///
    /// # Errors
    ///
    /// Returns `Err(current)` when the slot is no longer `Sleeping`.
    #[cfg_attr(
        not(any(test, feature = "steal")),
        expect(
            dead_code,
            reason = "the consumer is the slot relocation path, compiled only under the steal feature"
        )
    )]
    pub(crate) fn try_retire(&self) -> Result<(), TaskState> {
        match self.state.compare_exchange(
            TaskState::Sleeping as u8,
            TaskState::Retired as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(current) => Err(TaskState::from_u8(current)),
        }
    }

    /// Attempts the transition `expected -> next`. Returns the actually
    /// observed state on failure.
    ///
    /// The terminal check is performed *inside* the CAS-failure arm, not
    /// between a separate `load` and the CAS. This avoids the TOCTOU
    /// race where a concurrent terminal transition between load and CAS
    /// causes an infinite retry loop.
    ///
    /// # Errors
    ///
    /// Returns `Err(current)` when `expected` is already terminal (early
    /// reject without a CAS) or when a concurrent transition raced ahead.
    pub(crate) fn transition(&self, expected: TaskState, next: TaskState) -> Result<(), TaskState> {
        if expected.is_terminal() {
            return Err(self.load());
        }
        loop {
            match self.state.compare_exchange_weak(
                expected as u8,
                next as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current_raw) => {
                    let current = TaskState::from_u8(current_raw);
                    if current.is_terminal() || current != expected {
                        return Err(current);
                    }
                }
            }
        }
    }

    /// Convenience wrapper for the `Sleeping -> Woken` transition.
    ///
    /// # Errors
    ///
    /// Returns the observed state if the task is no longer
    /// [`TaskState::Sleeping`].
    #[inline]
    pub(crate) fn wake(&self) -> Result<(), TaskState> {
        self.transition(TaskState::Sleeping, TaskState::Woken)
    }

    /// Transition `Woken -> Running` for poll entry.
    ///
    /// # Errors
    ///
    /// Returns the observed state if the task is not `Woken`.
    #[inline]
    pub(crate) fn try_start_poll(&self) -> Result<(), TaskState> {
        self.transition(TaskState::Woken, TaskState::Running)
    }

    /// Transition `Running -> Done` after poll returns `Ready`.
    ///
    /// # Errors
    ///
    /// Returns the observed state if the task is not `Running`.
    #[inline]
    pub(crate) fn complete(&self) -> Result<(), TaskState> {
        self.transition(TaskState::Running, TaskState::Done)
    }

    /// Transition `Running -> Sleeping` after poll returns `Pending`.
    ///
    /// If a concurrent wake changed the state to `Woken` during
    /// poll, the CAS fails and the caller must re-enqueue the task.
    ///
    /// # Errors
    ///
    /// Returns the observed state (typically `Woken` from a
    /// re-entrant wake during poll).
    #[inline]
    pub(crate) fn suspend(&self) -> Result<(), TaskState> {
        self.transition(TaskState::Running, TaskState::Sleeping)
    }

    /// Forces a transition to [`TaskState::Cancelled`] from any
    /// non-terminal state. Returns `true` if the task moved to
    /// `Cancelled`, `false` if it was already terminal.
    ///
    /// `Done` is treated as terminal here: the output is sitting in
    /// the cell awaiting the join handle, and a late cancel must not
    /// overwrite that success.
    pub(crate) fn cancel(&self) -> bool {
        loop {
            let current = self.load();
            match current {
                TaskState::Sleeping | TaskState::Woken | TaskState::Running => {
                    if self.transition(current, TaskState::Cancelled).is_ok() {
                        return true;
                    }
                }
                TaskState::Done
                | TaskState::Cancelled
                | TaskState::Failed
                | TaskState::Taken
                | TaskState::Retired => {
                    return false;
                }
            }
        }
    }
}

impl Default for AtomicTaskState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_sleeping() {
        let state = AtomicTaskState::new();
        assert_eq!(state.load(), TaskState::Sleeping);
    }

    #[test]
    fn wake_from_sleeping_succeeds_and_sets_woken() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.wake() else {
            panic!("wake from Sleeping must succeed");
        };
        assert_eq!(state.load(), TaskState::Woken);
    }

    #[test]
    fn wake_from_woken_returns_woken() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.wake() else {
            panic!("first wake must succeed");
        };
        match state.wake() {
            Err(TaskState::Woken) => {}
            other => panic!("second wake should observe Woken, got {other:?}"),
        }
    }

    #[test]
    fn lifecycle_sleeping_woken_running_done() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let Ok(()) = state.transition(TaskState::Woken, TaskState::Running) else {
            panic!("Woken -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Done) else {
            panic!("Running -> Done must succeed");
        };
        assert_eq!(state.load(), TaskState::Done);
    }

    #[test]
    fn running_to_cancelled_succeeds() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Cancelled) else {
            panic!("Running -> Cancelled must succeed");
        };
        assert_eq!(state.load(), TaskState::Cancelled);
    }

    #[test]
    fn running_to_failed_succeeds() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Failed) else {
            panic!("Running -> Failed must succeed");
        };
        assert_eq!(state.load(), TaskState::Failed);
    }

    #[test]
    fn terminal_rejects_wake_and_transition() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Cancelled) else {
            panic!("Sleeping -> Cancelled must succeed");
        };
        match state.wake() {
            Err(TaskState::Cancelled) => {}
            other => panic!("wake on Cancelled must fail, got {other:?}"),
        }
        match state.transition(TaskState::Cancelled, TaskState::Sleeping) {
            Err(TaskState::Cancelled) => {}
            other => panic!("Cancelled is terminal, expected Err(Cancelled) got {other:?}"),
        }
    }

    #[test]
    fn done_to_taken_succeeds() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Done) else {
            panic!("Running -> Done must succeed");
        };
        let Ok(()) = state.transition(TaskState::Done, TaskState::Taken) else {
            panic!("Done -> Taken must succeed (output consumed by join handle)");
        };
        assert_eq!(state.load(), TaskState::Taken);
    }

    #[test]
    fn taken_rejects_further_transition() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Taken) else {
            panic!("Sleeping -> Taken must succeed");
        };
        match state.transition(TaskState::Taken, TaskState::Sleeping) {
            Err(TaskState::Taken) => {}
            other => panic!("Taken is terminal, expected Err(Taken) got {other:?}"),
        }
    }

    #[test]
    fn is_terminal_classifies_every_variant() {
        assert!(!TaskState::Sleeping.is_terminal());
        assert!(!TaskState::Woken.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(!TaskState::Done.is_terminal());
        assert!(TaskState::Cancelled.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(TaskState::Taken.is_terminal());
    }

    #[test]
    fn cancel_from_sleeping_succeeds() {
        let state = AtomicTaskState::new();
        assert!(state.cancel());
        assert_eq!(state.load(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_from_woken_succeeds() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.wake() else {
            panic!("wake from Sleeping must succeed");
        };
        assert!(state.cancel());
        assert_eq!(state.load(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_from_running_succeeds() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        assert!(state.cancel());
        assert_eq!(state.load(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_from_done_is_noop() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Done) else {
            panic!("Running -> Done must succeed");
        };
        assert!(!state.cancel(), "Done must reject cancel");
        assert_eq!(state.load(), TaskState::Done);
    }

    #[test]
    fn cancel_from_cancelled_is_noop() {
        let state = AtomicTaskState::new();
        assert!(state.cancel());
        assert!(!state.cancel(), "Cancelled re-cancel must return false");
        assert_eq!(state.load(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_from_failed_is_noop() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.transition(TaskState::Running, TaskState::Failed) else {
            panic!("Running -> Failed must succeed");
        };
        assert!(!state.cancel(), "Failed must reject cancel");
        assert_eq!(state.load(), TaskState::Failed);
    }

    #[test]
    fn cancel_from_taken_is_noop() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Taken) else {
            panic!("Sleeping -> Taken must succeed");
        };
        assert!(!state.cancel(), "Taken must reject cancel");
        assert_eq!(state.load(), TaskState::Taken);
    }

    #[test]
    fn from_u8_round_trips_every_variant() {
        for variant in [
            TaskState::Sleeping,
            TaskState::Woken,
            TaskState::Running,
            TaskState::Done,
            TaskState::Cancelled,
            TaskState::Failed,
            TaskState::Taken,
            TaskState::Retired,
        ] {
            assert_eq!(TaskState::from_u8(variant as u8), variant);
        }
    }

    #[test]
    fn retired_is_terminal() {
        assert!(TaskState::Retired.is_terminal());
    }

    #[test]
    fn claim_is_exclusive_until_released() {
        let state = AtomicTaskState::new();
        assert!(state.try_claim(), "an unclaimed slot must accept a claim");
        assert!(
            !state.try_claim(),
            "a held claim must reject a second claimer"
        );
        state.release_claim();
        assert!(
            state.try_claim(),
            "a released claim must be claimable again"
        );
    }

    #[test]
    fn try_retire_succeeds_only_from_sleeping() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.try_retire() else {
            panic!("a sleeping slot must retire");
        };
        assert_eq!(state.load(), TaskState::Retired);

        let woken = AtomicTaskState::new();
        let Ok(()) = woken.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        assert_eq!(woken.try_retire(), Err(TaskState::Woken));
        assert_eq!(
            woken.load(),
            TaskState::Woken,
            "a failed retire leaves the winner intact"
        );
    }

    #[test]
    fn cancel_on_retired_is_a_no_op() {
        let state = AtomicTaskState::new();
        let Ok(()) = state.try_retire() else {
            panic!("a sleeping slot must retire");
        };
        assert!(!state.cancel(), "a retired slot must reject a late cancel");
        assert_eq!(state.load(), TaskState::Retired);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::{sync::Arc, thread};

    use super::*;

    #[test]
    fn dual_woken_to_running_only_one_wins() {
        loom::model(|| {
            let state = Arc::new(AtomicTaskState::new());
            let Ok(()) = state.wake() else {
                panic!("Sleeping -> Woken must succeed");
            };
            let s1 = Arc::clone(&state);
            let s2 = Arc::clone(&state);
            let h1 = thread::spawn(move || s1.transition(TaskState::Woken, TaskState::Running));
            let h2 = thread::spawn(move || s2.transition(TaskState::Woken, TaskState::Running));
            let Ok(r1) = h1.join() else {
                panic!("h1 panicked");
            };
            let Ok(r2) = h2.join() else {
                panic!("h2 panicked");
            };
            let wins = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
            assert_eq!(wins, 1, "exactly one Woken -> Running must succeed");
            assert_eq!(state.load(), TaskState::Running);
        });
    }

    #[test]
    fn running_to_done_vs_cancelled_only_one_wins() {
        loom::model(|| {
            let state = Arc::new(AtomicTaskState::new());
            let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
                panic!("Sleeping -> Running must succeed");
            };
            let s1 = Arc::clone(&state);
            let s2 = Arc::clone(&state);
            let h1 = thread::spawn(move || s1.transition(TaskState::Running, TaskState::Done));
            let h2 = thread::spawn(move || s2.transition(TaskState::Running, TaskState::Cancelled));
            let Ok(r1) = h1.join() else {
                panic!("h1 panicked");
            };
            let Ok(r2) = h2.join() else {
                panic!("h2 panicked");
            };
            match (r1, r2, state.load()) {
                (Ok(()), Err(TaskState::Done), TaskState::Done) => {}
                (Err(TaskState::Cancelled), Ok(()), TaskState::Cancelled) => {}
                other => panic!("unexpected race outcome: {other:?}"),
            }
        });
    }

    #[test]
    fn wake_and_transition_woken_running_observe_consistent_state() {
        loom::model(|| {
            let state = Arc::new(AtomicTaskState::new());
            let s_wake = Arc::clone(&state);
            let s_run = Arc::clone(&state);
            let h_wake = thread::spawn(move || s_wake.wake());
            let h_run =
                thread::spawn(move || s_run.transition(TaskState::Woken, TaskState::Running));
            let Ok(r_wake) = h_wake.join() else {
                panic!("wake thread panicked");
            };
            let Ok(r_run) = h_run.join() else {
                panic!("transition thread panicked");
            };
            let Ok(()) = r_wake else {
                panic!("wake from Sleeping must succeed: {r_wake:?}");
            };
            match r_run {
                Ok(()) => assert_eq!(state.load(), TaskState::Running),
                Err(TaskState::Sleeping) => assert_eq!(state.load(), TaskState::Woken),
                other => panic!("unexpected r_run: {other:?}"),
            }
        });
    }

    // The relocation protocol's crux race: a thief claiming and retiring a
    // Sleeping slot against a concurrent cancel. Exactly one side owns the
    // task afterward, with no interleaving where both or neither win.
    #[test]
    fn claim_then_retire_vs_cancel_exactly_one_wins() {
        loom::model(|| {
            let state = Arc::new(AtomicTaskState::new());
            let canceler = Arc::clone(&state);
            let cancel_thread = thread::spawn(move || canceler.cancel());
            let relocated = state.try_claim() && state.try_retire().is_ok();
            let Ok(cancelled) = cancel_thread.join() else {
                panic!("the cancel thread must join cleanly");
            };
            assert!(
                relocated != cancelled,
                "exactly one of relocation and cancel owns the task",
            );
            let expected = if relocated {
                TaskState::Retired
            } else {
                TaskState::Cancelled
            };
            assert_eq!(state.load(), expected);
        });
    }

    #[test]
    fn double_thief_claim_admits_exactly_one() {
        loom::model(|| {
            let state = Arc::new(AtomicTaskState::new());
            let other = Arc::clone(&state);
            let thief = thread::spawn(move || other.try_claim());
            let mine = state.try_claim();
            let Ok(theirs) = thief.join() else {
                panic!("the thief thread must join cleanly");
            };
            assert!(mine != theirs, "exactly one thief claims the slot");
        });
    }
}
