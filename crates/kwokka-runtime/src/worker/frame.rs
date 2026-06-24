//! Per-worker poll frame -- the disjoint handle a polled task uses to reach
//! its worker without re-entering the task slab through the run-loop's borrow.
//!
//! [`PollFrame`] carries the polling task's [`TaskRef`], a pointer to the
//! worker's spawn inbox (a field disjoint from the slab), the slab pointer
//! itself (so a child cancel resolves a different slot), the first-child
//! snapshot (for settled-children walks that never read the parent slot), and
//! the worker's reap queue and I/O driver. The per-worker install/clear cycle
//! and the `poll_one` entry live in [`polling`](crate::worker::polling).

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
    task::Poll,
};

use kwokka_core::slab::{Slab, SlabKey};
use kwokka_io::{
    DriverType, IoDriver,
    operation::{IoRequest, SubmitResult},
};

use crate::{
    task::{
        JoinError, TaskRef,
        cell::{header::WakeData, slot::TaskSlot},
        state::TaskState,
    },
    timer::request::{TIMER_INBOX_CAPACITY, TimerInbox, TimerRequest},
    worker::{
        inbox::{PendingSpawn, SPAWN_INBOX_CAPACITY, SpawnInbox},
        reap::{REAP_QUEUE_CAPACITY, ReapQueue},
    },
};

/// The slab-free handle a polled task uses to reach its worker.
///
/// Carries the polling task's [`TaskRef`] (the structured parent of any child
/// it spawns) and a pointer to the worker's spawn inbox. The pointer targets a
/// field disjoint from the task slab, which is what makes reaching it during a
/// poll sound.
pub(crate) struct PollFrame {
    /// The task being polled -- the structured parent of its child spawns.
    pub(crate) current: TaskRef,
    /// The owning worker's spawn inbox, a field disjoint from its task slab.
    pub(crate) inbox: NonNull<SpawnInbox<SPAWN_INBOX_CAPACITY>>,
    /// The owning worker's task slab. A child cancel resolves a slot here at a
    /// *different* index than [`current`](PollFrame::current), so the read is
    /// disjoint from the parent poll borrow. [`poll_one`] derives this pointer
    /// and the parent `&mut` from one raw slab pointer.
    pub(crate) tasks: NonNull<Slab<TaskSlot>>,
    /// The parent's first child at poll entry, captured from the parent `&mut`
    /// before the poll. [`are_children_all_settled`](PollFrame::are_children_all_settled)
    /// walks from here so it never reads the parent slot (which the poll holds
    /// `&mut`), avoiding a self-alias.
    pub(crate) first_child: Option<TaskRef>,
    /// The owning worker's reap queue, a field disjoint from its task slab. A
    /// scope that settles records its parent here so the post-poll reap path
    /// frees the settled children's slots without re-borrowing the slab.
    pub(crate) reap: NonNull<ReapQueue<REAP_QUEUE_CAPACITY>>,
    /// The owning worker's io driver, or `None` for a test frame with no I/O
    /// backend. A field disjoint from the task slab, reached during a poll so
    /// the polled task can submit an op. The production run-loop always installs
    /// `Some`; `None` arises only in unit tests that build a frame directly.
    pub(crate) driver: Option<NonNull<DriverType>>,
    /// The completing I/O result for the polling task, captured from its header
    /// at poll entry like [`first_child`](PollFrame::first_child). An I/O future
    /// reads it through [`with_current`] without touching its own slot, which
    /// the poll holds `&mut`. `WakeData::EMPTY` until a completion drain stores a
    /// result; refreshed every poll so a result stored between polls is visible.
    pub(crate) wake_data: WakeData,
    /// Count of ops this poll submitted, applied to the polling task's
    /// header after the poll returns. The submit paths take `&self` and the
    /// task's own header is unreachable mid-poll (the poll holds the slot
    /// `&mut`), so the count rides the frame and [`poll_one`] lands it on
    /// the header through the `&mut` it still legitimately holds.
    pub(crate) submitted_ops: AtomicU16,
    /// The owning worker's timer-request inbox, or `None` for a test frame with
    /// no timer wiring. A field disjoint from the task slab, reached during a
    /// poll so a sleeping future can arm a timer without touching the wheel
    /// directly; the run-loop drains it after the poll and registers each
    /// request on the wheel against its own clock.
    pub(crate) timer_requests: Option<NonNull<TimerInbox<TIMER_INBOX_CAPACITY>>>,
}

impl PollFrame {
    /// Pushes a child spawn into the worker's inbox, handing it back when the
    /// inbox is full.
    pub(crate) fn push_spawn(&self, request: PendingSpawn) -> Option<PendingSpawn> {
        // SAFETY: Invariant -- `inbox` was formed from a `&mut SpawnInbox`
        // that no other live reference aliases for the duration of this call,
        // so this `&mut` is unique and carries write provenance. The
        // production caller (the worker run-loop) satisfies this by pointing
        // `inbox` at its spawn inbox, a field disjoint from the task slab it
        // holds `&mut` on across the poll. The disjoint-field basis is
        // `WorkerShard::spawn_inbox`, a field disjoint from `WorkerShard::tasks`
        // that `cycle::tick` borrows separately; the current push callers are
        // tests pointing at a stack-local inbox.
        // Precondition: the caller formed `inbox` from a non-aliased
        // `&mut SpawnInbox`, and one future polls at a time per worker, so at
        // most one such `&mut` exists at once.
        // Failure mode: aliasing the inbox with another live `&mut` (were it
        // the same allocation as the borrowed task slab) is
        // double-mutable-aliasing UB.
        let inbox = unsafe { &mut *self.inbox.as_ptr() };
        inbox.push(request)
    }

    /// Arms a timer for the polling task, returning `false` when no timer inbox
    /// is installed (a test frame) or the inbox is full.
    ///
    /// The future records a relative `delay_ticks`; the run-loop computes the
    /// absolute deadline against its clock when it drains the inbox after the
    /// poll, so this path never reads the clock.
    pub(crate) fn request_timer(&self, task_ref: TaskRef, delay_ticks: u64) -> bool {
        let Some(timer_requests) = self.timer_requests else {
            return false;
        };
        // SAFETY: Invariant -- `timer_requests` was formed from a
        // `&mut TimerInbox` that no other live reference aliases for this call,
        // so this `&mut` is unique and carries write provenance. The production
        // caller (the worker run-loop) points it at `WorkerShard::timer_requests`,
        // a field disjoint from `WorkerShard::tasks` the poll holds `&mut` on
        // across the window, so the write never aliases the parent poll borrow.
        // Precondition: the caller formed `timer_requests` from a non-aliased
        // `&mut TimerInbox`, and one future polls at a time per worker, so at
        // most one such `&mut` exists at once.
        // Failure mode: aliasing the inbox with another live `&mut` (were it the
        // same allocation as the borrowed task slab) is double-mutable-aliasing UB.
        let timer_requests = unsafe { &mut *timer_requests.as_ptr() };
        timer_requests.push(TimerRequest {
            task_ref,
            delay_ticks,
        })
    }

    /// Records `parent` in the worker's reap queue for post-poll reclamation of
    /// its settled scope children.
    ///
    /// Returns `false` when the queue is full -- the record is dropped and that
    /// scope's settled children wait for the slab drop instead.
    pub(crate) fn push_reap(&self, parent: TaskRef) -> bool {
        // SAFETY: Invariant -- `reap` was formed from a `&mut ReapQueue` that no
        // other live reference aliases for this call, so this `&mut` is unique
        // and carries write provenance. The production caller (the worker
        // run-loop) points `reap` at `WorkerShard::reap_queue`, a field disjoint
        // from `WorkerShard::tasks` that the poll holds `&mut` on across the
        // window, so the write never aliases the parent poll borrow; the current
        // push callers are tests pointing at a stack-local queue.
        // Precondition: the caller formed `reap` from a non-aliased
        // `&mut ReapQueue`, and one future polls at a time per worker, so at most
        // one such `&mut` exists at once.
        // Failure mode: aliasing the queue with another live `&mut` (were it the
        // same allocation as the borrowed task slab) is double-mutable-aliasing
        // UB.
        let reap = unsafe { &mut *self.reap.as_ptr() };
        reap.push(parent)
    }

    /// Submits a no-buffer op (accept, connect, timeout, cancel, `msg_ring`) on
    /// behalf of the polling task, returning the driver's submit result.
    ///
    /// Returns `None` when the frame carries no driver -- a test frame without
    /// an I/O backend. The production run-loop always installs the worker's
    /// driver, so `None` is reached only from unit tests that build a frame
    /// directly.
    pub(crate) fn submit_internal(&self, request: IoRequest<()>) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` was formed from `&mut WorkerShard::driver`
        // coerced to a `NonNull`, a field disjoint from `WorkerShard::tasks` the
        // poll holds `&mut` on across the window, so this shared reborrow never
        // aliases the parent poll borrow. `IoDriver::submit_internal` takes
        // `&self`, so a shared `&DriverType` suffices -- weaker than the `&mut`
        // reborrow `push_spawn` forms, and the pointer's write provenance (from
        // the `&mut`) dominates this shared read.
        // Precondition: reached only via `with_current` during a poll on this
        // worker, where the run-loop pointed `driver` at its shard's driver; the
        // `install` / `clear` bracket (`FrameGuard`, including unwind) keeps the
        // referent live for the poll window.
        // Failure mode: a read after `clear` would deref a dangling driver
        // pointer; the guard clears the frame before the referent is reclaimed.
        let result = unsafe { driver.as_ref().submit_internal(request) };
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted_ops.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Cancels `child` by flipping its task state to `Cancelled`.
    ///
    /// Reads the child's slot through the frame's slab pointer. Returns `true`
    /// when the state moved to `Cancelled`; `false` when the child was already
    /// terminal, no longer resolves (a reclaimed slot), is an arena-path ref
    /// (not stored in this slab), or is the polling task itself. A task cannot
    /// cancel itself through this path: its own slot is the live `&mut` parent
    /// of the poll, so the guard returns a no-op rather than aliasing it.
    pub(crate) fn cancel_child(&self, child: TaskRef) -> bool {
        // An arena-path child is not stored in this slab; a self-cancel would
        // alias the parent poll's `&mut`. Both resolve to a no-op before the
        // slab read, so `child` is a slab-path ref at a distinct index below.
        if child.is_arena() || child.index() == self.current.index() {
            return false;
        }
        let key = SlabKey::new(child.index(), child.generation());
        // SAFETY: Invariant -- `self.tasks` is the worker's live slab, the same
        // pointer `poll_one` derived the parent `&mut` from. `slot_ptr`
        // generation-checks the child and yields a pointer into the Vec heap
        // buffer at `child.index()`, a distinct element from the parent slot
        // (the guard above proved `child.index() != self.current.index()`), so
        // this shared read is structurally disjoint from the parent `&mut` held
        // across the poll -- distinct Vec indices never alias.
        // Precondition: reached only via `with_current` during a poll on this
        // worker, so `self` is the installed frame and the parent `&mut` is the
        // only other live reference into the slab; the guard above narrowed
        // `child` to a live slab-path ref at a distinct index.
        // Failure mode: a same-index child (excluded by the guard) would alias
        // the parent's exclusive `&mut` -- double-mutable-aliasing UB. `cancel`
        // is an atomic CAS on `&self`, sound even were the read to overlap.
        unsafe {
            (*self.tasks.as_ptr())
                .slot_ptr(key)
                .is_some_and(|slot| slot.as_ref().header().state.cancel())
        }
    }

    /// Polls the join of `child`, reading its slot through the frame's slab
    /// pointer.
    ///
    /// Resolves the child slot at a Vec index disjoint from the polling parent
    /// and branches on its state. Returns `Ready(Ok(T))` after moving the
    /// output out (advancing `Done -> Taken`), `Ready(Err(..))` for a cancelled
    /// or failed task, and `Pending` for a non-terminal task, a self-join, or an
    /// arena-path ref. A reclaimed slot resolves to the lost-task terminal.
    pub(crate) fn join_child<T>(&self, child: TaskRef) -> Poll<Result<T, JoinError>> {
        if child.is_arena() || child.index() == self.current.index() {
            // An arena-path child is not in this slab; a self-join would alias
            // the parent poll's `&mut`. Neither resolves here, so park.
            return Poll::Pending;
        }
        let key = SlabKey::new(child.index(), child.generation());
        // SAFETY: Invariant -- `self.tasks` is the worker's live slab, the same
        // pointer `poll_one` derived the parent `&mut` from. `slot_ptr`
        // generation-checks the child and yields a pointer into the Vec heap
        // buffer at `child.index()`, a distinct element from the parent slot
        // (the guard above proved `child.index() != self.current.index()`), so
        // this shared read is structurally disjoint from the parent `&mut` held
        // across the poll -- distinct Vec indices never alias.
        // Precondition: reached only via `with_current` during a poll on this
        // worker, so the parent `&mut` is the only other live slab reference;
        // the guard narrowed `child` to a live slab-path ref at a distinct
        // index. The byte move is delegated to `take_output_shared`, sound
        // through `TaskSlot`'s `UnsafeCell` and the `Done -> Taken` guard.
        // Failure mode: a same-index child (excluded by the guard) would alias
        // the parent's exclusive `&mut` -- double-mutable-aliasing UB.
        let resolved = unsafe { (*self.tasks.as_ptr()).slot_ptr(key).map(|p| &*p.as_ptr()) };
        let Some(slot) = resolved else {
            // The slot was reclaimed -- the joinee is gone. `Pending` would hang
            // forever (it can never reach `Done`), so resolve the lost-task
            // terminal.
            return Poll::Ready(Err(JoinError::Cancelled));
        };
        match slot.header().state.load() {
            // `take_output_shared` re-runs the `Done -> Taken` CAS; in the 0.1.0
            // single-handle-per-task model the joiner is the only reader, so the
            // CAS cannot lose a race between the load here and the take.
            TaskState::Done => Poll::Ready(Ok(slot.take_output_shared::<T>())),
            TaskState::Failed => Poll::Ready(Err(JoinError::Failed)),
            // `Cancelled` is the real terminal: cancelled before producing a value.
            TaskState::Cancelled => Poll::Ready(Err(JoinError::Cancelled)),
            // `Taken` is a double-join: an earlier join already moved the output
            // out. A logic error, surfaced as its own terminal rather than masked
            // as a cancellation.
            TaskState::Taken => Poll::Ready(Err(JoinError::AlreadyJoined)),
            TaskState::Sleeping | TaskState::Woken | TaskState::Running => Poll::Pending,
            // A relocated husk resolves through its live generation, but a
            // joinable task is never offered to the steal path: handle
            // minting exists only in tests today, and the production
            // minting path pins its task when it lands.
            TaskState::Retired => {
                unreachable!("a joined task is never relocated")
            }
        }
    }

    /// Returns `true` when every child of the polling task has settled --
    /// reached a terminal state (`Cancelled`/`Failed`/`Taken`) or `Done`.
    ///
    /// Walks from [`first_child`](PollFrame::first_child), captured at poll entry,
    /// along each child's `next_sibling`, so it never reads the parent slot the
    /// poll holds `&mut`. An empty list is trivially settled. `Done` counts as
    /// settled: a handle-less scope child has no joiner to advance it to `Taken`.
    pub(crate) fn are_children_all_settled(&self) -> bool {
        let mut next = self.first_child;
        while let Some(child) = next {
            let key = SlabKey::new(child.index(), child.generation());
            // SAFETY: Invariant -- `self.tasks` is the worker's live slab, the
            // same pointer `poll_one` derived the parent `&mut` from. `slot_ptr`
            // yields a pointer to a distinct Vec element -- a child reached from
            // `first_child`/`next_sibling`, never the parent itself (`push_child`
            // links only spawned children, never a task under itself) -- so this
            // shared read is structurally disjoint from the parent `&mut` held
            // across the poll. The walk only reads state/next_sibling; it forms
            // no `&mut` and mutates nothing.
            // Precondition: reached via `with_current` during a poll on this
            // worker; `first_child` was captured from the parent `&mut` at poll
            // entry, so the parent slot is not read here.
            // Failure mode: reading the parent's own slot would alias the parent
            // `&mut` -- excluded because the walk visits children only.
            let Some(slot) = (unsafe { (*self.tasks.as_ptr()).slot_ptr(key) }) else {
                // A reclaimed child slot. Reap runs post-poll, never during a
                // walk, so a child is not freed mid-walk; stop rather than chase
                // a dangling next_sibling if that invariant ever breaks.
                break;
            };
            // SAFETY: Invariant -- `slot` is a live child slot from `slot_ptr`
            // (generation-checked), at a distinct index from the parent. The
            // shared header read of state and next_sibling does not alias the
            // parent `&mut`.
            // Precondition: same poll window as the slot_ptr read above.
            // Failure mode: a stale slot would have failed the generation check
            // and returned `None` above.
            let header = unsafe { slot.as_ref().header() };
            let state = header.state.load();
            let is_settled = if state == TaskState::Retired {
                // A husk counts settled only once its task's settled note
                // landed; until then the task is alive on another worker.
                header.is_remote_settled
            } else {
                state.is_terminal() || state == TaskState::Done
            };
            if !is_settled {
                return false;
            }
            next = header.next_sibling;
        }
        true
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use kwokka_core::{Generation, id::Pip, namespace::Namespace};

    use super::*;
    use crate::task::{
        cell::header::{Slot, WakeData},
        state::TaskState,
    };

    struct Inert;
    impl Future for Inert {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    /// Inserts an inert task into `slab`, returning its key and a slab-path ref.
    fn seed_child(slab: &mut Slab<TaskSlot>, worker_id: u8) -> (SlabKey, TaskRef) {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Inert).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        (key, TaskRef::from_slab(worker_id, key))
    }

    /// A `current` ref whose index differs from any seeded child, so the
    /// self-cancel guard does not fire.
    fn other_ref() -> TaskRef {
        TaskRef::from_arena(0, u32::MAX, Generation::from_raw(1))
    }

    struct Ready42;
    impl Future for Ready42 {
        type Output = u32;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
            Poll::Ready(42)
        }
    }

    /// Inserts a `Ready42` task and drives it to `Done` with its output written.
    fn seed_done(slab: &mut Slab<TaskSlot>, worker_id: u8) -> (SlabKey, TaskRef) {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready42).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        {
            let Some(slot) = slab.get_mut(key) else {
                panic!("the task must resolve");
            };
            let Ok(()) = slot
                .header()
                .state
                .transition(TaskState::Sleeping, TaskState::Running)
            else {
                panic!("Sleeping -> Running must succeed");
            };
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                slot.poll_via_vtable(&mut context),
                Poll::Ready(())
            ));
            let Ok(()) = slot.header().state.complete() else {
                panic!("Running -> Done must succeed");
            };
        }
        (key, TaskRef::from_slab(worker_id, key))
    }

    #[test]
    fn are_children_all_settled_is_true_when_no_children() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(frame.are_children_all_settled());
    }

    #[test]
    fn are_children_all_settled_is_false_when_a_child_is_unsettled() {
        let mut slab = Slab::<TaskSlot>::new(2);
        // `seed_child` inserts an inert task in `Sleeping` -- not settled.
        let (_key, child) = seed_child(&mut slab, 40);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: Some(child),
        };
        assert!(!frame.are_children_all_settled());
    }

    #[test]
    fn are_children_all_settled_counts_done_as_settled() {
        let mut slab = Slab::<TaskSlot>::new(2);
        // A handle-less scope child completes at `Done` (no joiner advances it
        // to `Taken`), so the walk must treat `Done` as settled.
        let (_key, child) = seed_done(&mut slab, 40);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: Some(child),
        };
        assert!(frame.are_children_all_settled());
    }

    #[test]
    fn a_husk_settles_only_after_its_note_lands() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (key, child) = seed_child(&mut slab, 41);
        {
            let Some(slot) = slab.get(key) else {
                panic!("child must resolve");
            };
            let Ok(()) = slot.header().state.try_retire() else {
                panic!("Sleeping -> Retired must succeed");
            };
        }
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        // Each walk derives its frame inside its own borrow window, like a
        // real pass: the slab mutation between them would invalidate a
        // frame pointer formed earlier.
        {
            let frame = PollFrame {
                current: other_ref(),
                inbox: NonNull::from(&mut inbox),
                tasks: NonNull::from(&mut slab),
                driver: None,
                wake_data: WakeData::EMPTY,
                timer_requests: None,
                submitted_ops: AtomicU16::new(0),
                reap: NonNull::from(&mut reap),
                first_child: Some(child),
            };
            assert!(
                !frame.are_children_all_settled(),
                "a live-elsewhere husk must hold the scope open",
            );
        }
        let Some(slot) = slab.get_mut(key) else {
            panic!("husk must resolve");
        };
        slot.header_mut().is_remote_settled = true;
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: Some(child),
        };
        assert!(
            frame.are_children_all_settled(),
            "the landed note settles the husk",
        );
    }

    #[test]
    fn cancel_child_flips_sleeping_to_cancelled() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (child_key, child) = seed_child(&mut slab, 20);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(
            frame.cancel_child(child),
            "a sleeping child flips to cancelled"
        );
        let Some(slot) = slab.get(child_key) else {
            panic!("the child must still resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_child_noop_on_terminal() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (child_key, child) = seed_child(&mut slab, 21);
        {
            let Some(slot) = slab.get(child_key) else {
                panic!("the child must resolve");
            };
            let Ok(()) = slot
                .header()
                .state
                .transition(TaskState::Sleeping, TaskState::Running)
            else {
                panic!("Sleeping -> Running must succeed");
            };
            let Ok(()) = slot
                .header()
                .state
                .transition(TaskState::Running, TaskState::Failed)
            else {
                panic!("Running -> Failed must succeed");
            };
        }
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(
            !frame.cancel_child(child),
            "a terminal child rejects cancel"
        );
        let Some(slot) = slab.get(child_key) else {
            panic!("the child must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Failed);
    }

    #[test]
    fn cancel_child_noop_on_stale_generation() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let (stale_key, stale_child) = seed_child(&mut slab, 22);
        let Some(_) = slab.remove(stale_key) else {
            panic!("remove must succeed");
        };
        // Reinsert into the same index rolls the generation past `stale_child`.
        let (_fresh_key, _fresh) = seed_child(&mut slab, 22);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(
            !frame.cancel_child(stale_child),
            "a stale generation resolves to a no-op",
        );
    }

    #[test]
    fn cancel_child_noop_on_self() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let (key, task) = seed_child(&mut slab, 23);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: task,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(
            !frame.cancel_child(task),
            "a task cannot cancel itself through this path",
        );
        let Some(slot) = slab.get(key) else {
            panic!("the task must resolve");
        };
        assert_eq!(
            slot.header().state.load(),
            TaskState::Sleeping,
            "the self-cancel guard left the state untouched",
        );
    }

    #[test]
    fn cancel_child_noop_on_arena_ref() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let arena_child = TaskRef::from_arena(0, 0, Generation::from_raw(1));
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert!(
            !frame.cancel_child(arena_child),
            "an arena-path child is not stored in this slab",
        );
    }

    #[test]
    fn join_child_done_yields_output_and_marks_taken() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (key, child) = seed_done(&mut slab, 40);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(frame.join_child::<u32>(child), Poll::Ready(Ok(42)));
        let Some(slot) = slab.get(key) else {
            panic!("the task must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Taken);
    }

    #[test]
    fn join_child_double_join_is_err_already_joined() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (_key, child) = seed_done(&mut slab, 40);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(frame.join_child::<u32>(child), Poll::Ready(Ok(42)));
        assert_eq!(
            frame.join_child::<u32>(child),
            Poll::Ready(Err(JoinError::AlreadyJoined)),
            "a second join observes Taken and yields the double-join terminal",
        );
    }

    #[test]
    fn join_child_cancelled_is_err_cancelled() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (child_key, child) = seed_child(&mut slab, 41);
        {
            let Some(slot) = slab.get(child_key) else {
                panic!("the child must resolve");
            };
            assert!(slot.header().state.cancel());
        }
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(
            frame.join_child::<u32>(child),
            Poll::Ready(Err(JoinError::Cancelled)),
        );
    }

    #[test]
    fn join_child_failed_is_err_failed() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (child_key, child) = seed_child(&mut slab, 42);
        {
            let Some(slot) = slab.get(child_key) else {
                panic!("the child must resolve");
            };
            let Ok(()) = slot
                .header()
                .state
                .transition(TaskState::Sleeping, TaskState::Running)
            else {
                panic!("Sleeping -> Running must succeed");
            };
            let Ok(()) = slot
                .header()
                .state
                .transition(TaskState::Running, TaskState::Failed)
            else {
                panic!("Running -> Failed must succeed");
            };
        }
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(
            frame.join_child::<u32>(child),
            Poll::Ready(Err(JoinError::Failed)),
        );
    }

    #[test]
    fn join_child_non_terminal_is_pending() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let (_key, child) = seed_child(&mut slab, 43);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(frame.join_child::<u32>(child), Poll::Pending);
    }

    #[test]
    fn join_child_stale_generation_is_err_cancelled() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let (stale_key, stale_child) = seed_child(&mut slab, 44);
        let Some(_) = slab.remove(stale_key) else {
            panic!("remove must succeed");
        };
        let (_fresh_key, _fresh) = seed_child(&mut slab, 44);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(
            frame.join_child::<u32>(stale_child),
            Poll::Ready(Err(JoinError::Cancelled)),
            "a reclaimed joinee resolves to the lost-task terminal",
        );
    }

    #[test]
    fn join_child_self_join_is_pending() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let (_key, task) = seed_child(&mut slab, 45);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: task,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(
            frame.join_child::<u32>(task),
            Poll::Pending,
            "a task cannot join itself through the disjoint path",
        );
    }

    #[test]
    fn join_child_arena_ref_is_pending() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let arena_child = TaskRef::from_arena(0, 0, Generation::from_raw(1));
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: other_ref(),
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(frame.join_child::<u32>(arena_child), Poll::Pending);
    }
}
