//! Per-worker poll frame -- the disjoint handle a polled task uses to reach
//! its worker without re-entering the task slab through the run-loop's borrow.
//!
//! [`poll_one`] installs a [`PollFrame`] for the exact window of each poll and
//! clears it after. The polled task reads the frame back through
//! [`with_current`], keyed by its worker id, to reach two disjoint capabilities:
//! a spawn inbox (a field disjoint from the slab, so pushing a child spawn never
//! aliases the parent poll borrow) and the worker's slab pointer (so a child
//! cancel resolves a *different* slab slot than the one being polled -- distinct
//! Vec indices never alias). `poll_one` derives the exclusive parent slot and
//! the frame's slab pointer from one raw slab pointer, so the parent poll and a
//! mid-poll child read share one provenance root with no `&mut Slab` reassert in
//! the window. No thread-local: the frame pointers live in a per-worker `static`
//! array, the shape the wake registry already uses.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
use core::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicU16, Ordering},
    task::{Context, Poll},
};

use kwokka_io::boundary::{IoSeam, SeamGuard, WakeSlot};
use kwokka_io::{
    DriverType, IoDriver,
    operation::{IoRequest, SubmitResult},
};

use crate::worker::{
    WorkerId,
    inbox::{PendingSpawn, SPAWN_INBOX_CAPACITY, SpawnInbox},
    reap::{REAP_QUEUE_CAPACITY, ReapQueue},
};
use crate::{
    scheduler::dispatch::{PollOutcome, poll_task},
    task::{
        JoinError, TaskRef, header::WakeData, slot::TaskSlot, state::TaskState,
        waker::waker_from_task_ref,
    },
};
use kwokka_core::slab::{Slab, SlabKey};

/// One slot per routable [`WorkerId`] (the 7-bit worker space).
const FRAME_SLOTS: usize = TaskRef::WORKER_ID_MAX as usize + 1;

/// The installed poll frame for each worker, or null between polls, indexed by
/// worker id. `AtomicPtr<PollFrame>` is `Sync` regardless of `PollFrame`, so
/// the array is a sound `static` with no `unsafe impl`.
static FRAMES: [AtomicPtr<PollFrame>; FRAME_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; FRAME_SLOTS];

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

/// Installs `frame` as the current poll frame for `worker`.
///
/// Paired with [`clear`]; the worker brackets each `poll_task` so the frame is
/// visible only for that poll.
pub(crate) fn install(worker: WorkerId, frame: &PollFrame) {
    FRAMES[worker.raw() as usize].store(ptr::from_ref(frame).cast_mut(), Ordering::Release);
}

/// Clears the current poll frame for `worker`.
pub(crate) fn clear(worker: WorkerId) {
    FRAMES[worker.raw() as usize].store(ptr::null_mut(), Ordering::Release);
}

/// RAII bracket that installs a poll frame for the span of one poll and
/// clears it on drop, including an unwinding drop.
///
/// The worker wraps each `poll_task` in a guard so a future that panics
/// mid-poll cannot leave a dangling pointer in [`FRAMES`]: the guard's `Drop`
/// runs on the unwind path and nulls the slot before the stack `PollFrame` it
/// points at is reclaimed.
///
/// Not re-entrant. A second install for the same worker while a guard is live
/// overwrites the slot, and the inner guard's drop clears the frame the outer
/// poll still expects. The runtime polls one task at a time per worker, so no
/// nested install occurs today; a future nested-poll path (a `block_on` inside
/// a task) must revisit this before relying on the guard.
pub(crate) struct FrameGuard {
    worker: WorkerId,
}

impl FrameGuard {
    /// Installs `frame` for `worker`, returning a guard that clears it on drop.
    pub(crate) fn install(worker: WorkerId, frame: &PollFrame) -> Self {
        install(worker, frame);
        Self { worker }
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        clear(self.worker);
    }
}

/// Runs `f` with the poll frame installed for `worker`, or returns `None` when
/// no frame is installed.
pub(crate) fn with_current<R>(worker: WorkerId, f: impl FnOnce(&PollFrame) -> R) -> Option<R> {
    let frame_ptr = FRAMES[worker.raw() as usize].load(Ordering::Acquire);
    if frame_ptr.is_null() {
        return None;
    }
    // SAFETY: Invariant -- a non-null pointer in `FRAMES[worker]` was stored by
    // `install` at the top of that worker's `poll_task` and is cleared by
    // `clear` at the bottom, so the referent is a live stack `PollFrame` in the
    // run-loop frame above. The reader runs only inside its own poll on
    // `worker`, strictly between `install` and `clear`.
    // Precondition: `worker` is the id whose slot `install` populated (the
    // caller owns that routing), and the `install` / `clear` bracket clears the
    // frame even on unwind -- a future that panics mid-poll must not skip
    // `clear`, so the production bracketing (the worker tick) wraps it in a Drop
    // guard.
    // Failure mode: a read after `clear`, or after a panic skipped `clear`,
    // dereferences a dangling frame.
    let frame = unsafe { &*frame_ptr };
    Some(f(frame))
}

/// Polls the task at `parent_key` once with its poll frame installed.
///
/// A child cancel during the poll resolves a disjoint slot through the same
/// slab pointer the parent was derived from.
///
/// Returns `None` when `parent_key` no longer resolves -- a stale generation
/// whose slot was reclaimed -- so the worker skips it. The parent `&mut
/// TaskSlot` and any mid-poll child read both derive from `tasks` at distinct
/// indices, the structural disjointness the slab accessors document.
pub(crate) fn poll_one(
    tasks: NonNull<Slab<TaskSlot>>,
    parent_key: SlabKey,
    worker_id: WorkerId,
    current: TaskRef,
    inbox: NonNull<SpawnInbox<SPAWN_INBOX_CAPACITY>>,
    reap: NonNull<ReapQueue<REAP_QUEUE_CAPACITY>>,
    driver: Option<NonNull<DriverType>>,
) -> Option<PollOutcome> {
    // SAFETY: Invariant -- `tasks` points at the worker's live slab; the caller
    // (`cycle::tick`) formed it via `NonNull::from(&mut *tasks)` and does not
    // touch the slab again until this returns, so the `&mut Slab` reborrowed to
    // resolve the parent is the sole live one and the `&mut TaskSlot` it yields
    // is unique. `slot_ptr_mut` generation-checks the slot. The slot lives in
    // the Vec heap buffer; the shared re-borrow during the poll
    // (`PollFrame::cancel_child`) reaches a distinct element, structurally
    // disjoint from this `&mut` (distinct Vec indices never alias).
    // Precondition: one future polls at a time per worker, so no second
    // `&mut TaskSlot` for this slot exists; the caller holds no other live slab
    // reference across the poll.
    // Failure mode: a concurrent `&mut Slab` reassert in the window, or a child
    // cancel of this same slot (excluded by `cancel_child`'s self guard), would
    // alias this `&mut` -- double-mutable-aliasing UB.
    let parent: &mut TaskSlot = unsafe {
        let slot = (*tasks.as_ptr()).slot_ptr_mut(parent_key)?;
        &mut *slot.as_ptr()
    };
    // Capture first_child from the parent `&mut` before the poll so
    // `are_children_all_settled` can walk children without re-reading the parent
    // slot (which this poll holds `&mut`). Read fresh each poll, so a child
    // linked by a post-poll drain is visible on the next poll.
    let first_child = parent.header().first_child;
    // Captured from the parent `&mut` before the poll, like `first_child`, so an
    // I/O future reads its completion result without re-reading its own slot.
    let wake_data = parent.header().wake_data;
    let frame = PollFrame {
        current,
        inbox,
        tasks,
        first_child,
        reap,
        driver,
        wake_data,
        submitted_ops: AtomicU16::new(0),
    };
    let _guard = FrameGuard::install(worker_id, &frame);
    // The seam shares the frame's poll window: declared after the frame guard
    // so its drop clears the seam first (LIFO), and fed the same driver and
    // captured wake data so a future hosted outside this crate observes the
    // exact state a frame-routed future would.
    let seam = IoSeam::new(worker_id.raw(), driver, wake_slot_of(wake_data));
    let _seam_guard = SeamGuard::install(&seam);
    let waker = waker_from_task_ref(current);
    let mut context = Context::from_waker(&waker);
    let outcome = poll_task(parent, &mut context);
    // Land the ops this poll submitted -- through the frame or the seam -- on
    // the header through the parent `&mut` the poll still holds. The fold is
    // synchronous, before the run-loop returns to its completion drain, so an
    // increment always precedes the harvest decrement of the same op.
    let submitted = frame
        .submitted_ops
        .load(Ordering::Relaxed)
        .saturating_add(seam.submitted());
    if submitted > 0 {
        let header = parent.header_mut();
        header.in_flight_ops = header.in_flight_ops.saturating_add(submitted);
    }
    Some(outcome)
}

/// Converts the header's captured wake data into the seam's completion slot.
///
/// `None` mirrors `WakeData::EMPTY`: no completion has arrived for the
/// polling task. The `NO_BUF` sentinel folds into the slot's `Option`.
fn wake_slot_of(wake_data: WakeData) -> Option<WakeSlot> {
    wake_data.has_result.then(|| WakeSlot {
        result: wake_data.result,
        flags: wake_data.flags,
        buf_id: (wake_data.buf_id != WakeData::NO_BUF).then_some(wake_data.buf_id),
    })
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use kwokka_core::{id::Pip, namespace::Namespace};

    use super::*;
    use crate::task::{
        header::{Slot, WakeData},
        state::TaskState,
    };
    use kwokka_core::Generation;

    struct Inert;
    impl Future for Inert {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    struct Ready;
    impl Future for Ready {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
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

    fn worker(id: u8) -> WorkerId {
        let Ok(worker_id) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker_id
    }

    fn pending(parent: TaskRef) -> PendingSpawn {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Inert).into_erased();
        PendingSpawn { parent, cell }
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
    fn with_current_reaches_inbox() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = TaskRef::from_arena(3, 1, Generation::from_raw(1));
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker(3), &frame);
        let pushed = with_current(worker(3), |frame| frame.push_spawn(pending(parent)));
        clear(worker(3));
        // with_current ran (outer Some) and the push enqueued (inner None).
        assert!(
            matches!(pushed, Some(None)),
            "frame reached, spawn enqueued"
        );
        assert_eq!(inbox.len(), 1);
    }

    #[test]
    fn with_current_none_when_uninstalled() {
        assert!(with_current(worker(5), |_| ()).is_none());
    }

    #[test]
    fn clear_removes_the_frame() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = TaskRef::from_arena(4, 1, Generation::from_raw(1));
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker(4), &frame);
        clear(worker(4));
        assert!(with_current(worker(4), |_| ()).is_none());
    }

    #[test]
    fn distinct_workers_have_independent_frames() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = TaskRef::from_arena(6, 1, Generation::from_raw(1));
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker(6), &frame);
        // A different worker has no frame installed.
        assert!(with_current(worker(7), |_| ()).is_none());
        assert!(with_current(worker(6), |_| ()).is_some());
        clear(worker(6));
    }

    #[test]
    fn guard_clears_the_frame_on_drop() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = TaskRef::from_arena(10, 1, Generation::from_raw(1));
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        {
            let _guard = FrameGuard::install(worker(10), &frame);
            assert!(
                with_current(worker(10), |_| ()).is_some(),
                "the frame is visible while the guard is live",
            );
        }
        assert!(
            with_current(worker(10), |_| ()).is_none(),
            "dropping the guard clears the frame",
        );
    }

    #[test]
    fn guard_clears_the_frame_on_panic() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = TaskRef::from_arena(11, 1, Generation::from_raw(1));
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = FrameGuard::install(worker(11), &frame);
            panic!("a poll panicked mid-frame");
        }));
        assert!(unwound.is_err(), "the panic propagated past the guard");
        assert!(
            with_current(worker(11), |_| ()).is_none(),
            "the guard cleared the frame on the unwind path",
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
    fn poll_one_drives_a_ready_task_to_completion() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        {
            let Some(slot) = slab.get(key) else {
                panic!("the task must resolve");
            };
            let Ok(()) = slot.header().state.wake() else {
                panic!("Sleeping -> Woken must succeed");
            };
        }
        let task = TaskRef::from_slab(30, key);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let outcome = poll_one(
            NonNull::from(&mut slab),
            key,
            worker(30),
            task,
            NonNull::from(&mut inbox),
            NonNull::from(&mut reap),
            None,
        );
        assert_eq!(outcome, Some(PollOutcome::Completed));
        let Some(slot) = slab.get(key) else {
            panic!("the task must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Done);
    }

    // Records one submitted op on the installed frame, standing in for a
    // successful driver submit during the poll.
    struct SubmitProbe {
        worker: WorkerId,
    }
    impl Future for SubmitProbe {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let Some(()) = with_current(self.worker, |frame| {
                frame.submitted_ops.fetch_add(1, Ordering::Relaxed);
            }) else {
                panic!("a frame must be installed during the poll");
            };
            Poll::Ready(())
        }
    }

    #[test]
    fn poll_one_lands_submitted_ops_on_the_header() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let probe = SubmitProbe { worker: worker(31) };
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, probe).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        {
            let Some(slot) = slab.get(key) else {
                panic!("the task must resolve");
            };
            let Ok(()) = slot.header().state.wake() else {
                panic!("Sleeping -> Woken must succeed");
            };
        }
        let task = TaskRef::from_slab(31, key);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let outcome = poll_one(
            NonNull::from(&mut slab),
            key,
            worker(31),
            task,
            NonNull::from(&mut inbox),
            NonNull::from(&mut reap),
            None,
        );
        assert_eq!(outcome, Some(PollOutcome::Completed));
        let Some(slot) = slab.get(key) else {
            panic!("the task must resolve");
        };
        assert_eq!(
            slot.header().in_flight_ops,
            1,
            "the post-poll pass lands the frame count on the header",
        );
    }

    // Submits one timeout through the installed seam, standing in for an I/O
    // future hosted outside the runtime crate.
    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    struct SeamSubmitProbe {
        worker: u8,
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    impl Future for SeamSubmitProbe {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let submitted = IoSeam::with_current(self.worker, |seam| {
                seam.submit_internal(IoRequest::<()>::timeout(1_000_000))
            });
            assert!(
                matches!(submitted, Some(Some(SubmitResult::Submitted(_)))),
                "a seam with a driver must accept the timeout op",
            );
            Poll::Ready(())
        }
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn poll_one_lands_seam_submitted_ops_on_the_header() {
        let Ok(mut driver) = DriverType::for_platform(8) else {
            panic!("the platform driver must build on this host");
        };
        let mut slab = Slab::<TaskSlot>::new(1);
        let probe = SeamSubmitProbe { worker: 32 };
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, probe).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        {
            let Some(slot) = slab.get(key) else {
                panic!("the task must resolve");
            };
            let Ok(()) = slot.header().state.wake() else {
                panic!("Sleeping -> Woken must succeed");
            };
        }
        let task = TaskRef::from_slab(32, key);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let outcome = poll_one(
            NonNull::from(&mut slab),
            key,
            worker(32),
            task,
            NonNull::from(&mut inbox),
            NonNull::from(&mut reap),
            Some(NonNull::from(&mut driver)),
        );
        assert_eq!(outcome, Some(PollOutcome::Completed));
        let Some(slot) = slab.get(key) else {
            panic!("the task must resolve");
        };
        assert_eq!(
            slot.header().in_flight_ops,
            1,
            "a seam-routed submit lands on the same in-flight accounting",
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
            submitted_ops: AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        assert_eq!(frame.join_child::<u32>(arena_child), Poll::Pending);
    }
}
