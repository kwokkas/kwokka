//! Per-worker poll-frame install/clear cycle and the `poll_one` entry point.
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
    task::Context,
};

use kwokka_core::slab::{Slab, SlabKey};
use kwokka_io::{
    DriverType,
    boundary::{IoSeam, SeamGuard, WakeSlot},
    buffer::{
        multishot::{MultishotSlab, RecvMultishotSlab},
        oneshot::inflight::InflightBufSlab,
    },
};

use crate::{
    task::{
        TaskRef,
        cell::{
            header::WakeData,
            lifecycle::{PollOutcome, poll_task},
            slot::TaskSlot,
        },
        waker::waker_from_task_ref,
    },
    worker::{
        WorkerId,
        poll::frame::PollFrame,
        queue::{
            arm::{TIMER_INBOX_CAPACITY, TimerInbox},
            inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
            reap::{REAP_QUEUE_CAPACITY, ReapQueue},
        },
    },
};

/// One slot per routable [`WorkerId`] (the 7-bit worker space).
const FRAME_SLOTS: usize = TaskRef::WORKER_ID_MAX as usize + 1;

/// The installed poll frame for each worker, or null between polls, indexed by
/// worker id. `AtomicPtr<PollFrame>` is `Sync` regardless of `PollFrame`, so
/// the array is a sound `static` with no `unsafe impl`.
static FRAMES: [AtomicPtr<PollFrame>; FRAME_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; FRAME_SLOTS];

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
#[expect(
    clippy::too_many_arguments,
    reason = "disjoint slab / inbox / reap / driver / timer borrows; bundling \
              them recreates the borrow conflict the poll frame avoids"
)]
pub(crate) fn poll_one(
    tasks: NonNull<Slab<TaskSlot>>,
    parent_key: SlabKey,
    worker_id: WorkerId,
    current: TaskRef,
    inbox: NonNull<SpawnInbox<SPAWN_INBOX_CAPACITY>>,
    reap: NonNull<ReapQueue<REAP_QUEUE_CAPACITY>>,
    driver: Option<NonNull<DriverType>>,
    inflight_slab: Option<NonNull<InflightBufSlab>>,
    multishot_slab: Option<NonNull<MultishotSlab>>,
    recv_multishot_slab: Option<NonNull<RecvMultishotSlab>>,
    timer_requests: Option<NonNull<TimerInbox<TIMER_INBOX_CAPACITY>>>,
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
        timer_requests,
    };
    let _guard = FrameGuard::install(worker_id, &frame);
    // The seam shares the frame's poll window: declared after the frame guard
    // so its drop clears the seam first (LIFO), and fed the same driver and
    // captured wake data so a future hosted outside this crate observes the
    // exact state a frame-routed future would.
    let seam = IoSeam::new(
        worker_id.raw(),
        driver,
        inflight_slab,
        wake_slot_of(wake_data),
    )
    .with_multishot_slab(multishot_slab)
    .with_recv_multishot_slab(recv_multishot_slab);
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
        // An io-bound task stays on its issuing worker: pin it off the steal
        // path so its completion harvests on the ring that submitted the op,
        // even across the brief window after the op drains and before the
        // task's final poll runs.
        header.io_bound = true;
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
        sync::atomic::Ordering,
        task::{Context, Poll},
    };

    use kwokka_core::id::{Namespace, Pip};

    use super::*;
    use crate::{
        task::{
            cell::header::{Slot, WakeData},
            state::TaskState,
        },
        worker::queue::{
            inbox::{PendingSpawn, SPAWN_INBOX_CAPACITY, SpawnInbox},
            reap::{REAP_QUEUE_CAPACITY, ReapQueue},
        },
    };

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

    fn other_ref() -> TaskRef {
        use kwokka_core::Generation;
        TaskRef::from_arena(0, u32::MAX, Generation::from_raw(1))
    }

    #[test]
    fn with_current_reaches_inbox() {
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut slab = Slab::<TaskSlot>::new(1);
        let parent = {
            use kwokka_core::Generation;
            TaskRef::from_arena(3, 1, Generation::from_raw(1))
        };
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
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
        let parent = {
            use kwokka_core::Generation;
            TaskRef::from_arena(4, 1, Generation::from_raw(1))
        };
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
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
        let parent = {
            use kwokka_core::Generation;
            TaskRef::from_arena(6, 1, Generation::from_raw(1))
        };
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
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
        let parent = {
            use kwokka_core::Generation;
            TaskRef::from_arena(10, 1, Generation::from_raw(1))
        };
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
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
        let parent = {
            use kwokka_core::Generation;
            TaskRef::from_arena(11, 1, Generation::from_raw(1))
        };
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
    use kwokka_io::operation::{IoRequest, SubmitResult};

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
            None,
            None,
            None,
            None,
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
    fn other_ref_is_distinct_from_slab_indices() {
        let mut slab = Slab::<TaskSlot>::new(1);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        // `slab` / `inbox` / `reap` outlive `frame` so its `NonNull` fields stay
        // valid through the assert, even though `cancel_child` never reads them.
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
        // An arena ref is never in the slab, so cancel_child returns false
        // without reading the slab.
        assert!(!frame.cancel_child(other_ref()));
    }
}
