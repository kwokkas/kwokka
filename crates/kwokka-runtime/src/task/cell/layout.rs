//! [`TaskCell<F>`] and [`Slot<F>`]: the half of the cell that knows the
//! concrete future type `F`.
//!
//! Building a slot, erasing it, and the `poll` / `drop_in_place` vtable bodies
//! that recover `F` from a `*mut TaskHeader` all live here. Dispatching through
//! that vtable without knowing `F` is the other half, in
//! [`erased`](crate::task::cell::erased).
//!
//! `repr(C)` pins field order so a worker can cast a `*mut TaskHeader` back
//! to `*mut Slot<F>` (offset 0) when invoking the type-erased vtable, and
//! so the join handle can read the output at the fixed
//! `OUTPUT_OFFSET = size_of::<TaskHeader>()`. That offset contract is what
//! ties the two halves together.
#![allow(
    clippy::redundant_pub_crate,
    reason = "satisfies the workspace `unreachable_pub` lint on a private module"
)]

use core::{
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    pin::Pin,
    ptr::{self, NonNull},
    task::{Context, Poll},
};

use kwokka_core::id::{Namespace, Pip};

use crate::task::cell::{
    header::{TaskHeader, TaskVTable},
    slot::TaskSlot,
    state::TaskState,
};

/// `TaskCell` holding the future and (eventually) its output.
///
/// `repr(C)` puts `output` first so the join handle can read it at the
/// fixed offset [`Slot::OUTPUT_OFFSET`] without consulting `offset_of!`.
/// The cell is *not* an enum: which half is currently initialized is
/// encoded by the header's [`TaskState`](crate::task::cell::state::TaskState)
/// (Pending / Done / Taken) per the layout contract.
#[repr(C)]
pub(crate) struct TaskCell<F: Future> {
    /// Output written exactly once by `poll_fn` when the future returns
    /// `Ready`. Read at most once by the join handle's `Done -> Taken`
    /// transition. Uninitialized before `Done`.
    pub(crate) output: MaybeUninit<F::Output>,
    /// The future itself. Live while state is in
    /// {Sleeping, Woken, Running}; `drop_in_place_fn` consumes it on
    /// `Ready` or any cancellation path.
    pub(crate) future: F,
}

/// Concrete allocation unit. `Slot<F>` is what the slab or arena owns;
/// the runtime hands out `*mut TaskHeader` (offset 0) for type-erased
/// work.
#[repr(C)]
pub(crate) struct Slot<F: Future> {
    pub(crate) header: TaskHeader,
    pub(crate) cell: TaskCell<F>,
    /// Ties `Slot<F>` to `F` so the type carries `F`'s drop glue and
    /// covariance even if the `cell` field shape later changes.
    _marker: PhantomData<F>,
}

impl<F: Future> Slot<F> {
    /// Byte offset of `TaskCell<F>::output` from the start of the header.
    ///
    /// Equals `size_of::<TaskHeader>()` because `Slot<F>` is `repr(C)`
    /// with `header` first and `TaskCell<F>` second, and `TaskCell<F>` is
    /// `repr(C)` with `output` first. The join handle reads at this
    /// offset using `Handle<T>` knowledge of `F::Output`.
    pub(crate) const OUTPUT_OFFSET: usize = size_of::<TaskHeader>();

    /// Compile-time guard that [`Self::OUTPUT_OFFSET`] equals the real byte
    /// offset of the `cell` (hence of `output`, its first `repr(C)` field).
    ///
    /// `OUTPUT_OFFSET` is defined as `size_of::<TaskHeader>()`, which matches
    /// the `cell` offset only while that size is a multiple of
    /// `align_of::<TaskCell<F>>()`. An `F` whose future or output raises the
    /// cell alignment above the header's would push `cell` past
    /// `size_of::<TaskHeader>()` through `repr(C)` padding, and the erased
    /// [`TaskSlot::take_output`] read would land short of the output. This
    /// assert fires at monomorphisation for any such `F`, so the divergence
    /// never reaches runtime. Referenced from [`Slot::into_erased`].
    pub(crate) const OUTPUT_OFFSET_OK: () = assert!(
        Self::OUTPUT_OFFSET == core::mem::offset_of!(Self, cell),
        "OUTPUT_OFFSET diverged from the cell offset; F alignment exceeds the header padding",
    );

    /// Compile-time guard that `size_of::<Slot<F>>() <= 512`. The
    /// runtime enforces this for every concrete `F` by referencing the
    /// const, which triggers monomorphisation-time evaluation.
    const MAX_SLOT_BYTES: usize = 512;

    pub(crate) const SIZE_OK: () = assert!(
        size_of::<Self>() <= Self::MAX_SLOT_BYTES,
        "Slot<F> exceeds 512 bytes; reduce size_of::<F>() + size_of::<F::Output>()",
    );

    /// Per-`F` vtable. The compiler stamps one static per concrete `F`.
    pub(crate) const VTABLE: TaskVTable = TaskVTable {
        poll: Self::poll_fn,
        drop_in_place: Self::drop_in_place_fn,
        layout: core::alloc::Layout::new::<Self>(),
    };

    /// Constructs an in-place `Slot<F>` from raw parts. The `state`
    /// starts at `Sleeping`, `output` is uninitialized,
    /// `first_child`/`next_sibling` are `None`.
    ///
    /// Caller is responsible for placing the resulting value into a
    /// slab or arena slot -- this helper does not allocate.
    #[cfg(not(loom))]
    pub(crate) const fn new(pip: Pip, namespace: Namespace, future: F) -> Self {
        let () = Self::SIZE_OK;
        Self {
            header: TaskHeader::new(pip, namespace, &Self::VTABLE),
            cell: TaskCell {
                output: MaybeUninit::uninit(),
                future,
            },
            _marker: PhantomData,
        }
    }

    /// Loom variant of [`Slot::new`].
    #[cfg(loom)]
    pub(crate) fn new(pip: Pip, namespace: Namespace, future: F) -> Self {
        let () = Self::SIZE_OK;
        Self {
            header: TaskHeader::new(pip, namespace, &Self::VTABLE),
            cell: TaskCell {
                output: MaybeUninit::uninit(),
                future,
            },
            _marker: PhantomData,
        }
    }

    /// Compile-time guard that a `Slot<F>` fits the erased [`TaskSlot`] cell
    /// in both size and alignment.
    ///
    /// Referenced by [`Slot::into_erased`] so the check fires at
    /// monomorphisation, mirroring the [`Slot::SIZE_OK`] size-only guard.
    pub(crate) const ERASE_FITS: () = assert!(
        size_of::<Self>() <= TaskSlot::CELL_BYTES && align_of::<Self>() <= TaskSlot::CELL_ALIGN,
        "Slot<F> exceeds the TaskSlot cell budget (512 bytes / align 16)",
    );

    /// Moves this `Slot<F>` into a type-erased [`TaskSlot`] cell.
    ///
    /// The header lands at offset 0 so the vtable cast in [`Slot::poll_fn`]
    /// stays valid. The future is moved exactly once (here, into the cell);
    /// after the owning slab slot is written it is pointer-stable, satisfying
    /// the pin contract `poll_fn` relies on.
    pub(crate) const fn into_erased(self) -> TaskSlot {
        let () = Self::ERASE_FITS;
        let () = Self::OUTPUT_OFFSET_OK;
        let mut cell = TaskSlot::uninit();
        // SAFETY:
        // Invariant: `Slot<F>` is repr(C) header-first; `ERASE_FITS` proved
        //   size_of::<Slot<F>>() <= 512 and align_of::<Slot<F>>() <= 16 and
        //   TaskSlot is align(16), so the offset-0 write is in-bounds and
        //   aligned, landing the header at byte 0.
        // Precondition: `cell` is a fresh uninitialized TaskSlot, filled here
        //   before it can be dropped or read (no panic point stands between
        //   `uninit` and this write, so no unfilled cell ever drops).
        // Failure: an over-budget or over-aligned `F` would write out of
        //   bounds or misaligned -- excluded at compile time by `ERASE_FITS`;
        //   writing over a live cell would leak the prior task -- excluded by
        //   the unoccupied precondition.
        unsafe {
            ptr::write(ptr::from_mut(&mut cell).cast::<Self>(), self);
        }
        cell
    }

    /// vtable entry: poll the task's future once.
    ///
    /// The signature is a safe `fn` pointer per workspace anti-patterns
    /// rule (no `unsafe fn` on entire functions); the runtime contract
    /// below is the responsibility of whoever constructs the
    /// [`NonNull`] at the call site.
    ///
    /// # Contract (caller-side)
    ///
    /// * `ptr` must point to the `TaskHeader` of a live `Slot<F>` whose stamped vtable equals
    ///   [`Self::VTABLE`].
    /// * `ptr`'s provenance must cover the entire `Slot<F>` allocation, not just the leading
    ///   `TaskHeader` prefix.
    /// * The header's state must be [`TaskState::Running`] at entry.
    /// * The cell's `future` half must still be initialized.
    ///
    /// On `Poll::Ready`, the function writes the output into
    /// `cell.output`, drops the future in place, and returns
    /// `Poll::Ready(())`. State transition `Running -> Done` is the
    /// caller's responsibility.
    fn poll_fn(ptr: NonNull<TaskHeader>, cx: &mut Context<'_>) -> Poll<()> {
        let slot: *mut Self = ptr.as_ptr().cast();
        // SAFETY: `ptr` has slot-wide provenance per caller contract.
        // `cell` is a live field; the future is structurally pinned in
        // the slab/arena slot with exclusive access via scheduler
        // dequeue. Pin contract holds because we never move the future.
        // Violating provenance causes OOB UB; concurrent access or
        // reclaimed slot causes data race UB.
        let (cell_ptr, future_pin) = unsafe {
            let cell_ptr: *mut TaskCell<F> = &raw mut (*slot).cell;
            let future_ref: &mut F = &mut (*cell_ptr).future;
            (cell_ptr, Pin::new_unchecked(future_ref))
        };
        match future_pin.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(out) => {
                // SAFETY: state is `Running`, so `output` is uninit and
                // no other thread observes the slot. `Ready` is reached
                // at most once per task, making this the sole write and
                // the sole future drop site on this path. Double-write
                // leaks; double-drop causes UB.
                unsafe {
                    (*cell_ptr).output.write(out);
                    ptr::drop_in_place(&raw mut (*cell_ptr).future);
                }
                Poll::Ready(())
            }
        }
    }

    /// vtable entry: drop the still-live half of the cell.
    ///
    /// Same safe-by-signature `fn` shape as [`Self::poll_fn`]; the
    /// runtime contract is enforced at the call site that builds the
    /// [`NonNull`].
    ///
    /// # Contract (caller-side)
    ///
    /// * `ptr` must point to the `TaskHeader` of a live `Slot<F>` whose stamped vtable equals
    ///   [`Self::VTABLE`].
    /// * `ptr`'s provenance must cover the entire `Slot<F>`.
    /// * The caller must hold exclusive access to the slot.
    ///
    /// Branching on loaded state with `Acquire` ordering:
    ///
    /// * `Sleeping` / `Woken` / `Cancelled` -- drop the future; it is still live because `poll_fn`
    ///   never returned `Ready`. `Running` is handled by this arm too, but is sound only while the
    ///   future is live: the poll caller must transition `Running -> Done` before the slot can be
    ///   dropped, so the post-`Ready` window (future gone, state not yet `Done`) is never observed
    ///   here.
    /// * `Done` -- drop the output (future was already dropped by `poll_fn`; join handle never
    ///   consumed).
    /// * `Failed` / `Taken` -- no drops needed.
    fn drop_in_place_fn(ptr: NonNull<TaskHeader>) {
        let slot: *mut Self = ptr.as_ptr().cast();
        // SAFETY: `state` is a live field of the header; provenance
        // covers the full slot so the read is in-bounds.
        let state = unsafe { (*ptr.as_ptr()).state.load() };
        match state {
            TaskState::Sleeping | TaskState::Woken | TaskState::Running | TaskState::Cancelled => {
                // SAFETY:
                // Invariant: in Sleeping/Woken/Cancelled the future is still
                // initialized -- `poll_fn` never returned `Ready`, so it was
                // neither written out nor dropped. `Running` relies on the poll
                // caller transitioning `Running -> Done` before any drop path
                // runs, so the transient post-`Ready` window (future already
                // dropped by `poll_fn`, state not yet `Done`) is never observed
                // by this arm.
                // Precondition: the caller holds exclusive access to the slot
                // (single-worker dispatch), and `state` was just loaded `Acquire`.
                // Failure: dropping a slot left in `Running` after a `Ready`
                // poll -- e.g. a concurrent terminal transition winning the
                // post-`Ready` window before `complete()` -- double-drops the
                // future. The cancellation path must close that window.
                unsafe {
                    ptr::drop_in_place(&raw mut (*slot).cell.future);
                }
            }
            TaskState::Done => {
                // SAFETY: output half was written by `poll_fn` before
                // state moved to `Done`; future was dropped at the
                // same site.
                unsafe {
                    (*slot).cell.output.assume_init_drop();
                }
            }
            // Failed / Taken: the live half is already gone, so there is
            // nothing to drop. Taken means `poll_fn` dropped the future on
            // `Ready` and the join handle consumed the output. Failed means
            // the caller dropped the future at the failure site before moving
            // the state here -- a `Running -> Failed` transition must not be
            // made while the future is still live. Retired means the body
            // moved to another slab; the husk left behind owns neither half.
            TaskState::Failed | TaskState::Taken | TaskState::Retired => {}
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        mem::ManuallyDrop,
        sync::atomic::{AtomicUsize, Ordering},
        task::{RawWaker, RawWakerVTable, Waker},
    };

    use super::*;
    use crate::task::cell::{header::WakeData, state::TaskState};

    /// Counts how many times each side of the cell has been dropped, used by
    /// `drop_in_place` branching tests.
    #[derive(Default)]
    pub(super) struct DropCounts {
        pub(super) future: AtomicUsize,
        pub(super) output: AtomicUsize,
    }

    /// Future that records its drop in `counts.future` and yields a value
    /// whose drop records into `counts.output`.
    pub(super) struct ProbeFuture {
        pub(super) counts: &'static DropCounts,
        pub(super) ready: bool,
    }

    pub(super) struct ProbeOutput {
        counts: &'static DropCounts,
    }

    impl Drop for ProbeOutput {
        fn drop(&mut self) {
            self.counts.output.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Drop for ProbeFuture {
        fn drop(&mut self) {
            self.counts.future.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Future for ProbeFuture {
        type Output = ProbeOutput;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<ProbeOutput> {
            if self.ready {
                Poll::Ready(ProbeOutput {
                    counts: self.counts,
                })
            } else {
                Poll::Pending
            }
        }
    }

    pub(super) const fn dummy_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        // SAFETY: vtable pointers are no-ops that never dereference `data`.
        // Violating this would dereference a null pointer.
        unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) }
    }

    /// Build a vtable-grade `NonNull<TaskHeader>` from a slot. Provenance
    /// covers the entire `Slot<F>` (offset 0 cast through repr(C) header
    /// prefix) per the contract on `Slot::poll_fn` / `Slot::drop_in_place_fn`.
    pub(super) fn header_nn<F: Future>(slot: &mut Slot<F>) -> NonNull<TaskHeader> {
        NonNull::from(slot).cast()
    }

    /// Leaks a slot whose live cell half was already dropped by a vtable
    /// call, so the slot's own drop glue does not double-drop that half.
    const fn forget_husk<F: Future>(slot: Slot<F>) {
        let _husk = ManuallyDrop::new(slot);
    }

    #[test]
    fn wake_data_empty_has_no_buf_sentinel() {
        assert_eq!(
            WakeData::EMPTY,
            WakeData {
                result: 0,
                flags: 0,
                buf_id: WakeData::NO_BUF,
                has_result: false,
            }
        );
    }

    #[test]
    fn stored_zero_result_differs_from_empty() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        // A successful connect and a zero-byte read or recv at EOF all complete
        // with result 0; the stored data must still read as arrived.
        slot.header.store_io_result(0, 0, None);
        assert!(slot.header.wake_data.has_result);
        assert_ne!(slot.header.wake_data, WakeData::EMPTY);
    }

    #[test]
    fn wake_data_size_within_budget() {
        assert!(size_of::<WakeData>() <= 12);
    }

    #[test]
    fn header_size_within_budget() {
        assert!(size_of::<TaskHeader>() <= 128);
    }

    #[test]
    fn store_io_result_writes_wake_data() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        slot.header.store_io_result(42, 0x03, Some(7));
        assert_eq!(slot.header.wake_data.result, 42);
        assert_eq!(slot.header.wake_data.flags, 0x03);
        assert_eq!(slot.header.wake_data.buf_id, 7);
        assert!(slot.header.wake_data.has_result);
    }

    #[test]
    fn store_io_result_none_buf_id_sets_sentinel() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        slot.header.store_io_result(-22, 0, None);
        assert_eq!(slot.header.wake_data.result, -22);
        assert_eq!(slot.header.wake_data.flags, 0);
        assert_eq!(slot.header.wake_data.buf_id, WakeData::NO_BUF);
    }

    #[test]
    fn store_io_result_retires_one_in_flight_op() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        assert_eq!(slot.header.in_flight_ops, 0);
        slot.header.in_flight_ops = 2;
        slot.header.store_io_result(0, 0, None);
        assert_eq!(slot.header.in_flight_ops, 1);
        slot.header.store_io_result(0, 0, None);
        assert_eq!(slot.header.in_flight_ops, 0);
        // A completion with no paired submit saturates instead of wrapping.
        slot.header.store_io_result(0, 0, None);
        assert_eq!(slot.header.in_flight_ops, 0);
    }

    #[test]
    fn new_header_has_empty_wake_data() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        assert_eq!(slot.header.wake_data, WakeData::EMPTY);
    }

    #[test]
    fn output_offset_equals_header_size() {
        assert_eq!(Slot::<ProbeFuture>::OUTPUT_OFFSET, size_of::<TaskHeader>());
    }

    #[test]
    fn slot_field_order_is_repr_c() {
        use core::mem::offset_of;
        let header_off = offset_of!(Slot<ProbeFuture>, header);
        let cell_off = offset_of!(Slot<ProbeFuture>, cell);
        assert_eq!(
            header_off, 0,
            "header must be at offset 0 for type-erased cast"
        );
        assert!(
            cell_off >= size_of::<TaskHeader>(),
            "cell must follow header"
        );
    }

    #[test]
    fn slot_size_under_512_bytes_const_assert() {
        let () = Slot::<ProbeFuture>::SIZE_OK;
    }

    #[test]
    fn header_field_order_unchanged() {
        use core::mem::offset_of;
        let state_off = offset_of!(TaskHeader, state);
        let pip_off = offset_of!(TaskHeader, pip);
        let namespace_off = offset_of!(TaskHeader, namespace);
        let first_child_off = offset_of!(TaskHeader, first_child);
        let next_sibling_off = offset_of!(TaskHeader, next_sibling);
        let wake_data_off = offset_of!(TaskHeader, wake_data);
        let in_flight_ops_off = offset_of!(TaskHeader, in_flight_ops);
        let next_runnable_off = offset_of!(TaskHeader, next_runnable);
        let vtable_off = offset_of!(TaskHeader, vtable);
        assert!(state_off < pip_off);
        assert!(pip_off < namespace_off);
        assert!(namespace_off < first_child_off);
        assert!(first_child_off < next_sibling_off);
        assert!(next_sibling_off < wake_data_off);
        assert!(wake_data_off < in_flight_ops_off);
        assert!(in_flight_ops_off < next_runnable_off);
        assert!(
            next_runnable_off < vtable_off,
            "vtable must be last so prior offsets stay frozen"
        );
    }

    #[test]
    fn poll_fn_pending_keeps_output_uninit() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = slot.header.vtable.poll;
        let result = poll(header_nn(&mut slot), &mut cx);
        assert!(matches!(result, Poll::Pending));
    }

    #[test]
    fn poll_fn_ready_writes_output_and_drops_future() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: true,
            },
        );
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = slot.header.vtable.poll;
        let drop_in_place = slot.header.vtable.drop_in_place;
        let result = poll(header_nn(&mut slot), &mut cx);
        assert!(matches!(result, Poll::Ready(())));
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Running, TaskState::Done)
        else {
            panic!("Running -> Done must succeed");
        };
        drop_in_place(header_nn(&mut slot));
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 1);
        forget_husk(slot);
    }

    #[test]
    fn drop_in_place_pending_drops_future_only() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        let drop_in_place = slot.header.vtable.drop_in_place;
        drop_in_place(header_nn(&mut slot));
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
        forget_husk(slot);
    }

    #[test]
    fn drop_in_place_cancelled_drops_future_only() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Running, TaskState::Cancelled)
        else {
            panic!("Running -> Cancelled must succeed");
        };
        let drop_in_place = slot.header.vtable.drop_in_place;
        drop_in_place(header_nn(&mut slot));
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
        forget_husk(slot);
    }

    #[test]
    fn drop_in_place_failed_drops_neither_half() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        );
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Running, TaskState::Failed)
        else {
            panic!("Running -> Failed must succeed");
        };
        let before_future = COUNTS.future.load(Ordering::Relaxed);
        let before_output = COUNTS.output.load(Ordering::Relaxed);
        let drop_in_place = slot.header.vtable.drop_in_place;
        drop_in_place(header_nn(&mut slot));
        assert_eq!(
            COUNTS.future.load(Ordering::Relaxed),
            before_future,
            "drop_in_place_fn must not fire the future on Failed",
        );
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            before_output,
            "Failed never wrote output, so drop_in_place_fn must not fire it",
        );
        forget_husk(slot);
    }

    #[test]
    fn drop_in_place_taken_does_not_drop_either_half() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut slot = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: true,
            },
        );
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = slot.header.vtable.poll;
        let drop_in_place = slot.header.vtable.drop_in_place;
        // IGNORE: poll result is not under test; we need Ready to consume the future.
        let _ = poll(header_nn(&mut slot), &mut cx);
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Running, TaskState::Done)
        else {
            panic!("Running -> Done must succeed");
        };
        let Ok(()) = slot
            .header
            .state
            .transition(TaskState::Done, TaskState::Taken)
        else {
            panic!("Done -> Taken must succeed");
        };
        drop_in_place(header_nn(&mut slot));
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
        forget_husk(slot);
    }

    #[test]
    fn into_erased_preserves_header_at_offset_zero() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        )
        .into_erased();
        assert_eq!(cell.header().state.load(), TaskState::Sleeping);
    }
}
