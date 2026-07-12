//! [`TaskHeader`] and [`TaskVTable`] -- the `repr(C)` control block and its
//! layout contract.
//!
//! `repr(C)` pins field order so a worker can cast a `*mut TaskHeader` back
//! to `*mut Slot<F>` (offset 0) when invoking the type-erased vtable, and
//! so the join handle can read the output at a fixed
//! `OUTPUT_OFFSET = size_of::<TaskHeader>()`.
#![allow(
    clippy::redundant_pub_crate,
    reason = "satisfies the workspace `unreachable_pub` lint on a private module"
)]

use core::{
    alloc::Layout,
    ptr::NonNull,
    task::{Context, Poll},
};

use kwokka_core::id::{Namespace, Pip};

pub(crate) use crate::task::cell::layout::Slot;
use crate::task::{TaskRef, cell::state::AtomicTaskState};

/// I/O completion data stored by the worker drain loop for the task's
/// next poll to consume.
///
/// Written by [`TaskHeader::store_io_result`] when a CQE arrives.
/// Read by the task's future on the subsequent poll via
/// [`TaskHeader::wake_data`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct WakeData {
    /// Raw CQE result. Negative values are `-errno`.
    pub(crate) result: i32,
    /// Raw CQE flags (`CqeFlags::bits()`).
    pub(crate) flags: u32,
    /// Buffer ID from a `buf_ring`, or [`Self::NO_BUF`] when absent.
    pub(crate) buf_id: u16,
    /// Whether a completion has been stored since the task was created.
    ///
    /// Distinguishes a stored zero result (a successful connect, a zero-byte
    /// read or recv at EOF) from [`Self::EMPTY`]: both carry `result == 0`, so
    /// the result field alone cannot signal arrival. Occupies the struct's
    /// trailing padding, so the size stays within budget.
    pub(crate) has_result: bool,
}

impl WakeData {
    /// Sentinel indicating no buffer was selected.
    pub(crate) const NO_BUF: u16 = u16::MAX;

    /// Wake data with no stored I/O result.
    pub(crate) const EMPTY: Self = Self {
        result: 0,
        flags: 0,
        buf_id: Self::NO_BUF,
        has_result: false,
    };
}

/// Per-task control block. The first field of every [`Slot<F>`].
///
/// `repr(C)` is load-bearing: the runtime relies on `*mut Slot<F>` and
/// `*mut TaskHeader` aliasing at offset 0, and on the trailing
/// `TaskCell<F>` starting exactly `size_of::<TaskHeader>()` bytes past the
/// header (see [`Slot::OUTPUT_OFFSET`]).
///
/// # Field semantics
///
/// * `state` -- atomic CAS-based lifecycle (see [`AtomicTaskState`]).
/// * `pip` -- observability identity: a flat, per-worker-unique id. It carries no parent-child
///   relation; the tree lives in the `first_child` / `next_sibling` list below.
/// * `namespace` -- logical scope; the per-process interning is decided at the call site, not here.
/// * `first_child` / `next_sibling` -- intrusive children list, manipulated by helpers in the
///   `children` module. Both are [`Option<TaskRef>`] so absence is type-level rather than encoded
///   as a sentinel index.
/// * `wake_data` -- I/O completion result stored by the worker drain loop via
///   [`TaskHeader::store_io_result`]. The task's future consumes it on the next poll.
/// * `vtable` -- the type-erased entry points for `poll` and `drop_in_place`, stamped per `F` by
///   [`Slot::VTABLE`].
#[derive(Debug)]
#[repr(C)]
pub(crate) struct TaskHeader {
    pub(crate) state: AtomicTaskState,
    pub(crate) pip: Pip,
    pub(crate) namespace: Namespace,
    pub(crate) first_child: Option<TaskRef>,
    pub(crate) next_sibling: Option<TaskRef>,
    pub(crate) wake_data: WakeData,
    /// Count of submitted ops whose completion has not yet drained.
    ///
    /// A nonzero count marks the task non-stealable once the steal
    /// predicate reads it, so an in-flight op always completes on the ring
    /// that submitted it. Sits in the padding between `wake_data` and
    /// `next_runnable`, so the header size is unchanged.
    pub(crate) in_flight_ops: u16,
    /// Pinned to its worker: never offered to the steal path.
    ///
    /// Raised on the `block_on` root and on every child of the unbounded
    /// scope, whose spawn carries no `Send` bound -- a pinned task never
    /// relocates, which is what makes the missing bound sound. Shares the
    /// padding after `in_flight_ops`, so the header size is unchanged.
    pub(crate) is_pinned: bool,
    /// The settled note for this `Retired` husk's relocated task landed.
    ///
    /// A husk keeps its slot and sibling links while its task runs
    /// elsewhere; this bit is what turns it reap-eligible. Written only by
    /// the owning worker's note drain and read only on that worker, so the
    /// plain bool needs no atomicity. Shares the padding after
    /// `in_flight_ops`, so the header size is unchanged.
    pub(crate) is_remote_settled: bool,
    /// Whether the task has entered `poll` at least once on its owning worker.
    ///
    /// The runnable steal path offers only polled tasks: a freshly woken task
    /// that has never run is left for its owning worker, since relocating it
    /// before its first poll would move that first poll to the thief. Written
    /// by the owning worker on poll entry and read only on that worker, so the
    /// plain bool needs no atomicity, and it sits with the in-flight and flag
    /// bytes inside the header budget.
    pub(crate) has_polled: bool,
    /// Whether the task has ever submitted an in-flight op.
    ///
    /// An io-bound task stays on its issuing worker so its completions are
    /// harvested on the ring that submitted them; the steal predicates skip
    /// it. Sits with the other flag bytes inside the header budget.
    pub(crate) io_bound: bool,
    pub(crate) next_runnable: Option<TaskRef>,
    pub(crate) vtable: &'static TaskVTable,
}

const _: () = assert!(
    size_of::<TaskHeader>() <= 128,
    "TaskHeader exceeds 128-byte budget; review TaskCell<F> capacity",
);

impl TaskHeader {
    /// Constructs a new header in `Sleeping` with no children.
    #[cfg(not(loom))]
    #[inline]
    pub(crate) const fn new(pip: Pip, namespace: Namespace, vtable: &'static TaskVTable) -> Self {
        Self {
            state: AtomicTaskState::new(),
            pip,
            namespace,
            first_child: None,
            next_sibling: None,
            wake_data: WakeData::EMPTY,
            in_flight_ops: 0,
            is_pinned: false,
            is_remote_settled: false,
            has_polled: false,
            io_bound: false,
            next_runnable: None,
            vtable,
        }
    }

    /// Loom variant -- `loom::sync::atomic::AtomicU8::new` is not
    /// available in const context, so the const qualifier is dropped
    /// under `--cfg loom`.
    #[cfg(loom)]
    #[inline]
    pub(crate) fn new(pip: Pip, namespace: Namespace, vtable: &'static TaskVTable) -> Self {
        Self {
            state: AtomicTaskState::new(),
            pip,
            namespace,
            first_child: None,
            next_sibling: None,
            wake_data: WakeData::EMPTY,
            in_flight_ops: 0,
            is_pinned: false,
            is_remote_settled: false,
            has_polled: false,
            io_bound: false,
            next_runnable: None,
            vtable,
        }
    }

    /// Stores I/O completion data for the task's next poll to read.
    ///
    /// Called by the worker drain loop when a CQE arrives for this task.
    /// The task's future is intended to read `wake_data` on the
    /// subsequent poll once the scheduler spawn path lands. Each stored
    /// completion retires one in-flight op, the saturating pair of the
    /// post-poll increment.
    pub(crate) fn store_io_result(&mut self, result: i32, flags: u32, buf_id: Option<u16>) {
        self.wake_data = WakeData {
            result,
            flags,
            buf_id: buf_id.unwrap_or(WakeData::NO_BUF),
            has_result: true,
        };
        self.retire_in_flight_op();
    }

    /// Retires one in-flight op, the saturating pair of the post-poll increment.
    ///
    /// A buffered op retires through `store_io_result`, which stores its result
    /// and calls this; a multishot op retires here directly on its terminal CQE,
    /// which carries no per-task result to store.
    pub(crate) const fn retire_in_flight_op(&mut self) {
        self.in_flight_ops = self.in_flight_ops.saturating_sub(1);
    }

    /// Stamps `pip` into the header.
    ///
    /// The worker mints the id at drain time and stamps it here before the
    /// freshly spawned child is linked under its parent. The child cell is
    /// built with a detached id at spawn-request time, when the issuing
    /// worker counter is not in reach.
    pub(crate) const fn set_pip(&mut self, pip: Pip) {
        self.pip = pip;
    }
}

/// Type-erased entry points for a task. One static instance per `F`,
/// reachable via [`Slot::VTABLE`].
///
/// Both entry points are safe-by-signature `fn` pointers. The runtime
/// contract -- "the [`NonNull<TaskHeader>`] argument must point to a
/// live [`Slot<F>`] whose stamped vtable equals [`Slot::VTABLE`], with
/// provenance covering the entire slot" -- is documented on each
/// function and lives at the call site that constructs the [`NonNull`].
/// Per workspace policy, the `unsafe` surface is scoped to the actual
/// unsafe operations inside each body, not hoisted to the function
/// signature.
#[derive(Debug)]
#[repr(C)]
pub(crate) struct TaskVTable {
    /// Polls the task's future once. Caller must have transitioned the
    /// header state to `Running` before invoking.
    pub(crate) poll: fn(NonNull<TaskHeader>, &mut Context<'_>) -> Poll<()>,
    /// Drops the still-live half of the cell -- `future` if poll has
    /// not returned `Ready`, otherwise `output`. Decides which by
    /// reading `state` with `Acquire` ordering.
    pub(crate) drop_in_place: fn(NonNull<TaskHeader>),
    /// Allocation layout of the entire `Slot<F>` (header + cell). Used
    /// by the slab/arena allocator at spawn time and by dealloc paths.
    pub(crate) layout: Layout,
}
