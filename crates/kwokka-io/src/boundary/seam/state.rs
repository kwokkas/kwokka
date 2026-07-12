//! The seam itself: the installed `IoSeam`, its poll-window guard, and the
//! result shapes a submitted op resolves into.

use core::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicU16, Ordering},
};

use crate::{
    DriverType, IoDriver,
    boundary::cancel::{encode_multishot_sentinel, encode_recv_multishot_sentinel},
    buffer::{
        multishot::{
            MultishotSlab, MultishotSlotKey, NO_BUFFER, RecvMultishotSlab, RecvMultishotSlotKey,
        },
        oneshot::inflight::{INFLIGHT_BUF_STRIDE, InflightBufSlab, InflightSlotKey},
    },
    operation::{IoBuf, IoBufMut, IoRequest, SubmitResult},
};
#[cfg(unix)]
use crate::{addr::SockAddr, operation::core::msghdr};

/// One seam slot per possible worker id byte.
///
/// The runtime's routable worker space is 7-bit; sizing the array to the full
/// `u8` range keeps every decoded id in-bounds with no panic path.
const SEAM_SLOTS: usize = u8::MAX as usize + 1;

/// The installed seam for each worker, or null between polls, indexed by
/// worker id. `AtomicPtr<IoSeam>` is `Sync` regardless of `IoSeam`, so the
/// array is a sound `static` with no `unsafe impl`.
static SEAMS: [AtomicPtr<IoSeam>; SEAM_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; SEAM_SLOTS];

/// A completion result captured for the polling task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeSlot {
    /// Raw CQE result; negative values are `-errno`.
    pub result: i32,
    /// Raw CQE flags.
    pub flags: u32,
    /// Kernel-selected buffer id, when the op consumed a provided buffer.
    pub buf_id: Option<u16>,
}

/// The outcome of advancing a multishot stream by one completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultishotNext {
    /// A completion result: a nonnegative accept fd or a negative `-errno`.
    Item(i32),
    /// The op is in flight with an empty FIFO; poll again on wake.
    Pending,
    /// The op posted its terminal CQE and its FIFO is drained; the slot is freed.
    Ended,
}

/// The outcome of reserving a multishot slot for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultishotAlloc {
    /// A slot was reserved; the stream tags its SQE with `sentinel` and drains
    /// and cancels through `key`.
    Allocated {
        /// Handle that drains the slot's FIFO and cancels its op.
        key: MultishotSlotKey,
        /// `user_data` sentinel the multishot SQE carries.
        sentinel: u64,
    },
    /// Every slot is occupied; the stream degrades to a single-shot accept.
    Full,
    /// The seam carries no multishot registry (a test seam).
    Unsupported,
}

/// The outcome of advancing a multishot recv stream by one completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvMultishotNext {
    /// A completion: a nonnegative byte count paired with the kernel-selected
    /// buffer id it consumed (`None` at end of stream), or a negative `-errno`.
    Item {
        /// Byte count, or a negative `-errno`.
        result: i32,
        /// Kernel-selected provided buffer id, `None` when none was consumed.
        buf_id: Option<u16>,
    },
    /// The op is in flight with an empty FIFO; poll again on wake.
    Pending,
    /// The op posted its terminal CQE and its FIFO is drained; the slot is freed.
    Ended,
}

/// The outcome of reserving a multishot recv slot for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvMultishotAlloc {
    /// A slot was reserved; the stream tags its SQE with `sentinel` and drains
    /// and cancels through `key`.
    Allocated {
        /// Handle that drains the slot's FIFO and cancels its op.
        key: RecvMultishotSlotKey,
        /// `user_data` sentinel the multishot recv SQE carries.
        sentinel: u64,
    },
    /// Every slot is occupied; the stream degrades to a single-shot provided recv.
    Full,
    /// The seam carries no multishot recv registry (a test seam).
    Unsupported,
}

/// The per-worker submit/complete surface an I/O future polls through.
///
/// Lives on the runtime worker's stack for exactly one poll window, installed
/// and cleared by [`SeamGuard`]. Futures reach it with
/// [`IoSeam::with_current`] inside their own `poll` and
/// never hold it across an `.await`.
pub struct IoSeam {
    /// Worker id the seam is installed for.
    worker_id: u8,
    /// The worker's I/O driver, or `None` for a test seam with no backend.
    driver: Option<NonNull<DriverType>>,
    /// The worker's in-flight buffer registry, or `None` for a test seam. A
    /// buffered future allocates a slot here during its poll through
    /// [`IoSeam::allocate_slot`].
    inflight_slab: Option<NonNull<InflightBufSlab>>,
    /// The worker's multishot registry, or `None` for a test seam. A multishot
    /// stream allocates and drains a slot here through
    /// [`IoSeam::allocate_multishot_slot`]
    /// and [`IoSeam::multishot_next`].
    multishot_slab: Option<NonNull<MultishotSlab>>,
    /// The worker's multishot recv registry, or `None` for a test seam. A recv
    /// stream allocates and drains a slot here through
    /// [`IoSeam::allocate_recv_multishot_slot`] and [`IoSeam::recv_multishot_next`].
    recv_multishot_slab: Option<NonNull<RecvMultishotSlab>>,
    /// The polling task's captured completion result, when one has arrived.
    wake_slot: Option<WakeSlot>,
    /// Ops submitted through the seam this poll. The runtime folds the count
    /// onto the polling task's header after the poll returns -- the same
    /// landing its own submit paths use -- so in-flight accounting holds.
    submitted: AtomicU16,
}

impl IoSeam {
    /// Builds a seam for one poll window.
    #[must_use]
    pub const fn new(
        worker_id: u8,
        driver: Option<NonNull<DriverType>>,
        inflight_slab: Option<NonNull<InflightBufSlab>>,
        wake_slot: Option<WakeSlot>,
    ) -> Self {
        Self {
            worker_id,
            driver,
            inflight_slab,
            multishot_slab: None,
            recv_multishot_slab: None,
            wake_slot,
            submitted: AtomicU16::new(0),
        }
    }

    /// Attaches the worker's multishot registry to the seam.
    ///
    /// The run-loop calls this only on the production path; a test seam keeps
    /// `None` and a multishot stream resolves as unsupported.
    #[must_use]
    pub const fn with_multishot_slab(mut self, slab: Option<NonNull<MultishotSlab>>) -> Self {
        self.multishot_slab = slab;
        self
    }

    /// Attaches the worker's multishot recv registry to the seam.
    ///
    /// The run-loop calls this only on the production path; a test seam keeps
    /// `None` and a recv stream resolves as unsupported.
    #[must_use]
    pub const fn with_recv_multishot_slab(
        mut self,
        slab: Option<NonNull<RecvMultishotSlab>>,
    ) -> Self {
        self.recv_multishot_slab = slab;
        self
    }

    /// Runs `f` with the seam installed for `worker_id`, or returns `None`
    /// when no seam is installed.
    pub fn with_current<R>(worker_id: u8, f: impl FnOnce(&Self) -> R) -> Option<R> {
        let seam = SEAMS[worker_id as usize].load(Ordering::Acquire);
        if seam.is_null() {
            return None;
        }
        // SAFETY: Invariant -- a non-null pointer in `SEAMS[worker_id]` was
        // stored by `SeamGuard::install` over a live stack seam and is nulled
        // by the guard's drop (including unwind) before that stack frame is
        // reclaimed, so the referent outlives this call.
        // Precondition: the reader runs inside a poll on `worker_id`, strictly
        // between install and clear -- the bracket discipline the runtime's
        // poll frame already enforces for its own array.
        // Failure mode: a read after the guard dropped would deref a dangling
        // seam; the RAII bracket excludes it.
        let seam = unsafe { &*seam };
        Some(f(seam))
    }

    /// Returns the worker id the seam is installed for.
    #[must_use]
    pub const fn worker_id(&self) -> u8 {
        self.worker_id
    }

    /// Returns the completion result captured for the polling task, or
    /// `None` while the op is still in flight.
    #[must_use]
    pub const fn completion_result(&self) -> Option<WakeSlot> {
        self.wake_slot
    }

    /// Returns how many ops this poll submitted through the seam.
    ///
    /// The runtime reads the count after the poll and lands it on the polling
    /// task's header, pairing every increment with a completion-harvest
    /// decrement.
    #[must_use]
    pub fn submitted(&self) -> u16 {
        self.submitted.load(Ordering::Relaxed)
    }

    /// Submits a no-buffer op (accept, connect, timeout, cancel) for the
    /// polling task.
    ///
    /// Returns `None` when the seam carries no driver -- a test seam with no
    /// I/O backend. A successful submit raises the per-poll count the runtime
    /// folds onto the task's in-flight accounting.
    pub fn submit_internal(&self, request: IoRequest<()>) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; `IoDriver::submit_internal` takes `&self`, so this shared
        // reborrow aliases nothing mutably.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let result = unsafe { driver.as_ref().submit_internal(request) };
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Submits a no-buffer op bounded by a native `link_timeout` deadline for
    /// the polling task.
    ///
    /// Returns `None` when the seam carries no driver. A successful submit
    /// raises the per-poll count the runtime folds onto the task's in-flight
    /// accounting; the paired discard CQE is never task-attributed, so only the
    /// primary op counts. [`SubmitResult::Unsupported`] surfaces when the kernel
    /// lacks `link_timeout`, leaving the caller to fall back to the timer-wheel
    /// deadline (fallback parity).
    pub fn submit_linked_timeout_internal(
        &self,
        request: &IoRequest<()>,
        deadline_ns: u64,
    ) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; the submit takes `&self`, so this shared reborrow aliases
        // nothing mutably.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let result = unsafe {
            driver
                .as_ref()
                .submit_linked_timeout_internal(request, deadline_ns)
        };
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Submits a read-class op (the kernel writes into `request`'s buffer)
    /// for the polling task.
    ///
    /// Returns `None` when the seam carries no driver. A successful submit
    /// raises the per-poll count the runtime folds onto the task's in-flight
    /// accounting.
    pub fn submit_read<B: IoBufMut>(&self, request: IoRequest<B>) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; `IoDriver::submit_read` takes `&self`, so this shared reborrow
        // aliases nothing mutably. The buffer `B` moves into the driver call
        // by value; the kernel pointer built from it stays valid only while
        // `B`'s bytes outlive the CQE, which is `B`'s own `IoBufMut` contract
        // (the polling future owns the bytes and stays pinned), not this
        // block's driver-pointer soundness.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let result = unsafe { driver.as_ref().submit_read(request) };
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Submits a write-class op (the kernel reads from `request`'s buffer)
    /// for the polling task.
    ///
    /// Returns `None` when the seam carries no driver. A successful submit
    /// raises the per-poll count the runtime folds onto the task's in-flight
    /// accounting.
    pub fn submit<B: IoBuf>(&self, request: IoRequest<B>) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; `IoDriver::submit` takes `&self`, so this shared reborrow
        // aliases nothing mutably. The buffer `B` moves into the driver call
        // by value; the kernel pointer built from it stays valid only while
        // `B`'s bytes outlive the CQE, which is `B`'s own `IoBuf` contract
        // (the polling future owns the bytes and stays pinned), not this
        // block's driver-pointer soundness.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let result = unsafe { driver.as_ref().submit(request) };
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Submits a single-shot provided-buffer recv on `fd` for the polling
    /// task, addressed by `token` for the `user_data` round trip.
    ///
    /// The kernel selects a buffer from the driver's registered `buf_ring`
    /// group; the completion carries the chosen buffer id, read later via
    /// [`completion_result`](Self::completion_result). Returns `None` when the
    /// seam carries no driver, and `Some(SubmitResult::Unsupported)` when the
    /// backend registered no provided-buffer group -- the caller then takes the
    /// inline-buffer recv path (fallback parity). A successful submit raises the
    /// per-poll count the runtime folds onto the task's in-flight accounting.
    pub fn submit_provided_recv(&self, fd: i32, token: u64) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; both `provided_recv_group` and `submit_internal` take `&self`,
        // so this shared reborrow aliases nothing mutably.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let driver = unsafe { driver.as_ref() };
        let Some(group) = driver.provided_recv_group() else {
            return Some(SubmitResult::Unsupported);
        };
        let request = IoRequest::recv_provided(fd, group).with_user_data(token);
        let result = driver.submit_internal(request);
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Submits a multishot provided-buffer recv on `fd` for the polling task.
    ///
    /// `token` must be the recv-multishot sentinel the registry issues for this
    /// stream, not a bare task token: the completion drain routes a CQE into the
    /// recv-multishot slab only when its `user_data` is a recv-multishot
    /// sentinel (see
    /// [`is_recv_multishot_sentinel`](crate::boundary::is_recv_multishot_sentinel)),
    /// so a task token would
    /// misroute the stream's completions onto the single-shot wake path. The
    /// slab allocation that issues the sentinel lands with the drain-wiring
    /// slice; this entry is the submit half of that pair.
    ///
    /// One SQE streams a CQE per received buffer until cancelled; each carries
    /// the kernel-selected buffer id. The capability is probed up front: a
    /// backend without multishot recv (a kernel below 6.0) resolves
    /// `Some(SubmitResult::Unsupported)` synchronously rather than depending on
    /// the kernel to reject the SQE, and a backend with no registered
    /// provided-buffer group resolves the same. Either signals the caller to
    /// degrade to the single-shot provided recv (fallback parity). Returns
    /// `None` when the seam carries no driver. A successful submit raises the
    /// per-poll count the runtime folds onto the task's in-flight accounting.
    pub fn submit_recv_multishot_provided(&self, fd: i32, token: u64) -> Option<SubmitResult> {
        let driver = self.driver?;
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; `capabilities`, `provided_recv_group`, and `submit_internal`
        // all take `&self`, so this shared reborrow aliases nothing mutably.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let driver = unsafe { driver.as_ref() };
        if !driver.capabilities().multishot_recv {
            return Some(SubmitResult::Unsupported);
        }
        let Some(group) = driver.provided_recv_group() else {
            return Some(SubmitResult::Unsupported);
        };
        let request = IoRequest::recv_multishot_provided(fd, group).with_user_data(token);
        let result = driver.submit_internal(request);
        if matches!(result, SubmitResult::Submitted(_)) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Whether the backend supports zero-copy send (`SEND_ZC`, kernel 6.0+).
    ///
    /// A send future probes this up front: a backend that reports support
    /// submits [`IoRequest::send_zc`] so the kernel reads the buffer in place,
    /// otherwise the future falls back to a plain copying send (fallback
    /// parity). A seam with no driver (a test seam) reports `false`.
    ///
    /// [`IoRequest::send_zc`]: crate::operation::IoRequest::send_zc
    #[must_use]
    pub fn is_send_zc_supported(&self) -> bool {
        let Some(driver) = self.driver else {
            return false;
        };
        // SAFETY: Invariant -- `driver` points at the worker's live driver, a
        // field disjoint from the task storage the runtime borrows across the
        // poll; `capabilities` takes `&self`, so this shared reborrow aliases
        // nothing mutably.
        // Precondition: reached only via `with_current` during a poll on this
        // worker; the installing runtime keeps the referent live for the poll
        // window and the `SeamGuard` bracket clears the seam first.
        // Failure mode: a read after the guard dropped would deref a dangling
        // driver pointer; the bracket excludes it.
        let driver = unsafe { driver.as_ref() };
        driver.capabilities().send_zc
    }

    /// Allocates an in-flight buffer slot for a buffered op on the polling task.
    ///
    /// Returns the slot handle paired with a writable pointer to its
    /// `INFLIGHT_BUF_STRIDE` bytes -- the future builds an `InlineBuf` over the
    /// pointer to submit and, for a write-class op, copies its input into the
    /// slot first. Returns
    /// `None` when the seam carries no slab (a test seam with no backend) or the
    /// registry is full. The returned [`InflightSlotKey`] is `Copy`; the future
    /// holds it for the op lifetime and hands it to
    /// [`push_cancel_for_worker`](crate::boundary::push_cancel_for_worker) if it drops before
    /// the completion arrives.
    pub fn allocate_slot(&self, op_token: u64) -> Option<(InflightSlotKey, *mut u8)> {
        let mut slab = self.inflight_slab?;
        // SAFETY: Invariant -- `slab` points at the worker's `inflight_slab`
        // field, formed by the run-loop via `NonNull::from(&mut
        // shard.inflight_slab)` and disjoint from the driver, task slab, and
        // inboxes the runtime borrows separately across the poll. The field
        // lives on the worker, which outlives every poll window it hosts.
        // Precondition (why this `&mut` is unique for the window): the runtime
        // polls one task at a time per worker -- `poll_one` is not re-entrant,
        // and `SeamGuard` is not re-entrant, so at most one `IoSeam` is
        // installed per worker at a time. `allocate_slot`, `harvest_into`, and
        // `free_slot` are the only paths that form a `&mut InflightBufSlab`
        // during a poll, each runs to completion before the next, and the
        // run-loop does not touch `inflight_slab` again between forming the
        // pointer and the seam clearing. A second runtime in the same process
        // claims a different worker id with its own `InflightBufSlab`, so a
        // nested `block_on` aliases nothing here.
        // Failure mode: a second `&mut` into the slab within the poll window --
        // a reentrancy path the non-reentrant poll structure excludes -- aliases
        // this one (double-mutable-aliasing UB); a call after `SeamGuard` drops
        // derefs a dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        let key = slab.allocate(op_token)?;
        // `slot_ptr` cannot return `None` for the key `allocate` just produced:
        // the worker matches, the occupancy bit was just set, and the generation
        // is current, so `is_live` holds. The `?` keeps this panic-free against a
        // future occupancy-invariant regression rather than expressing a live
        // failure path.
        let ptr = slab.slot_ptr(key)?;
        Some((key, ptr))
    }

    /// Copies `src`'s initialized bytes into `key`'s slot.
    ///
    /// Generalizes the `ptr.copy_from_nonoverlapping` step every write-class
    /// buffered future repeats today: `src` supplies its bytes through the
    /// [`IoBuf`] contract (`as_ptr` / `bytes_init`) instead of a future's own
    /// inline array field, so a caller built over any `IoBuf` source populates
    /// its slot through one path. The caller still builds its own `InlineBuf`
    /// over the slot pointer and still calls `set_init`, unchanged from today;
    /// this method only replaces the raw copy step.
    ///
    /// Returns `false` without copying when `key` is stale, or when `src`'s
    /// initialized length exceeds the slot stride -- the caller then frees the
    /// slot and fails the submit rather than copy out of bounds. A seam with no
    /// slab reports `false`.
    #[must_use]
    pub fn copy_into_slot<B: IoBuf>(&self, key: InflightSlotKey, src: &B) -> bool {
        let Some(mut slab) = self.inflight_slab else {
            return false;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot` / `harvest_into`. Precondition: reached via
        // `with_current` during a poll on this worker; the `SeamGuard` bracket
        // keeps the referent live, and the `slot_ptr` shared read ends before
        // this call returns. Failure mode: a second `&mut` into the slab within
        // the poll window (excluded by the non-reentrant poll structure) aliases
        // this one (double-mutable-aliasing UB); a call after `SeamGuard` drops
        // derefs a dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        let Some(ptr) = slab.slot_ptr(key) else {
            return false;
        };
        let len = src.bytes_init();
        if len > INFLIGHT_BUF_STRIDE as usize {
            return false;
        }
        // SAFETY: Invariant -- `ptr` addresses `key`'s live slot, valid for
        // INFLIGHT_BUF_STRIDE writes; `src.as_ptr()` is valid for `len` reads per
        // the `IoBuf` contract (`src` is a shared borrow outliving this call);
        // the length check above keeps the write inside the slot, and the slot
        // and `src`'s own storage are always distinct allocations, so the copy
        // never overlaps. Precondition: `len <= INFLIGHT_BUF_STRIDE` is checked
        // immediately above. Failure mode: a source whose `bytes_init`
        // overstates its own backing storage is unsound in `src`'s own `IoBuf`
        // impl, not here; this call's own failure mode (a length past the slot)
        // is excluded by the check above.
        unsafe {
            ptr.copy_from_nonoverlapping(src.as_ptr(), len);
        }
        true
    }

    /// Copies `key`'s completed slot bytes into `dst` and frees the slot.
    ///
    /// Called on the completion poll of a read-class buffered future, once its
    /// CQE has arrived: `len` is the kernel-confirmed byte count, and the copy
    /// is clamped to both the slot stride and `dst`. Freeing the slot returns it
    /// to the registry with a bumped generation, so the future's later drop --
    /// which still holds the now-stale key -- pushes a cancel that the slab
    /// rejects as stale. A no-op when the seam carries no slab.
    pub fn harvest_into(&self, key: InflightSlotKey, len: usize, dst: &mut [u8]) {
        let Some(mut slab) = self.inflight_slab else {
            return;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot`. Precondition: reached via `with_current` during a
        // poll on this worker; the `SeamGuard` bracket keeps the referent live,
        // and the `slot_slice` shared reborrow ends before `free` takes the
        // `&mut` again. Failure mode: a second `&mut` into the slab within the
        // poll window (excluded by the non-reentrant poll structure) aliases
        // this one (double-mutable-aliasing UB); a call after `SeamGuard` drops
        // derefs a dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        if let Some(src) = slab.slot_slice(key, len) {
            let count = src.len().min(dst.len());
            dst[..count].copy_from_slice(&src[..count]);
        }
        slab.free(key);
    }

    /// Copies `key`'s completed slot bytes into `dst` and frees the slot,
    /// generalizing [`harvest_into`](Self::harvest_into) to any [`IoBufMut`]
    /// sink.
    ///
    /// Same calling contract as `harvest_into`: called on the completion poll of
    /// a read-class buffered future once its CQE has arrived, with `len` the
    /// kernel-confirmed byte count. The copy clamps to both the slot stride and
    /// `dst.capacity()`, and `dst.set_init(n)` records the copied length in place
    /// of the caller threading the count through its own return value. A no-op
    /// when the seam carries no slab.
    pub fn harvest_into_buf<B: IoBufMut>(&self, key: InflightSlotKey, len: usize, dst: &mut B) {
        let Some(mut slab) = self.inflight_slab else {
            return;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot` / `harvest_into`. Precondition: reached via
        // `with_current` during a poll on this worker; the `SeamGuard` bracket
        // keeps the referent live, and the `slot_slice` shared reborrow ends
        // before `free` takes the `&mut` again. Failure mode: a second `&mut`
        // into the slab within the poll window (excluded by the non-reentrant
        // poll structure) aliases this one (double-mutable-aliasing UB); a call
        // after `SeamGuard` drops derefs a dangling pointer (the bracket
        // excludes it).
        let slab = unsafe { slab.as_mut() };
        let mut count = 0;
        if let Some(src) = slab.slot_slice(key, len) {
            count = src.len().min(dst.capacity());
            // SAFETY: Invariant -- `dst.as_mut_ptr()` is valid for
            // `dst.capacity()` writes per the `IoBufMut` contract, and `count <=
            // dst.capacity()` by the `min` above; `src` is a `count`-byte shared
            // slice into the in-flight slot, a region distinct from `dst`'s own
            // storage, so the copy never overlaps. Precondition: `dst` is
            // exclusively borrowed for this call (the `&mut B` parameter), so no
            // other writer aliases the destination while this runs. Failure mode:
            // writing past `dst.capacity()` -- excluded by the `min` clamp above
            // -- would corrupt memory `dst` does not own.
            unsafe {
                dst.as_mut_ptr()
                    .copy_from_nonoverlapping(src.as_ptr(), count);
            }
        }
        dst.set_init(count);
        slab.free(key);
    }

    /// Frees `key`'s slot without reading it.
    ///
    /// Called on the completion poll of a write-class buffered future (the
    /// kernel read the slot, nothing to copy back) and on the submit-failure
    /// path of any buffered future (the slot was allocated but no op took
    /// ownership of its bytes). A stale key is a no-op in the slab; a seam with
    /// no slab is a no-op here.
    pub const fn free_slot(&self, key: InflightSlotKey) {
        let Some(mut slab) = self.inflight_slab else {
            return;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot`. Precondition: reached via `with_current` during a
        // poll on this worker; the `SeamGuard` bracket keeps the referent live.
        // Failure mode: a second `&mut` into the slab within the poll window
        // (excluded by the non-reentrant poll structure) aliases this one
        // (double-mutable-aliasing UB); a call after `SeamGuard` drops derefs a
        // dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        slab.free(key);
    }

    /// Lays out a `sendmsg` header for `key`'s slot and returns the `msghdr`
    /// pointer for the SQE.
    ///
    /// Copies `src`'s initialized bytes into the slot payload region and packs
    /// `addr` into its address region, both under the slot's own lifetime.
    /// Returns `None` when the seam carries no slab, `key` is stale, or `src`'s
    /// initialized length exceeds the payload capacity -- the caller then frees
    /// the slot and fails the submit rather than truncate the datagram.
    #[cfg(unix)]
    pub fn build_send_msg<B: IoBuf>(
        &self,
        key: InflightSlotKey,
        src: &B,
        addr: &SockAddr,
    ) -> Option<NonNull<libc::msghdr>> {
        let mut slab = self.inflight_slab?;
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot`. Precondition: reached via `with_current` during a
        // poll on this worker; the `SeamGuard` bracket keeps the referent live,
        // and the `slot_array_mut` reborrow ends before this call returns.
        // Failure mode: a second `&mut` into the slab within the poll window
        // (excluded by the non-reentrant poll structure) aliases this one
        // (double-mutable-aliasing UB); a call after `SeamGuard` drops derefs a
        // dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        let slot = slab.slot_array_mut(key)?;
        let len = src.bytes_init();
        if len > msghdr::MAX_MSG_INLINE_CAP {
            return None;
        }
        let payload = msghdr::payload_ptr(slot);
        // SAFETY: Invariant -- `payload` addresses `key`'s slot payload region
        // (`msghdr::payload_ptr`), valid for `MAX_MSG_INLINE_CAP` writes while
        // the slab lives and the slot stays occupied; `src.as_ptr()` is valid
        // for `len` reads per the `IoBuf` contract (`src` is a shared borrow
        // outliving this call); the length check above keeps the write inside
        // the payload region, and the slot and `src`'s own storage are always
        // distinct allocations, so the copy never overlaps. Precondition: `len
        // <= MAX_MSG_INLINE_CAP` is checked immediately above. Failure mode: a
        // source whose `bytes_init` overstates its backing storage is unsound
        // in `src`'s own `IoBuf` impl, not here; a length past the payload
        // region is excluded by the check above.
        unsafe {
            payload.copy_from_nonoverlapping(src.as_ptr(), len);
        }
        Some(msghdr::write_send_header(slot, len, addr))
    }

    /// Lays out a `recvmsg` header for `key`'s slot and returns the `msghdr`
    /// pointer for the SQE.
    ///
    /// Offers the kernel `cap` payload bytes (clamped to the slot's payload
    /// capacity) and the full address region as an out-parameter. Returns
    /// `None` when the seam carries no slab or `key` is stale.
    #[cfg(unix)]
    pub fn build_recv_msg(
        &self,
        key: InflightSlotKey,
        cap: usize,
    ) -> Option<NonNull<libc::msghdr>> {
        let mut slab = self.inflight_slab?;
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot`. Precondition: reached via `with_current` during a
        // poll on this worker; the `SeamGuard` bracket keeps the referent live,
        // and the `slot_array_mut` reborrow ends before this call returns.
        // Failure mode: a second `&mut` into the slab within the poll window
        // (excluded by the non-reentrant poll structure) aliases this one
        // (double-mutable-aliasing UB); a call after `SeamGuard` drops derefs a
        // dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        let slot = slab.slot_array_mut(key)?;
        let cap = cap.min(msghdr::MAX_MSG_INLINE_CAP);
        Some(msghdr::write_recv_header(slot, cap))
    }

    /// Reads the sender address the kernel wrote into `key`'s slot after a
    /// `recvmsg`, or `None` for a slab-less seam, a stale key, or a family the
    /// parse does not handle.
    ///
    /// Call on the completion poll once the CQE has arrived and BEFORE
    /// [`harvest_msg_payload`](Self::harvest_msg_payload) frees the slot.
    #[cfg(unix)]
    pub fn read_msg_sender(&self, key: InflightSlotKey) -> Option<SockAddr> {
        let slab = self.inflight_slab?;
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field,
        // reached via `with_current` during a poll on this worker; the
        // `SeamGuard` bracket keeps the referent live. This forms a SHARED
        // reference (`slot_array` reads only), weaker than the `&mut` the build
        // / harvest / free paths take. Precondition: single-poll-writer
        // discipline -- `poll_one` and `SeamGuard` are non-reentrant, so no
        // `&mut InflightBufSlab` is live while this shared reference exists.
        // Failure mode: a `&mut` into the slab overlapping this shared reborrow
        // would be aliasing UB (the non-reentrant poll excludes it); a call
        // after `SeamGuard` drops derefs a dangling pointer (the bracket
        // excludes it).
        let slab = unsafe { slab.as_ref() };
        msghdr::read_sender(slab.slot_array(key)?)
    }

    /// Copies `key`'s received datagram payload into `dst` and frees the slot.
    ///
    /// Called on the completion poll of a `recvmsg` future once its CQE has
    /// arrived, with `len` the kernel-confirmed byte count. The copy clamps to
    /// both the slot payload capacity and `dst.capacity()`, and `dst.set_init`
    /// records the copied length. Freeing the slot bumps its generation, so the
    /// future's later drop pushes a cancel the slab rejects as stale. A no-op
    /// when the seam carries no slab. Call AFTER
    /// [`read_msg_sender`](Self::read_msg_sender), which this frees out from
    /// under.
    #[cfg(unix)]
    pub fn harvest_msg_payload<B: IoBufMut>(&self, key: InflightSlotKey, len: usize, dst: &mut B) {
        let Some(mut slab) = self.inflight_slab else {
            return;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field, the
        // sole `&mut` into it for the non-reentrant poll window, exactly as in
        // `allocate_slot` / `harvest_into_buf`. Precondition: reached via
        // `with_current` during a poll on this worker; the `SeamGuard` bracket
        // keeps the referent live, and the `slot_array_mut` reborrow ends
        // before `free` takes the `&mut` again. Failure mode: a second `&mut`
        // into the slab within the poll window (excluded by the non-reentrant
        // poll structure) aliases this one (double-mutable-aliasing UB); a call
        // after `SeamGuard` drops derefs a dangling pointer (the bracket
        // excludes it).
        let slab = unsafe { slab.as_mut() };
        let mut count = 0;
        if let Some(slot) = slab.slot_array_mut(key) {
            let payload = msghdr::payload_ptr(slot);
            count = len.min(msghdr::MAX_MSG_INLINE_CAP).min(dst.capacity());
            // SAFETY: Invariant -- `payload` addresses `key`'s slot payload
            // region, readable once this op's CQE has arrived (the caller's
            // contract); `dst.as_mut_ptr()` is valid for `dst.capacity()`
            // writes per the `IoBufMut` contract, and `count <= dst.capacity()`
            // by the `min` clamp above; the payload region and `dst`'s own
            // storage are always distinct allocations, so the copy never
            // overlaps. Precondition: `dst` is exclusively borrowed for this
            // call, so no other writer aliases the destination. Failure mode:
            // writing past `dst.capacity()` -- excluded by the clamp -- would
            // corrupt memory `dst` does not own; reading before the CQE arrives
            // would race the kernel write -- excluded by the caller polling
            // only a completed op.
            unsafe {
                dst.as_mut_ptr().copy_from_nonoverlapping(payload, count);
            }
        }
        dst.set_init(count);
        slab.free(key);
    }

    /// Whether `key`'s in-flight slot has seen its `SEND_ZC` NOTIF.
    ///
    /// A resolve-on-NOTIF send future polls this by key during its own poll:
    /// `true` means the kernel released the buffer, so the future may resolve
    /// and free its slot. A seam with no slab, or a stale key, reads `false`.
    #[must_use]
    pub const fn slot_notif_ready(&self, key: InflightSlotKey) -> bool {
        let Some(slab) = self.inflight_slab else {
            return false;
        };
        // SAFETY: Invariant -- `slab` is the worker's `inflight_slab` field,
        // reached via `with_current` during a poll on this worker; the
        // `SeamGuard` bracket keeps the referent live. This forms a SHARED
        // reference (`is_notif_ready` reads only), weaker than the `&mut` the
        // allocate / harvest / free paths take.
        // Precondition: single-poll-writer discipline -- `poll_one` and
        // `SeamGuard` are non-reentrant, so no `&mut InflightBufSlab` is live
        // anywhere while this shared reference exists.
        // Failure mode: a `&mut` into the slab overlapping this shared reborrow
        // would be aliasing UB (the non-reentrant poll excludes it); a call
        // after `SeamGuard` drops derefs a dangling pointer (the bracket
        // excludes it).
        let slab = unsafe { slab.as_ref() };
        slab.is_notif_ready(key)
    }

    /// Reserves a multishot slot for the polling task.
    ///
    /// `owner_token` is the polling task, woken on each completion. On
    /// [`MultishotAlloc::Allocated`] the stream tags its multishot SQE with the
    /// returned sentinel, drains the slot's FIFO on later polls via
    /// [`IoSeam::multishot_next`], and hands the key to
    /// [`push_multishot_cancel_for_worker`](crate::boundary::push_multishot_cancel_for_worker) if
    /// it drops before the op terminates. Returns [`MultishotAlloc::Full`] when every slot is
    /// occupied, so the stream can degrade to single-shot, and [`MultishotAlloc::Unsupported`]
    /// when the seam carries no multishot registry (a test seam).
    pub fn allocate_multishot_slot(&self, owner_token: u64) -> MultishotAlloc {
        let Some(mut slab) = self.multishot_slab else {
            return MultishotAlloc::Unsupported;
        };
        // SAFETY: Invariant -- `slab` points at the worker's `multishot_slab`
        // field, formed by the run-loop via `NonNull::from(&mut ...)` and
        // disjoint from the driver, task slab, inflight slab, and inboxes the
        // runtime borrows separately across the poll. The field lives on the
        // worker, which outlives every poll window it hosts.
        // Precondition (why this `&mut` is unique for the window): the runtime
        // polls one task at a time per worker -- `poll_one` is not re-entrant and
        // `SeamGuard` is not re-entrant, so at most one `IoSeam` is installed per
        // worker, and `allocate_multishot_slot` / `multishot_next` are the only
        // paths that form a `&mut MultishotSlab` during a poll, each running to
        // completion before the next. A nested `block_on` claims a different
        // worker id with its own slab, so it aliases nothing here.
        // Failure mode: a second `&mut` into the slab within the poll window
        // aliases this one (double-mutable-aliasing UB); the non-reentrant poll
        // structure excludes it, and a call after `SeamGuard` drops derefs a
        // dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        slab.allocate(owner_token)
            .map_or(MultishotAlloc::Full, |key| MultishotAlloc::Allocated {
                key,
                sentinel: encode_multishot_sentinel(key),
            })
    }

    /// Advances `key`'s multishot stream by one completion.
    ///
    /// Returns the next queued result, `Pending` while the op is in flight with
    /// an empty FIFO, or `Ended` once the op posted its terminal CQE and the
    /// FIFO is drained -- freeing the slot. A seam with no multishot slab yields
    /// `Ended`.
    pub const fn multishot_next(&self, key: MultishotSlotKey) -> MultishotNext {
        let Some(mut slab) = self.multishot_slab else {
            return MultishotNext::Ended;
        };
        // SAFETY: identical contract to `allocate_multishot_slot` -- the sole
        // `&mut MultishotSlab` for the non-reentrant poll window, over the
        // worker's live `multishot_slab` field reached via `with_current`, each
        // slab-forming path running to completion before the next.
        let slab = unsafe { slab.as_mut() };
        if let Some(result) = slab.pop(key) {
            return MultishotNext::Item(result);
        }
        if slab.is_terminated(key) {
            slab.free(key);
            return MultishotNext::Ended;
        }
        MultishotNext::Pending
    }

    /// Frees `key`'s multishot slot without draining it.
    ///
    /// The stream calls this only when a submit fails right after an allocate,
    /// so the slot carries no in-flight op. A stale key is a no-op; a seam with
    /// no multishot slab is a no-op.
    pub const fn multishot_free(&self, key: MultishotSlotKey) {
        let Some(mut slab) = self.multishot_slab else {
            return;
        };
        // SAFETY: identical contract to `allocate_multishot_slot` -- the sole
        // `&mut MultishotSlab` for the non-reentrant poll window, over the
        // worker's live `multishot_slab` field reached via `with_current`.
        let slab = unsafe { slab.as_mut() };
        slab.free(key);
    }

    /// Reserves a multishot recv slot for the polling task.
    ///
    /// `owner_token` is the polling task, woken on each completion. On
    /// [`RecvMultishotAlloc::Allocated`] the stream tags its multishot recv SQE
    /// with the returned sentinel, drains the slot's FIFO on later polls via
    /// [`IoSeam::recv_multishot_next`], and hands the
    /// key to
    /// [`push_recv_multishot_cancel_for_worker`](crate::boundary::push_recv_multishot_cancel_for_worker) if it drops before the op
    /// terminates. Returns [`RecvMultishotAlloc::Full`] when every slot is
    /// occupied, so the stream can degrade to a single-shot provided recv, and
    /// [`RecvMultishotAlloc::Unsupported`] when the seam carries no recv registry
    /// (a test seam).
    pub fn allocate_recv_multishot_slot(&self, owner_token: u64) -> RecvMultishotAlloc {
        let Some(mut slab) = self.recv_multishot_slab else {
            return RecvMultishotAlloc::Unsupported;
        };
        // SAFETY: Invariant -- `slab` points at the worker's `recv_multishot_slab`
        // field, formed by the run-loop via `NonNull::from(&mut ...)` and
        // disjoint from the driver, task slab, inflight slab, accept multishot
        // slab, and inboxes the runtime borrows separately across the poll. The
        // field lives on the worker, which outlives every poll window it hosts.
        // Precondition (why this `&mut` is unique for the window): the runtime
        // polls one task at a time per worker -- `poll_one` is not re-entrant and
        // `SeamGuard` is not re-entrant, so at most one `IoSeam` is installed per
        // worker, and `allocate_recv_multishot_slot` / `recv_multishot_next` /
        // `recv_multishot_free` are the only paths that form a
        // `&mut RecvMultishotSlab` during a poll, each running to completion
        // before the next. A nested `block_on` claims a different worker id with
        // its own slab, so it aliases nothing here.
        // Failure mode: a second `&mut` into the slab within the poll window
        // aliases this one (double-mutable-aliasing UB); the non-reentrant poll
        // structure excludes it, and a call after `SeamGuard` drops derefs a
        // dangling pointer (the bracket excludes it).
        let slab = unsafe { slab.as_mut() };
        slab.allocate(owner_token)
            .map_or(RecvMultishotAlloc::Full, |key| {
                RecvMultishotAlloc::Allocated {
                    key,
                    sentinel: encode_recv_multishot_sentinel(key),
                }
            })
    }

    /// Advances `key`'s multishot recv stream by one completion.
    ///
    /// Returns the next queued data `(result, buf_id)`; then, once the FIFO is
    /// drained, the stashed terminal completion as the stream's final item;
    /// `Pending` while the op is in flight with an empty FIFO; or `Ended` once
    /// the terminal has been handed back and the slot freed. A seam with no recv
    /// slab yields `Ended`. A [`NO_BUFFER`](crate::buffer::multishot::recv) entry
    /// (end of stream or a negative result) reports `buf_id: None`.
    ///
    /// The terminal completion lives in the slot rather than the FIFO, so it is
    /// delivered intact even when a deep consumer backlog overflowed the FIFO,
    /// keeping a clean close distinct from an error end (#230).
    ///
    /// Buffer-recycle contract: a `Some(buf_id)` handed back in an
    /// [`RecvMultishotNext::Item`] names a kernel-selected provided buffer the
    /// caller now owns. The drain recycles a buffer only on the paths where no
    /// consumer takes it (a stale, overflowed, or dropped stream); a buffer
    /// successfully dequeued here is NOT recycled by the runtime, so the caller
    /// must read it and return it to the driver's pool, or the pool entry leaks
    /// until teardown. This mirrors the borrow-then-recycle discipline the
    /// single-shot provided-recv path enforces through its buffer view's drop.
    pub fn recv_multishot_next(&self, key: RecvMultishotSlotKey) -> RecvMultishotNext {
        let Some(mut slab) = self.recv_multishot_slab else {
            return RecvMultishotNext::Ended;
        };
        // SAFETY: identical contract to `allocate_recv_multishot_slot` -- the sole
        // `&mut RecvMultishotSlab` for the non-reentrant poll window, over the
        // worker's live `recv_multishot_slab` field reached via `with_current`,
        // each slab-forming path running to completion before the next.
        let slab = unsafe { slab.as_mut() };
        if let Some((result, buf_id)) = slab.pop(key) {
            let buf_id = if buf_id == NO_BUFFER {
                None
            } else {
                Some(buf_id)
            };
            return RecvMultishotNext::Item { result, buf_id };
        }
        // The FIFO is drained; the terminal completion is stashed separately, so
        // it survives even a backlog that overflowed the FIFO (#230). Deliver it
        // as the stream's final item -- a clean close (result 0) or an error end
        // (negative errno) -- before ending the stream on the next poll.
        if let Some((result, buf_id)) = slab.take_terminal(key) {
            let buf_id = if buf_id == NO_BUFFER {
                None
            } else {
                Some(buf_id)
            };
            return RecvMultishotNext::Item { result, buf_id };
        }
        if slab.is_terminated(key) {
            slab.free(key);
            return RecvMultishotNext::Ended;
        }
        RecvMultishotNext::Pending
    }

    /// Frees `key`'s multishot recv slot without draining it.
    ///
    /// The stream calls this only when a submit fails right after an allocate,
    /// so the slot carries no in-flight op. A stale key is a no-op; a seam with
    /// no recv slab is a no-op.
    pub fn recv_multishot_free(&self, key: RecvMultishotSlotKey) {
        let Some(mut slab) = self.recv_multishot_slab else {
            return;
        };
        // SAFETY: identical contract to `allocate_recv_multishot_slot` -- the
        // sole `&mut RecvMultishotSlab` for the non-reentrant poll window, over
        // the worker's live `recv_multishot_slab` field reached via
        // `with_current`.
        let slab = unsafe { slab.as_mut() };
        slab.free(key);
    }
}

/// RAII bracket that installs a seam for one poll window and clears it on
/// drop, including an unwinding drop.
///
/// Not re-entrant: a second install for the same worker while a guard is
/// live overwrites the slot, and the inner guard's drop clears the seam the
/// outer poll still expects. The runtime polls one task at a time per
/// worker, so no nested install occurs today.
pub struct SeamGuard {
    /// Worker slot to clear on drop.
    worker_id: u8,
}

impl SeamGuard {
    /// Installs `seam` for its worker, returning the guard that clears it.
    #[must_use]
    pub fn install(seam: &IoSeam) -> Self {
        SEAMS[seam.worker_id as usize].store(ptr::from_ref(seam).cast_mut(), Ordering::Release);
        Self {
            worker_id: seam.worker_id,
        }
    }
}

impl Drop for SeamGuard {
    fn drop(&mut self) {
        SEAMS[self.worker_id as usize].store(ptr::null_mut(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::cancel::{
        is_multishot_sentinel, is_recv_multishot_sentinel, multishot_sentinel_generation,
        multishot_sentinel_slot,
    };

    struct MockBuf {
        data: [u8; 8],
        len: usize,
        cap: usize,
    }

    impl MockBuf {
        fn seeded(bytes: &[u8]) -> Self {
            let mut data = [0u8; 8];
            let len = bytes.len().min(8);
            data[..len].copy_from_slice(&bytes[..len]);
            Self { data, len, cap: 8 }
        }

        fn with_capacity(cap: usize) -> Self {
            Self {
                data: [0u8; 8],
                len: 0,
                cap: cap.min(8),
            }
        }

        fn oversized() -> Self {
            Self {
                data: [0u8; 8],
                len: INFLIGHT_BUF_STRIDE as usize + 1,
                cap: 8,
            }
        }
    }

    impl IoBuf for MockBuf {
        fn as_ptr(&self) -> *const u8 {
            self.data.as_ptr()
        }

        fn bytes_init(&self) -> usize {
            self.len
        }
    }

    impl IoBufMut for MockBuf {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.data.as_mut_ptr()
        }

        fn capacity(&self) -> usize {
            self.cap
        }

        fn set_init(&mut self, count: usize) {
            self.len = count;
        }
    }

    #[test]
    fn with_current_is_none_when_uninstalled() {
        assert_eq!(IoSeam::with_current(200, IoSeam::worker_id), None);
    }

    #[test]
    fn guard_brackets_install_and_clear() {
        let seam = IoSeam::new(201, None, None, None);
        {
            let _guard = SeamGuard::install(&seam);
            assert_eq!(IoSeam::with_current(201, IoSeam::worker_id), Some(201));
            assert_eq!(
                IoSeam::with_current(201, IoSeam::completion_result),
                Some(None),
                "no captured result means the op is still in flight",
            );
            let submitted = IoSeam::with_current(201, |current| {
                current.submit_internal(IoRequest::<()>::timeout(1))
            });
            assert_eq!(
                submitted,
                Some(None),
                "a driverless seam refuses the submit instead of counting it",
            );
            assert_eq!(IoSeam::with_current(201, IoSeam::submitted), Some(0));
        }
        assert_eq!(
            IoSeam::with_current(201, IoSeam::worker_id),
            None,
            "the guard clears the slot on drop",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejected_submit_leaves_the_count_untouched() {
        let mut driver = DriverType::Epoll(());
        let seam = IoSeam::new(202, Some(NonNull::from(&mut driver)), None, None);
        let result = seam.submit_internal(IoRequest::<()>::timeout(1));
        assert!(
            matches!(result, Some(SubmitResult::Unsupported)),
            "the epoll stub refuses the op",
        );
        assert_eq!(seam.submitted(), 0, "only Submitted raises the count");
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn submitted_op_raises_the_count() {
        let Ok(mut driver) = DriverType::for_platform(8) else {
            panic!("the platform driver must build on this host");
        };
        let seam = IoSeam::new(203, Some(NonNull::from(&mut driver)), None, None);
        let request = IoRequest::<()>::timeout(1_000_000).with_user_data(0);
        let Some(result) = seam.submit_internal(request) else {
            panic!("a seam with a driver must reach the submit path");
        };
        assert!(
            matches!(result, SubmitResult::Submitted(_)),
            "the ring accepts a timeout op",
        );
        assert_eq!(seam.submitted(), 1, "a submitted op raises the count");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_multishot_submit_gated() {
        let mut driver = DriverType::Epoll(());
        let seam = IoSeam::new(204, Some(NonNull::from(&mut driver)), None, None);
        // Epoll reports no multishot-recv capability, so the submit resolves
        // Unsupported up front without touching the ring.
        assert_eq!(
            seam.submit_recv_multishot_provided(5, 0x1),
            Some(SubmitResult::Unsupported),
            "a backend without multishot recv degrades synchronously",
        );
        assert_eq!(seam.submitted(), 0, "a gated submit raises no count");
    }

    #[test]
    fn send_zc_unsupported_without_a_driver() {
        let seam = IoSeam::new(206, None, None, None);
        assert!(
            !seam.is_send_zc_supported(),
            "a seam with no driver reports no zero-copy send",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn send_zc_unsupported_on_epoll() {
        let mut driver = DriverType::Epoll(());
        let seam = IoSeam::new(207, Some(NonNull::from(&mut driver)), None, None);
        // Epoll reports no io_uring capabilities, so the shared driver reborrow
        // reads send_zc as false and the future takes the plain-send fallback.
        assert!(
            !seam.is_send_zc_supported(),
            "the epoll stub reports no zero-copy send capability",
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn recv_multishot_submit_reaches_ring() {
        let Ok(mut driver) = DriverType::for_platform(8) else {
            panic!("the platform driver must build on this host");
        };
        let seam = IoSeam::new(205, Some(NonNull::from(&mut driver)), None, None);
        // On a 6.0+ kernel with a registered group the submit reaches the ring;
        // a kernel missing either degrades to Unsupported (fallback parity).
        let Some(outcome) = seam.submit_recv_multishot_provided(5, 0x1) else {
            panic!("a seam with a driver must reach the submit path");
        };
        assert!(
            matches!(
                outcome,
                SubmitResult::Submitted(_) | SubmitResult::Unsupported
            ),
            "the submit either reaches the ring or degrades cleanly",
        );
    }

    #[test]
    fn allocate_slot_returns_a_key() {
        let Ok(mut slab) = InflightBufSlab::new(5, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(5, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, ptr)) = seam.allocate_slot(0xABCD) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        assert_eq!(key.op_token, 0xABCD);
        assert_eq!(key.worker_id, 5);
        assert!(!ptr.is_null(), "a live slot yields a writable pointer");
    }

    #[test]
    fn allocate_slot_needs_a_slab() {
        let seam = IoSeam::new(6, None, None, None);
        assert!(
            seam.allocate_slot(0).is_none(),
            "a seam with no slab cannot allocate",
        );
    }

    #[test]
    fn slot_notif_ready_reads_the_flag_through_the_seam() {
        let Ok(mut slab) = InflightBufSlab::new(12, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        {
            let seam = IoSeam::new(12, None, Some(NonNull::from(&mut slab)), None);
            assert!(
                !seam.slot_notif_ready(key),
                "a fresh slot is not notif-ready through the seam",
            );
        }
        // Arm the flag on the slab, then observe it through a fresh seam.
        slab.mark_notif_ready_by_op_token(0xABCD);
        let seam = IoSeam::new(12, None, Some(NonNull::from(&mut slab)), None);
        assert!(
            seam.slot_notif_ready(key),
            "the seam observes the notif-ready flag by key",
        );
    }

    #[test]
    fn slot_notif_ready_needs_a_slab() {
        let seam = IoSeam::new(13, None, None, None);
        let key = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 13,
            op_token: 0,
        };
        assert!(
            !seam.slot_notif_ready(key),
            "a seam with no slab reports not-ready",
        );
    }

    #[test]
    fn harvest_into_copies_then_frees() {
        let Ok(mut slab) = InflightBufSlab::new(9, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(9, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, ptr)) = seam.allocate_slot(0x1234) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        // SAFETY: `ptr` addresses the slot's stride-wide region for the slot's
        // lifetime; this test writes 4 bytes well within the stride and reads
        // them back through the harvest path, with no other reference aliasing.
        // Failure mode: a write past the stride would corrupt an adjacent slot
        // or mmap page.
        unsafe {
            ptr.copy_from(b"ping".as_ptr(), 4);
        }
        let mut out = [0u8; 8];
        seam.harvest_into(key, 4, &mut out);
        assert_eq!(&out[..4], b"ping");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot
                    && reused.generation == key.generation + 1),
            "harvest frees the slot so it is reused with a bumped generation",
        );
    }

    #[test]
    fn harvest_into_clamps_to_dst() {
        let Ok(mut slab) = InflightBufSlab::new(10, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(10, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, ptr)) = seam.allocate_slot(0) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        // SAFETY: `ptr` addresses the slot's stride-wide region; this test
        // writes 8 bytes within the stride, exclusively owned for the test.
        // Failure mode: a write past the stride would corrupt an adjacent slot
        // or mmap page.
        unsafe {
            ptr.copy_from(b"overflow".as_ptr(), 8);
        }
        let mut out = [0u8; 4];
        seam.harvest_into(key, 8, &mut out);
        assert_eq!(&out, b"over", "the copy clamps to the destination length");
    }

    #[test]
    fn free_slot_returns_the_slot() {
        let Ok(mut slab) = InflightBufSlab::new(11, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(11, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        seam.free_slot(key);
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot
                    && reused.generation == key.generation + 1),
            "free returns the slot for reuse with a bumped generation",
        );
    }

    #[test]
    fn harvest_and_free_need_no_slab() {
        let seam = IoSeam::new(12, None, None, None);
        let key = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 12,
            op_token: 0,
        };
        // A seam with no slab is a no-op for both, never a panic.
        seam.harvest_into(key, 4, &mut [0u8; 4]);
        seam.free_slot(key);
    }

    #[test]
    fn copy_into_slot_writes_the_source_bytes() {
        let Ok(mut slab) = InflightBufSlab::new(20, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(20, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _)) = seam.allocate_slot(0x1234) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        let src = MockBuf::seeded(b"ping");
        assert!(
            seam.copy_into_slot(key, &src),
            "the source copies into the live slot",
        );
        let mut out = [0u8; 8];
        seam.harvest_into(key, 4, &mut out);
        assert_eq!(&out[..4], b"ping", "the slot carries the copied bytes");
    }

    #[test]
    fn copy_into_slot_needs_a_slab() {
        let seam = IoSeam::new(21, None, None, None);
        let key = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 21,
            op_token: 0,
        };
        let src = MockBuf::seeded(b"x");
        assert!(
            !seam.copy_into_slot(key, &src),
            "a seam with no slab cannot copy",
        );
    }

    #[test]
    fn copy_into_slot_rejects_a_stale_key() {
        let Ok(mut slab) = InflightBufSlab::new(22, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(22, None, Some(NonNull::from(&mut slab)), None);
        // A key the slab never issued: the slot is unoccupied, so `slot_ptr`
        // rejects it and no copy runs.
        let stale = InflightSlotKey {
            slot: 0,
            generation: 7,
            worker_id: 22,
            op_token: 0,
        };
        let src = MockBuf::seeded(b"ping");
        assert!(
            !seam.copy_into_slot(stale, &src),
            "a stale key copies nothing",
        );
    }

    #[test]
    fn copy_into_slot_rejects_oversized_source() {
        let Ok(mut slab) = InflightBufSlab::new(23, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(23, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        let src = MockBuf::oversized();
        assert!(
            !seam.copy_into_slot(key, &src),
            "a source past the slot stride copies nothing",
        );
    }

    #[test]
    fn harvest_into_buf_copies_then_frees() {
        let Ok(mut slab) = InflightBufSlab::new(24, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(24, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, ptr)) = seam.allocate_slot(0x5678) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        // SAFETY: `ptr` addresses the slot's stride-wide region for the slot's
        // lifetime; this test writes 4 bytes well within the stride, exclusively
        // owned for the test. Failure mode: a write past the stride would corrupt
        // an adjacent slot or mmap page.
        unsafe {
            ptr.copy_from(b"pong".as_ptr(), 4);
        }
        let mut dst = MockBuf::with_capacity(8);
        seam.harvest_into_buf(key, 4, &mut dst);
        assert_eq!(dst.bytes_init(), 4, "set_init records the copied length");
        assert_eq!(&dst.data[..4], b"pong", "the slot bytes land in the sink");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot
                    && reused.generation == key.generation + 1),
            "the harvest freed the slot for reuse with a bumped generation",
        );
    }

    #[cfg(unix)]
    #[test]
    fn build_send_msg_round_trips_sender_and_payload() {
        use std::net::{Ipv4Addr, SocketAddrV4};
        let Ok(mut slab) = InflightBufSlab::new(30, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(30, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _ptr)) = seam.allocate_slot(0x9abc) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), 9000));
        let src = MockBuf::seeded(b"ping");
        assert!(
            seam.build_send_msg(key, &src, &addr).is_some(),
            "a live slot lays out the send header",
        );
        assert_eq!(
            seam.read_msg_sender(key),
            Some(addr),
            "the send packed the peer address into the slot",
        );
        let mut dst = MockBuf::with_capacity(8);
        seam.harvest_msg_payload(key, 4, &mut dst);
        assert_eq!(
            &dst.data[..4],
            b"ping",
            "the payload copies out of the slot"
        );
        assert_eq!(dst.bytes_init(), 4);
    }

    #[cfg(unix)]
    #[test]
    fn build_recv_msg_lays_out_the_header() {
        let Ok(mut slab) = InflightBufSlab::new(31, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(31, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _ptr)) = seam.allocate_slot(0xdef0) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        assert!(
            seam.build_recv_msg(key, 512).is_some(),
            "a live slot lays out the recv header",
        );
        assert_eq!(
            seam.read_msg_sender(key),
            None,
            "no sender is present before the kernel writes one",
        );
    }

    #[cfg(unix)]
    #[test]
    fn build_send_msg_rejects_an_oversized_payload() {
        use std::net::{Ipv4Addr, SocketAddrV4};
        let Ok(mut slab) = InflightBufSlab::new(32, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(32, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, _ptr)) = seam.allocate_slot(0x1357) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80));
        assert!(
            seam.build_send_msg(key, &MockBuf::oversized(), &addr)
                .is_none(),
            "a payload past the slot capacity is refused, not truncated",
        );
    }

    #[cfg(unix)]
    #[test]
    fn msg_seam_needs_a_slab() {
        use std::net::{Ipv4Addr, SocketAddrV4};
        let seam = IoSeam::new(33, None, None, None);
        let key = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 33,
            op_token: 0,
        };
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80));
        assert!(
            seam.build_send_msg(key, &MockBuf::seeded(b"x"), &addr)
                .is_none(),
        );
        assert!(seam.build_recv_msg(key, 128).is_none());
        assert_eq!(seam.read_msg_sender(key), None);
        let mut dst = MockBuf::seeded(b"stale");
        seam.harvest_msg_payload(key, 4, &mut dst);
        assert_eq!(dst.bytes_init(), 5, "no slab leaves the sink untouched");
    }

    #[test]
    fn harvest_into_buf_clamps_to_dst_capacity() {
        let Ok(mut slab) = InflightBufSlab::new(25, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(25, None, Some(NonNull::from(&mut slab)), None);
        let Some((key, ptr)) = seam.allocate_slot(0) else {
            panic!("a seam carrying a slab allocates a slot");
        };
        // SAFETY: `ptr` addresses the slot's stride-wide region; this test writes
        // 8 bytes within the stride, exclusively owned for the test. Failure mode:
        // a write past the stride would corrupt an adjacent slot or mmap page.
        unsafe {
            ptr.copy_from(b"overflow".as_ptr(), 8);
        }
        let mut dst = MockBuf::with_capacity(4);
        seam.harvest_into_buf(key, 8, &mut dst);
        assert_eq!(dst.bytes_init(), 4, "the copy clamps to dst capacity");
        assert_eq!(&dst.data[..4], b"over", "only the clamped prefix lands");
    }

    #[test]
    fn harvest_into_buf_needs_no_slab() {
        let seam = IoSeam::new(26, None, None, None);
        let key = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 26,
            op_token: 0,
        };
        // Seed a non-zero init to prove a true no-op: with no slab the method
        // returns before touching `set_init`, so `bytes_init` stays untouched.
        let mut dst = MockBuf::seeded(b"stale");
        seam.harvest_into_buf(key, 4, &mut dst);
        assert_eq!(
            dst.bytes_init(),
            5,
            "a seam with no slab leaves the sink untouched",
        );
    }

    #[test]
    fn multishot_seam_allocates_and_decodes_sentinel() {
        let mut slab = MultishotSlab::new(21, 4);
        let seam =
            IoSeam::new(21, None, None, None).with_multishot_slab(Some(NonNull::from(&mut slab)));
        let MultishotAlloc::Allocated { key, sentinel } = seam.allocate_multishot_slot(0xABCD)
        else {
            panic!("a seam carrying a multishot slab allocates a slot");
        };
        assert_eq!(key.worker_id, 21);
        assert!(
            is_multishot_sentinel(sentinel),
            "the SQE user_data is a multishot sentinel",
        );
        assert_eq!(
            multishot_sentinel_slot(sentinel),
            key.slot,
            "the sentinel names the allocated slot",
        );
        assert_eq!(
            multishot_sentinel_generation(sentinel),
            (key.generation & 0xFFFF) as u16,
            "the sentinel carries the slot generation",
        );
    }

    #[test]
    fn multishot_seam_next_is_pending_while_armed() {
        let mut slab = MultishotSlab::new(22, 4);
        let seam =
            IoSeam::new(22, None, None, None).with_multishot_slab(Some(NonNull::from(&mut slab)));
        let MultishotAlloc::Allocated { key, .. } = seam.allocate_multishot_slot(0x9) else {
            panic!("a seam carrying a multishot slab allocates a slot");
        };
        assert_eq!(
            seam.multishot_next(key),
            MultishotNext::Pending,
            "a freshly armed slot with an empty FIFO polls Pending",
        );
    }

    #[test]
    fn multishot_seam_drains_then_ends_and_frees() {
        // Queue a terminal completion before the seam forms its pointer, so the
        // only slab reference the seam holds is the last-derived one.
        let mut slab = MultishotSlab::new(24, 4);
        let Some(key) = slab.allocate(0x5) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        slab.push(key.slot, gen_low16, 7, false);
        let seam =
            IoSeam::new(24, None, None, None).with_multishot_slab(Some(NonNull::from(&mut slab)));
        assert_eq!(
            seam.multishot_next(key),
            MultishotNext::Item(7),
            "the queued result drains first",
        );
        assert_eq!(
            seam.multishot_next(key),
            MultishotNext::Ended,
            "an empty terminated FIFO ends the stream",
        );
        let MultishotAlloc::Allocated { key: reused, .. } = seam.allocate_multishot_slot(0x5)
        else {
            panic!("ending the stream freed the slot for reuse");
        };
        assert_eq!(reused.slot, key.slot);
        assert_eq!(
            reused.generation,
            key.generation + 1,
            "the freed slot is reused with a bumped generation",
        );
    }

    #[test]
    fn multishot_seam_free_returns_the_slot() {
        let mut slab = MultishotSlab::new(25, 4);
        let seam =
            IoSeam::new(25, None, None, None).with_multishot_slab(Some(NonNull::from(&mut slab)));
        let MultishotAlloc::Allocated { key, .. } = seam.allocate_multishot_slot(0) else {
            panic!("a seam carrying a multishot slab allocates a slot");
        };
        seam.multishot_free(key);
        let MultishotAlloc::Allocated { key: reused, .. } = seam.allocate_multishot_slot(0) else {
            panic!("freeing the slot returns it for reuse");
        };
        assert_eq!(reused.slot, key.slot);
        assert_eq!(
            reused.generation,
            key.generation + 1,
            "free bumps the generation so a stale handle is rejected",
        );
    }

    #[test]
    fn multishot_seam_reports_full_when_saturated() {
        let mut slab = MultishotSlab::new(26, 2);
        let seam =
            IoSeam::new(26, None, None, None).with_multishot_slab(Some(NonNull::from(&mut slab)));
        assert!(matches!(
            seam.allocate_multishot_slot(0x1),
            MultishotAlloc::Allocated { .. }
        ));
        assert!(matches!(
            seam.allocate_multishot_slot(0x2),
            MultishotAlloc::Allocated { .. }
        ));
        assert_eq!(
            seam.allocate_multishot_slot(0x3),
            MultishotAlloc::Full,
            "a saturated registry reports Full so the stream degrades to single-shot",
        );
    }

    #[test]
    fn multishot_seam_needs_a_slab() {
        let seam = IoSeam::new(31, None, None, None);
        assert_eq!(
            seam.allocate_multishot_slot(0),
            MultishotAlloc::Unsupported,
            "a seam with no multishot slab reports Unsupported",
        );
        let key = MultishotSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 31,
        };
        assert_eq!(
            seam.multishot_next(key),
            MultishotNext::Ended,
            "a seam with no multishot slab resolves the stream as ended",
        );
        // free with no slab is a no-op, never a panic.
        seam.multishot_free(key);
    }

    #[test]
    fn recv_multishot_seam_allocates_and_decodes_sentinel() {
        let Ok(mut slab) = RecvMultishotSlab::new(41, 4) else {
            panic!("the registry mmap must succeed");
        };
        let seam = IoSeam::new(41, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        let RecvMultishotAlloc::Allocated { key, sentinel } =
            seam.allocate_recv_multishot_slot(0xABCD)
        else {
            panic!("a seam carrying a recv slab allocates a slot");
        };
        assert_eq!(key.worker_id, 41);
        assert!(
            is_recv_multishot_sentinel(sentinel),
            "the SQE user_data is a recv-multishot sentinel",
        );
        assert_eq!(
            multishot_sentinel_slot(sentinel),
            key.slot,
            "the sentinel names the allocated slot",
        );
        assert_eq!(
            multishot_sentinel_generation(sentinel),
            (key.generation & 0xFFFF) as u16,
            "the sentinel carries the slot generation",
        );
    }

    #[test]
    fn recv_multishot_seam_next_is_pending_while_armed() {
        let Ok(mut slab) = RecvMultishotSlab::new(42, 4) else {
            panic!("the registry mmap must succeed");
        };
        let seam = IoSeam::new(42, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        let RecvMultishotAlloc::Allocated { key, .. } = seam.allocate_recv_multishot_slot(0x9)
        else {
            panic!("a seam carrying a recv slab allocates a slot");
        };
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Pending,
            "a freshly armed slot with an empty FIFO polls Pending",
        );
    }

    #[test]
    fn recv_multishot_seam_drains_then_ends_and_frees() {
        // Queue a terminal completion before the seam forms its pointer, so the
        // only slab reference the seam holds is the last-derived one.
        let Ok(mut slab) = RecvMultishotSlab::new(44, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x5) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        slab.push(key.slot, gen_low16, 7, 3, false);
        let seam = IoSeam::new(44, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Item {
                result: 7,
                buf_id: Some(3),
            },
            "the queued result drains first, carrying its buffer id",
        );
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Ended,
            "an empty terminated FIFO ends the stream",
        );
        let RecvMultishotAlloc::Allocated { key: reused, .. } =
            seam.allocate_recv_multishot_slot(0x5)
        else {
            panic!("ending the stream freed the slot for reuse");
        };
        assert_eq!(reused.slot, key.slot);
        assert_eq!(
            reused.generation,
            key.generation + 1,
            "the freed slot is reused with a bumped generation",
        );
    }

    #[test]
    fn recv_multishot_seam_item_without_buffer_reports_none() {
        let Ok(mut slab) = RecvMultishotSlab::new(45, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x5) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // An end-of-stream (zero-length) completion carries no provided buffer.
        slab.push(key.slot, gen_low16, 0, NO_BUFFER, false);
        let seam = IoSeam::new(45, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Item {
                result: 0,
                buf_id: None,
            },
            "a NO_BUFFER entry reports buf_id None",
        );
    }

    #[test]
    fn recv_multishot_seam_surfaces_error_end_behind_a_full_fifo() {
        use crate::buffer::multishot::MULTISHOT_FIFO_DEPTH;

        // #230: a consumer far enough behind to fill the FIFO must still see the
        // terminal error, not a bare `Ended` that reads like a clean close.
        let Ok(mut slab) = RecvMultishotSlab::new(48, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x5) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            slab.push(key.slot, gen_low16, value, 0, true);
        }
        // The terminal error arrives on a saturated FIFO.
        slab.push(key.slot, gen_low16, -104, NO_BUFFER, false);
        let seam = IoSeam::new(48, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(
                seam.recv_multishot_next(key),
                RecvMultishotNext::Item {
                    result: value,
                    buf_id: Some(0),
                },
            );
        }
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Item {
                result: -104,
                buf_id: None,
            },
            "the terminal error is delivered as the final item, not swallowed",
        );
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Ended,
            "the stream ends only after the terminal error is handed back",
        );
    }

    #[test]
    fn recv_multishot_seam_free_returns_the_slot() {
        let Ok(mut slab) = RecvMultishotSlab::new(46, 4) else {
            panic!("the registry mmap must succeed");
        };
        let seam = IoSeam::new(46, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        let RecvMultishotAlloc::Allocated { key, .. } = seam.allocate_recv_multishot_slot(0) else {
            panic!("a seam carrying a recv slab allocates a slot");
        };
        seam.recv_multishot_free(key);
        let RecvMultishotAlloc::Allocated { key: reused, .. } =
            seam.allocate_recv_multishot_slot(0)
        else {
            panic!("freeing the slot returns it for reuse");
        };
        assert_eq!(reused.slot, key.slot);
        assert_eq!(
            reused.generation,
            key.generation + 1,
            "free bumps the generation so a stale handle is rejected",
        );
    }

    #[test]
    fn recv_multishot_seam_reports_full_when_saturated() {
        let Ok(mut slab) = RecvMultishotSlab::new(47, 2) else {
            panic!("the registry mmap must succeed");
        };
        let seam = IoSeam::new(47, None, None, None)
            .with_recv_multishot_slab(Some(NonNull::from(&mut slab)));
        assert!(matches!(
            seam.allocate_recv_multishot_slot(0x1),
            RecvMultishotAlloc::Allocated { .. }
        ));
        assert!(matches!(
            seam.allocate_recv_multishot_slot(0x2),
            RecvMultishotAlloc::Allocated { .. }
        ));
        assert_eq!(
            seam.allocate_recv_multishot_slot(0x3),
            RecvMultishotAlloc::Full,
            "a saturated registry reports Full so the stream degrades to single-shot",
        );
    }

    #[test]
    fn recv_multishot_seam_needs_a_slab() {
        let seam = IoSeam::new(51, None, None, None);
        assert_eq!(
            seam.allocate_recv_multishot_slot(0),
            RecvMultishotAlloc::Unsupported,
            "a seam with no recv slab reports Unsupported",
        );
        let key = RecvMultishotSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 51,
        };
        assert_eq!(
            seam.recv_multishot_next(key),
            RecvMultishotNext::Ended,
            "a seam with no recv slab resolves the stream as ended",
        );
        // free with no slab is a no-op, never a panic.
        seam.recv_multishot_free(key);
    }
}
