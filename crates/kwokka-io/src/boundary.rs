//! Cross-crate I/O seam -- the boundary that lets sibling crates host
//! completion futures.
//!
//! The runtime installs an [`IoSeam`] for the exact window of each task poll
//! (mirroring its poll-frame discipline) and registers a [`WakerDecoder`] once
//! at startup. An I/O future living outside the runtime crate reaches its
//! worker in three steps: decode the polling task's binding from the waker via
//! [`decode_waker`], submit through [`IoSeam::with_current`] keyed by the
//! decoded worker id, and read the completion result back with
//! [`IoSeam::completion_result`] on a later poll.
//!
//! No scheduler type crosses the boundary: the binding is a `u64` token (the
//! request's `user_data` round-trip key) plus a `u8` worker id. Every op
//! submitted through the seam lands on the same per-poll count the runtime's
//! own submit paths use, so in-flight accounting -- the predicate that pins a
//! task to the worker whose ring holds its op -- is preserved by construction.
//!
//! The poll boundary is internal infrastructure for the kwokka workspace crates; it is
//! not re-exported by the `kwokka` facade and carries no stability promise.

use core::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicU16, Ordering},
    task::Waker,
};
use std::os::fd::{FromRawFd, OwnedFd};

use crate::{
    DriverType, IoDriver,
    buffer::{
        inflight::{InflightBufSlab, InflightSlotKey},
        multishot::{MultishotPush, MultishotSlab, MultishotSlotKey},
    },
    operation::{CqeFlags, IoBuf, IoBufMut, IoRequest, SubmitResult, SubmitToken},
};

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

/// The registered waker decoder, or null before the runtime registers one.
static WAKER_DECODER: AtomicPtr<WakerDecoder> = AtomicPtr::new(ptr::null_mut());

/// The task-side binding an I/O future needs from its waker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakerBinding {
    /// Raw task identity token, embedded as the request's `user_data` so the
    /// completion drain routes the CQE back to the submitting task.
    pub token: u64,
    /// Worker id the task is resident on -- the [`IoSeam::with_current`] key.
    pub worker_id: u8,
}

/// Decoder the runtime registers to translate its task wakers for the seam.
///
/// Returns `None` for a waker the runtime did not build (a combinator
/// wrapper, a noop waker), in which case the future must not submit.
pub type WakerDecoder = fn(&Waker) -> Option<WakerBinding>;

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

/// Adopts a nonnegative accept-completion result as an owned descriptor.
///
/// Returns `None` for a negative result -- an `-errno`, not a descriptor.
///
/// Call this only on the result of an accept-class completion. A
/// nonnegative accept result names a descriptor the kernel just created
/// for this process, with no other owner. Adopting any other integer
/// asserts ownership of a descriptor this process may not own, and the
/// returned handle closes it on drop -- an IO-safety violation
/// (incorrect close), not a memory-safety violation.
pub fn adopt_accepted_fd(result: i32) -> Option<OwnedFd> {
    if result < 0 {
        return None;
    }
    // SAFETY: Invariant -- a nonnegative accept-class CQE result is a
    // freshly created descriptor the kernel handed to this process, with
    // exactly one owner: the adopter. Precondition: the caller passes an
    // accept-completion result per the documented contract above; the sign
    // check excludes errno results. Failure mode: adopting a value that is
    // not an accept result claims a descriptor owned elsewhere -- it closes
    // on drop and use-after-close races follow. This is an IO-safety
    // concern (incorrect close), not a memory-safety concern: no pointer
    // dereference occurs.
    Some(unsafe { OwnedFd::from_raw_fd(result) })
}

/// Registers the runtime's waker decoder, keeping the first registration.
///
/// Called by the runtime before any task polls. Idempotent: a runtime rebuilt
/// in the same process re-registers the same translation, so first-wins is
/// the correct merge.
pub fn register_decoder(decoder: &'static WakerDecoder) {
    let new = ptr::from_ref(decoder).cast_mut();
    // IGNORE: a lost exchange means a decoder is already registered, which is
    // the desired end state -- first registration wins by design.
    let _ =
        WAKER_DECODER.compare_exchange(ptr::null_mut(), new, Ordering::AcqRel, Ordering::Acquire);
}

/// Decodes the polling task's seam binding from its waker.
///
/// Returns `None` before the runtime registered a decoder, or when the waker
/// did not come from the runtime -- a future must treat both as "do not
/// submit".
pub fn decode_waker(waker: &Waker) -> Option<WakerBinding> {
    let decoder = WAKER_DECODER.load(Ordering::Acquire);
    if decoder.is_null() {
        return None;
    }
    // SAFETY: Invariant -- a non-null pointer in `WAKER_DECODER` was stored by
    // `register_decoder` from a `&'static WakerDecoder`, so the referent is a
    // live static function pointer for the rest of the process.
    // Precondition: `register_decoder` is the only writer and its signature
    // admits only `'static` references.
    // Failure mode: a non-static store would leave a dangling referent; the
    // signature makes that unrepresentable in safe code.
    let decoder = unsafe { *decoder };
    decoder(waker)
}

/// The per-worker submit/complete surface an I/O future polls through.
///
/// Lives on the runtime worker's stack for exactly one poll window, installed
/// and cleared by [`SeamGuard`]. Futures reach it with
/// [`IoSeam::with_current`] inside their own `poll` and never hold it across
/// an `.await`.
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
    /// stream allocates and drains a slot here through [`IoSeam::allocate_multishot_slot`]
    /// and [`IoSeam::multishot_next`].
    multishot_slab: Option<NonNull<MultishotSlab>>,
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

    /// Allocates an in-flight buffer slot for a buffered op on the polling task.
    ///
    /// Returns the slot handle paired with a writable pointer to its
    /// `INFLIGHT_BUF_STRIDE` bytes -- the future builds an `InlineBuf` over the
    /// pointer to submit and, for a write-class op, copies its input into the
    /// slot first. Returns
    /// `None` when the seam carries no slab (a test seam with no backend) or the
    /// registry is full. The returned [`InflightSlotKey`] is `Copy`; the future
    /// holds it for the op lifetime and hands it to [`push_cancel_for_worker`]
    /// if it drops before the completion arrives.
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

    /// Reserves a multishot slot for the polling task.
    ///
    /// `owner_token` is the polling task, woken on each completion. On
    /// [`MultishotAlloc::Allocated`] the stream tags its multishot SQE with the
    /// returned sentinel, drains the slot's FIFO on later polls via
    /// [`IoSeam::multishot_next`], and hands the key to
    /// [`push_multishot_cancel_for_worker`] if it drops before the op terminates.
    /// Returns [`MultishotAlloc::Full`] when every slot is occupied, so the
    /// stream can degrade to single-shot, and [`MultishotAlloc::Unsupported`]
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

/// Per-worker cancel-inbox capacity.
///
/// Sized to hold one cancel per droppable op across both per-worker registries
/// -- the buffered-op inflight slab and the multishot slab -- so a worker can
/// queue a cancel for every occupied slot in the same tick and never drop one.
/// No op can drop twice before a drain (its slot stays occupied until the drain
/// reclaims it), so the sum of the two capacities bounds every pending cancel.
pub const CANCEL_INBOX_CAPACITY: usize = crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize
    + crate::buffer::multishot::DEFAULT_MULTISHOT_CAP as usize;

/// Fixed-capacity ring of pending cancels for dropped buffered futures.
///
/// A buffered future whose op is still in flight cannot free its bytes on
/// drop -- the kernel still holds the pointer. It instead pushes its
/// [`InflightSlotKey`] here; the owning worker drains the ring each tick,
/// submits a cancel SQE, and marks the slot retire-pending, and the completion
/// drain frees the slot once the kernel signals the op is done.
///
/// The caller keeps an in-flight buffered op pinned to its worker, so every
/// push runs on the owning worker thread. The ring is therefore single-writer
/// and needs no atomics.
///
/// At [`CANCEL_INBOX_CAPACITY`] there is one slot per op that can drop between
/// drains -- across the inflight and multishot slabs -- so overflow is a safety
/// backstop rather than a steady-state case: a full ring drops the cancel, a
/// bounded leak, and the op's own completion still reclaims the slot, so no byte
/// storage leaks permanently.
pub struct CancelInbox<const N: usize> {
    /// Pending cancels, oldest at `head`. `InflightSlotKey` is `Copy`, so a
    /// dropped entry leaks only the cancel request, never owned storage. A
    /// multishot cancel rides the same key with its `op_token` set to the
    /// multishot sentinel, which the drain routes to the multishot registry.
    slots: [Option<InflightSlotKey>; N],
    /// Ring read cursor, always in `[0, N)`.
    head: usize,
    /// Count of queued cancels; `(head + len) % N` is the next write slot.
    len: usize,
}

impl<const N: usize> CancelInbox<N> {
    /// Creates an empty cancel inbox.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is zero.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(N > 0, "N must be positive");
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            len: 0,
        }
    }

    /// Queues a cancel for a dropped buffered future's in-flight op.
    ///
    /// A full ring drops the cancel -- a bounded leak: the op's own completion
    /// still reclaims the slot, so no byte storage leaks. The caller does not
    /// retry; the original CQE frees the slot either way. At
    /// [`CANCEL_INBOX_CAPACITY`] the ring holds every op that can drop between
    /// drains, so the full case is a backstop, not a steady state.
    pub const fn push_cancel(&mut self, key: InflightSlotKey) {
        if self.len >= N {
            return;
        }
        self.slots[(self.head + self.len) % N] = Some(key);
        self.len += 1;
    }

    /// Pops the oldest pending cancel, or `None` when the inbox is empty.
    pub const fn pop(&mut self) -> Option<InflightSlotKey> {
        if self.len == 0 {
            return None;
        }
        let key = self.slots[self.head].take();
        self.head = (self.head + 1) % N;
        self.len -= 1;
        key
    }

    /// Number of pending cancels.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// `true` when no cancels are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for CancelInbox<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-worker capacity for pending single-shot accept cancels.
///
/// Holds a token from a dropped `accept()` between its cancel submission and the
/// op's completion. The window is short (usually one drain), so a small ring
/// suffices; a full ring drops the record, a bounded leak of one descriptor.
pub const ACCEPT_CANCEL_CAPACITY: usize = 32;

/// [`InflightSlotKey`] `slot` marker for a slotless single-shot accept cancel.
///
/// A single-shot accept carries no inflight slab slot -- it submits under the
/// polling task's token. This reserved slot routes its cancel to
/// [`submit_accept_cancel`] rather than the buffered-op path; no real slab slot
/// reaches `u16::MAX` (the inflight cap is far smaller).
const ACCEPT_CANCEL_SLOT: u16 = u16::MAX;

/// Per-worker set of dropped single-shot accepts awaiting their completion.
///
/// A dropped `accept()` cancels its op and records the op's token here; the
/// completion drain closes the accepted fd if the op still produced one, rather
/// than orphaning it in the task wake slot.
pub struct AcceptCancelSet<const N: usize> {
    /// Pending tokens packed in `[0, len)`; order does not matter.
    tokens: [u64; N],
    /// Count of pending tokens.
    len: usize,
}

impl<const N: usize> AcceptCancelSet<N> {
    /// Creates an empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tokens: [0; N],
            len: 0,
        }
    }

    /// Records `token` as a cancelled accept awaiting disposal.
    ///
    /// A full set drops the record: the op's fd is a bounded leak, never
    /// corruption, and the caller does not retry.
    pub(crate) const fn insert(&mut self, token: u64) {
        if self.len < N {
            self.tokens[self.len] = token;
            self.len += 1;
        }
    }

    /// Removes `token` if pending, reporting whether it was.
    pub(crate) const fn take(&mut self, token: u64) -> bool {
        let mut index = 0;
        while index < self.len {
            if self.tokens[index] == token {
                self.tokens[index] = self.tokens[self.len - 1];
                self.len -= 1;
                return true;
            }
            index += 1;
        }
        false
    }

    /// `true` when no cancelled accept is pending.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for AcceptCancelSet<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// One cancel-inbox slot per possible worker id byte, like [`SEAM_SLOTS`].
const CANCEL_INBOX_SLOTS: usize = u8::MAX as usize + 1;

/// The installed cancel inbox for each worker, or null outside a run-loop,
/// indexed by worker id.
///
/// Unlike [`SEAMS`], which is poll-window scoped, this is installed for the
/// worker's whole run-loop: a buffered future's `Drop` runs outside the poll
/// window (task reap, or an early cancel-drop) yet still on the owning worker
/// thread, so the cancel must reach the inbox without the poll bracket.
/// `AtomicPtr<CancelInbox>` is `Sync` regardless of `CancelInbox`, so the array
/// is a sound `static` with no `unsafe impl`.
static CANCEL_INBOXES: [AtomicPtr<CancelInbox<CANCEL_INBOX_CAPACITY>>; CANCEL_INBOX_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; CANCEL_INBOX_SLOTS];

/// RAII bracket that installs a worker's cancel inbox for its whole run-loop
/// and clears it on drop.
///
/// Declared after the `WorkerShard` local in each run-loop entry, so Rust LIFO
/// drop clears the static before the shard -- and its `cancel_inbox` field --
/// is reclaimed. A buffered future dropped during shard teardown then finds a
/// null slot and its [`push_cancel_for_worker`] is a no-op, an accepted bounded
/// leak the same as an overflowed ring.
///
/// Not re-entrant: one run-loop per worker installs one guard.
pub struct CancelInboxGuard {
    /// Worker slot to clear on drop.
    worker_id: u8,
}

impl CancelInboxGuard {
    /// Installs `inbox` for `worker_id` for the run-loop, returning the guard
    /// that clears it.
    ///
    /// Takes `&mut` only to form the pointer; the guard stores no reference, so
    /// the caller's borrow of the inbox ends when this returns and the run-loop
    /// can borrow the owning shard again.
    #[must_use]
    pub fn install(worker_id: u8, inbox: &mut CancelInbox<CANCEL_INBOX_CAPACITY>) -> Self {
        CANCEL_INBOXES[worker_id as usize].store(ptr::from_mut(inbox), Ordering::Release);
        Self { worker_id }
    }
}

impl Drop for CancelInboxGuard {
    fn drop(&mut self) {
        CANCEL_INBOXES[self.worker_id as usize].store(ptr::null_mut(), Ordering::Release);
    }
}

/// Queues a cancel for a dropped buffered future's in-flight op on its worker.
///
/// A no-op when no inbox is installed for `key.worker_id`: the worker's
/// run-loop already tore down, so the op's own completion frees the slot during
/// shutdown, a bounded leak like an overflowed ring.
pub fn push_cancel_for_worker(key: InflightSlotKey) {
    let inbox = CANCEL_INBOXES[key.worker_id as usize].load(Ordering::Acquire);
    if inbox.is_null() {
        return;
    }
    // SAFETY: Invariant -- a non-null pointer in `CANCEL_INBOXES[key.worker_id]`
    // was stored by `CancelInboxGuard::install` over the owning `WorkerShard`'s
    // `cancel_inbox` field. The guard is declared after the shard in the
    // run-loop entry, so Rust LIFO drop nulls this slot before the shard, and
    // its field, is reclaimed; a non-null load therefore names a live field.
    // Precondition (why the single-writer contract holds): a buffered future's
    // `Drop` fires only on the owning worker's thread, because submitting a
    // buffered op sets `header.io_bound = true` in the post-poll fold, which
    // pins the task off the steal path so it never migrates to another worker.
    // Callers must invoke this only from such a future's `Drop`, so the worker
    // that installed the inbox is the only thread that ever pushes -- the inbox
    // needs no atomics. A future that cleared `io_bound`, or a non-buffered
    // future reaching here, would break that contract and must re-establish it.
    // Failure mode: null is the early return above. A cross-thread push (a task
    // with `io_bound = false`) races the single writer; the call-site invariant
    // excludes it. A dangling pointer cannot arise -- LIFO drop order excludes it.
    let inbox = unsafe { &mut *inbox };
    inbox.push_cancel(key);
}

/// Queues a cancel for a dropped single-shot accept on its worker.
///
/// The accept op carries the polling task's `token` as its `user_data` and holds
/// no inflight slab slot, so the queued key uses the `ACCEPT_CANCEL_SLOT`
/// marker; the drain routes it to [`submit_accept_cancel`]. Submitting the accept
/// set the task `io_bound`, so its `Drop` runs on the owning worker and the push
/// is single-writer, the same contract [`push_cancel_for_worker`] holds.
pub fn push_accept_cancel_for_worker(worker_id: u8, token: u64) {
    push_cancel_for_worker(InflightSlotKey {
        slot: ACCEPT_CANCEL_SLOT,
        generation: 0,
        worker_id,
        op_token: token,
    });
}

/// Queues a cancel for a dropped multishot stream's op on its worker.
///
/// The stream's `Drop` calls this. Like a buffered future, a live multishot op
/// is `io_bound`, so the drop runs on the owning worker thread and the push is
/// single-writer. The multishot slot rides an [`InflightSlotKey`] whose
/// `op_token` is the multishot sentinel; the worker's cancel drain routes it to
/// the multishot registry. A no-op when no inbox is installed (a bounded leak at
/// worker teardown, reclaimed by the op's terminal completion).
pub fn push_multishot_cancel_for_worker(key: MultishotSlotKey) {
    push_cancel_for_worker(InflightSlotKey {
        slot: key.slot,
        generation: key.generation,
        worker_id: key.worker_id,
        op_token: encode_multishot_sentinel(key),
    });
}

/// `user_data` marker for a buffered-op cancel completion.
///
/// Tags every cancel SQE the worker's cancel-drain submits so the completion
/// drain recognizes the cancel op's own CQE and routes it to
/// [`reclaim_cancel_completion`]. The slot is usually freed on the original
/// op's completion (see [`reclaim_dropped_slot`]); the cancel CQE frees it only
/// on `-ENOENT`, where the target already completed and no op completion is
/// coming. `io_uring` async cancel is best-effort, so a `0` or `-EALREADY`
/// result leaves the target still completing and never drives a free. The
/// upper 32 bits are all set: the arena tag bit, a worker id of 127, and a
/// maximal generation. That is the arena address space's exhaustion corner,
/// reached only when both the worker id and the generation are maxed out, so a
/// real completion never aliases the marker in practice. The low 32 bits carry
/// the slot and its low 16 generation bits.
///
/// This gives the marker the same narrow window as the wake fd's `u64::MAX`,
/// which sits in that corner at a maximal offset. The two stay disjoint: the
/// marker never encodes an all-ones low half, and [`is_cancel_sentinel`]
/// excludes the wake value. A slab-path handle clears the arena tag bit, so it
/// never aliases either.
const CANCEL_TOKEN_BASE: u64 = 0xFFFF_FFFF_0000_0000;

/// Upper-32-bit mask isolating the [`CANCEL_TOKEN_BASE`] marker.
const CANCEL_TOKEN_HIGH_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// Encodes the cancel-completion `user_data` for `key`: the marker, the slot at
/// bits 0..16, and the low 16 bits of the slot's generation at bits 16..32.
///
/// The slot and generation are read back only on a cancel completion that
/// reports `-ENOENT` (see [`reclaim_cancel_completion`]): the target op already
/// completed, so no op-token completion will free the slot, and the generation
/// guards a stale cancel from freeing a slot the same op token has since reused.
const fn encode_cancel_sentinel(key: InflightSlotKey) -> u64 {
    CANCEL_TOKEN_BASE | ((key.generation & 0xFFFF) << 16) | key.slot as u64
}

/// Whether `user_data` is a cancel-completion sentinel.
///
/// The completion drain calls this to recognize the cancel op's own CQE and
/// route it to [`reclaim_cancel_completion`] instead of the task-wake path. The
/// slot is normally reclaimed on the original op's completion (see
/// [`reclaim_dropped_slot`]); the cancel CQE frees it only on a `-ENOENT`
/// result.
///
/// The marker fills the upper 32 bits, which the wake fd's `u64::MAX` also
/// does, so the wake value is excluded here to keep the two predicates disjoint
/// on their own. The drain tests the wake fd first regardless.
pub const fn is_cancel_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == CANCEL_TOKEN_BASE
        && user_data != crate::wake::WAKE_FD_USER_DATA
}

/// `user_data` marker base for a multishot completion.
///
/// A multishot op posts many CQEs sharing one `user_data`, so its completions
/// route to the [`MultishotSlab`]
/// rather than the per-task wake slot. The upper 32 bits read `0xFFFF_FFFE`:
/// the arena tag bit, worker id 127, and generation `MAX - 1`, one corner below
/// the cancel base. That keeps the three completion sentinels disjoint -- the
/// wake fd is `u64::MAX` and the cancel base is `0xFFFF_FFFF_0000_0000`, both
/// upper-32 `0xFFFF_FFFF`, while this reads `0xFFFF_FFFE`. It is unreachable for
/// the same reason the cancel corner is: generation `MAX - 1` needs ~2^24 slot
/// reuses. The low 32 bits carry the slot and its low 16 generation bits, the
/// same layout [`encode_cancel_sentinel`] uses.
const MULTISHOT_TOKEN_BASE: u64 = 0xFFFF_FFFE_0000_0000;

/// Encodes the multishot-completion `user_data` for `key`.
pub(crate) const fn encode_multishot_sentinel(key: MultishotSlotKey) -> u64 {
    MULTISHOT_TOKEN_BASE | ((key.generation & 0xFFFF) << 16) | key.slot as u64
}

/// Whether `user_data` is a multishot-completion sentinel.
///
/// The completion drain calls this to route the CQE into the
/// [`MultishotSlab`]. The marker shares
/// the upper-32 isolation mask with the cancel sentinel but sits one corner
/// below it, so no wake-value guard is needed: `u64::MAX` reads upper-32
/// `0xFFFF_FFFF`, already excluded.
pub const fn is_multishot_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == MULTISHOT_TOKEN_BASE
}

/// The slot index a multishot sentinel names.
pub(crate) const fn multishot_sentinel_slot(user_data: u64) -> u16 {
    (user_data & 0xFFFF) as u16
}

/// The low 16 generation bits a multishot sentinel carries.
pub(crate) const fn multishot_sentinel_generation(user_data: u64) -> u16 {
    ((user_data >> 16) & 0xFFFF) as u16
}

/// The wake and retire targets a multishot CQE resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultishotCompletion {
    /// The owner task to wake, set when the result was queued for it.
    pub wake: Option<u64>,
    /// The owner task whose one counted SQE retires, set on the terminal
    /// (no-`MORE`) CQE regardless of wake, so the worker's in-flight accounting
    /// pairs with the submit even when the owning stream already dropped.
    pub retire: Option<u64>,
}

/// Routes a multishot op's completion CQE into the worker's registry.
///
/// The completion drain calls this on a CQE whose `user_data` is a multishot
/// sentinel (see [`is_multishot_sentinel`]). It queues the result for the owning
/// stream and returns the [`MultishotCompletion`] targets: [`wake`](MultishotCompletion::wake)
/// names the owner to wake when a result was queued, and [`retire`](MultishotCompletion::retire)
/// names the owner to retire on the terminal (no-`MORE`) CQE so the in-flight
/// count pairs with the submit even when the stream already dropped. Nothing is
/// queued when the slot is stale, the FIFO overflowed, or the stream dropped --
/// a cancel-pending slot, whose intermediate results are discarded and whose
/// terminal CQE frees the slot here.
///
/// A discarded nonnegative accept result is a kernel-created fd; it is closed
/// here so a dropped or overflowed stream does not leak the descriptor.
pub fn push_multishot_completion(
    slab: &mut MultishotSlab,
    user_data: u64,
    result: i32,
    flags: CqeFlags,
) -> MultishotCompletion {
    let slot = multishot_sentinel_slot(user_data);
    let generation = multishot_sentinel_generation(user_data);
    let is_more = flags.contains(CqeFlags::MORE);
    // The one SQE `poll_one` counted retires when the op posts its terminal CQE,
    // live or cancel-pending. Read the owner before any free so a cancel-pending
    // terminal still retires it; a stale slot reads `None` and retires nothing.
    let owner = slab.owner(slot, generation);
    let retire = if is_more { None } else { owner };
    if slab.is_cancel_pending(slot, generation) {
        // The owning stream dropped. Each intermediate CQE (`MORE` set) carries
        // an accepted fd it will never take, so close it; the terminal CQE (the
        // op's `-ECANCELED`, or the cancel op's own status) carries no
        // descriptor and frees the slot.
        if is_more {
            dispose_accept_result(result);
        } else {
            slab.free_by_slot(slot, generation);
        }
        return MultishotCompletion { wake: None, retire };
    }
    let wake = match slab.push(slot, generation, result, is_more) {
        MultishotPush::Queued => owner,
        MultishotPush::Overflowed | MultishotPush::Stale => {
            // Only an intermediate CQE carries a descriptor; a terminal status
            // is not an fd.
            if is_more {
                dispose_accept_result(result);
            }
            None
        }
    };
    MultishotCompletion { wake, retire }
}

/// Closes a nonnegative accept result the owning stream will never observe.
///
/// A negative result is an `-errno`, not a descriptor; [`adopt_accepted_fd`]
/// returns `None` and the drop is a no-op.
fn dispose_accept_result(result: i32) {
    drop(adopt_accepted_fd(result));
}

/// Disposes the descriptor from a cancelled single-shot accept's completion.
///
/// The completion drain calls this on every task-token CQE. When `token` names a
/// dropped accept recorded by [`submit_accept_cancel`], the op still produced a
/// descriptor the caller will never take, so it is closed here (a negative
/// result is an `-errno`, not an fd) and the CQE is consumed. Returns whether it
/// handled the CQE; the common empty-set case is an `O(1)` miss.
pub fn dispose_cancelled_accept<const N: usize>(
    accepts: &mut AcceptCancelSet<N>,
    token: u64,
    result: i32,
) -> bool {
    if accepts.is_empty() || !accepts.take(token) {
        return false;
    }
    dispose_accept_result(result);
    true
}

/// Submits a cancel for a dropped buffered future's in-flight op and marks its
/// slot retire-pending.
///
/// The worker's cancel-drain calls this for each [`InflightSlotKey`] popped
/// from the cancel inbox. It marks the slot retire-pending, then submits an
/// `ASYNC_CANCEL` SQE to hurry the op toward completion. The slot is freed when
/// the original op posts its completion (see [`reclaim_dropped_slot`]), or, if
/// that op already completed before the cancel, on the cancel's own `-ENOENT`
/// completion (see [`reclaim_cancel_completion`]).
///
/// A refused submit (a full ring, a backend without cancel) leaves the slot
/// retire-pending. If the original op completes, its completion reclaims the
/// slot; otherwise this is a bounded leak until worker teardown, never a slot
/// freed under an in-flight kernel write.
pub fn submit_cancel(driver: &DriverType, slab: &mut InflightBufSlab, key: InflightSlotKey) {
    slab.mark_retire_pending(key);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal returns a best-effort SubmitResult; a refused
    // cancel leaves the slot retire-pending as a bounded leak reclaimed at
    // worker teardown, never a use-after-free.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped multishot stream's op and marks its slot
/// cancel-pending.
///
/// Called by the cancel drain for a queued cancel whose `op_token` is a
/// multishot sentinel. It closes any accepts already queued for the gone stream,
/// marks the registry slot cancel-pending, then submits an `ASYNC_CANCEL`
/// targeting the multishot op by its sentinel `user_data`. The op's terminal
/// completion (its `-ECANCELED`, or the cancel op's own status) frees the slot
/// through [`push_multishot_completion`]; intermediate accepts arriving after the
/// mark are closed there, so no descriptor leaks either way.
pub fn submit_multishot_cancel(
    driver: &DriverType,
    slab: &mut MultishotSlab,
    key: InflightSlotKey,
) {
    let slot = MultishotSlotKey {
        slot: key.slot,
        generation: key.generation,
        worker_id: key.worker_id,
    };
    // The dropped stream will never take the accepts already queued in its FIFO;
    // close each one so the descriptor does not leak. A negative result is an
    // -errno, not an fd, and disposes as a no-op.
    while let Some(result) = slab.pop(slot) {
        dispose_accept_result(result);
    }
    slab.mark_cancel_pending(slot);
    let request =
        IoRequest::<()>::cancel(SubmitToken::new(key.op_token)).with_user_data(key.op_token);
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the slot
    // cancel-pending, and the op's own completions still drive the free, so this
    // is a bounded hurry-up, never a leak or a use-after-free.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped single-shot accept.
///
/// The accept op holds no slab slot, so this only submits an `ASYNC_CANCEL`
/// targeting the op by its `user_data` token and records the token in `accepts`.
/// A completion arriving after the cancel disposes the accepted fd through
/// [`dispose_cancelled_accept`] rather than orphaning it in the wake slot; the
/// cancel's own CQE decodes to a slot no registry owns, so
/// [`reclaim_cancel_completion`] treats it as a no-op.
pub fn submit_accept_cancel<const N: usize>(
    driver: &DriverType,
    accepts: &mut AcceptCancelSet<N>,
    key: InflightSlotKey,
) {
    accepts.insert(key.op_token);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the accept
    // running, and its completion still routes through dispose_cancelled_accept.
    let _ = driver.submit_internal(request);
}

/// Routes a queued cancel to the mechanism that owns its op.
///
/// A cancel whose `op_token` is a multishot sentinel targets the multishot
/// registry; the `ACCEPT_CANCEL_SLOT` marker targets a slotless single-shot
/// accept; every other cancel is a buffered op's in-flight slot.
pub fn submit_cancel_for<const N: usize>(
    driver: &DriverType,
    inflight: &mut InflightBufSlab,
    multishot: &mut MultishotSlab,
    accepts: &mut AcceptCancelSet<N>,
    key: InflightSlotKey,
) {
    if is_multishot_sentinel(key.op_token) {
        submit_multishot_cancel(driver, multishot, key);
    } else if key.slot == ACCEPT_CANCEL_SLOT {
        submit_accept_cancel(driver, accepts, key);
    } else {
        submit_cancel(driver, inflight, key);
    }
}

/// Reclaims the retire-pending slot whose op matches `op_token`, if any.
///
/// The completion drain calls this on every task-token CQE. When the owning
/// future has dropped, the slot is retire-pending and its op's completion frees
/// it here -- that CQE is the kernel's done-with-the-bytes signal, for every
/// cancel outcome. When the future is still live no slot is retire-pending for
/// that op, so this is a no-op and the future frees through its own harvest
/// path instead.
pub fn reclaim_dropped_slot(slab: &mut InflightBufSlab, op_token: u64) {
    slab.free_by_op_token(op_token);
}

/// Reclaims a slot on a cancel completion whose target op is already gone.
///
/// The cancel op's own CQE is normally not a free trigger: the original op's
/// completion frees the slot by `op_token` through [`reclaim_dropped_slot`]. The
/// one exception is `-ENOENT`, which means the target op completed and posted
/// its single CQE before the cancel was issued, so no op-token completion is
/// coming for this slot. Only then is the slot freed here, decoded from the
/// sentinel `user_data` and matched on its low 16 generation bits. Every other
/// result (`0` and `-EALREADY`, where the target still has a completion coming,
/// plus `-EINVAL` or any other error) is a no-op, so a slot the kernel may
/// still be writing is never freed early.
pub fn reclaim_cancel_completion(slab: &mut InflightBufSlab, sentinel_user_data: u64, result: i32) {
    // ABI: -ENOENT (errno 2) means the target request could not be located
    // because it completed before the cancel was issued (or an invalid id was
    // used), per io_uring_prep_cancel.3. Its own CQE was already drained, so
    // this cancel completion is the last signal that the slot is free.
    const CANCEL_TARGET_GONE: i32 = -2;
    if result != CANCEL_TARGET_GONE {
        return;
    }
    let slot = (sentinel_user_data & 0xFFFF) as u16;
    let generation_low16 = ((sentinel_user_data >> 16) & 0xFFFF) as u16;
    slab.free_if_retire_pending(slot, generation_low16);
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // The stubs recognize only the noop waker, mirroring how the runtime
    // decoder recognizes only its own vtable.
    fn stub_binding(waker: &Waker) -> Option<WakerBinding> {
        waker.will_wake(Waker::noop()).then_some(WakerBinding {
            token: 7,
            worker_id: 3,
        })
    }

    fn other_binding(waker: &Waker) -> Option<WakerBinding> {
        waker.will_wake(Waker::noop()).then_some(WakerBinding {
            token: 9,
            worker_id: 5,
        })
    }

    static STUB: WakerDecoder = stub_binding;
    static OTHER: WakerDecoder = other_binding;

    #[test]
    fn decoder_registration_is_first_wins() {
        let waker = Waker::noop();
        assert_eq!(
            decode_waker(waker),
            None,
            "no decoder is registered before the runtime starts",
        );
        register_decoder(&STUB);
        let expected = WakerBinding {
            token: 7,
            worker_id: 3,
        };
        assert_eq!(decode_waker(waker), Some(expected));
        register_decoder(&OTHER);
        assert_eq!(
            decode_waker(waker),
            Some(expected),
            "a second registration loses to the first",
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

    fn cancel_key(slot: u16) -> InflightSlotKey {
        InflightSlotKey {
            slot,
            generation: 0,
            worker_id: 3,
            op_token: u64::from(slot),
        }
    }

    #[test]
    fn cancel_push_pop_fifo() {
        let mut inbox = CancelInbox::<4>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        let Some(first) = inbox.pop() else {
            panic!("pop must yield the first cancel");
        };
        assert_eq!(first.slot, 0);
        let Some(second) = inbox.pop() else {
            panic!("pop must yield the second cancel");
        };
        assert_eq!(second.slot, 1);
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_full_inbox_leaks() {
        let mut inbox = CancelInbox::<2>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        inbox.push_cancel(cancel_key(2));
        assert_eq!(
            inbox.len(),
            2,
            "a full inbox drops the overflow cancel as a bounded leak"
        );
        let Some(first) = inbox.pop() else {
            panic!("the queued cancels survive the overflow");
        };
        assert_eq!(
            first.slot, 0,
            "the overflow did not displace a queued cancel"
        );
    }

    #[test]
    fn cancel_inbox_capacity_covers_both_slabs() {
        let droppable = crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize
            + crate::buffer::multishot::DEFAULT_MULTISHOT_CAP as usize;
        assert!(
            CANCEL_INBOX_CAPACITY >= droppable,
            "the inbox holds a cancel for every op that can drop between drains, \
             so no worker cancel is silently dropped in production",
        );
    }

    #[test]
    fn cancel_inbox_wraps_across_capacity() {
        // Fill, drain half, refill: exercises the modulo write past the array end
        // so the ring stays correct without a power-of-two capacity.
        let mut inbox = CancelInbox::<3>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(0));
        inbox.push_cancel(cancel_key(2));
        inbox.push_cancel(cancel_key(3));
        assert_eq!(inbox.len(), 3, "the ring refilled to capacity after wrap");
        assert_eq!(inbox.pop().map(|k| k.slot), Some(1));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(2));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(3));
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_pop_empty_returns_none() {
        let mut inbox = CancelInbox::<2>::new();
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_len_empty_occupancy() {
        let mut inbox = CancelInbox::<4>::new();
        assert!(inbox.is_empty());
        assert_eq!(inbox.len(), 0);
        inbox.push_cancel(cancel_key(0));
        assert_eq!(inbox.len(), 1);
        assert!(!inbox.is_empty());
        assert!(inbox.pop().is_some());
        assert!(inbox.is_empty());
    }

    #[test]
    fn cancel_wrap_around_reuses_slots() {
        let mut inbox = CancelInbox::<2>::new();
        inbox.push_cancel(cancel_key(0));
        assert!(inbox.pop().is_some());
        inbox.push_cancel(cancel_key(1));
        inbox.push_cancel(cancel_key(2));
        let Some(second) = inbox.pop() else {
            panic!("pop must yield after wrap");
        };
        assert_eq!(second.slot, 1);
    }

    #[test]
    fn cancel_default_is_empty() {
        let inbox = CancelInbox::<4>::default();
        assert!(inbox.is_empty());
    }

    #[test]
    fn cancel_guard_routes_then_clears() {
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        {
            let _guard = CancelInboxGuard::install(7, &mut inbox);
            push_cancel_for_worker(InflightSlotKey {
                slot: 1,
                generation: 0,
                worker_id: 7,
                op_token: 0xBEEF,
            });
        }
        // The guard dropped, so the static is null and this push is a no-op.
        push_cancel_for_worker(InflightSlotKey {
            slot: 2,
            generation: 0,
            worker_id: 7,
            op_token: 0,
        });
        let Some(key) = inbox.pop() else {
            panic!("the in-guard push reached the inbox");
        };
        assert_eq!(key.slot, 1);
        assert_eq!(key.op_token, 0xBEEF);
        assert!(inbox.pop().is_none(), "the post-guard push was a no-op");
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
    fn cancel_sentinel_excludes_other_tokens() {
        assert!(
            !is_cancel_sentinel(crate::wake::WAKE_FD_USER_DATA),
            "the wake fd marker is not a cancel sentinel",
        );
        assert!(
            !is_cancel_sentinel(0x7FFF_FFFF_FFFF_FFFF),
            "a slab-path task token keeps its top bit clear",
        );
        assert!(
            !is_cancel_sentinel(0x8000_0000_0000_0005),
            "the previous marker corner (arena worker 0, generation 0) no longer aliases",
        );
        let sentinel = CANCEL_TOKEN_BASE | (0xAB << 16) | 0x05;
        assert!(is_cancel_sentinel(sentinel), "the marker is recognized");
    }

    #[test]
    fn multishot_sentinel_round_trips() {
        let key = MultishotSlotKey {
            slot: 0x2A,
            generation: 0xABCD,
            worker_id: 3,
        };
        let sentinel = encode_multishot_sentinel(key);
        assert!(is_multishot_sentinel(sentinel));
        assert_eq!(multishot_sentinel_slot(sentinel), 0x2A);
        assert_eq!(multishot_sentinel_generation(sentinel), 0xABCD);
    }

    #[test]
    fn multishot_sentinel_excludes_other_markers() {
        assert!(
            !is_multishot_sentinel(CANCEL_TOKEN_BASE),
            "the cancel corner reads upper-32 0xFFFF_FFFF, not 0xFFFF_FFFE",
        );
        assert!(
            !is_multishot_sentinel(crate::wake::WAKE_FD_USER_DATA),
            "the wake fd reads upper-32 0xFFFF_FFFF",
        );
        assert!(
            !is_cancel_sentinel(MULTISHOT_TOKEN_BASE),
            "the multishot corner is not a cancel sentinel",
        );
        assert!(
            !is_multishot_sentinel(0x7FFF_FFFF_FFFF_FFFF),
            "a slab-path task token keeps its top bit clear",
        );
    }

    #[test]
    fn sentinel_carries_slot_and_generation() {
        let key = InflightSlotKey {
            slot: 0x2A,
            generation: 0x1_0007,
            worker_id: 3,
            op_token: 0,
        };
        let sentinel = encode_cancel_sentinel(key);
        assert_eq!(
            sentinel & 0xFFFF,
            u64::from(key.slot),
            "the slot sits at bits 0..16"
        );
        assert_eq!(
            (sentinel >> 16) & 0xFFFF,
            0x0007,
            "the generation low 16 bits sit at bits 16..32",
        );
        assert!(is_cancel_sentinel(sentinel), "the marker is set");
    }

    #[test]
    fn reclaim_frees_dropped_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        // The original op's completion, keyed by op_token, frees the slot.
        reclaim_dropped_slot(&mut slab, 0xAA);
        let Some(next) = slab.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.slot, key.slot,
            "the slot is reused after its op completion reclaims it",
        );
    }

    #[test]
    fn cancel_enoent_frees_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        let sentinel = encode_cancel_sentinel(key);
        // -ENOENT means the target op already completed, so this cancel reclaims.
        reclaim_cancel_completion(&mut slab, sentinel, -2);
        let Some(next) = slab.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.slot, key.slot,
            "a -ENOENT cancel frees the slot for reuse"
        );
    }

    #[test]
    fn cancel_success_leaves_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        let sentinel = encode_cancel_sentinel(key);
        // 0 and -EALREADY mean the target still has a completion coming, so the
        // slot must not be freed here.
        reclaim_cancel_completion(&mut slab, sentinel, 0);
        reclaim_cancel_completion(&mut slab, sentinel, -114);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a still-completing cancel leaves the slot live",
        );
    }

    #[test]
    fn cancel_generation_mismatch_leaves_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        // A stale sentinel carrying a different generation must not free the slot.
        let stale =
            CANCEL_TOKEN_BASE | (((key.generation + 1) & 0xFFFF) << 16) | u64::from(key.slot);
        reclaim_cancel_completion(&mut slab, stale, -2);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a mismatched generation leaves the slot live",
        );
    }

    #[test]
    fn cancel_enoent_ignores_live_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        // Not retire-pending: a live future still owns the slot.
        let sentinel = encode_cancel_sentinel(key);
        reclaim_cancel_completion(&mut slab, sentinel, -2);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a live slot is never freed by a cancel completion",
        );
    }

    #[test]
    fn non_sentinel_routes_to_task() {
        assert!(
            !is_cancel_sentinel(0x1234_5678),
            "a slab-path task token routes to the task path, not slot reclaim",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_marks_retire_pending() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xBEEF) else {
            panic!("allocate must succeed");
        };
        submit_cancel(&driver, &mut slab, key);
        assert!(
            slab.is_retire_pending(key.slot),
            "the slot is marked retire-pending even when the backend refuses the cancel",
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
    fn multishot_completion_queues_and_wakes_owner() {
        let mut slab = MultishotSlab::new(15, 4);
        let Some(key) = slab.allocate(0xF00D) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, 9, CqeFlags::MORE),
            MultishotCompletion {
                wake: Some(0xF00D),
                retire: None,
            },
            "an intermediate completion wakes the owner without retiring the op",
        );
        assert_eq!(slab.pop(key), Some(9), "the result reached the FIFO");
    }

    #[test]
    fn multishot_completion_terminal_retires_live_owner() {
        let mut slab = MultishotSlab::new(23, 4);
        let Some(key) = slab.allocate(0xBEEF) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        // A live terminal CQE queues its result (wake) and retires the one SQE.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -104, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: Some(0xBEEF),
                retire: Some(0xBEEF),
            },
            "a live terminal CQE both wakes and retires the owner",
        );
    }

    #[test]
    fn multishot_completion_cancel_pending_frees_on_terminal() {
        let mut slab = MultishotSlab::new(16, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        slab.mark_cancel_pending(key);
        let sentinel = encode_multishot_sentinel(key);
        // An intermediate CQE for a dropped stream wakes nothing; a negative
        // result carries no fd, so the dispose path closes nothing.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a cancel-pending intermediate wakes and retires nothing",
        );
        assert!(
            slab.is_live(key),
            "the slot survives until its terminal CQE"
        );
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -125, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: None,
                retire: Some(0x1),
            },
            "the terminal CQE retires the owner without waking",
        );
        assert!(
            !slab.is_live(key),
            "the terminal CQE frees the cancel-pending slot",
        );
    }

    #[test]
    fn multishot_completion_stale_sentinel_wakes_nothing() {
        let mut slab = MultishotSlab::new(17, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        slab.free(key);
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a sentinel naming a freed slot routes to nothing",
        );
    }

    #[test]
    fn multishot_completion_overflow_wakes_nothing() {
        use crate::buffer::multishot::MULTISHOT_FIFO_DEPTH;

        let mut slab = MultishotSlab::new(18, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            slab.push(key.slot, gen_low16, value, true);
        }
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "an overflowing completion routes to nothing",
        );
        // The terminal CQE retires the owner even when the full FIFO drops it.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -104, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: None,
                retire: Some(0x1),
            },
            "a terminal CQE still retires the owner when the FIFO is full",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn multishot_cancel_drains_queued_results() {
        let driver = DriverType::Epoll(());
        let mut slab = MultishotSlab::new(19, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // Two accepts sit unconsumed in the FIFO when the stream drops.
        slab.push(key.slot, gen_low16, -7, true);
        slab.push(key.slot, gen_low16, -9, true);
        let cancel = InflightSlotKey {
            slot: key.slot,
            generation: key.generation,
            worker_id: key.worker_id,
            op_token: encode_multishot_sentinel(key),
        };
        submit_multishot_cancel(&driver, &mut slab, cancel);
        assert_eq!(slab.pop(key), None, "the cancel drained the queued results");
        assert!(
            slab.is_cancel_pending(key.slot, gen_low16),
            "the slot is marked cancel-pending",
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn multishot_cancel_closes_queued_accept_fds() {
        use std::os::fd::IntoRawFd;

        let driver = DriverType::Epoll(());
        let mut slab = MultishotSlab::new(20, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // A real owned descriptor stands in for a queued accepted connection.
        let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let fd = listener.into_raw_fd();
        slab.push(key.slot, gen_low16, fd, true);
        let cancel = InflightSlotKey {
            slot: key.slot,
            generation: key.generation,
            worker_id: key.worker_id,
            op_token: encode_multishot_sentinel(key),
        };
        submit_multishot_cancel(&driver, &mut slab, cancel);
        // SAFETY: Invariant -- `fd` was owned via `into_raw_fd` and just disposed
        // by the cancel above, so no live handle aliases it. Precondition -- this
        // is a non-destructive `F_GETFD` probe, closing nothing. Failure mode --
        // none; a probe of a closed fd reports `EBADF`, which is the assertion.
        let still_open = unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1;
        assert!(!still_open, "the cancel closed the queued accepted fd");
    }

    #[test]
    fn accept_cancel_set_tracks_tokens() {
        let mut set = AcceptCancelSet::<4>::new();
        assert!(set.is_empty());
        set.insert(0xAA);
        set.insert(0xBB);
        assert!(!set.is_empty());
        assert!(set.take(0xAA), "a recorded token is pending");
        assert!(!set.take(0xAA), "a taken token is no longer pending");
        assert!(set.take(0xBB));
        assert!(set.is_empty());
    }

    #[test]
    fn accept_cancel_set_full_drops_the_record() {
        let mut set = AcceptCancelSet::<2>::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);
        assert!(set.take(1));
        assert!(set.take(2));
        assert!(!set.take(3), "a full set drops the overflow record");
    }

    #[test]
    fn push_accept_cancel_carries_the_slotless_marker() {
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        {
            let _guard = CancelInboxGuard::install(9, &mut inbox);
            push_accept_cancel_for_worker(9, 0xABCD);
        }
        let Some(key) = inbox.pop() else {
            panic!("the accept cancel reached the inbox");
        };
        assert_eq!(
            key.slot, ACCEPT_CANCEL_SLOT,
            "the slotless marker rides along"
        );
        assert_eq!(key.op_token, 0xABCD);
        assert_eq!(key.worker_id, 9);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_for_routes_the_accept_marker() {
        let driver = DriverType::Epoll(());
        let Ok(mut inflight) = InflightBufSlab::new(7, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let mut multishot = MultishotSlab::new(7, 4);
        let mut accepts = AcceptCancelSet::<4>::new();
        let key = InflightSlotKey {
            slot: ACCEPT_CANCEL_SLOT,
            generation: 0,
            worker_id: 7,
            op_token: 0xF00D,
        };
        submit_cancel_for(&driver, &mut inflight, &mut multishot, &mut accepts, key);
        assert!(
            accepts.take(0xF00D),
            "the accept marker routes the token into the accept set",
        );
    }

    #[test]
    fn dispose_cancelled_accept_consumes_recorded_tokens() {
        let mut accepts = AcceptCancelSet::<4>::new();
        assert!(
            !dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "an empty set disposes nothing",
        );
        accepts.insert(0x1);
        assert!(
            dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "a recorded token is consumed (a negative result closes no fd)",
        );
        assert!(
            !dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "the token is gone after disposal",
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn dispose_cancelled_accept_closes_a_real_fd() {
        use std::os::fd::IntoRawFd;

        let mut accepts = AcceptCancelSet::<4>::new();
        let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let fd = listener.into_raw_fd();
        accepts.insert(0x7);
        assert!(dispose_cancelled_accept(&mut accepts, 0x7, fd));
        // SAFETY: Invariant -- `fd` was owned via `into_raw_fd` and just disposed
        // above, so no live handle aliases it. Precondition -- this is a
        // non-destructive `F_GETFD` probe, closing nothing. Failure mode -- none;
        // a probe of a closed fd reports `EBADF`, which is the assertion.
        let still_open = unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1;
        assert!(!still_open, "the disposal closed the accepted fd");
    }
}
