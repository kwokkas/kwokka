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
    buffer::inflight::{InflightBufSlab, InflightSlotKey},
    operation::{IoBuf, IoBufMut, IoRequest, SubmitResult, SubmitToken},
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
            wake_slot,
            submitted: AtomicU16::new(0),
        }
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
/// Sized to one slot per possible in-flight buffered op, so a worker can queue
/// a cancel for every occupied slot in the same tick. A power of two, as
/// [`CancelInbox`] requires.
pub const CANCEL_INBOX_CAPACITY: usize = crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize;

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
/// `N` must be a power of two. At [`CANCEL_INBOX_CAPACITY`] there is one slot
/// per in-flight op, so overflow is a safety backstop rather than a
/// steady-state case: a full ring drops the cancel, a bounded leak, and the
/// op's own completion still reclaims the slot, so no byte storage leaks
/// permanently.
pub struct CancelInbox<const N: usize> {
    /// Pending cancels, oldest at `head`. `InflightSlotKey` is `Copy`, so a
    /// dropped entry leaks only the cancel request, never owned storage.
    slots: [Option<InflightSlotKey>; N],
    head: usize,
    tail: usize,
}

impl<const N: usize> CancelInbox<N> {
    /// Creates an empty cancel inbox.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is not a power of two or is zero.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            tail: 0,
        }
    }

    /// Queues a cancel for a dropped buffered future's in-flight op.
    ///
    /// A full ring drops the cancel -- a bounded leak: the op's own completion
    /// still reclaims the slot, so no byte storage leaks. The caller does not
    /// retry; the original CQE frees the slot either way.
    pub const fn push_cancel(&mut self, key: InflightSlotKey) {
        if self.tail.wrapping_sub(self.head) >= N {
            return;
        }
        self.slots[self.tail & (N - 1)] = Some(key);
        self.tail = self.tail.wrapping_add(1);
    }

    /// Pops the oldest pending cancel, or `None` when the inbox is empty.
    pub const fn pop(&mut self) -> Option<InflightSlotKey> {
        if self.head == self.tail {
            return None;
        }
        let key = self.slots[self.head & (N - 1)].take();
        self.head = self.head.wrapping_add(1);
        key
    }

    /// Number of pending cancels.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    /// `true` when no cancels are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<const N: usize> Default for CancelInbox<N> {
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

/// `user_data` marker for a buffered-op cancel completion.
///
/// Tags every cancel SQE the worker's cancel-drain submits so the completion
/// drain recognizes the cancel op's own CQE and ignores it. The slot is freed
/// on the original op's completion (see [`reclaim_dropped_slot`]), never on the
/// cancel CQE: `io_uring` async cancel is best-effort, so the target op can
/// still be in flight when the cancel completes. Bit 63 is set, which a slab-path
/// task handle never sets (its tag bit is clear), and the upper 32 bits read
/// exactly `0x8000_0000`, distinct from the wake fd's `u64::MAX`. The low bits
/// carry the slot for traceability only.
///
/// Like the wake sentinel, this assumes the slab-only runtime: an arena-path
/// task that submitted I/O could set bit 63 too, so revisit before arena tasks
/// reach the seam.
const CANCEL_TOKEN_BASE: u64 = 1 << 63;

/// Upper-32-bit mask isolating the [`CANCEL_TOKEN_BASE`] marker.
const CANCEL_TOKEN_HIGH_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// Encodes the cancel-completion `user_data` for `key`: the marker plus the
/// slot at bits 0..16, for traceability only. The payload is never read back to
/// drive a free (the original op's completion does that by `op_token`), so no
/// generation is encoded.
const fn encode_cancel_sentinel(key: InflightSlotKey) -> u64 {
    CANCEL_TOKEN_BASE | key.slot as u64
}

/// Whether `user_data` is a cancel-completion sentinel.
///
/// The completion drain calls this to recognize the cancel op's own CQE, which
/// it ignores: the slot is reclaimed on the original op's completion (see
/// [`reclaim_dropped_slot`]), not here.
pub const fn is_cancel_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == CANCEL_TOKEN_BASE
}

/// Submits a cancel for a dropped buffered future's in-flight op and marks its
/// slot retire-pending.
///
/// The worker's cancel-drain calls this for each [`InflightSlotKey`] popped
/// from the cancel inbox. It marks the slot retire-pending, then submits an
/// `ASYNC_CANCEL` SQE only to hurry the op toward completion; the cancel is
/// best-effort and its own CQE is ignored. The slot is freed when the original
/// op posts its completion (see [`reclaim_dropped_slot`]), which is the
/// kernel's signal it is done with the bytes.
///
/// A refused submit (a full ring, a backend without cancel) leaves the slot
/// retire-pending; the original op still completes and reclaims the slot, so at
/// worst its bytes wait for that completion, never freed under an in-flight
/// kernel write.
pub fn submit_cancel(driver: &DriverType, slab: &mut InflightBufSlab, key: InflightSlotKey) {
    slab.mark_retire_pending(key);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal returns a best-effort SubmitResult; a refused
    // cancel leaves the slot retire-pending as a bounded leak reclaimed at
    // worker teardown, never a use-after-free.
    let _ = driver.submit_internal(request);
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
        let sentinel = CANCEL_TOKEN_BASE | (0xAB << 16) | 0x05;
        assert!(is_cancel_sentinel(sentinel), "the marker is recognized");
    }

    #[test]
    fn sentinel_carries_slot() {
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
            "the slot sits in the low 16 bits, for traceability",
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
}
