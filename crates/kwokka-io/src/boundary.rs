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
    operation::{IoBuf, IoBufMut, IoRequest, SubmitResult},
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
        wake_slot: Option<WakeSlot>,
    ) -> Self {
        Self {
            worker_id,
            driver,
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

    #[test]
    fn with_current_is_none_when_uninstalled() {
        assert_eq!(IoSeam::with_current(200, IoSeam::worker_id), None);
    }

    #[test]
    fn guard_brackets_install_and_clear() {
        let seam = IoSeam::new(201, None, None);
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
        let seam = IoSeam::new(202, Some(NonNull::from(&mut driver)), None);
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
        let seam = IoSeam::new(203, Some(NonNull::from(&mut driver)), None);
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
}
