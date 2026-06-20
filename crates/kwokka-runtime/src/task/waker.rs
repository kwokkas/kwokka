//! Turns a [`TaskRef`] into a [`Waker`] without allocation by encoding
//! the 64-bit handle through the `RawWaker` data pointer slot.
//!
//! Strict-provenance: the data pointer is built with
//! [`core::ptr::without_provenance`] so the integer round-trip is sound on
//! targets that distinguish address-only and provenance-carrying pointers.
//!
//! Wake callbacks route through `schedule_wake` into the owning worker's
//! wake inbox in the worker registry. The worker drains that inbox into
//! its run queue each tick. The drain itself lands with the bootstrap
//! run-loop.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) satisfies unreachable_pub on this private module"
)]

use core::{
    ptr,
    task::{RawWaker, RawWakerVTable, Waker},
};

use kwokka_io::boundary::{WakerBinding, WakerDecoder};

use crate::task::TaskRef;

/// Requires a 64-bit target so `TaskRef` fits in `*const ()`.
const _: () = assert!(
    usize::BITS == 64,
    "kwokka-runtime requires a 64-bit target so TaskRef fits in *const ()",
);

/// Builds a [`Waker`] whose `RawWaker::data` encodes the given [`TaskRef`].
///
/// The returned waker is `Clone + Send + Sync` per stdlib contract; clones
/// are zero-cost integer copies.
pub(crate) fn waker_from_task_ref(task_ref: TaskRef) -> Waker {
    let data = task_ref_to_data(task_ref);
    // SAFETY: `VTABLE` is a process-wide `'static` table and `data` is
    // built by `task_ref_to_data` to encode a valid `TaskRef`. The data
    // pointer is provenance-free (`TaskRef` is Copy) so clone is a bitwise
    // copy and drop is a no-op -- no allocation lifetime to manage. Violation
    // would produce a waker with a corrupt data pointer, causing incorrect
    // task routing on wake.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

/// Converts a [`TaskRef`] into the integer-shaped data pointer.
#[inline]
#[allow(
    clippy::cast_possible_truncation,
    reason = "module-level static assert pins usize::BITS == 64, so u64 -> usize is lossless"
)]
const fn task_ref_to_data(task_ref: TaskRef) -> *const () {
    let bits = task_ref.raw() as usize;
    ptr::without_provenance(bits)
}

/// Recovers a [`TaskRef`] from a waker's `data` pointer.
///
/// `pub(crate)` so [`scope`](crate::task::scope) can decode the polling task's
/// `TaskRef` from `cx.waker()`. Sound only when the waker's vtable is [`VTABLE`]
/// (the caller verifies that first); a foreign data pointer decodes to a garbage
/// `TaskRef`.
#[inline]
pub(crate) fn data_to_task_ref(data: *const ()) -> TaskRef {
    let bits = data.addr() as u64;
    TaskRef::from_raw(bits)
}

/// Process-wide vtable shared by every kwokka task waker.
///
/// `pub(crate)` so [`scope`](crate::task::scope) can compare a waker's vtable
/// pointer against this address: a different vtable means the waker did not come
/// from [`waker_from_task_ref`], so its data is not a decodable [`TaskRef`].
pub(crate) static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

/// Clone callback: integer copy of the data pointer plus the same vtable.
unsafe fn clone_fn(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

/// Consume-by-value wake. Routes to the owning worker's run queue.
unsafe fn wake_fn(data: *const ()) {
    let task_ref = data_to_task_ref(data);
    schedule_wake(task_ref);
}

/// Wake by reference. Same routing as [`wake_fn`].
unsafe fn wake_by_ref_fn(data: *const ()) {
    let task_ref = data_to_task_ref(data);
    schedule_wake(task_ref);
}

/// Enqueues the task for re-polling on the owning worker.
///
/// Routes the handle into the owning worker's wake inbox via the worker
/// registry; the worker drains it into the run queue on its next tick. A
/// full inbox drops the wake. A delivered wake signals the target's
/// endpoint, which unparks the worker only when it is parked.
#[inline]
fn schedule_wake(task_ref: TaskRef) {
    if crate::worker::registry::enqueue(task_ref).is_err() {
        // A full wake inbox drops the wake (lost wakeup). Unreachable in
        // steady state while the inbox capacity exceeds the worker task
        // count; backpressure and lens visibility are deferred to 0.2.0.
        return;
    }
    crate::worker::registry::signal(task_ref.worker_id());
}

/// Drop callback: no-op because [`TaskRef`] is `Copy` and the data pointer
/// is just an integer.
const unsafe fn drop_fn(_data: *const ()) {}

/// Decodes a runtime task waker into the seam's worker/token binding.
///
/// Returns `None` for any waker not built by [`waker_from_task_ref`]: a
/// foreign vtable means the data pointer is not a [`TaskRef`] encoding. The
/// vtable check and the decode stay on this side of the crate boundary, so
/// `TaskRef` and [`VTABLE`] remain runtime-private.
fn decode_for_seam(waker: &Waker) -> Option<WakerBinding> {
    if !ptr::eq(ptr::from_ref(waker.vtable()), ptr::from_ref(&VTABLE)) {
        return None;
    }
    let task_ref = data_to_task_ref(waker.data());
    Some(WakerBinding {
        token: task_ref.raw(),
        worker_id: task_ref.worker_id(),
    })
}

/// Registers the runtime waker decoder with the I/O seam.
///
/// Idempotent: the seam keeps the first registration, and every runtime in
/// the process registers this same translation, so repeated calls (one per
/// worker shard) are no-ops.
pub(crate) fn register_seam_decoder() {
    static DECODER: WakerDecoder = decode_for_seam;
    kwokka_io::boundary::register_decoder(&DECODER);
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use kwokka_core::Generation;

    fn fake_ref(worker_id: u8, generation: u32, index: u32) -> TaskRef {
        TaskRef::from_arena(worker_id, index, Generation::from_raw(generation))
    }

    #[test]
    fn waker_from_task_ref_round_trips_through_data_pointer() {
        let original = fake_ref(7, 42, 0xDEAD_BEEF);
        let waker = waker_from_task_ref(original);
        let recovered = data_to_task_ref(waker.data());
        assert_eq!(recovered, original);
    }

    #[test]
    fn cloned_waker_preserves_task_ref() {
        let original = fake_ref(3, 1, 99);
        let waker = waker_from_task_ref(original);
        // CLONE: Waker::clone exercises the vtable clone_fn callback.
        let cloned = waker.clone();
        assert!(waker.will_wake(&cloned));
        let recovered = data_to_task_ref(cloned.data());
        assert_eq!(recovered, original);
    }

    #[test]
    fn waker_will_wake_matches_for_independent_constructions() {
        let task_ref = fake_ref(0, 1, 1);
        let first = waker_from_task_ref(task_ref);
        let second = waker_from_task_ref(task_ref);
        assert!(first.will_wake(&second));
    }

    #[test]
    fn waker_will_wake_distinguishes_distinct_refs() {
        let first = waker_from_task_ref(fake_ref(0, 1, 1));
        let second = waker_from_task_ref(fake_ref(0, 1, 2));
        assert!(!first.will_wake(&second));
    }

    #[test]
    fn drop_fn_is_a_noop() {
        let _ = waker_from_task_ref(fake_ref(0, 1, 1));
    }

    #[test]
    fn wake_does_not_panic_on_non_worker_thread() {
        let waker = waker_from_task_ref(fake_ref(5, 1, 1));
        waker.wake();
    }

    #[test]
    fn wake_by_ref_does_not_panic_on_non_worker_thread() {
        let waker = waker_from_task_ref(fake_ref(6, 1, 1));
        waker.wake_by_ref();
    }

    #[test]
    fn wake_routes_task_ref_to_owning_worker_inbox() {
        let task = fake_ref(7, 1, 1);
        let waker = waker_from_task_ref(task);
        waker.wake();
        let Ok(worker_id) = crate::worker::WorkerId::new(7) else {
            panic!("worker id within range");
        };
        assert_eq!(crate::worker::registry::pop(worker_id), Some(task));
    }
}
