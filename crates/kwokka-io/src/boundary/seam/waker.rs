//! Decoding the polling task's binding out of its `Waker`.

use core::{
    ptr,
    sync::atomic::{AtomicPtr, Ordering},
    task::Waker,
};
#[cfg(test)]
use core::{
    sync::atomic::AtomicUsize,
    task::{RawWaker, RawWakerVTable},
};

/// The registered waker decoder, or null before the runtime registers one.
static WAKER_DECODER: AtomicPtr<WakerDecoder> = AtomicPtr::new(ptr::null_mut());

/// The task-side binding an I/O future needs from its waker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakerBinding {
    /// Raw task identity token, embedded as the request's `user_data` so the
    /// completion drain routes the CQE back to the submitting task.
    pub token: u64,
    /// Worker id the task is resident on -- the
    /// [`IoSeam::with_current`](crate::boundary::IoSeam::with_current) key.
    pub worker_id: u8,
}

/// Decoder the runtime registers to translate its task wakers for the seam.
///
/// Returns `None` for a waker the runtime did not build (a combinator
/// wrapper, a noop waker), in which case the future must not submit.
pub type WakerDecoder = fn(&Waker) -> Option<WakerBinding>;

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

/// The token every test waker carries.
///
/// The value is arbitrary. A test asserts that a cancellation carries the same
/// token its binding did, never that the token equals some literal.
#[cfg(test)]
pub(crate) const TEST_TOKEN: u64 = 7;

/// The vtable a test waker carries, so [`test_decoder`] can tell one apart from
/// a waker it did not build.
#[cfg(test)]
static TEST_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_test, wake_test, wake_test, drop_test);

#[cfg(test)]
unsafe fn clone_test(data: *const ()) -> RawWaker {
    RawWaker::new(data, &TEST_VTABLE)
}

#[cfg(test)]
const unsafe fn wake_test(_data: *const ()) {}

#[cfg(test)]
const unsafe fn drop_test(_data: *const ()) {}

/// The decoder every test in this crate registers.
#[cfg(test)]
pub(crate) static TEST_DECODER: WakerDecoder = test_decoder;

/// Hands out a worker id no other test in this binary holds.
///
/// Every test that installs into a worker-keyed process-global -- the seam, a
/// cancel inbox, the provided pool -- reserves its id here. Choosing ids by hand
/// is what let two tests on different threads write the same slot and clobber
/// each other.
///
/// # Panics
///
/// Panics once the id space is spent. A `u8` addresses 256 slots and the suite
/// reserves far fewer, so exhaustion means reservations are leaking rather than
/// that the bound is too low. Wrapping would quietly restore the collision this
/// exists to prevent.
#[cfg(test)]
pub(crate) fn reserve_worker_id() -> u8 {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let next = NEXT.fetch_add(1, Ordering::Relaxed);
    let Ok(worker_id) = u8::try_from(next) else {
        panic!("the test worker id space is spent");
    };
    worker_id
}

/// Builds a waker that carries `worker_id`, the way a runtime waker carries the
/// task identity the production decoder reads back.
///
/// A test used to poll with a noop waker against a decoder that answered a
/// constant. A noop waker carries nothing, so the decoder had nothing to read,
/// and every test in the binary resolved to the same worker id and the same seam
/// slot. This carries the id, so a test can hold one nothing else holds.
#[cfg(test)]
pub(crate) fn test_waker(worker_id: u8) -> Waker {
    let data = ptr::without_provenance::<()>(worker_id as usize);
    // SAFETY: Invariant -- `TEST_VTABLE` is a `'static` table and `data` is an
    // address-only pointer built by `without_provenance`, carrying no borrow tag
    // and no allocation. Nothing dereferences it: the stdlib passes it through
    // the vtable opaquely, `clone_test` copies the integer, and `wake_test` and
    // `drop_test` are empty, so the vtable owns no lifetime to manage and
    // `test_decoder` reads the value back with `.addr()` rather than through the
    // pointer. That keeps the round-trip sound under strict provenance.
    // Precondition: only `test_decoder` reads the data back, and only after
    // `is_test_vtable` confirms the vtable is this one.
    // Failure mode: pairing this data pointer with a foreign vtable would decode
    // a garbage worker id and route a completion to the wrong seam.
    unsafe { Waker::from_raw(RawWaker::new(data, &TEST_VTABLE)) }
}

/// Decodes a test waker into its binding, and refuses every other waker.
///
/// Mirrors the production contract: a waker the runtime did not build is not
/// routable, so the future must not submit against it.
#[cfg(test)]
fn test_decoder(waker: &Waker) -> Option<WakerBinding> {
    if !is_test_vtable(waker) {
        return None;
    }
    let Ok(worker_id) = u8::try_from(waker.data().addr()) else {
        return None;
    };
    Some(WakerBinding {
        token: TEST_TOKEN,
        worker_id,
    })
}

/// Whether `waker` was built by [`test_waker`] rather than handed in from
/// elsewhere.
#[cfg(test)]
fn is_test_vtable(waker: &Waker) -> bool {
    ptr::eq(ptr::from_ref(waker.vtable()), &raw const TEST_VTABLE)
}

#[cfg(test)]
mod tests {
    use super::*;

    static OTHER: WakerDecoder = other_binding;

    // A decoder that answers differently, used to prove the first registration
    // is the one that stays. It refuses a foreign waker like the real one does.
    fn other_binding(waker: &Waker) -> Option<WakerBinding> {
        if !is_test_vtable(waker) {
            return None;
        }
        Some(WakerBinding {
            token: 9,
            worker_id: 5,
        })
    }

    #[test]
    fn a_foreign_waker_is_not_routable() {
        register_decoder(&TEST_DECODER);
        assert_eq!(
            decode_waker(Waker::noop()),
            None,
            "a waker the runtime did not build carries no binding",
        );
    }

    #[test]
    fn decoder_registration_is_first_wins() {
        let worker_id = reserve_worker_id();
        register_decoder(&TEST_DECODER);
        let waker = test_waker(worker_id);
        let expected = WakerBinding {
            token: TEST_TOKEN,
            worker_id,
        };
        assert_eq!(decode_waker(&waker), Some(expected));
        register_decoder(&OTHER);
        assert_eq!(
            decode_waker(&waker),
            Some(expected),
            "a second registration loses to the first",
        );
    }

    #[test]
    fn a_reserved_worker_id_is_handed_out_once() {
        let first = reserve_worker_id();
        let second = reserve_worker_id();
        assert_ne!(first, second, "two reservations never share an id");
    }
}
