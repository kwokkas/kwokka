//! Decoding the polling task's binding out of its `Waker`.

use core::{
    ptr,
    sync::atomic::{AtomicPtr, Ordering},
    task::Waker,
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

#[cfg(test)]
mod tests {
    use super::*;

    static STUB: WakerDecoder = stub_binding;
    static OTHER: WakerDecoder = other_binding;

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
}
