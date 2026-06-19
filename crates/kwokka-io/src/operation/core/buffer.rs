//! Buffer ownership traits for completion-based I/O.
//!
//! Completion-based I/O requires the caller to surrender buffer ownership to
//! the driver for the duration of an operation. The driver holds the buffer
//! until the CQE arrives, then returns it. These traits express that contract.
//!
//! # Ownership model
//!
//! Buffers are moved into `IoRequest` and returned via the completion path.
//! The `'static` bound prevents dangling pointers: the kernel may read or
//! write the buffer at any point after submission until the CQE is processed.

use std::{
    io::{IoSlice, IoSliceMut},
    ptr::NonNull,
};

/// Read-only buffer for write and send operations.
///
/// The kernel reads from this buffer during the operation. Implementors must
/// ensure the pointer returned by [`as_ptr`][IoBuf::as_ptr] remains valid and
/// stable for the lifetime of the submitted operation (until CQE arrival).
pub trait IoBuf: 'static {
    /// Pointer to the start of the initialized data.
    fn as_ptr(&self) -> *const u8;

    /// Number of initialized bytes available for the kernel to read.
    fn bytes_init(&self) -> usize;

    /// Registered buffer slot index, if this buffer was registered
    /// with the ring via `register_buffers`. `None` for unregistered buffers.
    fn registered(&self) -> Option<u16> {
        None
    }

    /// Invokes `f` with the buffer expressed as a vectored slice, if supported.
    ///
    /// `None` for single-buffer implementors (the default); backends
    /// fall back to [`as_ptr`][IoBuf::as_ptr] + [`bytes_init`][IoBuf::bytes_init].
    /// `Some(R)` for vectored implementors (e.g. `IoVec<B, N>`), where
    /// the backend writes the SQE inside the callback and returns a token.
    ///
    /// The callback pattern avoids self-referential structs and heap allocation:
    /// the vectored slice is built on the caller's stack inside the closure and
    /// never escapes. [`IoSlice`] is ABI-layout-compatible with the platform
    /// `iovec` (std guarantee on POSIX targets), so backends may cast
    /// `&[IoSlice<'_>]` to `*const iovec` inside the callback with an
    /// appropriate `// SAFETY:` annotation.
    ///
    /// Backend-internal API -- user code does not call this directly.
    fn with_iovec<R>(&self, _f: impl FnOnce(&[IoSlice<'_>]) -> R) -> Option<R> {
        None
    }
}

/// Mutable buffer for read and receive operations.
///
/// The kernel writes into this buffer during the operation. After the CQE
/// arrives, the caller must call [`set_init`][IoBufMut::set_init] to declare
/// how many bytes are now initialized before reading the data.
pub trait IoBufMut: IoBuf {
    /// Mutable pointer to the start of the buffer's allocated region.
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Total allocated capacity in bytes.
    fn capacity(&self) -> usize;

    /// Marks the first `n` bytes as initialized after a kernel write.
    ///
    /// Must only be called once the CQE confirms the kernel has written at least
    /// `n` bytes into the buffer. `n` must not exceed [`capacity`][IoBufMut::capacity].
    fn set_init(&mut self, n: usize);

    /// Invokes `f` with the buffer expressed as a mutable vectored slice, if supported.
    ///
    /// Mutable counterpart to [`IoBuf::with_iovec`]. Used for `readv`-style
    /// operations where the kernel writes into scattered buffers. `None`
    /// for single-buffer implementors (default).
    ///
    /// Backend-internal API -- user code does not call this directly.
    fn with_iovec_mut<R>(&mut self, _f: impl FnOnce(&mut [IoSliceMut<'_>]) -> R) -> Option<R> {
        None
    }
}

/// A buffer borrowed from a pinned future's inline storage.
///
/// `InlineBuf` holds a raw pointer and capacity describing a byte region the
/// constructing future owns and keeps alive for the full duration of the
/// kernel operation. The `init` count records how many bytes the kernel wrote
/// once the CQE confirms the completion.
///
/// It suits a single buffered operation whose buffer lives inline in the
/// future's poll state, with no heap allocation. The type is intentionally
/// `!Send`: the bytes live in a future pinned to one worker, so the buffer must
/// not cross threads while the kernel holds it.
pub struct InlineBuf {
    ptr: NonNull<u8>,
    cap: usize,
    init: usize,
}

impl InlineBuf {
    /// Creates an `InlineBuf` over the caller-owned `cap`-byte region at `ptr`.
    ///
    /// # Safety
    ///
    /// The caller must uphold all of the following from construction until the
    /// CQE that completes the associated operation:
    ///
    /// - Invariant: `ptr` is non-null and points to a live allocation valid for writes across `[0,
    ///   cap)` within one allocated object, with `cap <= isize::MAX`.
    /// - Precondition: the region is exclusively owned for the operation -- no other reference may
    ///   alias it while the kernel holds the buffer -- and the owning future stays pinned, not
    ///   dropping the storage until after [`set_init`](IoBufMut::set_init) records the CQE result.
    /// - Failure mode: a null, aliased, or freed pointer lets the kernel write invalid memory --
    ///   undefined behavior.
    pub const unsafe fn new(ptr: *mut u8, cap: usize) -> Self {
        // SAFETY: Invariant -- the caller guarantees `ptr` is non-null (it
        // addresses the future's live inline buffer). Precondition: `ptr` is
        // valid for `cap` writes and outlives the CQE. Failure mode: a null
        // pointer breaks NonNull's niche and the later kernel write targets
        // invalid memory.
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self { ptr, cap, init: 0 }
    }
}

impl IoBuf for InlineBuf {
    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    fn bytes_init(&self) -> usize {
        self.init
    }
}

impl IoBufMut for InlineBuf {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn capacity(&self) -> usize {
        self.cap
    }

    fn set_init(&mut self, n: usize) {
        self.init = n;
    }
}

#[cfg(test)]
mod inline_buf_tests {
    use super::*;

    #[test]
    fn ptr_round_trip() {
        let mut storage = [0u8; 64];
        // SAFETY: storage outlives the InlineBuf within this test body and is
        // exclusively owned; no other reference aliases it during the test.
        let buf = unsafe { InlineBuf::new(storage.as_mut_ptr(), 64) };
        assert_eq!(buf.as_ptr(), storage.as_ptr());
        assert_eq!(buf.bytes_init(), 0);
        assert_eq!(buf.capacity(), 64);
    }

    #[test]
    fn mut_ptr_matches_const_ptr() {
        let mut storage = [0u8; 32];
        // SAFETY: storage outlives the InlineBuf and is exclusively owned here.
        let mut buf = unsafe { InlineBuf::new(storage.as_mut_ptr(), 32) };
        assert_eq!(buf.as_mut_ptr().cast_const(), buf.as_ptr());
    }

    #[test]
    fn set_init_records_written_length() {
        let mut storage = [0u8; 16];
        // SAFETY: storage outlives the InlineBuf and is exclusively owned here.
        let mut buf = unsafe { InlineBuf::new(storage.as_mut_ptr(), 16) };
        assert_eq!(buf.bytes_init(), 0);
        buf.set_init(10);
        assert_eq!(buf.bytes_init(), 10);
    }

    #[test]
    fn registered_is_none() {
        let mut storage = [0u8; 4];
        // SAFETY: storage outlives the InlineBuf and is exclusively owned here.
        let buf = unsafe { InlineBuf::new(storage.as_mut_ptr(), 4) };
        assert!(buf.registered().is_none());
    }
}
