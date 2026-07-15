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
//!
//! # Canonical implementors
//!
//! [`InlineBuf`] borrows a pointer into a pinned future's own storage or a
//! worker's in-flight slot. [`FixedBuf`] and `[u8; N]` own their bytes
//! directly instead -- `FixedBuf` for a send source with a partial
//! initialized length, `[u8; N]` for a recv destination whose received count
//! is threaded through the future's own return value rather than `set_init`.

use std::ptr::NonNull;

/// Read-only buffer for write and send operations.
///
/// The kernel reads from this buffer during the operation. Implementors must
/// ensure the pointer returned by [`as_ptr`][IoBuf::as_ptr] remains valid and
/// stable for the lifetime of the submitted operation (until CQE arrival).
///
/// `Unpin` is a supertrait bound: every completion future built over an
/// `IoBuf` (`RecvFuture`, `SendFuture`, and the file / zero-copy futures)
/// holds it behind an `Option<B>` field and reaches it with `Pin::get_mut`
/// on each poll. The kernel-facing memory these futures submit is always the
/// worker's own in-flight slot or the future's own inline storage, addressed
/// through `InlineBuf`, never a pinned reference into `B` itself, so no
/// implementor legitimately needs to be `!Unpin`.
pub trait IoBuf: 'static + Unpin {
    /// Pointer to the start of the initialized data.
    fn as_ptr(&self) -> *const u8;

    /// Number of initialized bytes available for the kernel to read.
    fn bytes_init(&self) -> usize;

    /// Registered buffer slot index, if this buffer was registered
    /// with the ring via `register_buffers`. `None` for unregistered buffers.
    fn registered(&self) -> Option<u16> {
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

/// A fixed-size array is always fully initialized memory in safe Rust, so it
/// implements [`IoBuf`] with a constant `bytes_init`: the whole array is valid
/// for the kernel to send or be written into.
impl<const N: usize> IoBuf for [u8; N] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        N
    }
}

/// The received byte count is threaded through the owning future's
/// `(io::Result<usize>, B)` return value, not through [`IoBuf`] state, so
/// `set_init` has nothing to record here.
impl<const N: usize> IoBufMut for [u8; N] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        N
    }

    fn set_init(&mut self, _n: usize) {}
}

/// An owned, fixed-capacity buffer for a single send operation.
///
/// Holds up to `N` bytes with a runtime-tracked initialized length, so a
/// caller sending fewer than `N` bytes keeps that partial length through the
/// [`IoBuf`] contract without a heap allocation. A send reads
/// [`bytes_init`][IoBuf::bytes_init] bytes starting at
/// [`as_ptr`][IoBuf::as_ptr] and never writes back, so `FixedBuf` implements
/// [`IoBuf`] only, not [`IoBufMut`].
///
/// The name describes its fixed capacity, not kernel registration: `FixedBuf`
/// never touches `IORING_REGISTER_BUFFERS`.
pub struct FixedBuf<const N: usize> {
    data: [u8; N],
    len: usize,
}

impl<const N: usize> FixedBuf<N> {
    /// Builds a buffer over `data`, sending its first `len` bytes (clamped to
    /// `N`).
    #[must_use]
    pub const fn new(data: [u8; N], len: usize) -> Self {
        Self {
            data,
            len: if len < N { len } else { N },
        }
    }
}

impl<const N: usize> IoBuf for FixedBuf<N> {
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len
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

#[cfg(test)]
mod array_buf_tests {
    use super::*;

    #[test]
    fn array_reports_full_capacity_as_init() {
        let buf = [0u8; 16];
        assert_eq!(buf.bytes_init(), 16);
        assert_eq!(buf.capacity(), 16);
    }

    #[test]
    fn array_set_init_is_a_no_op() {
        let mut buf = [1u8; 4];
        buf.set_init(2);
        assert_eq!(
            buf.bytes_init(),
            4,
            "the array's own length is the report, not set_init",
        );
    }
}

#[cfg(test)]
mod fixed_buf_tests {
    use super::*;

    #[test]
    fn clamps_len_to_capacity() {
        let buf = FixedBuf::new([0u8; 4], 10);
        assert_eq!(buf.bytes_init(), 4);
    }

    #[test]
    fn keeps_len_under_capacity() {
        let buf = FixedBuf::new(*b"payload!", 4);
        assert_eq!(buf.bytes_init(), 4);
        assert_eq!(buf.as_ptr(), buf.data.as_ptr());
    }
}
