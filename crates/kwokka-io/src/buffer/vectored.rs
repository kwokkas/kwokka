//! [`IoVec`] -- fixed-count vectored I/O buffer wrapper.

#![allow(dead_code, reason = "pending vectored submission wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io::IoSlice;

use crate::operation::IoBuf;

/// Wraps `N` buffers as a single [`IoBuf`] for vectored I/O submission.
///
/// Backends call [`IoBuf::with_iovec`] on an `IoVec` to obtain an
/// `&[IoSlice<'_>]` slice and route the operation to a `writev`-style SQE
/// (e.g. `IORING_OP_WRITEV`). Single-buffer backends ignore `with_iovec` and
/// fall back to `as_ptr` / `bytes_init` of the first element.
///
/// # Limitations
///
/// - `N` is a compile-time constant. A future `IoVecDyn` type will support runtime-length lists.
/// - The kernel's `UIO_MAXIOV` limit is 1024; submitting with `N > 1024` produces a syscall error.
///   This type does not validate `N` at construction.
/// - `IoVec<B, 1>` is valid but carries unnecessary overhead; prefer a plain `B: IoBuf` for
///   single-buffer operations.
pub(crate) struct IoVec<B: IoBuf, const N: usize> {
    bufs: [B; N],
}

impl<B: IoBuf, const N: usize> IoVec<B, N> {
    /// Constructs an `IoVec` from an array of `N` buffers.
    pub(crate) const fn new(bufs: [B; N]) -> Self {
        Self { bufs }
    }

    /// Reference to the underlying buffer array.
    pub(crate) const fn bufs(&self) -> &[B; N] {
        &self.bufs
    }
}

impl<B: IoBuf, const N: usize> IoBuf for IoVec<B, N> {
    /// Fallback for single-buffer backends -- pointer into the first element.
    fn as_ptr(&self) -> *const u8 {
        self.bufs[0].as_ptr()
    }

    /// Fallback for single-buffer backends -- initialized byte count of the
    /// first element.
    fn bytes_init(&self) -> usize {
        self.bufs[0].bytes_init()
    }

    fn with_iovec<R>(&self, f: impl FnOnce(&[IoSlice<'_>]) -> R) -> Option<R> {
        let slices: [IoSlice<'_>; N] = std::array::from_fn(|idx| {
            // SAFETY: Invariant -- IoBuf guarantees as_ptr() is valid for
            // bytes_init() bytes for the duration of &self. The slice
            // lifetime is tied to the closure scope.
            // Precondition: each self.bufs[idx] upholds IoBuf contract.
            // Failure mode: invalid ptr or wrong len in IoBuf impl causes
            // UB at the read site (dangling read or buffer overrun).
            let bytes = unsafe {
                std::slice::from_raw_parts(self.bufs[idx].as_ptr(), self.bufs[idx].bytes_init())
            };
            IoSlice::new(bytes)
        });
        Some(f(&slices))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockBuf {
        data: [u8; 64],
        len: usize,
    }

    impl MockBuf {
        fn new(len: usize) -> Self {
            Self {
                data: [0u8; 64],
                len: len.min(64),
            }
        }

        fn filled(bytes: &[u8]) -> Self {
            let mut buf = Self::new(0);
            let count = bytes.len().min(64);
            buf.data[..count].copy_from_slice(&bytes[..count]);
            buf.len = count;
            buf
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

    #[test]
    fn new_stores_buffers() {
        let iov = IoVec::new([MockBuf::filled(&[1, 2, 3]), MockBuf::filled(&[4, 5])]);
        assert_eq!(iov.bufs()[0].bytes_init(), 3);
        assert_eq!(iov.bufs()[1].bytes_init(), 2);
    }

    #[test]
    fn as_ptr_returns_first_element_ptr() {
        let iov = IoVec::new([MockBuf::filled(&[10; 4])]);
        assert_eq!(iov.as_ptr(), iov.bufs()[0].as_ptr());
    }

    #[test]
    fn bytes_init_returns_first_len() {
        let iov = IoVec::new([MockBuf::new(8), MockBuf::new(16)]);
        assert_eq!(iov.bytes_init(), 8);
    }

    #[test]
    fn with_iovec_returns_some_with_correct_slice_count() {
        let iov = IoVec::new([MockBuf::new(4), MockBuf::new(8), MockBuf::new(2)]);
        let count = iov.with_iovec(|slices| slices.len());
        assert_eq!(count, Some(3));
    }

    #[test]
    fn with_iovec_slice_lengths_match_bytes_init() {
        let iov = IoVec::new([MockBuf::new(5), MockBuf::new(10)]);
        iov.with_iovec(|slices| {
            assert_eq!(slices[0].len(), 5);
            assert_eq!(slices[1].len(), 10);
        });
    }

    #[test]
    fn iovec_is_send() {
        fn require_send<T: Send>() {}
        require_send::<IoVec<MockBuf, 2>>();
    }
}
