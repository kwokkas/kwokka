//! Anonymous mmap region for address-stable buffer storage.
//!
//! Wraps `libc::mmap` / `libc::munmap` to allocate contiguous memory
//! from the OS page allocator rather than the process heap. This avoids
//! `Box` / `Vec` while providing the address stability required by the
//! `io_uring` kernel ABI for registered and provided buffers.

#![allow(dead_code, reason = "pending buf_ring wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::{io, ptr::NonNull};

/// Anonymous mmap region.
///
/// Allocates `len` bytes via `MAP_ANONYMOUS | MAP_PRIVATE`. The region
/// is zero-filled by the kernel and page-aligned. Dropped via `munmap`.
///
/// Used by [`BufRingPool`](crate::buffer::ring::pool::BufRingPool) for buffer storage
/// that the kernel writes into during multishot recv/accept.
pub(crate) struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
}

impl MmapRegion {
    /// Allocate an anonymous mmap region of `len` bytes.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if `len` is zero or `mmap` fails.
    pub(crate) fn new(len: usize) -> io::Result<Self> {
        let Ok(len) = NonZeroLen::try_from(len) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mmap length must be non-zero",
            ));
        };
        let len = len.0;

        // SAFETY: Invariant -- MAP_ANONYMOUS + MAP_PRIVATE allocates
        // fresh zero-filled pages with no backing file. The null fd (-1)
        // and zero offset are required by MAP_ANONYMOUS.
        // Precondition: len > 0 (checked above).
        // Failure mode: MAP_FAILED return on OOM or rlimit; handled
        // below via the error check.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };

        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let Some(ptr) = NonNull::new(raw.cast::<u8>()) else {
            return Err(io::Error::other("mmap returned null"));
        };

        Ok(Self { ptr, len })
    }

    /// Pointer to the start of the region.
    pub(crate) const fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Length in bytes.
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// View the region as a byte slice.
    #[allow(
        clippy::missing_const_for_fn,
        reason = "slice::from_raw_parts is not const-stable"
    )]
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: Invariant -- ptr and len were returned by a successful
        // mmap call. The region is valid for reads for its entire length.
        // Precondition: no concurrent Rust-side writes to the region
        // (caller upholds single-writer invariant at the pool level).
        // Failure mode: dangling ptr after munmap causes UB; prevented
        // by Drop taking &mut self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        // SAFETY: Invariant -- ptr and len were returned by a successful
        // mmap call in new(). munmap with the same ptr+len is valid.
        // Precondition: no other references to the region exist (Drop
        // takes &mut self, enforcing exclusive access).
        // Failure mode: mismatched ptr/len causes undefined behavior;
        // this cannot happen because ptr and len are immutable after
        // construction.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

// SAFETY: Invariant -- MmapRegion holds a NonNull<u8> pointing to
// anonymous mmap memory. mmap regions are process-wide with no thread
// affinity; the kernel manages page-level access.
// Precondition: the region is not unmapped before all references are
// dropped (guaranteed by Drop impl taking &mut self).
// Failure mode: use-after-munmap dereferences freed pages (SIGSEGV).
unsafe impl Send for MmapRegion {}

// SAFETY: Invariant -- MmapRegion fields (ptr, len) are immutable after
// construction; no Rust-side mutation occurs through &MmapRegion.
// Concurrent reads of the ptr/len fields are safe.
// Precondition: no Rust reference into the mmap region is live while
// the kernel holds the buffer for I/O. This lifecycle contract is
// enforced at the BufRingPool level, not by MmapRegion itself.
// Failure mode: a live &[u8] slice concurrent with a kernel write is
// a data race (UB). Callers must ensure buffer hand-off discipline.
unsafe impl Sync for MmapRegion {}

struct NonZeroLen(usize);

impl TryFrom<usize> for NonZeroLen {
    type Error = ();

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value == 0 { Err(()) } else { Ok(Self(value)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_allocates_region() {
        let Ok(region) = MmapRegion::new(4096) else {
            panic!("mmap must succeed for 4096 bytes");
        };
        assert_eq!(region.len(), 4096);
        assert!(!region.as_ptr().is_null());
    }

    #[test]
    fn new_zero_length_returns_error() {
        let result = MmapRegion::new(0);
        assert!(result.is_err());
    }

    #[test]
    fn region_is_zero_filled() {
        let Ok(region) = MmapRegion::new(4096) else {
            panic!("mmap must succeed");
        };
        // SAFETY: Invariant -- region.as_ptr() points to 4096 valid
        // zero-filled bytes from mmap.
        // Precondition: region.len() == 4096.
        // Failure mode: out-of-bounds read if len is wrong.
        let slice = unsafe { std::slice::from_raw_parts(region.as_ptr(), region.len()) };
        assert!(slice.iter().all(|&byte| byte == 0));
    }

    #[test]
    fn drop_does_not_panic() {
        let Ok(region) = MmapRegion::new(4096) else {
            panic!("mmap must succeed");
        };
        drop(region);
    }

    #[test]
    fn mmap_region_is_send_and_sync() {
        fn require_send<T: Send>() {}
        fn require_sync<T: Sync>() {}
        require_send::<MmapRegion>();
        require_sync::<MmapRegion>();
    }
}
