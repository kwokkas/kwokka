//! Vectored I/O: the `N`-buffer wrappers and the in-flight-slot `iovec` layout.
//!
//! A `readv` / `writev` op stores its `iovec` array and the gathered or
//! scattered payload contiguously at the start of one per-worker in-flight slot,
//! so the array the kernel reads (`writev`) or the region it writes (`readv`)
//! stays pinned for the op's lifetime -- long after submission, until the CQE
//! arrives. Offsets are computed from `size_of`, never hardcoded:
//!
//! ```text
//! [0, PAYLOAD_OFFSET)     [libc::iovec; count]   the array the SQE points at
//! [PAYLOAD_OFFSET, ..)    payload                each iovec.iov_base points here
//! ```
//!
//! The payload region holds every buffer's bytes back to back: a write gathers
//! them into it before submit, a read has the kernel fill it and scatters them
//! out on completion. Keeping the kernel-facing bytes in the slot rather than in
//! the caller's buffers is what makes an early drop safe -- the same discipline
//! the single-buffer futures hold. The one raw-pointer step, forming the
//! `&mut [u8; SLOT_LEN]` over a live slot, lives in the seam that owns the slot
//! lifetime, not here; the layout writer below only takes that array by
//! reference.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::ptr::NonNull;

use crate::{
    buffer::oneshot::inflight::INFLIGHT_BUF_STRIDE,
    operation::{IoBuf, IoBufMut},
};

/// Slot width in bytes, matching the in-flight slot stride.
const SLOT_LEN: usize = INFLIGHT_BUF_STRIDE as usize;

/// Size of one `iovec` entry.
const IOVEC_SIZE: usize = size_of::<libc::iovec>();

/// Wraps `N` source buffers for a `writev`.
///
/// Each element contributes its initialized bytes to the gathered payload. A
/// single-element `IoVec` is valid but pointless -- prefer a plain `B: IoBuf`.
/// `N` is a compile-time constant; a runtime-length list is a later addition.
pub struct IoVec<B: IoBuf, const N: usize> {
    bufs: [B; N],
}

impl<B: IoBuf, const N: usize> IoVec<B, N> {
    /// Constructs an `IoVec` from an array of `N` source buffers.
    pub const fn new(bufs: [B; N]) -> Self {
        Self { bufs }
    }

    /// The underlying buffer array.
    pub(crate) const fn bufs(&self) -> &[B; N] {
        &self.bufs
    }

    /// Moves the buffers out, returned to the caller alongside the byte count.
    pub(crate) fn into_bufs(self) -> [B; N] {
        self.bufs
    }
}

/// Wraps `N` destination buffers for a `readv`.
///
/// Each element offers its capacity to the scattered payload. `N` is a
/// compile-time constant; a runtime-length list is a later addition.
pub struct IoVecMut<B: IoBufMut, const N: usize> {
    bufs: [B; N],
}

impl<B: IoBufMut, const N: usize> IoVecMut<B, N> {
    /// Constructs an `IoVecMut` from an array of `N` destination buffers.
    pub const fn new(bufs: [B; N]) -> Self {
        Self { bufs }
    }

    /// The underlying buffer array.
    pub(crate) const fn bufs(&self) -> &[B; N] {
        &self.bufs
    }

    /// The underlying buffer array, mutably, for the completion scatter.
    pub(crate) const fn bufs_mut(&mut self) -> &mut [B; N] {
        &mut self.bufs
    }

    /// Moves the buffers out, returned to the caller alongside the byte count.
    pub(crate) fn into_bufs(self) -> [B; N] {
        self.bufs
    }
}

/// Largest total payload that fits after a `count`-entry `iovec` array, or
/// `None` when the array alone overflows the slot.
pub(crate) fn max_payload(count: usize) -> Option<usize> {
    SLOT_LEN.checked_sub(count.checked_mul(IOVEC_SIZE)?)
}

/// Pointer to the payload region of `slot`, past the `count`-entry `iovec`
/// array. The gather source or scatter destination, addressed under the slot's
/// own lifetime.
///
/// # Panics
///
/// Panics if `count` is large enough that the `iovec` array fills the slot; the
/// seam checks [`max_payload`] before calling, so a submit is refused rather
/// than reaching this.
pub(crate) fn payload_ptr(slot: &mut [u8; SLOT_LEN], count: usize) -> *mut u8 {
    slot[count * IOVEC_SIZE..].as_mut_ptr()
}

/// Writes a `count`-entry `iovec` array at the start of `slot`, each entry
/// pointing at its slice of the payload region sized by `lens`. Returns the
/// array pointer for the SQE, or `None` when the array plus payload overflows
/// the slot.
///
/// The same array serves both directions: a `writev` fills the payload first, a
/// `readv` has the kernel fill it after. Call last among the slot writes -- the
/// returned pointer is invalidated by any later `&mut` reborrow of `slot`.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "the slot base is page-aligned and IOVEC_SIZE is an 8-multiple, so every iovec lands aligned; the entries are written unaligned regardless"
)]
pub(crate) fn write_iovecs(
    slot: &mut [u8; SLOT_LEN],
    lens: &[usize],
) -> Option<NonNull<libc::iovec>> {
    let count = lens.len();
    let payload_at = count.checked_mul(IOVEC_SIZE)?;
    let total = lens
        .iter()
        .try_fold(0usize, |sum, &len| sum.checked_add(len))?;
    if payload_at.checked_add(total)? > SLOT_LEN {
        return None;
    }
    let array_ptr = NonNull::from(&mut *slot).cast::<libc::iovec>();
    // Derive `base` from `array_ptr` rather than a second `slot` reborrow: a
    // fresh `&mut` reborrow would invalidate `array_ptr`'s tag under Stacked
    // Borrows, leaving the returned pointer dangling at the SQE.
    let base = array_ptr.as_ptr().cast::<u8>();
    let mut cursor = payload_at;
    for (idx, &len) in lens.iter().enumerate() {
        let entry = libc::iovec {
            iov_base: base.wrapping_add(cursor).cast::<libc::c_void>(),
            iov_len: len,
        };
        // SAFETY: Invariant -- `idx < count`, so `idx * IOVEC_SIZE` is within the
        // `count`-entry array at the slot start, in bounds for a write; the
        // check above keeps `cursor` (the iov_base offset) inside the slot.
        // `write_unaligned` imposes no alignment requirement (the slot is
        // page-aligned regardless, so the kernel's aligned reads are sound). The
        // stored `iov_base` addresses this same slot, kept alive by the caller
        // until the CQE. Precondition: `payload_at + total <= SLOT_LEN`, checked
        // above. Failure mode: an index or cursor past the slot would write out
        // of bounds -- excluded by the array type and the bounds check.
        unsafe {
            base.add(idx * IOVEC_SIZE)
                .cast::<libc::iovec>()
                .write_unaligned(entry);
        }
        cursor += len;
    }
    Some(array_ptr)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockBuf {
        data: [u8; 64],
        len: usize,
    }

    impl MockBuf {
        fn filled(bytes: &[u8]) -> Self {
            let mut data = [0u8; 64];
            let count = bytes.len().min(64);
            data[..count].copy_from_slice(&bytes[..count]);
            Self { data, len: count }
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
    fn max_payload_leaves_room_for_the_array() {
        assert_eq!(max_payload(2), Some(SLOT_LEN - 2 * IOVEC_SIZE));
        assert_eq!(max_payload(0), Some(SLOT_LEN));
        // An array that fills the whole slot leaves zero payload, not None.
        assert_eq!(max_payload(SLOT_LEN / IOVEC_SIZE), Some(0));
        // One entry past that overflows the slot.
        assert_eq!(max_payload(SLOT_LEN / IOVEC_SIZE + 1), None);
    }

    #[test]
    fn write_iovecs_points_each_entry_at_its_payload_slice() {
        let mut slot = [0u8; SLOT_LEN];
        let base = slot.as_ptr();
        let Some(array) = write_iovecs(&mut slot, &[3, 5, 2]) else {
            panic!("three small buffers fit one slot");
        };
        let payload_at = 3 * IOVEC_SIZE;
        for (idx, (expected_len, expected_off)) in [
            (3usize, payload_at),
            (5, payload_at + 3),
            (2, payload_at + 8),
        ]
        .into_iter()
        .enumerate()
        {
            // SAFETY: `array` points at the three-entry iovec array just written
            // inside `slot`, a live stack array; `read_unaligned` copies an entry
            // out without forming a reference, so the array's alignment is fine.
            let entry = unsafe { array.as_ptr().add(idx).read_unaligned() };
            assert_eq!(entry.iov_len, expected_len);
            assert_eq!(
                entry.iov_base.cast::<u8>().cast_const(),
                base.wrapping_add(expected_off),
            );
        }
    }

    #[test]
    fn write_iovecs_refuses_a_payload_past_the_slot() {
        let mut slot = [0u8; SLOT_LEN];
        assert_eq!(write_iovecs(&mut slot, &[SLOT_LEN]), None);
    }

    #[test]
    fn payload_ptr_starts_past_the_array() {
        let mut slot = [0u8; SLOT_LEN];
        let base = slot.as_ptr();
        assert_eq!(
            payload_ptr(&mut slot, 4).cast_const(),
            base.wrapping_add(4 * IOVEC_SIZE),
        );
    }

    #[test]
    fn vectors_are_send() {
        fn require_send<T: Send>() {}
        require_send::<IoVec<MockBuf, 2>>();
    }
}
