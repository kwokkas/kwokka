//! In-flight-slot layout for a datagram op's ancillary `msghdr`.
//!
//! A `sendmsg` / `recvmsg` op stores its `libc::msghdr`, its `iovec`, the
//! peer or sender address, and the payload contiguously at the start of one
//! per-worker in-flight slot, so the whole self-referential structure stays
//! pinned for the op's lifetime -- the kernel reads (send) or writes (recv)
//! through the `msghdr` until the CQE arrives, long after submission. Offsets
//! are computed from `size_of`, never hardcoded:
//!
//! ```text
//! [0, IOVEC_OFFSET)             libc::msghdr
//! [IOVEC_OFFSET, ADDR_OFFSET)   libc::iovec        (msg_iov points here)
//! [ADDR_OFFSET, PAYLOAD_OFFSET) address bytes      (msg_name points here)
//! [PAYLOAD_OFFSET, SLOT_LEN)    payload            (iovec.iov_base points here)
//! ```
//!
//! Every function takes the slot as a `&mut [u8; SLOT_LEN]` (or `&`), so bounds
//! are guaranteed by the array type and no function is `unsafe`. The struct
//! writes go through `write_unaligned`, imposing no alignment requirement; the
//! kernel and later reads still see aligned data because the slot base is a
//! page-multiple `MmapRegion` offset. The single raw-pointer step -- forming the
//! `&mut [u8; SLOT_LEN]` over a live slot -- lives in the seam that owns the
//! slot lifetime, not here.

use std::ptr::NonNull;

use crate::{addr::SockAddr, buffer::oneshot::inflight::INFLIGHT_BUF_STRIDE};

/// Slot width in bytes, as a `usize` for the array types below.
const SLOT_LEN: usize = INFLIGHT_BUF_STRIDE as usize;

/// Byte offset of the `iovec` within the slot.
const IOVEC_OFFSET: usize = size_of::<libc::msghdr>();

/// Byte offset of the address region within the slot.
const ADDR_OFFSET: usize = IOVEC_OFFSET + size_of::<libc::iovec>();

/// The `msg_namelen` a recv submits: the full address capacity offered to the
/// kernel as an out-parameter, and the single source for the address-region
/// width (a `u32` because the widening to `ADDR_LEN`'s `usize` cannot truncate).
const RECV_NAMELEN: u32 = 128;

/// Bytes reserved for the address region: a `sockaddr_storage`-compatible span
/// matching [`SockAddr::pack_into`](crate::addr::SockAddr::pack_into)'s buffer,
/// derived from `RECV_NAMELEN` so the two cannot drift.
const ADDR_LEN: usize = RECV_NAMELEN as usize;

/// Byte offset of the payload within the slot.
const PAYLOAD_OFFSET: usize = ADDR_OFFSET + ADDR_LEN;

/// Largest datagram payload that fits one in-flight slot after the ancillary
/// header. Standard-MTU UDP fits comfortably; a jumbo datagram does not (a
/// bigger-stride pool would, left to a follow-up). Underflows at const-eval --
/// a compile error -- if the header ever outgrows the slot stride.
pub(crate) const MAX_MSG_INLINE_CAP: usize = SLOT_LEN - PAYLOAD_OFFSET;

/// Pointer to the payload region of `slot`: the send source, or the recv
/// destination. The caller copies bytes to or from it under the slot's own
/// lifetime.
pub(crate) fn payload_ptr(slot: &mut [u8; SLOT_LEN]) -> *mut u8 {
    slot[PAYLOAD_OFFSET..].as_mut_ptr()
}

/// Lays out a `sendmsg` header over `slot`: packs `addr` into the address region
/// and points the `iovec` at the payload region the caller has already filled
/// with `payload_len` bytes. Returns the `msghdr` pointer for the SQE.
pub(crate) fn write_send_header(
    slot: &mut [u8; SLOT_LEN],
    payload_len: usize,
    addr: &SockAddr,
) -> NonNull<libc::msghdr> {
    let mut packed = [0u8; ADDR_LEN];
    let addr_len = addr.pack_into(&mut packed);
    slot[ADDR_OFFSET..PAYLOAD_OFFSET].copy_from_slice(&packed);
    write_layout(slot, payload_len, addr_len)
}

/// Lays out a `recvmsg` header over `slot`: offers the kernel `cap` payload bytes
/// and the full address region as an out-parameter. Returns the `msghdr` pointer.
pub(crate) fn write_recv_header(slot: &mut [u8; SLOT_LEN], cap: usize) -> NonNull<libc::msghdr> {
    write_layout(slot, cap, RECV_NAMELEN)
}

/// Writes the `iovec` and `msghdr` at their computed offsets and returns the
/// header pointer into `slot`.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "the slot base is page-aligned and every offset is an 8-multiple, so the msghdr/iovec pointers land aligned; the struct bytes are written unaligned regardless"
)]
fn write_layout(slot: &mut [u8; SLOT_LEN], iov_len: usize, addr_len: u32) -> NonNull<libc::msghdr> {
    let header_ptr = NonNull::from(&mut *slot).cast::<libc::msghdr>();
    // Derive `base` from `header_ptr` rather than a second `slot.as_mut_ptr()`
    // reborrow: a fresh `&mut` reborrow of the slot would invalidate
    // `header_ptr`'s tag under Stacked Borrows, leaving the returned pointer
    // dangling when the caller dereferences it.
    let base = header_ptr.as_ptr().cast::<u8>();
    let iov = libc::iovec {
        iov_base: base.wrapping_add(PAYLOAD_OFFSET).cast::<libc::c_void>(),
        iov_len,
    };
    // SAFETY: IOVEC_OFFSET and 0 are compile-time offsets inside the SLOT_LEN-byte
    // `slot`, so `.add` stays in-bounds and the pointers are valid for writes. A
    // zeroed `msghdr` is a valid inert POD before the used fields are set;
    // `write_unaligned` imposes no alignment requirement (the slot is page-aligned
    // regardless, so later aligned reads are sound). The stored
    // msg_name / msg_iov / iov_base pointers address this same slot, kept alive by
    // the caller until the CQE; msg_control / msg_flags stay zero (no ancillary
    // data). Failure mode: a wrong offset would write out of bounds -- excluded by
    // the array type.
    unsafe {
        let mut header: libc::msghdr = core::mem::zeroed();
        header.msg_name = base.wrapping_add(ADDR_OFFSET).cast::<libc::c_void>();
        header.msg_namelen = addr_len;
        header.msg_iov = base.wrapping_add(IOVEC_OFFSET).cast::<libc::iovec>();
        header.msg_iovlen = 1;
        base.add(IOVEC_OFFSET)
            .cast::<libc::iovec>()
            .write_unaligned(iov);
        base.add(0).cast::<libc::msghdr>().write_unaligned(header);
    }
    header_ptr
}

/// Reads the sender address the kernel wrote into `slot`'s address region after
/// a `recvmsg`, or `None` for a family this parse does not handle.
///
/// Never trusts the kernel-written `msg_namelen`: [`SockAddr::unpack`] reads the
/// family discriminant and applies a fixed per-family layout. Call after the CQE
/// confirms completion and before the slot is freed.
pub(crate) fn read_sender(slot: &[u8; SLOT_LEN]) -> Option<SockAddr> {
    let mut bytes = [0u8; ADDR_LEN];
    bytes.copy_from_slice(&slot[ADDR_OFFSET..PAYLOAD_OFFSET]);
    SockAddr::unpack(&bytes)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use super::*;

    #[test]
    fn max_cap_leaves_room_for_the_header() {
        assert_eq!(MAX_MSG_INLINE_CAP + PAYLOAD_OFFSET, SLOT_LEN);
        assert_eq!(PAYLOAD_OFFSET % 8, 0, "every sub-offset is 8-aligned");
    }

    #[test]
    fn send_header_wires_iovec_name_and_payload() {
        let mut slot = [0u8; SLOT_LEN];
        let base = slot.as_ptr();
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 5), 8080));
        payload_ptr(&mut slot);
        slot[PAYLOAD_OFFSET] = 0xAB;
        slot[PAYLOAD_OFFSET + 1] = 0xCD;
        let header = write_send_header(&mut slot, 2, &addr);
        // SAFETY: `header` points at the msghdr just written inside `slot`, a
        // live stack array outliving this read; `read_unaligned` copies it out
        // without forming a reference, so the array's 1-byte alignment is fine
        // (production slots are page-aligned; the test array is not).
        let hdr = unsafe { header.as_ptr().read_unaligned() };
        assert_eq!(hdr.msg_iovlen, 1);
        assert_eq!(hdr.msg_namelen, 16, "a V4 sockaddr packs to 16 bytes");
        assert_eq!(
            hdr.msg_name.cast::<u8>().cast_const(),
            base.wrapping_add(ADDR_OFFSET),
        );
        // SAFETY: msg_iov points at the iovec written inside `slot`.
        let iov = unsafe { hdr.msg_iov.read_unaligned() };
        assert_eq!(iov.iov_len, 2);
        assert_eq!(
            iov.iov_base.cast::<u8>().cast_const(),
            base.wrapping_add(PAYLOAD_OFFSET),
        );
        assert_eq!(read_sender(&slot), Some(addr));
    }

    #[test]
    fn recv_header_offers_capacity_and_reads_the_sender() {
        let mut slot = [0u8; SLOT_LEN];
        let header = write_recv_header(&mut slot, 512);
        // SAFETY: `header` points at the msghdr just written inside `slot`;
        // `read_unaligned` copies it out without forming a reference, so the
        // stack array's 1-byte alignment is fine.
        let hdr = unsafe { header.as_ptr().read_unaligned() };
        assert_eq!(
            hdr.msg_namelen, RECV_NAMELEN,
            "the full OUT address capacity"
        );
        // SAFETY: msg_iov points at the iovec written inside `slot`.
        assert_eq!(unsafe { hdr.msg_iov.read_unaligned() }.iov_len, 512);
        assert_eq!(read_sender(&slot), None, "nothing written yet");
        // Simulate the kernel writing a V6 sender address into the OUT region.
        let sender = SockAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 0, 9));
        let mut packed = [0u8; ADDR_LEN];
        sender.pack_into(&mut packed);
        slot[ADDR_OFFSET..PAYLOAD_OFFSET].copy_from_slice(&packed);
        assert_eq!(read_sender(&slot), Some(sender));
    }
}
