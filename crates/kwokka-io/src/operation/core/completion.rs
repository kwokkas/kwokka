//! I/O completion event and CQE flag types.

use crate::operation::SubmitToken;

/// I/O completion event wrapping an `io_uring` CQE.
///
/// `result` is kept as a raw `i32` (negative = `-errno`). The completion
/// futures decode it into an `io::Result` before user code sees it; a
/// direct `Completion` consumer converts the raw value itself.
/// A `NOTIF` CQE surfaces here with [`CqeFlags::NOTIF`] set; the runtime
/// completion drain absorbs it to release the send buffer, so user code
/// never sees it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    /// Opaque token from the originating `IoRequest` - equal to the
    /// `user_data` submitted to the ring.
    pub token: SubmitToken,
    /// Raw CQE result field. Negative values are `-errno`.
    pub result: i32,
    /// CQE flags (`F_MORE`, `F_NOTIF`, `F_BUFFER`, etc.).
    pub flags: CqeFlags,
    /// Buffer ID selected by the kernel from a `buf_ring`, present when
    /// [`CqeFlags::BUFFER`] is set.
    pub buf_id: Option<u16>,
}

impl Completion {
    /// `true` if this is the `NOTIF` sentinel from a `SEND_ZC`
    /// two-stage completion. The runtime completion drain absorbs these to
    /// release the send buffer; user code never sees them.
    pub const fn is_notif(self) -> bool {
        self.flags.contains(CqeFlags::NOTIF)
    }

    /// `true` when more CQEs are expected for this operation
    /// (multishot accept, multishot recv, etc.).
    pub const fn has_more(self) -> bool {
        self.flags.contains(CqeFlags::MORE)
    }

    /// `true` if the operation succeeded (`result >= 0`).
    pub const fn is_ok(self) -> bool {
        self.result >= 0
    }
}

/// `io_uring` CQE flags.
///
/// Written without the `bitflags` crate to avoid an external dependency.
/// Bit positions match `IORING_CQE_F_*` from `linux/io_uring.h`
/// (verified against `io-uring` crate `sys_aarch64.rs:138-142`).
/// The mapping is applied in the `uring/` backend when constructing a
/// [`Completion`] from a raw CQE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CqeFlags(u32);

impl CqeFlags {
    /// No flags set.
    pub const EMPTY: Self = Self(0);
    /// `buf_id` field is valid - buffer was selected from a `buf_ring`
    /// (`IORING_CQE_F_BUFFER`, bit 0).
    pub const BUFFER: Self = Self(1 << 0);
    /// More CQEs will follow for this request (`IORING_CQE_F_MORE`, bit 1).
    pub const MORE: Self = Self(1 << 1);
    /// Socket buffer still has data after receive (`IORING_CQE_F_SOCK_NONEMPTY`, bit 2).
    pub const SOCK_NONEMPTY: Self = Self(1 << 2);
    /// `SEND_ZC` notification CQE - driver-internal sentinel, not user-visible
    /// (`IORING_CQE_F_NOTIF`, bit 3).
    pub const NOTIF: Self = Self(1 << 3);
    /// Buffer ring has more available slots (`IORING_CQE_F_BUF_MORE`, bit 4).
    /// Requires kernel 6.12+.
    pub const BUF_MORE: Self = Self(1 << 4);

    /// Constructs flags from a raw bit pattern.
    pub const fn new(bits: u32) -> Self {
        Self(bits)
    }

    /// Raw bit pattern.
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// `true` if all bits in `other` are set in `self`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl core::ops::BitOr for CqeFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl core::ops::BitAnd for CqeFlags {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> SubmitToken {
        SubmitToken::new(1)
    }

    fn completion(flags: CqeFlags, result: i32) -> Completion {
        Completion {
            token: token(),
            result,
            flags,
            buf_id: None,
        }
    }

    #[test]
    fn cqe_flags_default_is_empty() {
        assert_eq!(CqeFlags::default(), CqeFlags::EMPTY);
        assert_eq!(CqeFlags::default().bits(), 0);
    }

    #[test]
    fn cqe_flags_bit_positions_are_distinct() {
        let flags = [
            CqeFlags::BUFFER,
            CqeFlags::MORE,
            CqeFlags::SOCK_NONEMPTY,
            CqeFlags::NOTIF,
            CqeFlags::BUF_MORE,
        ];
        for (i, a) in flags.iter().enumerate() {
            for (j, b) in flags.iter().enumerate() {
                if i != j {
                    assert_eq!((*a & *b), CqeFlags::EMPTY);
                }
            }
        }
    }

    #[test]
    fn cqe_flags_contains_self() {
        assert!(CqeFlags::MORE.contains(CqeFlags::MORE));
        assert!(CqeFlags::NOTIF.contains(CqeFlags::NOTIF));
    }

    #[test]
    fn cqe_flags_bitor_combines_bits() {
        let combined = CqeFlags::MORE | CqeFlags::BUFFER;
        assert!(combined.contains(CqeFlags::MORE));
        assert!(combined.contains(CqeFlags::BUFFER));
        assert!(!combined.contains(CqeFlags::NOTIF));
    }

    #[test]
    fn cqe_flags_bitand_masks_bits() {
        let combined = CqeFlags::MORE | CqeFlags::NOTIF;
        assert_eq!(combined & CqeFlags::MORE, CqeFlags::MORE);
        assert_eq!(combined & CqeFlags::BUFFER, CqeFlags::EMPTY);
    }

    #[test]
    fn cqe_flags_new_roundtrips_bits() {
        let bits = 0b1010_u32;
        assert_eq!(CqeFlags::new(bits).bits(), bits);
    }

    #[test]
    fn completion_is_ok_for_non_negative_result() {
        assert!(completion(CqeFlags::EMPTY, 0).is_ok());
        assert!(completion(CqeFlags::EMPTY, 42).is_ok());
        assert!(!completion(CqeFlags::EMPTY, -1).is_ok());
    }

    #[test]
    fn completion_is_notif_checks_flag() {
        assert!(completion(CqeFlags::NOTIF, 0).is_notif());
        assert!(!completion(CqeFlags::MORE, 0).is_notif());
    }

    #[test]
    fn completion_has_more_checks_flag() {
        assert!(completion(CqeFlags::MORE, 4).has_more());
        assert!(!completion(CqeFlags::BUFFER, 4).has_more());
    }

    #[test]
    fn completion_buf_id_present_when_buffer_flag_set() {
        let buffered = Completion {
            token: token(),
            result: 64,
            flags: CqeFlags::BUFFER,
            buf_id: Some(7),
        };
        assert!(buffered.flags.contains(CqeFlags::BUFFER));
        assert_eq!(buffered.buf_id, Some(7));
    }
}
