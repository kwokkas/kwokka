//! `io_uring` setup tiers and SQE/CQE flag mapping.
//!
//! `SetupTier` records which setup flags the kernel accepted during
//! ring creation. `sqe_flags` maps [`OpFlags`] to the `io_uring` SQE
//! flag bits. CQE flag helpers wrap the `io_uring::cqueue` utilities.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use io_uring::squeue;

use crate::operation::OpFlags;

/// Setup configuration tier achieved during ring creation.
///
/// The probe attempts tier 1 first; on `EINVAL` fallback to tier 2.
/// With the 6.0+ kernel minimum, only tiers 1 and 2 are reachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetupTier {
    /// 6.1+: `DEFER_TASKRUN` + `COOP_TASKRUN` + `SINGLE_ISSUER`.
    Optimal,
    /// 6.0+: `COOP_TASKRUN` (5.19) + `SINGLE_ISSUER` (6.0).
    Baseline,
}

/// Map [`OpFlags`] to `io_uring` SQE flags.
///
/// `fixed_fd` maps to `FIXED_FILE` and `buffer_select` to `BUFFER_SELECT`;
/// the io-uring crate sets the SQE `buf_group` field on the `Recv` op but
/// not the `BUFFER_SELECT` bit for a single-shot recv, so that bit is set
/// here. Other [`OpFlags`] fields affect opcode selection in the submission
/// path, not SQE flags.
pub(crate) fn sqe_flags(op_flags: OpFlags) -> squeue::Flags {
    let mut flags = squeue::Flags::empty();
    if op_flags.fixed_fd {
        flags |= squeue::Flags::FIXED_FILE;
    }
    if op_flags.buffer_select {
        flags |= squeue::Flags::BUFFER_SELECT;
    }
    flags
}

/// Returns `true` if the CQE signals more completions from a multishot op.
pub(crate) fn is_cqe_more(cqe_flags: u32) -> bool {
    io_uring::cqueue::more(cqe_flags)
}

/// Returns `true` if the CQE is a zero-copy send notification.
pub(crate) fn is_cqe_notif(cqe_flags: u32) -> bool {
    io_uring::cqueue::notif(cqe_flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_tier_is_copy() {
        let tier = SetupTier::Optimal;
        let copy = tier;
        assert_eq!(tier, copy);
    }

    #[test]
    fn sqe_flags_empty_when_no_fixed_fd() {
        let flags = sqe_flags(OpFlags::new());
        assert!(flags.is_empty());
    }

    #[test]
    fn sqe_flags_fixed_file_when_fixed_fd() {
        let op_flags = OpFlags::new().with_fixed_fd(true);
        let flags = sqe_flags(op_flags);
        assert!(flags.contains(squeue::Flags::FIXED_FILE));
    }

    #[test]
    fn sqe_flags_buffer_select_when_set() {
        let op_flags = OpFlags::new().with_buffer_select(true);
        let flags = sqe_flags(op_flags);
        assert!(flags.contains(squeue::Flags::BUFFER_SELECT));
    }

    #[test]
    fn sqe_flags_ignores_non_sqe_op_flags() {
        let op_flags = OpFlags::new()
            .with_fixed_buf(true)
            .with_zero_copy(true)
            .with_multishot(true)
            .with_vectored(true);
        let flags = sqe_flags(op_flags);
        assert!(flags.is_empty());
    }
}
