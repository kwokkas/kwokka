//! Multishot accept and recv helpers.
//!
//! Multishot ops submit a single SQE that produces multiple CQEs,
//! each with `IORING_CQE_F_MORE` set until the final completion.
//! When the capability is absent, the driver re-submits a single-shot
//! op on each completion to emulate the multishot behavior.

#![allow(dead_code, reason = "pending multishot wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::operation::CqeFlags;

/// Returns `true` if the completion signals more CQEs from a
/// multishot op (`IORING_CQE_F_MORE`).
pub(crate) const fn is_multishot_continuation(flags: CqeFlags) -> bool {
    flags.contains(CqeFlags::MORE)
}

/// Returns `true` if the multishot op has terminated (final CQE
/// without `MORE` flag). The driver should re-submit if the op
/// is still wanted.
pub(crate) const fn is_multishot_terminated(flags: CqeFlags) -> bool {
    !flags.contains(CqeFlags::MORE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn more_flag_is_continuation() {
        assert!(is_multishot_continuation(CqeFlags::MORE));
    }

    #[test]
    fn no_more_flag_is_terminated() {
        assert!(is_multishot_terminated(CqeFlags::EMPTY));
    }

    #[test]
    fn more_flag_is_not_terminated() {
        assert!(!is_multishot_terminated(CqeFlags::MORE));
    }

    #[test]
    fn combined_flags_still_detect_more() {
        let flags = CqeFlags::MORE | CqeFlags::BUFFER;
        assert!(is_multishot_continuation(flags));
        assert!(!is_multishot_terminated(flags));
    }
}
