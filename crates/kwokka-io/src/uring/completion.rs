//! CQE drain for the `io_uring` backend.
//!
//! Translates raw `io_uring` completion queue entries into
//! [`Completion`] values. NOTIF CQEs from `SEND_ZC` are absorbed
//! silently and never surface to the caller.

#![allow(dead_code, reason = "pending completion-translation wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use io_uring::cqueue;

use crate::{
    operation::{Completion, CqeFlags, SubmitToken},
    uring::setup::flags::{is_cqe_more, is_cqe_notif},
};

/// Drain up to `max` completions from `cq`, appending to `out`.
///
/// NOTIF CQEs (`IORING_CQE_F_NOTIF` from `SEND_ZC` two-stage
/// completions) are consumed internally and not appended.
/// Returns the number of completions appended (excluding absorbed
/// NOTIFs).
pub(crate) fn drain_completions(
    cq: &mut cqueue::CompletionQueue<'_>,
    max: usize,
    out: &mut [Completion],
) -> usize {
    let capacity = max.min(out.len());
    let mut count = 0;

    for cqe in cq {
        if count >= capacity {
            break;
        }

        let raw_flags = cqe.flags();

        if is_cqe_notif(raw_flags) {
            continue;
        }

        let buf_id = cqueue::buffer_select(raw_flags);
        let flags = translate_flags(raw_flags);

        out[count] = Completion {
            token: SubmitToken::new(cqe.user_data()),
            result: cqe.result(),
            flags,
            buf_id,
        };
        count += 1;
    }

    count
}

/// Convert a single CQE into a `Completion`, or `None` if it is a
/// NOTIF sentinel.
pub(crate) fn translate_cqe(cqe: &cqueue::Entry) -> Option<Completion> {
    let raw_flags = cqe.flags();

    if is_cqe_notif(raw_flags) {
        return None;
    }

    let buf_id = cqueue::buffer_select(raw_flags);
    let flags = translate_flags(raw_flags);

    Some(Completion {
        token: SubmitToken::new(cqe.user_data()),
        result: cqe.result(),
        flags,
        buf_id,
    })
}

fn translate_flags(raw: u32) -> CqeFlags {
    let mut flags = CqeFlags::EMPTY;
    if cqueue::buffer_select(raw).is_some() {
        flags = flags | CqeFlags::BUFFER;
    }
    if is_cqe_more(raw) {
        flags = flags | CqeFlags::MORE;
    }
    flags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_flags_empty() {
        let flags = translate_flags(0);
        assert_eq!(flags, CqeFlags::EMPTY);
    }

    #[test]
    fn translate_flags_more() {
        let flags = translate_flags(CqeFlags::MORE.bits());
        assert!(flags.contains(CqeFlags::MORE));
        assert!(!flags.contains(CqeFlags::BUFFER));
    }

    #[test]
    fn translate_flags_buffer() {
        let raw = CqeFlags::BUFFER.bits() | (7 << 16);
        let flags = translate_flags(raw);
        assert!(flags.contains(CqeFlags::BUFFER));
    }

    #[test]
    fn translate_flags_more_and_buffer() {
        let raw = CqeFlags::MORE.bits() | CqeFlags::BUFFER.bits() | (3 << 16);
        let flags = translate_flags(raw);
        assert!(flags.contains(CqeFlags::MORE));
        assert!(flags.contains(CqeFlags::BUFFER));
    }
}
