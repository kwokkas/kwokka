//! `IORING_OP_MSG_RING` helpers for cross-worker wake.
//!
//! Builds a `msg_ring` SQE that delivers a sentinel `result` and
//! `user_data` to a target ring. The target worker's completion
//! drain recognizes the sentinel and routes to the wake path.

#![allow(dead_code, reason = "pending msg_ring wake wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use io_uring::{opcode, squeue::Entry, types::Fd};

/// Build a `MSG_RING` SQE targeting another worker's ring.
///
/// `target_ring_fd` is the file descriptor of the destination ring.
/// `result` is echoed in the target CQE's result field (typically
/// `WAKE_SENTINEL`). `user_data` identifies the task to wake on
/// the target worker.
pub(crate) fn build_msg_ring_entry(target_ring_fd: i32, result: i32, user_data: u64) -> Entry {
    opcode::MsgRingData::new(Fd(target_ring_fd), result, user_data, None).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_ring_entry_builds_without_panic() {
        let _entry = build_msg_ring_entry(10, i32::MIN, 0xBEEF);
    }
}
