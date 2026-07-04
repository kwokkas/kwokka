//! SQE builders for control opcodes: timeout, cancel, `msg_ring`, `poll_add`, `poll_remove`.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
#![allow(
    clippy::cast_sign_loss,
    reason = "fd-to-registered-index casts (i32 -> u32) are inherent to the Fixed(fd) io_uring ABI"
)]

use io_uring::{
    opcode,
    squeue::{Entry, Flags},
    types::{Fd, Fixed, Timespec},
};

use crate::operation::OpFlags;

/// Build a timeout SQE.
pub(crate) fn build_timeout(duration_ns: u64, ts: &mut Timespec) -> Entry {
    let secs = duration_ns / 1_000_000_000;
    #[allow(
        clippy::cast_possible_truncation,
        reason = "nanosecond remainder always < 1_000_000_000"
    )]
    let nsecs = (duration_ns % 1_000_000_000) as u32;
    *ts = Timespec::new().sec(secs).nsec(nsecs);
    opcode::Timeout::new(ts).build()
}

/// Build an async-cancel SQE.
pub(crate) fn build_cancel(user_data: u64) -> Entry {
    opcode::AsyncCancel::new(user_data).build()
}

/// Build a `msg_ring` SQE targeting another ring.
///
/// `result` and `sentinel` become the target ring's CQE `res` and `user_data`
/// -- two independent channels per `io_uring_prep_msg_ring.3`.
/// `IOSQE_CQE_SKIP_SUCCESS` suppresses the source ring's own completion on
/// success, so a wake costs no local CQE; a submit failure still posts one,
/// carrying the same sentinel for the source-side drain to recognize.
pub(crate) fn build_msg_ring(target_ring_fd: i32, result: i32, sentinel: u64) -> Entry {
    opcode::MsgRingData::new(Fd(target_ring_fd), result, sentinel, None)
        .build()
        .flags(Flags::SKIP_SUCCESS)
}

/// Build a poll-add SQE.
pub(crate) fn build_poll_add(fd: i32, events: u32, flags: OpFlags) -> Entry {
    if flags.fixed_fd {
        opcode::PollAdd::new(Fixed(fd as u32), events).build()
    } else {
        opcode::PollAdd::new(Fd(fd), events).build()
    }
}

/// Build a poll-remove SQE.
pub(crate) fn build_poll_remove(user_data: u64) -> Entry {
    opcode::PollRemove::new(user_data).build()
}
