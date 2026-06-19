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
    squeue::Entry,
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

/// Build a `msg_ring` SQE.
pub(crate) fn build_msg_ring(target_ring_fd: i32, msg: u64) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "msg truncated to i32 result field; the full u64 is carried in user_data"
    )]
    let result_i32 = msg as i32;
    opcode::MsgRingData::new(Fd(target_ring_fd), result_i32, msg, None).build()
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
