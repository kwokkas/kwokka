//! SQE builders for the zero-copy send opcode (`SEND_ZC`).

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
    types::{Fd, Fixed},
};

use crate::operation::OpFlags;

/// Build a zero-copy send SQE (`io_uring_prep_send_zc.3`, kernel 6.0+).
///
/// The kernel reads the buffer at `ptr` in place instead of copying it into
/// kernel space, so the buffer must stay alive until the notification. A
/// zero-copy send posts two CQEs sharing the op `user_data`: the send result,
/// then a notification once the kernel has released the buffer.
pub(crate) fn build_send_zc(fd: i32, ptr: *const u8, len: usize, flags: OpFlags) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "len bounded by buffer init bytes"
    )]
    let len = len as u32;
    if flags.fixed_fd {
        opcode::SendZc::new(Fixed(fd as u32), ptr, len).build()
    } else {
        opcode::SendZc::new(Fd(fd), ptr, len).build()
    }
}
