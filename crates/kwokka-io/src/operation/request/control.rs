//! Ops that steer the ring rather than move bytes: timeout, cancel, the
//! cross-ring wake, and poll arm/disarm.

use crate::operation::{
    ControlPayload, IoRequest, OpPayload,
    core::{OpCode, SubmitToken},
};

impl IoRequest<()> {
    /// Arm a completion timeout.
    #[doc(hidden)]
    pub fn timeout(duration_ns: u64) -> Self {
        Self::build(
            -1,
            OpCode::Timeout,
            OpPayload::Control(ControlPayload::Timeout { duration_ns }),
        )
    }

    /// Cancel an in-flight operation.
    #[doc(hidden)]
    pub fn cancel(target: SubmitToken) -> Self {
        Self::build(
            -1,
            OpCode::Cancel,
            OpPayload::Control(ControlPayload::Cancel { target }),
        )
    }

    /// Wake another ring via `IORING_OP_MSG_RING`.
    ///
    /// Posts [`MSG_RING_WAKE_USER_DATA`](crate::boundary::MSG_RING_WAKE_USER_DATA)
    /// as the target ring's CQE `user_data`, and as this op's own SQE
    /// `user_data`, so a rare source-side failure CQE (`SKIP_SUCCESS` drops the
    /// success CQE) is recognized by the same sentinel rather than misrouted
    /// onto a task slot. The target's completion drain unparks and discards it.
    #[doc(hidden)]
    pub fn msg_ring_wake(target_ring_fd: i32) -> Self {
        Self::build(
            target_ring_fd,
            OpCode::MsgRing,
            OpPayload::Control(ControlPayload::MsgRing {
                result: 0,
                sentinel: crate::boundary::MSG_RING_WAKE_USER_DATA,
            }),
        )
        .with_user_data(crate::boundary::MSG_RING_WAKE_USER_DATA)
    }

    /// Poll a file descriptor for readiness.
    #[doc(hidden)]
    pub fn poll_add(fd: i32, events: u32) -> Self {
        Self::build(
            fd,
            OpCode::Poll,
            OpPayload::Control(ControlPayload::PollAdd { events }),
        )
    }

    /// Remove a poll watch.
    #[doc(hidden)]
    pub fn poll_remove(fd: i32, token: SubmitToken) -> Self {
        Self::build(
            fd,
            OpCode::Poll,
            OpPayload::Control(ControlPayload::PollRemove { token }),
        )
    }
}
