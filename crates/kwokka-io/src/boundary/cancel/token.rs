//! Completion `user_data` sentinels.
//!
//! Most CQEs carry a task token the completion drain routes to a wake slot. The
//! rest carry one of the markers here: a cancel completion, a multishot accept
//! or recv result, a cross-ring `msg_ring` wake, or the discarded half of a
//! linked-timeout pair.
//!
//! The markers occupy successive corners of the upper 32 bits, descending from
//! the wake fd's `u64::MAX`. Each corner needs both a maximal worker id and a
//! near-maximal generation to occur naturally, so a real task token never
//! aliases one, and the predicates below stay disjoint from each other.

use crate::buffer::{
    multishot::{MultishotSlotKey, RecvMultishotSlotKey},
    oneshot::inflight::InflightSlotKey,
};

/// `user_data` marker for a buffered-op cancel completion.
///
/// Tags every cancel SQE the worker's cancel-drain submits so the completion
/// drain recognizes the cancel op's own CQE and routes it to
/// [`reclaim_cancel_completion`](crate::boundary::reclaim_cancel_completion). The slot is usually
/// freed on the original op's completion (see
/// [`reclaim_dropped_slot`](crate::boundary::reclaim_dropped_slot)); the cancel CQE frees it only
/// on `-ENOENT`, where the target already completed and no op completion is
/// coming. `io_uring` async cancel is best-effort, so a `0` or `-EALREADY`
/// result leaves the target still completing and never drives a free. The
/// upper 32 bits are all set: the arena tag bit, a worker id of 127, and a
/// maximal generation. That is the arena address space's exhaustion corner,
/// reached only when both the worker id and the generation are maxed out, so a
/// real completion never aliases the marker in practice. The low 32 bits carry
/// the slot and its low 16 generation bits.
///
/// This gives the marker the same narrow window as the wake fd's `u64::MAX`,
/// which sits in that corner at a maximal offset. The two stay disjoint: the
/// marker never encodes an all-ones low half, and [`is_cancel_sentinel`]
/// excludes the wake value. A slab-path handle clears the arena tag bit, so it
/// never aliases either.
const CANCEL_TOKEN_BASE: u64 = 0xFFFF_FFFF_0000_0000;

/// Upper-32-bit mask isolating the [`CANCEL_TOKEN_BASE`] marker.
const CANCEL_TOKEN_HIGH_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// Encodes the cancel-completion `user_data` for `key`: the marker, the slot at
/// bits 0..16, and the low 16 bits of the slot's generation at bits 16..32.
///
/// The slot and generation are read back only on a cancel completion that
/// reports `-ENOENT` (see
/// [`reclaim_cancel_completion`](crate::boundary::reclaim_cancel_completion)): the target op
/// already completed, so no op-token completion will free the slot, and the generation
/// guards a stale cancel from freeing a slot the same op token has since reused.
pub(crate) const fn encode_cancel_sentinel(key: InflightSlotKey) -> u64 {
    CANCEL_TOKEN_BASE | ((key.generation & 0xFFFF) << 16) | key.slot as u64
}

/// Whether `user_data` is a cancel-completion sentinel.
///
/// The completion drain calls this to recognize the cancel op's own CQE and
/// route it to [`reclaim_cancel_completion`](crate::boundary::reclaim_cancel_completion) instead of
/// the task-wake path. The slot is normally reclaimed on the original op's completion (see
/// [`reclaim_dropped_slot`](crate::boundary::reclaim_dropped_slot)); the cancel CQE frees it only
/// on a `-ENOENT` result.
///
/// The marker fills the upper 32 bits, which the wake fd's `u64::MAX` also
/// does, so the wake value is excluded here to keep the two predicates disjoint
/// on their own. The drain tests the wake fd first regardless.
pub const fn is_cancel_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == CANCEL_TOKEN_BASE
        && user_data != crate::wake::WAKE_FD_USER_DATA
}

/// `user_data` marker base for a multishot completion.
///
/// A multishot op posts many CQEs sharing one `user_data`, so its completions
/// route to the [`MultishotSlab`](crate::buffer::multishot::MultishotSlab)
/// rather than the per-task wake slot. The upper 32 bits read `0xFFFF_FFFE`:
/// the arena tag bit, worker id 127, and generation `MAX - 1`, one corner below
/// the cancel base. That keeps the three completion sentinels disjoint -- the
/// wake fd is `u64::MAX` and the cancel base is `0xFFFF_FFFF_0000_0000`, both
/// upper-32 `0xFFFF_FFFF`, while this reads `0xFFFF_FFFE`. It is unreachable for
/// the same reason the cancel corner is: generation `MAX - 1` needs ~2^24 slot
/// reuses. The low 32 bits carry the slot and its low 16 generation bits, the
/// same layout [`encode_cancel_sentinel`] uses.
const MULTISHOT_TOKEN_BASE: u64 = 0xFFFF_FFFE_0000_0000;

/// Encodes the multishot-completion `user_data` for `key`.
pub(crate) const fn encode_multishot_sentinel(key: MultishotSlotKey) -> u64 {
    MULTISHOT_TOKEN_BASE | ((key.generation & 0xFFFF) << 16) | key.slot as u64
}

/// Whether `user_data` is a multishot-completion sentinel.
///
/// The completion drain calls this to route the CQE into the
/// [`MultishotSlab`](crate::buffer::multishot::MultishotSlab). The marker shares
/// the upper-32 isolation mask with the cancel sentinel but sits one corner
/// below it, so no wake-value guard is needed: `u64::MAX` reads upper-32
/// `0xFFFF_FFFF`, already excluded.
pub const fn is_multishot_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == MULTISHOT_TOKEN_BASE
}

/// The slot index a multishot sentinel names.
pub(crate) const fn multishot_sentinel_slot(user_data: u64) -> u16 {
    (user_data & 0xFFFF) as u16
}

/// The low 16 generation bits a multishot sentinel carries.
pub(crate) const fn multishot_sentinel_generation(user_data: u64) -> u16 {
    ((user_data >> 16) & 0xFFFF) as u16
}

/// `user_data` marker base for a multishot recv completion.
///
/// A multishot recv posts many CQEs sharing one `user_data`, routed to the
/// [`RecvMultishotSlab`](crate::buffer::multishot::RecvMultishotSlab) rather than the per-task wake
/// slot. The upper 32 bits read `0xFFFF_FFFD`: the arena tag bit, worker id 127, and generation
/// `MAX - 2`, one corner below the multishot accept base. That keeps all four
/// completion sentinels disjoint by upper-32 -- wake and cancel read
/// `0xFFFF_FFFF`, multishot accept `0xFFFF_FFFE`, and this `0xFFFF_FFFD`. It is
/// unreachable for the same reason the others are: generation `MAX - 2` needs
/// ~2^24 slot reuses, and a slab-path token clears the arena tag bit entirely.
/// The low 32 bits carry the slot and its low 16 generation bits, the layout
/// [`multishot_sentinel_slot`] and [`multishot_sentinel_generation`] decode.
const RECV_MULTISHOT_TOKEN_BASE: u64 = 0xFFFF_FFFD_0000_0000;

/// Encodes the multishot-recv-completion `user_data` for `key`.
pub(crate) const fn encode_recv_multishot_sentinel(key: RecvMultishotSlotKey) -> u64 {
    RECV_MULTISHOT_TOKEN_BASE | ((key.generation & 0xFFFF) << 16) | key.slot as u64
}

/// Whether `user_data` is a multishot-recv-completion sentinel.
///
/// The completion drain calls this to route the CQE into the
/// [`RecvMultishotSlab`](crate::buffer::multishot::RecvMultishotSlab). It sits one corner below the
/// multishot accept base, so no wake-value guard is needed: `u64::MAX` reads upper-32
/// `0xFFFF_FFFF`, already excluded.
pub const fn is_recv_multishot_sentinel(user_data: u64) -> bool {
    user_data & CANCEL_TOKEN_HIGH_MASK == RECV_MULTISHOT_TOKEN_BASE
}

/// `user_data` marker for a cross-ring `msg_ring` wake.
///
/// The `IORING_OP_MSG_RING` analog of the wake fd's
/// [`WAKE_FD_USER_DATA`](crate::wake::WAKE_FD_USER_DATA). A peer worker posts
/// this as the target CQE's `user_data` purely to break the target's park; the
/// completion drain recognizes it and unparks without a task route or a stored
/// result. The upper 32 bits read `0xFFFF_FFFC`: the arena tag
/// bit, worker id 127, and generation `MAX - 3`, one corner below the
/// multishot-recv base, so all five completion sentinels stay disjoint by
/// upper-32 -- wake fd and cancel `0xFFFF_FFFF`, multishot accept `0xFFFF_FFFE`,
/// multishot recv `0xFFFF_FFFD`, and this `0xFFFF_FFFC`. Unlike the per-slot
/// sentinels it names no suboperation, so the whole value is the fixed marker
/// with a zero low half; recognition is an exact-value match, not an upper-32
/// mask.
const MSG_RING_WAKE_TOKEN_BASE: u64 = 0xFFFF_FFFC_0000_0000;

/// The CQE `user_data` a cross-ring `msg_ring` wake carries.
///
/// The target ring receives it as the delivered wake; the source ring sees it
/// on the `SKIP_SUCCESS` failure CQE, so a rare send failure is recognized
/// rather than misrouted onto a task slot.
pub const MSG_RING_WAKE_USER_DATA: u64 = MSG_RING_WAKE_TOKEN_BASE;

/// Whether `user_data` marks a cross-ring `msg_ring` wake.
///
/// The completion drain calls this to recognize a peer's `IORING_OP_MSG_RING`
/// CQE and unpark without a task route. It is an exact-value match against the
/// fixed marker, disjoint from every per-slot sentinel corner and from the wake
/// fd's `u64::MAX`.
pub const fn is_msg_ring_wake(user_data: u64) -> bool {
    user_data == MSG_RING_WAKE_USER_DATA
}

/// `user_data` marker for the discarded half of a linked-timeout pair.
///
/// A `submit_linked_timeout_internal` op carries `IOSQE_IO_LINK`; the paired
/// `IORING_OP_LINK_TIMEOUT` SQE tags its own CQE with this fixed marker. That
/// CQE (`-ETIME` / `-ECANCELED` / `-ENOENT` per `io_uring_prep_link_timeout.3`)
/// is pure noise: the primary op's CQE already carries the outcome the caller
/// observes, and the kernel cancels the timeout once the primary is gone, so
/// no per-slot registry is needed. The upper 32 bits read `0xFFFF_FFFB`: the
/// arena tag bit, worker id 127, and generation `MAX - 4`, one corner below the
/// `msg_ring` wake base, so all six completion sentinels stay disjoint by
/// upper-32 -- wake fd and cancel `0xFFFF_FFFF`, multishot accept `0xFFFF_FFFE`,
/// multishot recv `0xFFFF_FFFD`, `msg_ring` `0xFFFF_FFFC`, and this
/// `0xFFFF_FFFB`. Recognition is an exact-value match, not an upper-32 mask,
/// like the `msg_ring` wake.
const LINK_TIMEOUT_TOKEN_BASE: u64 = 0xFFFF_FFFB_0000_0000;

/// The CQE `user_data` the link-timeout half of a linked pair carries.
///
/// The completion drain recognizes it and drops the CQE without a task route or
/// a slot free -- the primary op's own CQE is the caller's result.
pub(crate) const LINK_TIMEOUT_DISCARD_USER_DATA: u64 = LINK_TIMEOUT_TOKEN_BASE;

/// Whether `user_data` marks the discarded half of a linked-timeout pair.
///
/// The completion drain calls this to recognize the paired
/// `IORING_OP_LINK_TIMEOUT` CQE and drop it. It is an exact-value match against
/// the fixed marker, disjoint from every per-slot sentinel corner and from the
/// wake fd's `u64::MAX`.
pub const fn is_link_timeout_discard(user_data: u64) -> bool {
    user_data == LINK_TIMEOUT_DISCARD_USER_DATA
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_sentinel_excludes_other_tokens() {
        assert!(
            !is_cancel_sentinel(crate::wake::WAKE_FD_USER_DATA),
            "the wake fd marker is not a cancel sentinel",
        );
        assert!(
            !is_cancel_sentinel(0x7FFF_FFFF_FFFF_FFFF),
            "a slab-path task token keeps its top bit clear",
        );
        assert!(
            !is_cancel_sentinel(0x8000_0000_0000_0005),
            "the previous marker corner (arena worker 0, generation 0) no longer aliases",
        );
        let sentinel = CANCEL_TOKEN_BASE | (0xAB << 16) | 0x05;
        assert!(is_cancel_sentinel(sentinel), "the marker is recognized");
    }

    #[test]
    fn multishot_sentinel_round_trips() {
        let key = MultishotSlotKey {
            slot: 0x2A,
            generation: 0xABCD,
            worker_id: 3,
        };
        let sentinel = encode_multishot_sentinel(key);
        assert!(is_multishot_sentinel(sentinel));
        assert_eq!(multishot_sentinel_slot(sentinel), 0x2A);
        assert_eq!(multishot_sentinel_generation(sentinel), 0xABCD);
    }

    #[test]
    fn multishot_sentinel_excludes_other_markers() {
        assert!(
            !is_multishot_sentinel(CANCEL_TOKEN_BASE),
            "the cancel corner reads upper-32 0xFFFF_FFFF, not 0xFFFF_FFFE",
        );
        assert!(
            !is_multishot_sentinel(crate::wake::WAKE_FD_USER_DATA),
            "the wake fd reads upper-32 0xFFFF_FFFF",
        );
        assert!(
            !is_cancel_sentinel(MULTISHOT_TOKEN_BASE),
            "the multishot corner is not a cancel sentinel",
        );
        assert!(
            !is_multishot_sentinel(0x7FFF_FFFF_FFFF_FFFF),
            "a slab-path task token keeps its top bit clear",
        );
    }

    #[test]
    fn sentinel_carries_slot_and_generation() {
        let key = InflightSlotKey {
            slot: 0x2A,
            generation: 0x1_0007,
            worker_id: 3,
            op_token: 0,
        };
        let sentinel = encode_cancel_sentinel(key);
        assert_eq!(
            sentinel & 0xFFFF,
            u64::from(key.slot),
            "the slot sits at bits 0..16"
        );
        assert_eq!(
            (sentinel >> 16) & 0xFFFF,
            0x0007,
            "the generation low 16 bits sit at bits 16..32",
        );
        assert!(is_cancel_sentinel(sentinel), "the marker is set");
    }

    #[test]
    fn msg_ring_wake_sentinel_is_recognized_and_disjoint() {
        assert!(
            is_msg_ring_wake(MSG_RING_WAKE_USER_DATA),
            "the marker recognizes itself",
        );
        // Disjoint from every other completion sentinel and the wake fd.
        assert!(!is_msg_ring_wake(crate::wake::WAKE_FD_USER_DATA));
        assert!(!is_msg_ring_wake(CANCEL_TOKEN_BASE));
        assert!(!is_msg_ring_wake(MULTISHOT_TOKEN_BASE));
        assert!(!is_msg_ring_wake(RECV_MULTISHOT_TOKEN_BASE));
        // The other predicates reject the msg_ring marker.
        assert!(!is_cancel_sentinel(MSG_RING_WAKE_USER_DATA));
        assert!(!is_multishot_sentinel(MSG_RING_WAKE_USER_DATA));
        assert!(!is_recv_multishot_sentinel(MSG_RING_WAKE_USER_DATA));
        // One corner below the multishot-recv base.
        assert_eq!(
            MSG_RING_WAKE_USER_DATA >> 32,
            (RECV_MULTISHOT_TOKEN_BASE >> 32) - 1,
        );
    }

    #[test]
    fn link_timeout_discard_sentinel_is_recognized_and_disjoint() {
        assert!(
            is_link_timeout_discard(LINK_TIMEOUT_DISCARD_USER_DATA),
            "the marker recognizes itself",
        );
        // Disjoint from every other completion sentinel and the wake fd.
        assert!(!is_link_timeout_discard(crate::wake::WAKE_FD_USER_DATA));
        assert!(!is_link_timeout_discard(CANCEL_TOKEN_BASE));
        assert!(!is_link_timeout_discard(MULTISHOT_TOKEN_BASE));
        assert!(!is_link_timeout_discard(RECV_MULTISHOT_TOKEN_BASE));
        assert!(!is_link_timeout_discard(MSG_RING_WAKE_USER_DATA));
        // The other predicates reject the link-timeout marker.
        assert!(!is_cancel_sentinel(LINK_TIMEOUT_DISCARD_USER_DATA));
        assert!(!is_multishot_sentinel(LINK_TIMEOUT_DISCARD_USER_DATA));
        assert!(!is_recv_multishot_sentinel(LINK_TIMEOUT_DISCARD_USER_DATA));
        assert!(!is_msg_ring_wake(LINK_TIMEOUT_DISCARD_USER_DATA));
        // One corner below the msg_ring wake base.
        assert_eq!(
            LINK_TIMEOUT_DISCARD_USER_DATA >> 32,
            (MSG_RING_WAKE_USER_DATA >> 32) - 1,
        );
    }
}
