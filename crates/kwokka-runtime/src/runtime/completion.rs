//! Draining what the kernel finished: one pass over the driver's completion
//! queue, and the bookkeeping each kind of completion needs.
//!
//! A completion may wake a task, retire an in-flight op, hand back a provided
//! buffer, or be a sentinel that means none of those. Sorting them is the bulk
//! of this module; the run-loop in [`bootstrap`](crate::runtime::bootstrap)
//! only asks for the pass.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::slab::SlabKey;
use kwokka_io::{
    IoDriver,
    boundary::{
        dispose_cancelled_op, is_cancel_sentinel, is_link_timeout_discard, is_msg_ring_wake,
        is_multishot_sentinel, is_recv_multishot_sentinel, mark_notif_expected,
        push_multishot_completion, push_recv_multishot_completion, reclaim_cancel_completion,
        reclaim_dropped_slot, reclaim_notif,
    },
    operation::Completion,
    wake,
};

use crate::{
    runtime::bootstrap::arm_wake,
    task::TaskRef,
    worker::{park::wake::wake_local, shard::state::WorkerShard},
};

/// CQEs read per completion-drain pass.
///
/// A multishot op can post up to this many CQEs sharing one `user_data` in a
/// single pass, so `kwokka_io::buffer::multishot::MULTISHOT_FIFO_DEPTH` must not
/// be smaller; a runtime-side test enforces that bound.
const COMPLETION_BATCH: usize = 64;

/// Retires the one in-flight SQE a terminal multishot CQE accounted for, so the
/// owning task's in-flight count settles even when the CQE carries no wake.
fn retire_multishot_owner(shard: &mut WorkerShard, owner: u64) {
    let task_ref = TaskRef::from_raw(owner);
    let key = SlabKey::new(task_ref.index(), task_ref.generation());
    if let Some(slot) = shard.tasks.get_mut(key) {
        slot.header_mut().retire_in_flight_op();
    }
}

/// Drains ready completions, storing each result into its task and waking it.
///
/// Reads a batch of CQEs from the driver and maps each back to its task via
/// the `user_data` the submit stamped (`TaskRef::raw()`), records the result
/// in the task header for its next poll, then makes the task runnable. A
/// `user_data` that no longer resolves (a recycled slot) is dropped.
pub(crate) fn drain_completions(shard: &mut WorkerShard, wake_fd: i32) {
    // IGNORE: an interrupted flush retries next pass. Deferred task work
    // must run before the CQ read: on a DEFER_TASKRUN ring the kernel
    // posts CQEs only at a GETEVENTS enter, and a worker that never parks
    // would otherwise starve its completions.
    let _ = shard.driver.flush_deferred();
    let mut completions = [Completion::default(); COMPLETION_BATCH];
    let count = shard
        .driver
        .poll_completions(COMPLETION_BATCH, &mut completions);
    for completion in &completions[..count] {
        let user_data = completion.token.user_data();
        if user_data == wake::WAKE_FD_USER_DATA {
            // A remote signal completed the park. The work it announces
            // sits in the wake inbox, drained right after this pass;
            // re-arm so the next signal lands too.
            arm_wake(shard, wake_fd);
            continue;
        }
        if is_msg_ring_wake(user_data) {
            // Peer msg_ring wake, or its rare failure CQE; already in the inbox.
            continue;
        }
        if is_link_timeout_discard(user_data) {
            // The paired LINK_TIMEOUT SQE's own CQE (-ETIME / -ECANCELED /
            // -ENOENT per io_uring_prep_link_timeout.3). The primary op's own
            // CQE carries the outcome the caller observes, so drop this one with
            // no route, wake, or slot free.
            continue;
        }
        if completion.is_notif() {
            // The notification half of a SEND_ZC op: the kernel has released the
            // send buffer. Free a dropped future's slot now, or arm a live
            // future's slot so it frees on its next poll, then wake the awaiting
            // task. The primary CQE already stored the byte count, so this never
            // stores again.
            reclaim_notif(&mut shard.inflight_slab, user_data);
            wake_local(
                &mut shard.tasks,
                &mut shard.run_queue,
                TaskRef::from_raw(user_data),
            );
            continue;
        }
        if is_cancel_sentinel(user_data) {
            // The cancel op's own CQE. Usually a no-op here: the target op's
            // completion reclaims the slot. The exception is a -ENOENT result,
            // meaning the target already completed before the cancel, so no op
            // completion is coming and this reclaims the slot from the sentinel.
            reclaim_cancel_completion(&mut shard.inflight_slab, user_data, completion.result);
            continue;
        }
        if is_multishot_sentinel(user_data) {
            // A multishot op's CQE. Route the result into its registry FIFO and
            // wake the owning stream; a stale, overflowed, or cancel-pending slot
            // wakes nothing, and a terminal cancel CQE frees the slot in there.
            // The terminal CQE also retires the one SQE `poll_one` counted, even
            // when no wake is returned, so the task's in-flight count settles.
            let outcome = push_multishot_completion(
                &mut shard.multishot_slab,
                user_data,
                completion.result,
                completion.flags,
            );
            if let Some(owner) = outcome.wake {
                wake_local(
                    &mut shard.tasks,
                    &mut shard.run_queue,
                    TaskRef::from_raw(owner),
                );
            }
            if let Some(owner) = outcome.retire {
                retire_multishot_owner(shard, owner);
            }
            continue;
        }
        if is_recv_multishot_sentinel(user_data) {
            // A multishot recv op's CQE. Route the `(result, buf_id)` into its
            // registry FIFO and wake the owning stream; the buffer id is read and
            // recycled inside on every stale, overflowed, or cancel-pending CQE
            // (recv reports a selected buffer on intermediate and terminal CQEs
            // alike), and a terminal cancel CQE frees the slot in there. The
            // terminal CQE also retires the one SQE `poll_one` counted, even when
            // no wake is returned, so the task's in-flight count settles.
            let outcome = push_recv_multishot_completion(
                &mut shard.recv_multishot_slab,
                &shard.driver,
                user_data,
                completion.result,
                completion.flags,
                completion.buf_id,
            );
            if let Some(owner) = outcome.wake {
                wake_local(
                    &mut shard.tasks,
                    &mut shard.run_queue,
                    TaskRef::from_raw(owner),
                );
            }
            if let Some(owner) = outcome.retire {
                retire_multishot_owner(shard, owner);
            }
            continue;
        }
        // The original op's completion is the kernel's done-with-the-bytes
        // signal. If a dropped buffered future owned this op, free its slot now;
        // a live future's slot is not retire-pending, so this is a no-op and it
        // frees through its own harvest path.
        if dispose_cancelled_op(
            &shard.driver,
            &mut shard.accept_cancels,
            &mut shard.provided_recv_cancels,
            &mut shard.connect_cancels,
            user_data,
            completion.result,
            completion.buf_id,
        ) {
            // A dropped single-shot accept's or provided recv's op completed;
            // its fd was closed or its buffer recycled here, so there is
            // nothing to reclaim into a slot or wake.
            continue;
        }
        if completion.has_more() {
            // A SEND_ZC primary CQE carrying F_MORE: the notification releasing
            // the buffer is still coming. Mark the slot notif-expected so a
            // racing -ENOENT cancel cannot free it before the kernel is done,
            // and skip the reclaim here -- the notification frees it. Multishot
            // ops set F_MORE too but route through their sentinels above, so
            // F_MORE on this path is a zero-copy send.
            mark_notif_expected(&mut shard.inflight_slab, user_data);
        } else {
            reclaim_dropped_slot(&mut shard.inflight_slab, user_data);
        }
        let task_ref = TaskRef::from_raw(user_data);
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        if let Some(slot) = shard.tasks.get_mut(key) {
            slot.header_mut().store_io_result(
                completion.result,
                completion.flags.bits(),
                completion.buf_id,
            );
        }
        wake_local(&mut shard.tasks, &mut shard.run_queue, task_ref);
    }
}

#[cfg(test)]
mod tests {
    use kwokka_core::Generation;
    use kwokka_io::boundary::{
        is_cancel_sentinel, is_link_timeout_discard, is_multishot_sentinel,
        is_recv_multishot_sentinel,
    };

    use crate::task::TaskRef;

    #[test]
    fn arena_completions_clear_the_marker() {
        // A real arena-path completion must never be misread as a buffered-op
        // cancel sentinel in the drain. The narrowed marker aliases an arena
        // handle only at the worker-127 / max-generation corner; every
        // reachable encoding stays clear.
        let reachable = [
            TaskRef::from_arena(0, 0, Generation::ZERO),
            TaskRef::from_arena(0, u32::MAX, Generation::ZERO),
            TaskRef::from_arena(5, 0xDEAD_BEEF, Generation::from_raw(1)),
            TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, Generation::ZERO),
            TaskRef::from_arena(0, 0, Generation::from_raw(Generation::MAX)),
        ];
        for task in reachable {
            assert!(
                !is_cancel_sentinel(task.raw()),
                "arena completion aliases the cancel marker: {:#018x}",
                task.raw(),
            );
        }
    }

    #[test]
    fn marker_corner_matches_wake() {
        // The one residual collision is the worker-127 / max-generation corner,
        // identical to the wake fd's window. An arena handle there is read as a
        // marker, except the maximal-offset point, which is the wake sentinel
        // (u64::MAX) and stays excluded.
        let max = Generation::from_raw(Generation::MAX);
        let corner = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, max);
        assert!(is_cancel_sentinel(corner.raw()));

        let wake = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, u32::MAX, max);
        assert_eq!(wake.raw(), u64::MAX);
        assert!(!is_cancel_sentinel(wake.raw()));
    }

    #[test]
    fn arena_clears_the_multishot_marker() {
        // A real arena-path completion must never be misread as a multishot
        // sentinel in the drain. The multishot corner sits at worker 127 /
        // generation MAX - 1; every reachable encoding, and the cancel corner
        // (generation MAX), stays clear.
        let reachable = [
            TaskRef::from_arena(0, 0, Generation::ZERO),
            TaskRef::from_arena(0, u32::MAX, Generation::ZERO),
            TaskRef::from_arena(5, 0xDEAD_BEEF, Generation::from_raw(1)),
            TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, Generation::ZERO),
            TaskRef::from_arena(0, 0, Generation::from_raw(Generation::MAX)),
            TaskRef::from_arena(
                TaskRef::WORKER_ID_MAX,
                7,
                Generation::from_raw(Generation::MAX),
            ),
        ];
        for task in reachable {
            assert!(
                !is_multishot_sentinel(task.raw()),
                "arena completion aliases the multishot marker: {:#018x}",
                task.raw(),
            );
        }
    }

    #[test]
    fn multishot_marker_below_cancel_corner() {
        // The multishot corner is worker 127 at generation MAX - 1, one below
        // the cancel corner, so the two markers never alias the same encoding.
        let below_max = Generation::from_raw(Generation::MAX - 1);
        let corner = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, below_max);
        assert!(is_multishot_sentinel(corner.raw()));
        assert!(!is_cancel_sentinel(corner.raw()));
    }

    #[test]
    fn recv_marker_below_multishot_corner() {
        // The recv-multishot corner is worker 127 at generation MAX - 2, one
        // below the multishot corner, so all three markers stay disjoint.
        let below_multishot = Generation::from_raw(Generation::MAX - 2);
        let corner = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, below_multishot);
        assert!(is_recv_multishot_sentinel(corner.raw()));
        assert!(!is_multishot_sentinel(corner.raw()));
        assert!(!is_cancel_sentinel(corner.raw()));
    }

    #[test]
    fn link_timeout_marker_below_msg_ring_corner() {
        // The link-timeout discard corner is worker 127 at generation MAX - 4,
        // one below the msg_ring wake corner (MAX - 3), so the arena encoding
        // lands exactly on the io-side marker and aliases no other sentinel.
        let below_msg_ring = Generation::from_raw(Generation::MAX - 4);
        let corner = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, below_msg_ring);
        assert!(is_link_timeout_discard(corner.raw()));
        assert!(!is_recv_multishot_sentinel(corner.raw()));
        assert!(!is_multishot_sentinel(corner.raw()));
        assert!(!is_cancel_sentinel(corner.raw()));
    }

    #[test]
    fn arena_clears_the_recv_marker() {
        // No reachable arena TaskRef, including the worker-127 / max-generation
        // corners the accept-multishot and cancel markers occupy, aliases the
        // recv-multishot marker.
        let reachable = [
            TaskRef::from_arena(0, 0, Generation::ZERO),
            TaskRef::from_arena(0, u32::MAX, Generation::ZERO),
            TaskRef::from_arena(5, 0xDEAD_BEEF, Generation::from_raw(1)),
            TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, Generation::ZERO),
            TaskRef::from_arena(0, 0, Generation::from_raw(Generation::MAX)),
            TaskRef::from_arena(
                TaskRef::WORKER_ID_MAX,
                7,
                Generation::from_raw(Generation::MAX),
            ),
            TaskRef::from_arena(
                TaskRef::WORKER_ID_MAX,
                7,
                Generation::from_raw(Generation::MAX - 1),
            ),
        ];
        for task in reachable {
            assert!(!is_recv_multishot_sentinel(task.raw()));
        }
    }

    #[test]
    fn drain_batch_fits_multishot_fifo() {
        // A drain pass can post a full batch of same-op multishot CQEs; the
        // per-slot FIFO must hold them all. Enforces the cross-crate invariant
        // that the comment on both sides depends on.
        assert!(
            super::COMPLETION_BATCH <= kwokka_io::buffer::multishot::MULTISHOT_FIFO_DEPTH as usize
        );
    }
}
