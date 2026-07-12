//! Blocking run-loop substrate -- per-pass draining, parking, and root-task
//! lifecycle over one worker shard.
//!
//! The free functions here are the loop pieces every runtime entry composes:
//! the affine [`Runtime::block_on`] drives one shard on the calling thread,
//! and the work-stealing entry drives one shard per worker thread through
//! the same passes. Each pass flushes deferred kernel task work (a
//! `DEFER_TASKRUN` ring posts CQEs only at a GETEVENTS enter) and drains
//! driver completions first, then the wake inbox, runs one cooperative
//! tick, drains child spawns, and reaps settled scope children --
//! completions land first so a task woken by I/O is already runnable when
//! the tick polls it.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{future::Future, ptr::NonNull};

use kwokka_core::{id::Namespace, slab::SlabKey};
use kwokka_io::{
    IoDriver,
    boundary::{
        CancelInboxGuard, ProvidedPoolGuard, RecvCancelInboxGuard, dispose_cancelled_op,
        is_cancel_sentinel, is_link_timeout_discard, is_msg_ring_wake, is_multishot_sentinel,
        is_recv_multishot_sentinel, mark_notif_expected, push_multishot_completion,
        push_recv_multishot_completion, reclaim_cancel_completion, reclaim_dropped_slot,
        reclaim_notif, submit_cancel_for, submit_recv_multishot_cancel,
    },
    operation::Completion,
    wake,
};

use crate::{
    runtime::handle::Runtime,
    task::{
        Affine, TaskRef,
        cell::{lifecycle::spawn_insert, state::TaskState},
    },
    timer::wheel::clock::SystemClock,
    worker::{
        cycle::{self, Tick},
        park::wake::wake_local,
        queue::reap,
        registry,
        shard::state::WorkerShard,
    },
};
#[cfg(feature = "steal")]
use crate::{
    scheduler::stealing::{handoff, relocate::SettledNote},
    worker::park::wake::wake_or_forward,
};

/// Arms the worker's wake fd on its ring so a remote signal can complete a
/// park.
///
/// A non-uring backend reports `Unsupported` and the park stays CQE- and
/// timer-driven.
pub(crate) fn arm_wake(shard: &WorkerShard, wake_fd: i32) {
    shard.driver.arm_wake_read(wake_fd, wake::WAKE_FD_USER_DATA);
}

/// Runs one full scheduler pass over `shard` and reports whether the tick
/// found work.
///
/// Composes the cancel drain, the completion drain, the wake drain, one
/// cooperative tick, the spawn drain, and the settled-children reap, in that
/// order.
///
/// The cancel drain runs before the completion drain so every slot dropped by a
/// prior pass is marked retire-pending before this pass reads the CQ ring: a
/// dropped op's own completion (which frees its slot by `op_token`) may already
/// be waiting there, and the free only fires on a retire-pending slot.
pub(crate) fn run_pass(shard: &mut WorkerShard, wake_fd: i32) -> Tick {
    drain_cancels(shard);
    drain_completions(shard, wake_fd);
    #[cfg(feature = "steal")]
    drain_settled_notes(shard);
    #[cfg(feature = "steal")]
    serve_steals(shard);
    #[cfg(feature = "steal")]
    receive_handoffs(shard);
    drain_wakes(shard);
    #[cfg(feature = "steal")]
    let outcome = cycle::tick(
        &mut shard.tasks,
        &mut shard.timer,
        &mut shard.run_queue,
        &mut shard.spawn_inbox,
        &mut shard.reap_queue,
        &mut shard.timer_requests,
        shard.id,
        Some(NonNull::from(&mut shard.driver)),
        Some(NonNull::from(&mut shard.inflight_slab)),
        Some(NonNull::from(&mut shard.multishot_slab)),
        Some(NonNull::from(&mut shard.recv_multishot_slab)),
        &shard.forward,
    );
    #[cfg(not(feature = "steal"))]
    let outcome = cycle::tick(
        &mut shard.tasks,
        &mut shard.timer,
        &mut shard.run_queue,
        &mut shard.spawn_inbox,
        &mut shard.reap_queue,
        &mut shard.timer_requests,
        shard.id,
        Some(NonNull::from(&mut shard.driver)),
        Some(NonNull::from(&mut shard.inflight_slab)),
        Some(NonNull::from(&mut shard.multishot_slab)),
        Some(NonNull::from(&mut shard.recv_multishot_slab)),
    );
    cycle::drain_spawns(
        &mut shard.tasks,
        &mut shard.run_queue,
        &mut shard.spawn_inbox,
        shard.id,
        &mut shard.pip_seq,
    );
    reap::reap_settled(&mut shard.tasks, &mut shard.reap_queue);
    #[cfg(feature = "steal")]
    report_settled_relocations(shard);
    outcome
}

/// Serves one pending steal request against this worker's own slab.
///
/// Pops at most one request per pass. The handoff core retires the first
/// stealable resident out (Sleeping pass first, then Woken pass from the
/// run queue) and records the forward route in the same straight-line step;
/// the reply ships through the thief's handoff ring with an unpark signal
/// chasing it. Runs ahead of the wake drain so a stale wake naming the
/// fresh husk already finds its route.
#[cfg(feature = "steal")]
fn serve_steals(shard: &mut WorkerShard) {
    let Some(request) = registry::pop_steal_request(shard.id) else {
        return;
    };
    let thief_id = request.thief_id;
    let reply = handoff::serve_steal(
        &mut shard.tasks,
        &mut shard.forward,
        &shard.origins,
        &mut shard.run_queue,
        shard.id.raw(),
        request,
    );
    // IGNORE: the single-in-flight discipline bounds the thief's ring to
    // one pending reply, so the push cannot bounce. A bounced delivery
    // would drop here -- releasing the carried future but stranding the
    // victim-side husk unreleased (its settled note never comes) and the
    // thief's reservation until its shutdown unreserve.
    let _ = registry::push_handoff(thief_id, reply);
    registry::signal(Some(&shard.driver), thief_id);
}

/// Drains this worker's handoff ring: each delivered body installs under
/// its promised key and wakes onto the local run queue, a declined
/// promise is withdrawn, and the in-flight steal resolves either way.
///
/// Runs before the wake drain and the tick so an installed task is
/// runnable in the same pass that received it.
#[cfg(feature = "steal")]
fn receive_handoffs(shard: &mut WorkerShard) {
    while let Some(reply) = registry::pop_handoff(shard.id) {
        match handoff::receive_handoff(&mut shard.tasks, &mut shard.origins, reply) {
            handoff::Received::Installed(task_ref) => {
                wake_local(&mut shard.tasks, &mut shard.run_queue, task_ref);
            }
            handoff::Received::Withdrawn => {}
        }
        shard.pending_steal = None;
    }
}

/// Reports each settled relocated resident back to its victim.
///
/// The note marks the husk remote-settled, the signal unparks the victim
/// to drain it, and the resident's slot frees once the note lands. A
/// bounced note keeps the slot and the origin for a later pass.
#[cfg(feature = "steal")]
fn report_settled_relocations(shard: &mut WorkerShard) {
    // Bind the driver before the disjoint `&mut tasks`/`&mut origins` borrows so
    // the closure captures only `&shard.driver`, not the whole shard.
    let driver = &shard.driver;
    handoff::report_settled(&mut shard.tasks, &mut shard.origins, |origin| {
        let note = SettledNote {
            victim_key: origin.victim_key,
        };
        if registry::push_settled(origin.victim_id, note).is_err() {
            return false;
        }
        registry::signal(Some(driver), origin.victim_id);
        true
    });
}

/// Drains settled notes, marking each named `Retired` husk reap-eligible.
///
/// Runs before the wake drain and the tick, so however a relocated
/// child's parent gets woken this pass, the scope walk already counts
/// that child settled. A note whose key no longer resolves, or resolves
/// to a slot that is not a `Retired` husk, is dropped -- the generation
/// check stops a reused slot from being marked.
#[cfg(feature = "steal")]
fn drain_settled_notes(shard: &mut WorkerShard) {
    while let Some(note) = registry::pop_settled(shard.id) {
        let Some(slot) = shard.tasks.get_mut(note.victim_key) else {
            continue;
        };
        if slot.header().state.load() == TaskState::Retired {
            slot.header_mut().is_remote_settled = true;
        }
    }
}

/// Drains the wake-registry inbox into the run queue.
///
/// A wake naming a relocated slot re-routes to its new worker through the
/// forward table -- the notes drained just before this carry no routes
/// (those are recorded victim-locally at ship time), so every route a
/// stale wake needs is already in place.
fn drain_wakes(shard: &mut WorkerShard) {
    while let Some(task_ref) = registry::pop(shard.id) {
        #[cfg(feature = "steal")]
        wake_or_forward(
            &mut shard.tasks,
            &mut shard.run_queue,
            &shard.forward,
            Some(&shard.driver),
            task_ref,
        );
        #[cfg(not(feature = "steal"))]
        wake_local(&mut shard.tasks, &mut shard.run_queue, task_ref);
    }
}

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
fn drain_completions(shard: &mut WorkerShard, wake_fd: i32) {
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

/// Submits a cancel for every dropped buffered future queued this pass.
///
/// A future whose buffered op is still in flight pushes its slot key to the
/// worker's cancel inbox on drop. This drains the inbox, marking each slot
/// retire-pending and submitting an `ASYNC_CANCEL` SQE that only hurries the op
/// toward completion. The slot is reclaimed later, on the original op's own
/// completion in [`drain_completions`], never on the cancel's CQE. Runs before
/// the completion drain so every slot dropped by a prior pass is retire-pending
/// before this pass reads an op completion that may already be waiting.
fn drain_cancels(shard: &mut WorkerShard) {
    while let Some(key) = shard.cancel_inbox.pop() {
        submit_cancel_for(
            &shard.driver,
            &mut shard.inflight_slab,
            &mut shard.multishot_slab,
            &mut shard.accept_cancels,
            &mut shard.provided_recv_cancels,
            &mut shard.connect_cancels,
            key,
        );
    }
    // Recv-multishot cancels ride a dedicated per-worker inbox, not the shared
    // ring, so they drain here on their own key type. Each recycles any buffers
    // still queued for the gone stream, marks the slot cancel-pending, and
    // submits a hurry-up cancel; the op's terminal completion frees the slot.
    while let Some(key) = shard.recv_cancel_inbox.pop() {
        submit_recv_multishot_cancel(&shard.driver, &mut shard.recv_multishot_slab, key);
    }
}

/// Parks the driver until the next completion or timer deadline.
pub(crate) fn park_for_next_event(shard: &WorkerShard) {
    let deadline = shard
        .timer
        .next_expiry()
        .map(SystemClock::ticks_to_duration);
    // IGNORE: park returns the wait outcome (or -ETIME / EINTR); the loop
    // re-ticks regardless, so the result carries no decision.
    let _ = shard.driver.park(deadline);
}

/// Inserts `future` as the root task, wakes it onto the run queue, and
/// returns its slab key.
///
/// # Panics
///
/// Panics if the root task cannot be spawned into the runtime slab.
pub(crate) fn spawn_root<F: Future>(shard: &mut WorkerShard, future: F) -> SlabKey {
    let worker = shard.id.raw();
    let pip = shard.issue_pip();
    let Ok(root) = spawn_insert(&mut shard.tasks, worker, pip, Namespace::ROOT, future) else {
        panic!("root task spawn into the runtime slab must succeed");
    };
    let key = SlabKey::new(root.index(), root.generation());
    let Some(slot) = shard.tasks.get_mut(key) else {
        panic!("the just-spawned root must resolve");
    };
    // The root is pinned: take_root_output reads it back from this shard,
    // so it must never relocate.
    slot.header_mut().is_pinned = true;
    wake_local(&mut shard.tasks, &mut shard.run_queue, root);
    key
}

/// Whether the root task has reached `Done`.
///
/// # Panics
///
/// Panics if the root slot disappears before completion, or if the task
/// terminates abnormally (cancelled, failed, or output taken early).
pub(crate) fn root_settled(shard: &WorkerShard, root_key: SlabKey) -> bool {
    let Some(slot) = shard.tasks.get(root_key) else {
        panic!("root task slot must remain live until it completes");
    };
    match slot.header().state.load() {
        TaskState::Done => true,
        TaskState::Cancelled | TaskState::Failed => panic!("root task terminated abnormally"),
        TaskState::Sleeping | TaskState::Woken | TaskState::Running => false,
        TaskState::Taken => panic!("root task output taken before completion"),
        TaskState::Retired => panic!("root task retired while block_on still owns it"),
    }
}

/// Reads the completed root task's output and recycles its slab slot.
///
/// # Panics
///
/// Panics if the root slot is missing when its output is retrieved.
pub(crate) fn take_root_output<T>(shard: &mut WorkerShard, root_key: SlabKey) -> T {
    let Some(slot) = shard.tasks.get_mut(root_key) else {
        panic!("root task slot must remain live for output retrieval");
    };
    let output = slot.take_output::<T>();
    shard.tasks.remove(root_key);
    output
}

impl Runtime<Affine> {
    /// Runs `future` to completion on this runtime, blocking the calling
    /// thread, and returns its output.
    ///
    /// The root future is spawned, driven by the run-loop until it settles,
    /// and its output is read back. Each pass runs one cooperative tick; on
    /// an idle pass (no runnable task and no timer due) the worker parks on
    /// the driver until the next completion or timer deadline. A future that
    /// wakes itself (or is woken by a timer) drives forward without parking.
    ///
    /// # Panics
    ///
    /// Panics if the root task cannot be spawned into the runtime slab, or if
    /// it terminates abnormally (cancelled or failed). A recoverable error is
    /// the future's own `Output` and does not panic.
    pub fn block_on<F: Future>(&mut self, future: F) -> F::Output {
        let worker_id = self.shard.id.raw();
        let _cancel_guard = CancelInboxGuard::install(worker_id, &mut self.shard.cancel_inbox);
        let _recv_cancel_guard =
            RecvCancelInboxGuard::install(worker_id, &mut self.shard.recv_cancel_inbox);
        // The pool outlives this run (it is driver-owned); the guard scopes
        // handle access to the run-loop, clearing the slot on exit.
        let _pool_guard = ProvidedPoolGuard::install(worker_id, &self.shard.driver);
        let root_key = spawn_root(&mut self.shard, future);
        arm_wake(&self.shard, self.wake_fd);
        loop {
            let outcome = run_pass(&mut self.shard, self.wake_fd);
            if root_settled(&self.shard, root_key) {
                break;
            }
            if outcome == Tick::Idle {
                park_for_next_event(&self.shard);
            }
        }
        take_root_output::<F::Output>(&mut self.shard, root_key)
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
