//! One pass of the blocking run-loop over a worker shard: what it drains, in
//! what order, and when it parks.
//!
//! [`run_pass`] is the whole module in one function; the rest is the wake and
//! cancel drains it owns, plus the park. What a pass calls out to lives next
//! door: the completion drain in [`completion`](crate::runtime::completion) and
//! the steal steps in [`steal`](crate::runtime::steal). The traffic with
//! [`root`](crate::runtime::root) runs the other way, since `block_on` is what
//! drives this loop.
//!
//! Order is the point. Each pass flushes deferred kernel task work (a
//! `DEFER_TASKRUN` ring posts CQEs only at a GETEVENTS enter) and drains driver
//! completions first, then the wake inbox, runs one cooperative tick, drains
//! child spawns, and reaps settled scope children. Completions land first so a
//! task woken by I/O is already runnable when the tick polls it.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::ptr::NonNull;

use kwokka_io::{
    boundary::{submit_cancel_for, submit_recv_multishot_cancel},
    wake,
};

#[cfg(not(feature = "steal"))]
use crate::worker::park::wake::wake_local;
use crate::{
    runtime::completion::drain_completions,
    timer::wheel::clock::SystemClock,
    worker::{
        cycle::{self, Tick},
        queue::reap,
        registry,
        shard::state::WorkerShard,
    },
};
#[cfg(feature = "steal")]
use crate::{
    runtime::steal::{
        drain_settled_notes, receive_handoffs, report_settled_relocations, serve_steals,
    },
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
