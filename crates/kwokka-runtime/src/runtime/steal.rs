//! The steal protocol as the run-loop sees it: serving a thief, receiving a
//! handed-off task, and reporting where a relocated task went.
//!
//! Each function is one step the pass composes when the `steal` feature is on.
//! The protocol itself lives in
//! [`stealing`](crate::scheduler::stealing); these are the calls that drive it
//! from a worker's own loop.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::{
    scheduler::stealing::{handoff, relocate::SettledNote},
    task::cell::state::TaskState,
    worker::{park::wake::wake_local, registry, shard::state::WorkerShard},
};

/// Serves one pending steal request against this worker's own slab.
///
/// Pops at most one request per pass. The handoff core retires the first
/// stealable resident out (Sleeping pass first, then Woken pass from the
/// run queue) and records the forward route in the same straight-line step;
/// the reply ships through the thief's handoff ring with an unpark signal
/// chasing it. Runs ahead of the wake drain so a stale wake naming the
/// fresh husk already finds its route.
pub(crate) fn serve_steals(shard: &mut WorkerShard) {
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
pub(crate) fn receive_handoffs(shard: &mut WorkerShard) {
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
pub(crate) fn report_settled_relocations(shard: &mut WorkerShard) {
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
pub(crate) fn drain_settled_notes(shard: &mut WorkerShard) {
    while let Some(note) = registry::pop_settled(shard.id) {
        let Some(slot) = shard.tasks.get_mut(note.victim_key) else {
            continue;
        };
        if slot.header().state.load() == TaskState::Retired {
            slot.header_mut().is_remote_settled = true;
        }
    }
}
