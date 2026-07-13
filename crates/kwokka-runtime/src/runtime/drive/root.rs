//! The root task: the future a runtime was handed, and the `block_on` that
//! drives it.
//!
//! A root is spawned into the worker's slab like any other task; what makes it
//! the root is that the loop stops when it settles and its output is what
//! `block_on` returns.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::future::Future;

use kwokka_core::{id::Namespace, slab::SlabKey};
use kwokka_io::boundary::{CancelInboxGuard, ProvidedPoolGuard, RecvCancelInboxGuard};

use crate::{
    runtime::{
        build::handle::Runtime,
        crew::stealing::park_bracketed,
        drive::turn::{arm_wake, park_for_next_event, run_pass},
    },
    task::{
        Affine, Stealing,
        cell::{lifecycle::spawn_insert, state::TaskState},
    },
    worker::{cycle::Tick, park::wake::wake_local, shard::state::WorkerShard},
};

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

impl Runtime<Stealing> {
    /// Runs `future` to completion on the lead worker, blocking the calling
    /// thread, and returns its output.
    ///
    /// The root task is pinned to the lead worker: it is spawned into the
    /// lead shard, driven by the lead's run-loop, and its output is read
    /// back on this thread. Sibling workers keep parking between calls, so
    /// the runtime can run another future after this one returns; the crew
    /// shuts down when the runtime drops.
    ///
    /// The idle park is the one step that differs from the affine root: a
    /// lead with siblings parks through the endpoint's parked bracket, so a
    /// cross-worker wake or the shutdown broadcast always completes it.
    ///
    /// The `Send` bound is the work-stealing admission contract. The root
    /// itself never migrates, but every future entering this runtime
    /// satisfies the bound the steal path relies on.
    ///
    /// # Panics
    ///
    /// Panics if the root task cannot be spawned into the lead shard, or if
    /// it terminates abnormally (cancelled or failed). A recoverable error
    /// is the future's own `Output` and does not panic.
    pub fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
    {
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
                park_bracketed(&mut self.shard);
            }
        }
        take_root_output::<F::Output>(&mut self.shard, root_key)
    }
}
