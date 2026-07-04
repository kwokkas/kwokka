//! Per-worker wake inbox and wake-endpoint tables.
//!
//! A task waker routes a wake into the owning worker's inbox via
//! [`enqueue`]; the worker drains its inbox into the local run queue each
//! tick via [`pop`]. The inboxes are an inline `static` array sized to the
//! full `WorkerId` space, so a worker owns slot `i` by index with no
//! registration step and no pointer lifetime to manage.
//!
//! Each slot is an [`MpscRing`]. The affine single-worker runtime drives
//! it single-producer (the lone worker, including re-entrant wakes during
//! its own poll) and single-consumer, where the enqueue compare-exchange
//! never contends. The work-stealing runtime, whose wakes cross worker
//! threads, drives the same ring with many producers -- the MPSC contract
//! holds for both with no API change.

#[cfg(not(loom))]
use crate::sync::mpsc::MpscRing;
#[cfg(not(loom))]
use crate::worker::park::endpoint::EndpointCell;
use crate::{task::TaskRef, worker::WorkerId};

/// Per-worker inbox capacity. A power of two for the ring mask, sized to
/// exceed the per-worker live-task count so a push is loss-free in steady
/// state.
pub(crate) const INBOX_CAPACITY: usize = 256;

/// One inbox slot per routable [`WorkerId`] (the 7-bit worker space).
#[cfg(not(loom))]
const INBOX_SLOTS: usize = TaskRef::WORKER_ID_MAX as usize + 1;

/// One worker's wake inbox: a bounded ring of pending [`TaskRef`] handles.
#[cfg(not(loom))]
type WakeInbox = MpscRing<TaskRef, INBOX_CAPACITY>;

/// One wake inbox per worker, indexed by worker id. `MpscRing<TaskRef, _>`
/// is `Sync` (`TaskRef` is a `Send` 64-bit handle), so the array is a sound
/// `static`. Gated out of the loom build: loom atomics offer no const
/// construction, and loom models drive [`MpscRing`] directly rather than
/// through the global registry.
#[cfg(not(loom))]
static INBOXES: [WakeInbox; INBOX_SLOTS] = [const { MpscRing::new() }; INBOX_SLOTS];

/// Routes a wake to the owning worker's inbox.
///
/// Decodes the target worker from `task_ref` and pushes the handle into
/// that worker's inbox; the worker drains it on its next tick.
///
/// # Errors
///
/// Returns `Err(task_ref)` when the target inbox is full -- a dropped wake
/// (lost wakeup). While `INBOX_CAPACITY` exceeds the worker's task capacity
/// this is unreachable in steady state; the caller owns the drop policy.
#[cfg(not(loom))]
pub(crate) fn enqueue(task_ref: TaskRef) -> Result<(), TaskRef> {
    INBOXES[task_ref.worker_id() as usize].push(task_ref)
}

/// Loom variant -- the inbox table is gated out of the loom build, and no
/// loom model routes wakes through the global registry.
#[cfg(loom)]
pub(crate) fn enqueue(_task_ref: TaskRef) -> Result<(), TaskRef> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never routes wakes through the global registry")
}

/// Pops the next pending wake for `worker_id`, or `None` when its inbox is
/// empty. Called by the owning worker to drain into the run queue.
#[cfg(not(loom))]
pub(crate) fn pop(worker_id: WorkerId) -> Option<TaskRef> {
    INBOXES[worker_id.raw() as usize].pop()
}

/// Loom variant -- the inbox table is gated out of the loom build, and no
/// loom model drains wakes through the global registry.
#[cfg(loom)]
pub(crate) fn pop(_worker_id: WorkerId) -> Option<TaskRef> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never drains wakes through the global registry")
}

/// One wake endpoint per worker, beside its inbox. A producer consults the
/// target's cell after a successful [`enqueue`] and signals the published
/// fd only when the worker is parked; the owning worker publishes at
/// bootstrap and brackets each park (see [`EndpointCell`]).
#[cfg(not(loom))]
static ENDPOINTS: [EndpointCell; INBOX_SLOTS] = [const { EndpointCell::new() }; INBOX_SLOTS];

/// Publishes `event_fd` and the worker's own `ring_fd` as the wake targets of
/// `worker_id`.
#[cfg(not(loom))]
pub(crate) fn publish_endpoint(worker_id: WorkerId, event_fd: i32, ring_fd: Option<i32>) {
    ENDPOINTS[worker_id.raw() as usize].publish(event_fd, ring_fd);
}

/// Withdraws the wake endpoint of `worker_id`; later signals resolve to
/// nothing.
#[cfg(not(loom))]
pub(crate) fn withdraw_endpoint(worker_id: WorkerId) {
    ENDPOINTS[worker_id.raw() as usize].withdraw();
}

/// Brackets a park on `worker_id`'s endpoint: raise before the final inbox
/// re-check, clear on wake-up.
#[cfg(not(loom))]
pub(crate) fn set_parked(worker_id: WorkerId, is_parked: bool) {
    ENDPOINTS[worker_id.raw() as usize].set_parked(is_parked);
}

/// Signals the wake endpoint of `worker_id` when it is published and
/// parked; a running, unpublished, or withdrawn worker costs nothing.
///
/// Takes the raw id so the producer side mirrors [`enqueue`]'s
/// `TaskRef`-decoded routing.
#[cfg(not(loom))]
pub(crate) fn signal(worker_id: u8) {
    let Some(target) = ENDPOINTS[worker_id as usize].signal_target() else {
        return;
    };
    // IGNORE: a failed eventfd write races endpoint withdrawal; the worker
    // is already draining toward shutdown and needs no unpark.
    let _ = kwokka_io::wake::signal_wake_fd(target.event_fd);
}

/// Loom variant -- the endpoint table is gated out of the loom build; the
/// parked-handshake model drives [`EndpointCell`] directly.
#[cfg(loom)]
pub(crate) fn signal(_worker_id: u8) {
    // Models drive the cell directly; reaching this stub is a model bug.
    unreachable!("the loom build never signals through the global registry")
}

/// Loom variant -- the endpoint table is gated out of the loom build; the
/// parked-handshake model drives [`EndpointCell`] directly.
#[cfg(loom)]
pub(crate) fn publish_endpoint(_worker_id: WorkerId, _event_fd: i32, _ring_fd: Option<i32>) {
    // Models drive the cell directly; reaching this stub is a model bug.
    unreachable!("the loom build never publishes through the global registry")
}

/// Loom variant -- the endpoint table is gated out of the loom build; the
/// parked-handshake model drives [`EndpointCell`] directly.
#[cfg(loom)]
pub(crate) fn withdraw_endpoint(_worker_id: WorkerId) {
    // Models drive the cell directly; reaching this stub is a model bug.
    unreachable!("the loom build never withdraws through the global registry")
}

/// Loom variant -- the endpoint table is gated out of the loom build; the
/// parked-handshake model drives [`EndpointCell`] directly.
#[cfg(loom)]
pub(crate) fn set_parked(_worker_id: WorkerId, _is_parked: bool) {
    // Models drive the cell directly; reaching this stub is a model bug.
    unreachable!("the loom build never parks through the global registry")
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use kwokka_core::Generation;

    use super::{
        INBOX_CAPACITY, enqueue, pop, publish_endpoint, set_parked, signal, withdraw_endpoint,
    };
    use crate::{task::TaskRef, worker::WorkerId};

    fn task_for(worker_id: u8) -> TaskRef {
        TaskRef::from_arena(worker_id, 1, Generation::ZERO)
    }

    fn worker(id: u8) -> WorkerId {
        let Ok(worker_id) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker_id
    }

    #[test]
    fn enqueue_then_pop_round_trips() {
        let task = task_for(10);
        let Ok(()) = enqueue(task) else {
            panic!("enqueue into an empty inbox must succeed");
        };
        assert_eq!(pop(worker(10)), Some(task));
        assert_eq!(pop(worker(10)), None);
    }

    #[test]
    fn pop_empty_inbox_returns_none() {
        assert_eq!(pop(worker(12)), None);
    }

    #[test]
    fn full_inbox_reports_err() {
        let task = task_for(11);
        for _ in 0..INBOX_CAPACITY {
            let Ok(()) = enqueue(task) else {
                panic!("inbox must accept up to its capacity");
            };
        }
        assert_eq!(enqueue(task), Err(task));
    }

    #[test]
    fn an_unpublished_endpoint_signal_is_silent() {
        // Worker 90 never publishes; the signal resolves to nothing and
        // must not reach a write.
        signal(90);
    }

    #[test]
    fn endpoint_gating_swallows_every_non_parked_signal() {
        let id = worker(91);
        publish_endpoint(id, 5, None);
        // Published but running: resolves to nothing, no write happens.
        signal(91);
        set_parked(id, true);
        withdraw_endpoint(id);
        // Withdrawn: the parked flag went with the cell, still silent.
        signal(91);
        set_parked(id, false);
    }
}
