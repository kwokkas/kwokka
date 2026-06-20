//! Process-wide wake registry -- one bounded inbox per worker, keyed by
//! [`WorkerId`].
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

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "steal")]
use crate::scheduler::stealing::relocate::{HandoffMsg, SettledNote, StealRequest};
#[cfg(not(loom))]
use crate::sync::mpsc::MpscRing;
use crate::task::TaskRef;
use crate::worker::WorkerId;
#[cfg(not(loom))]
use crate::worker::endpoint::EndpointCell;

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

/// One wake endpoint per worker, beside its inbox. A producer consults the
/// target's cell after a successful [`enqueue`] and signals the published
/// fd only when the worker is parked; the owning worker publishes at
/// bootstrap and brackets each park (see [`EndpointCell`]).
#[cfg(not(loom))]
static ENDPOINTS: [EndpointCell; INBOX_SLOTS] = [const { EndpointCell::new() }; INBOX_SLOTS];

/// Publishes `event_fd` as the wake target of `worker_id`.
#[cfg(not(loom))]
pub(crate) fn publish_endpoint(worker_id: WorkerId, event_fd: i32) {
    ENDPOINTS[worker_id.raw() as usize].publish(event_fd);
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
    let Some(fd) = ENDPOINTS[worker_id as usize].signal_target() else {
        return;
    };
    // IGNORE: a failed eventfd write races endpoint withdrawal; the worker
    // is already draining toward shutdown and needs no unpark.
    let _ = kwokka_io::wake::signal_wake_fd(fd);
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
pub(crate) fn publish_endpoint(_worker_id: WorkerId, _event_fd: i32) {
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

/// Loom variant -- the inbox table is gated out of the loom build, and no
/// loom model drains wakes through the global registry.
#[cfg(loom)]
pub(crate) fn pop(_worker_id: WorkerId) -> Option<TaskRef> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never drains wakes through the global registry")
}

/// Per-victim settled-note capacity. Bounded by the victim's husk count,
/// which the slab capacity bounds in turn; sized to match the wake inbox.
#[cfg(all(feature = "steal", not(loom)))]
const SETTLED_NOTE_CAPACITY: usize = 256;

/// One settled-note ring per worker, beside its wake inbox. The new owner
/// of a relocated task pushes the note; the victim drains its ring each
/// pass, ahead of the wake drain, marking the named husks reap-eligible.
#[cfg(all(feature = "steal", not(loom)))]
static SETTLED_NOTES: [MpscRing<SettledNote, SETTLED_NOTE_CAPACITY>; INBOX_SLOTS] =
    [const { MpscRing::new() }; INBOX_SLOTS];

/// Routes a settled note to the victim worker that owns the husk.
///
/// # Errors
///
/// Returns the note back when the victim's ring is full; the sender keeps
/// it and retries on a later pass. The husk stays unreaped meanwhile,
/// which only delays the slot's release.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn push_settled(victim_id: u8, note: SettledNote) -> Result<(), SettledNote> {
    SETTLED_NOTES[victim_id as usize].push(note)
}

/// Pops the next settled note for `worker_id`, or `None` when its ring is
/// empty. Called by the owning worker's note drain.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn pop_settled(worker_id: WorkerId) -> Option<SettledNote> {
    SETTLED_NOTES[worker_id.raw() as usize].pop()
}

/// Loom variant -- the settled-note table is gated out of the loom build;
/// models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn push_settled(_victim_id: u8, _note: SettledNote) -> Result<(), SettledNote> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never routes settled notes through the global registry")
}

/// Loom variant -- the settled-note table is gated out of the loom build;
/// models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn pop_settled(_worker_id: WorkerId) -> Option<SettledNote> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never drains settled notes through the global registry")
}

/// Per-victim steal-request capacity. The single-in-flight discipline
/// bounds live requests to one per thief, so the ring only absorbs the
/// burst between two victim passes; a full ring fails the push and the
/// thief retries on a later idle pass.
#[cfg(all(feature = "steal", not(loom)))]
const STEAL_REQUEST_CAPACITY: usize = 8;

/// Per-thief handoff capacity. One reply is ever pending under the
/// single-in-flight discipline; 2 keeps the ring mask a power of two.
#[cfg(all(feature = "steal", not(loom)))]
const HANDOFF_CAPACITY: usize = 2;

/// One steal-request ring per victim, beside its wake inbox. A thief
/// posts the destination it reserved in its own slab; the victim serves
/// one request per pass.
#[cfg(all(feature = "steal", not(loom)))]
static STEAL_REQUESTS: [MpscRing<StealRequest, STEAL_REQUEST_CAPACITY>; INBOX_SLOTS] =
    [const { MpscRing::new() }; INBOX_SLOTS];

/// One handoff ring per thief, carrying the victim's reply: the relocated
/// body or a decline. Bodies travel by value, so this is the one table
/// whose element is slot-sized; the single-in-flight discipline is what
/// keeps the footprint flat.
#[cfg(all(feature = "steal", not(loom)))]
static HANDOFFS: [MpscRing<HandoffMsg, HANDOFF_CAPACITY>; INBOX_SLOTS] =
    [const { MpscRing::new() }; INBOX_SLOTS];

/// Routes a steal request to the victim worker that will serve it.
///
/// # Errors
///
/// Returns the request back when the victim's ring is full; the thief
/// withdraws its reservation and retries on a later idle pass.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn push_steal_request(victim_id: u8, request: StealRequest) -> Result<(), StealRequest> {
    STEAL_REQUESTS[victim_id as usize].push(request)
}

/// Pops the next pending steal request for `worker_id`, or `None` when
/// its ring is empty. Called by the owning worker's serve step.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn pop_steal_request(worker_id: WorkerId) -> Option<StealRequest> {
    STEAL_REQUESTS[worker_id.raw() as usize].pop()
}

/// Routes a handoff reply to the thief that posted the request.
///
/// # Errors
///
/// Returns the message back when the thief's ring is full -- unreachable
/// under the single-in-flight discipline; the caller owns the retry.
#[cfg(all(feature = "steal", not(loom)))]
#[expect(
    clippy::result_large_err,
    reason = "the bounce mirrors the ring contract: the returned message is transient, moved \
              back to the sender for a later-pass retry, never stored"
)]
pub(crate) fn push_handoff(thief_id: u8, msg: HandoffMsg) -> Result<(), HandoffMsg> {
    HANDOFFS[thief_id as usize].push(msg)
}

/// Pops the next handoff reply for `worker_id`, or `None` when its ring
/// is empty. Called by the owning worker's receive step.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn pop_handoff(worker_id: WorkerId) -> Option<HandoffMsg> {
    HANDOFFS[worker_id.raw() as usize].pop()
}

/// Whether `worker_id` has a pending steal request.
///
/// Consulted inside the park bracket: a request that landed before the
/// parked flag was visible got a swallowed signal, so the bracket
/// re-checks the ring itself before committing to the park.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn has_steal_request(worker_id: WorkerId) -> bool {
    !STEAL_REQUESTS[worker_id.raw() as usize].is_empty()
}

/// Whether `worker_id` has a pending handoff reply.
///
/// Consulted inside the park bracket, like
/// [`has_steal_request`] -- a reply whose signal raced the parked flag
/// must abort the park or the thief sleeps on a delivered body.
#[cfg(all(feature = "steal", not(loom)))]
pub(crate) fn has_handoff(worker_id: WorkerId) -> bool {
    !HANDOFFS[worker_id.raw() as usize].is_empty()
}

/// Loom variant -- the steal-request table is gated out of the loom
/// build; models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn push_steal_request(
    _victim_id: u8,
    _request: StealRequest,
) -> Result<(), StealRequest> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never routes steal requests through the global registry")
}

/// Loom variant -- the steal-request table is gated out of the loom
/// build; models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn pop_steal_request(_worker_id: WorkerId) -> Option<StealRequest> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never drains steal requests through the global registry")
}

/// Loom variant -- the handoff table is gated out of the loom build;
/// models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn push_handoff(_thief_id: u8, _msg: HandoffMsg) -> Result<(), HandoffMsg> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never routes handoffs through the global registry")
}

/// Loom variant -- the handoff table is gated out of the loom build;
/// models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn pop_handoff(_worker_id: WorkerId) -> Option<HandoffMsg> {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never drains handoffs through the global registry")
}

/// Loom variant -- the steal-request table is gated out of the loom
/// build; models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn has_steal_request(_worker_id: WorkerId) -> bool {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never consults steal requests through the global registry")
}

/// Loom variant -- the handoff table is gated out of the loom build;
/// models drive the ring directly.
#[cfg(all(feature = "steal", loom))]
pub(crate) fn has_handoff(_worker_id: WorkerId) -> bool {
    // Models drive the ring directly; reaching this stub is a model bug.
    unreachable!("the loom build never consults handoffs through the global registry")
}

/// Process-global occupancy bitmap handing out unique [`WorkerId`]s.
///
/// One bit per id over the 7-bit worker space, so two words cover every slot
/// of the per-worker tables. Claims are compare-exchange loops on plain
/// atomics: no lock, no allocation, const-constructible.
struct WorkerIdAllocator {
    /// Occupancy bits; bit `i` of word `w` covers id `w * 64 + i`.
    words: [AtomicU64; 2],
}

/// The single process-wide allocator. Two runtimes in one process claim
/// distinct ids here, so their per-worker table slots never collide.
static WORKER_ALLOC: WorkerIdAllocator = WorkerIdAllocator {
    words: [AtomicU64::new(0), AtomicU64::new(0)],
};

impl WorkerIdAllocator {
    /// Claims the lowest free id, or `None` when the space is exhausted.
    fn claim_one(&self) -> Option<WorkerId> {
        for (word_index, word) in self.words.iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            while bits != u64::MAX {
                let free = (!bits).trailing_zeros();
                match word.compare_exchange_weak(
                    bits,
                    bits | (1u64 << free),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let Ok(raw) = u8::try_from(word_index * 64 + free as usize) else {
                            return None;
                        };
                        return WorkerId::new(raw).ok();
                    }
                    Err(actual) => bits = actual,
                }
            }
        }
        None
    }

    /// Claims `count` contiguous ids, or `None` when no run fits.
    ///
    /// Bounded to one word (64 ids) so the claim stays a single
    /// compare-exchange; a spurious failure rescans the word from the start.
    fn claim_block(&self, count: usize) -> Option<WorkerId> {
        if count == 0 || count > 64 {
            return None;
        }
        let run = if count == 64 {
            u64::MAX
        } else {
            (1u64 << count) - 1
        };
        for (word_index, word) in self.words.iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            let mut shift = 0;
            while shift + count <= 64 {
                let mask = run << shift;
                if bits & mask != 0 {
                    shift += 1;
                    continue;
                }
                match word.compare_exchange_weak(
                    bits,
                    bits | mask,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let Ok(raw) = u8::try_from(word_index * 64 + shift) else {
                            return None;
                        };
                        return WorkerId::new(raw).ok();
                    }
                    Err(actual) => {
                        bits = actual;
                        shift = 0;
                    }
                }
            }
        }
        None
    }

    /// Releases one claimed id back to the pool.
    fn release(&self, id: WorkerId) {
        let raw = id.raw() as usize;
        let mask = 1u64 << (raw % 64);
        self.words[raw / 64].fetch_and(!mask, Ordering::Release);
    }

    /// Releases a block claimed by [`WorkerIdAllocator::claim_block`].
    fn release_block(&self, start: WorkerId, count: usize) {
        if count == 0 || count > 64 {
            return;
        }
        let run = if count == 64 {
            u64::MAX
        } else {
            (1u64 << count) - 1
        };
        let raw = start.raw() as usize;
        // A claimed block never crosses a word by construction.
        self.words[raw / 64].fetch_and(!(run << (raw % 64)), Ordering::Release);
    }
}

/// Claims the lowest free worker id for a new runtime.
pub(crate) fn claim_one() -> Option<WorkerId> {
    WORKER_ALLOC.claim_one()
}

/// Claims `count` contiguous worker ids for a multi-worker runtime.
pub(crate) fn claim_block(count: usize) -> Option<WorkerId> {
    WORKER_ALLOC.claim_block(count)
}

/// Releases one claimed worker id.
pub(crate) fn release(id: WorkerId) {
    WORKER_ALLOC.release(id);
}

/// Releases a contiguous block of worker ids.
pub(crate) fn release_block(start: WorkerId, count: usize) {
    WORKER_ALLOC.release_block(start, count);
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use kwokka_core::Generation;

    // Each test uses a distinct worker id so its inbox slot is never shared
    // with another test thread: the registry is a process-wide static and
    // these tests exercise the single-consumer drain path.

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
    fn claims_are_distinct_while_held() {
        let Some(first) = claim_one() else {
            panic!("the id space must have a free slot");
        };
        let Some(second) = claim_one() else {
            panic!("the id space must have a second free slot");
        };
        assert_ne!(first, second, "two live claims never share an id");
        release(first);
        release(second);
    }

    #[test]
    fn released_id_is_claimable_again() {
        let Some(first) = claim_one() else {
            panic!("the id space must have a free slot");
        };
        release(first);
        let Some(second) = claim_one() else {
            panic!("a released id must leave the pool claimable");
        };
        release(second);
    }

    #[test]
    fn block_claims_do_not_overlap() {
        let Some(first) = claim_block(8) else {
            panic!("an 8-wide block must fit the id space");
        };
        let Some(second) = claim_block(8) else {
            panic!("a second 8-wide block must fit the id space");
        };
        let lower = first.raw().min(second.raw());
        let upper = first.raw().max(second.raw());
        assert!(lower + 8 <= upper, "blocks must be disjoint");
        release_block(first, 8);
        release_block(second, 8);
    }

    #[test]
    fn block_bounds_are_rejected() {
        assert_eq!(claim_block(0), None);
        assert_eq!(claim_block(65), None);
    }

    #[cfg(feature = "steal")]
    #[test]
    fn a_settled_note_round_trips_to_the_victim() {
        use crate::scheduler::stealing::relocate::SettledNote;
        use kwokka_core::slab::SlabKey;
        let key = SlabKey::new(3, Generation::from_raw(1));
        let Ok(()) = push_settled(13, SettledNote { victim_key: key }) else {
            panic!("push into an empty settled ring must succeed");
        };
        let Some(note) = pop_settled(worker(13)) else {
            panic!("the pushed note must drain");
        };
        assert_eq!(note.victim_key.index(), key.index());
        assert_eq!(note.victim_key.generation(), key.generation());
        assert!(pop_settled(worker(13)).is_none());
    }

    #[cfg(feature = "steal")]
    #[test]
    fn a_steal_request_round_trips_to_the_victim() {
        use crate::scheduler::stealing::relocate::StealRequest;
        use kwokka_core::slab::SlabKey;
        assert!(pop_steal_request(worker(14)).is_none());
        let dest = TaskRef::from_slab(15, SlabKey::new(2, Generation::from_raw(1)));
        let Ok(()) = push_steal_request(14, StealRequest { thief_id: 15, dest }) else {
            panic!("push into an empty request ring must succeed");
        };
        let Some(request) = pop_steal_request(worker(14)) else {
            panic!("the pushed request must drain");
        };
        assert_eq!(request.thief_id, 15);
        assert_eq!(request.dest, dest);
        assert!(pop_steal_request(worker(14)).is_none());
    }

    #[cfg(feature = "steal")]
    #[test]
    fn a_handoff_reply_round_trips_to_the_thief() {
        use kwokka_core::{id::Pip, namespace::Namespace};

        use crate::{
            scheduler::stealing::relocate::{HandoffMsg, move_out},
            task::{header::Slot, slot::TaskSlot},
        };
        use kwokka_core::slab::{Slab, SlabKey};

        let mut victim = Slab::<TaskSlot>::new(1);
        let cell = Slot::new(
            Pip::issue(16, 1),
            Namespace::ROOT,
            core::future::pending::<()>(),
        )
        .into_erased();
        let Ok(key) = victim.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        let Some(stolen) = move_out(slot, key) else {
            panic!("a sleeping task must relocate");
        };
        let dest = TaskRef::from_slab(17, SlabKey::new(0, Generation::from_raw(1)));
        let delivery = HandoffMsg::Delivered {
            dest,
            victim_id: 16,
            task: stolen,
        };
        let Ok(()) = push_handoff(17, delivery) else {
            panic!("push into an empty handoff ring must succeed");
        };
        let Some(HandoffMsg::Delivered {
            dest: replied,
            victim_id,
            task,
        }) = pop_handoff(worker(17))
        else {
            panic!("the pushed delivery must drain");
        };
        assert_eq!(replied, dest);
        assert_eq!(victim_id, 16);
        assert_eq!(task.nid(), Pip::issue(16, 1));
        assert_eq!(task.victim_key().index(), key.index());
        assert!(pop_handoff(worker(17)).is_none());

        let Ok(()) = push_handoff(17, HandoffMsg::Declined { dest }) else {
            panic!("push of a decline must succeed");
        };
        let Some(HandoffMsg::Declined { dest: declined }) = pop_handoff(worker(17)) else {
            panic!("the pushed decline must drain");
        };
        assert_eq!(declined, dest);
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
        publish_endpoint(id, 5);
        // Published but running: resolves to nothing, no write happens.
        signal(91);
        set_parked(id, true);
        withdraw_endpoint(id);
        // Withdrawn: the parked flag went with the cell, still silent.
        signal(91);
        set_parked(id, false);
    }
}
