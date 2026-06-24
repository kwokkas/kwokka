//! Cross-worker steal channels: settled-note ring, steal-request ring, and
//! handoff ring.
//!
//! All three tables are gated behind `#[cfg(feature = "steal")]` and are
//! absent from loom builds (loom models drive the rings directly).

#[cfg(feature = "steal")]
use crate::scheduler::stealing::relocate::{HandoffMsg, SettledNote, StealRequest};
#[cfg(all(feature = "steal", not(loom)))]
use crate::sync::mpsc::MpscRing;
#[cfg(all(feature = "steal", not(loom)))]
use crate::task::TaskRef;
use crate::worker::WorkerId;

/// One inbox slot per routable [`WorkerId`] (the 7-bit worker space).
#[cfg(all(feature = "steal", not(loom)))]
const INBOX_SLOTS: usize = TaskRef::WORKER_ID_MAX as usize + 1;

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

#[cfg(feature = "steal")]
#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use kwokka_core::Generation;

    use super::{
        pop_handoff, pop_settled, pop_steal_request, push_handoff, push_settled, push_steal_request,
    };
    use crate::{task::TaskRef, worker::WorkerId};

    fn worker(id: u8) -> WorkerId {
        let Ok(worker_id) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker_id
    }

    #[test]
    fn a_settled_note_round_trips_to_the_victim() {
        use kwokka_core::slab::SlabKey;

        use crate::scheduler::stealing::relocate::SettledNote;
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

    #[test]
    fn a_steal_request_round_trips_to_the_victim() {
        use kwokka_core::slab::SlabKey;

        use crate::scheduler::stealing::relocate::StealRequest;
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

    #[test]
    fn a_handoff_reply_round_trips_to_the_thief() {
        use kwokka_core::{
            id::Pip,
            namespace::Namespace,
            slab::{Slab, SlabKey},
        };

        use crate::{
            scheduler::stealing::relocate::{HandoffMsg, move_out},
            task::cell::{header::Slot, slot::TaskSlot},
        };

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
        assert_eq!(task.pip(), Pip::issue(16, 1));
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
}
