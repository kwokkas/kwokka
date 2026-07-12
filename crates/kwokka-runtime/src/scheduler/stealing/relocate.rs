//! Cross-slab relocation of a suspended task -- the steal transport.
//!
//! A thief moves a sleeping task's erased cell out of the victim worker's
//! slab and installs it in its own. The protocol orders the atomic
//! interlock before any byte moves:
//!
//! 1. Claim the slot ([`AtomicTaskState::try_claim`]) -- thief-vs-thief exclusion.
//! 2. Gate on stealability -- a task with in-flight I/O or linked children stays on its submitting
//!    ring and owning slab.
//! 3. Retire the source ([`AtomicTaskState::try_retire`], `Sleeping -> Retired`). Failure means a
//!    concurrent wake or cancel won the slot; the move aborts having touched no bytes.
//! 4. Copy the cell into the transport. After `Retired` no transition out of `Sleeping` can
//!    succeed, so the task can be neither enqueued nor polled while the bytes move.
//!
//! The source slot is left a `Retired` husk under its live generation; the
//! victim worker releases it via
//! [`Slab::retire_slot`](kwokka_core::slab::Slab::retire_slot), keyed by
//! [`StolenTask::victim_key`]. The retired state always lands before the
//! generation rolls, so a stale handle either observes `Retired` through
//! the live generation or misses the rolled generation entirely.

use core::{marker::PhantomData, mem::MaybeUninit, ptr};

use kwokka_core::{
    id::Pip,
    slab::{Slab, SlabKey},
};

use crate::task::{
    TaskRef,
    cell::{slot::TaskSlot, state::AtomicTaskState},
};

/// A task body in flight between a victim slab and a thief slab.
///
/// Owns exactly the relocated cell plus the identity needed to forward
/// stale handles. Holds no pointer into either slab: the cell travels by
/// value, and the source slot is already `Retired` when this value exists.
///
/// Dropping an uninstalled `StolenTask` releases the carried future through
/// the cell's own drop glue exactly once: the transport state is reset to
/// `Sleeping` at copy time, and the `Retired` husk left at the source drops
/// nothing.
pub(crate) struct StolenTask {
    /// The relocated cell, header at offset 0.
    cell: TaskSlot,
    /// Steal-stable identity, read from the relocated header. Identity
    /// never changes across a move; the location route is keyed by
    /// [`StolenTask::victim_key`], not by id.
    pip: Pip,
    /// Source slot key, for the victim-side release.
    victim_key: SlabKey,
    /// Suppresses the erased cell's accidental auto-`Send` so the explicit
    /// assertion below stays the single reviewed source of the transport's
    /// thread-crossing contract.
    send_guard: PhantomData<*mut ()>,
}

impl StolenTask {
    /// Steal-stable identity of the relocated task.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed only in test assertions; observability consumers land in a later PR"
        )
    )]
    pub(crate) const fn pip(&self) -> Pip {
        self.pip
    }

    /// Source slot awaiting the victim-side
    /// [`Slab::retire_slot`](kwokka_core::slab::Slab::retire_slot) release.
    pub(crate) const fn victim_key(&self) -> SlabKey {
        self.victim_key
    }
}

// SAFETY:
// Invariant: a `StolenTask` is exclusive single-transfer ownership of the
// relocated cell. `move_out` commits `Sleeping -> Retired` and holds the
// relocation claim before any byte is copied, so the victim side can
// neither poll nor enqueue the task while the transport exists, and the
// `Retired` husk left behind drops nothing.
// Precondition: every future entering a stealing crew's slab is admitted
// `Send + 'static` (the `Runtime::<Stealing>::block_on` bound and the
// send-bounded scope spawn), and steal requests name only crew siblings:
// an affine worker's slab, which admits `!Send` futures, is never a
// victim because live worker-id claims are disjoint and a thief sweeps
// only its own crew's contiguous id block.
// Failure mode: shipping a `!Send` body across threads would run state
// pinned to the victim thread on the thief -- undefined behavior at the
// future's thread-affinity boundary.
unsafe impl Send for StolenTask {}

/// Notice from a task's new owner back to the victim worker: the task
/// relocated out of `victim_key` has settled.
///
/// The victim's note drain marks the `Retired` husk remote-settled, which
/// is what lets the parent's walk count the relocated child settled and
/// the reap release the husk slot. Sent by the new owner when the task
/// reaches a settled state; carried by the per-victim registry ring.
#[derive(Clone, Copy)]
pub(crate) struct SettledNote {
    /// The husk slot awaiting release on the victim.
    pub(crate) victim_key: SlabKey,
}

/// A thief's claim ticket: the reserved destination its victim ships to.
///
/// The thief reserves the destination in its own slab before posting the
/// request, so the victim can record the forwarding route to a key that
/// is already promised -- the route and the source retire commit in one
/// straight-line serve step, with no half-entry window.
#[derive(Clone, Copy)]
pub(crate) struct StealRequest {
    /// Worker that posted the request and owns the reserved slot.
    pub(crate) thief_id: u8,
    /// Promised destination: the thief's reserved slot, pre-resolved to
    /// the [`TaskRef`] the relocated task will live under.
    pub(crate) dest: TaskRef,
}

/// One reply through a thief's handoff ring: a shipped body or a decline.
///
/// Self-contained by design -- the receive step needs no memory of which
/// victim it asked, so a reply that outlives a shutdown-bound requester
/// still resolves cleanly.
#[expect(
    clippy::large_enum_variant,
    reason = "the delivery arm carries the relocated cell by value -- the transport's purpose; \
              boxing is banned by the allocation policy and ring slots are max-variant-sized \
              regardless"
)]
pub(crate) enum HandoffMsg {
    /// The victim relocated a task; install it under the promised `dest`.
    Delivered {
        /// The promised destination carried back from the request.
        dest: TaskRef,
        /// Worker the body shipped from -- the husk under
        /// [`StolenTask::victim_key`] lives in this worker's slab.
        victim_id: u8,
        /// The relocated body in transport.
        task: StolenTask,
    },
    /// The victim had nothing stealable; the thief unreserves `dest`.
    Declined {
        /// The promised destination to withdraw.
        dest: TaskRef,
    },
}

/// Moves the sleeping task in `slot` out of the victim slab into transport.
///
/// `slot` is the cell that `key` resolves to in the victim slab. The
/// caller performs that resolution: the steal loop forms the shared slot
/// reference through its per-slot raw window without asserting the whole
/// slab, so the proof obligation for cross-worker access lives at the call
/// site, not behind this signature. A mismatched `key` corrupts the
/// release and forwarding records, not memory.
///
/// Returns `None` -- with the slot's bytes untouched -- when another thief
/// holds the claim, the task is not stealable (in-flight I/O, linked
/// children, or a pinned task), or a concurrent wake or cancel wins the
/// retire window. On
/// success the source slot is a `Retired` husk whose claim stays held as
/// the move-window marker; the victim worker releases the slot itself,
/// keyed by [`StolenTask::victim_key`]. The claim grants the holder
/// exclusive access to the slot body (see [`AtomicTaskState::try_claim`]),
/// so the victim-side reclaim paths must not free a claimed slot.
pub(crate) fn move_out(slot: &TaskSlot, key: SlabKey) -> Option<StolenTask> {
    let header = slot.header();
    if !header.state.try_claim() {
        return None;
    }
    if header.in_flight_ops != 0 || header.first_child.is_some() || header.is_pinned {
        header.state.release_claim();
        return None;
    }
    if header.state.try_retire().is_err() {
        header.state.release_claim();
        return None;
    }
    let mut cell = copy_cell(slot);
    cell.header_mut().state = AtomicTaskState::new();
    let pip = cell.header().pip;
    Some(StolenTask {
        cell,
        pip,
        victim_key: key,
        send_guard: PhantomData,
    })
}

/// Moves the woken task in `slot` out of the victim slab into transport.
///
/// Mirrors [`move_out`] for the `Woken` state: the retire CAS commits
/// `Woken -> Retired`, after which no poll can enter (`Woken` guard fails)
/// and no further wake can commit (`Retired` is terminal for the CAS).
/// The stealability guards mirror those in [`move_out`] -- in-flight I/O,
/// linked children, and pinned tasks are all rejected before any bytes move.
///
/// Returns `None` -- with the slot's bytes untouched -- when another thief
/// holds the claim, the task is not stealable, or a concurrent cancel wins
/// the retire window. On success the source slot is a `Retired` husk whose
/// claim stays held as the move-window marker; the victim worker releases
/// the slot itself, keyed by [`StolenTask::victim_key`].
pub(crate) fn move_out_woken(slot: &TaskSlot, key: SlabKey) -> Option<StolenTask> {
    let header = slot.header();
    if !header.state.try_claim() {
        return None;
    }
    if header.in_flight_ops != 0 || header.first_child.is_some() || header.is_pinned {
        header.state.release_claim();
        return None;
    }
    if header.state.try_retire_woken().is_err() {
        header.state.release_claim();
        return None;
    }
    let mut cell = copy_cell(slot);
    cell.header_mut().state = AtomicTaskState::new();
    let pip = cell.header().pip;
    Some(StolenTask {
        cell,
        pip,
        victim_key: key,
        send_guard: PhantomData,
    })
}

/// Copies the source cell's bytes into an owned transport cell.
const fn copy_cell(source: &TaskSlot) -> TaskSlot {
    let mut cell = MaybeUninit::<TaskSlot>::uninit();
    // SAFETY:
    // Invariant: the source state committed `Sleeping or Woken -> Retired`
    // before this call, so no transition out of the pre-retire state can
    // succeed any more: the task can be neither enqueued (wake fails its
    // compare-exchange against `Retired`) nor polled (poll entry requires
    // `Woken`), and concurrent wake or cancel attempts cannot commit a write
    // after `Retired` is committed -- their compare-exchange failures do not
    // constitute conflicting writes under the Rust/C++ memory model. The
    // copy therefore races no write, and the non-atomic read of the
    // atomic's bytes is sound on the same basis. The `UnsafeCell` interior
    // is not frozen by the shared borrow, so the read through it carries
    // in-bounds shared-read-write provenance. The destination is a fresh
    // local, so the ranges never overlap, and every byte pattern is a
    // valid `TaskSlot`.
    // Precondition: the caller performed the retire compare-exchange on
    // this very slot and holds the relocation claim.
    // Failure mode: copying before the retire commits races a concurrent
    // poll's exclusive write to the future bytes -- undefined behavior.
    unsafe {
        ptr::copy_nonoverlapping(
            ptr::from_ref(source).cast::<u8>(),
            cell.as_mut_ptr().cast::<u8>(),
            size_of::<TaskSlot>(),
        );
        cell.assume_init()
    }
}

/// Installs a relocated task into the thief slab under its promised key.
///
/// The thief reserved `dest` before posting the steal request, so the
/// victim recorded the forwarding route to this exact key at ship time;
/// installing anywhere else would strand that route. Returns `None` once
/// the body lands. A `dest` that does not name this slab's currently
/// reserved slot hands the transport back; dropping it releases the
/// carried future exactly once.
#[must_use]
pub(crate) fn move_in(
    thief: &mut Slab<TaskSlot>,
    dest: SlabKey,
    stolen: StolenTask,
) -> Option<StolenTask> {
    let StolenTask {
        cell,
        pip,
        victim_key,
        send_guard,
    } = stolen;
    let Err(cell) = thief.install(dest, cell) else {
        return None;
    };
    Some(StolenTask {
        cell,
        pip,
        victim_key,
        send_guard,
    })
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
    };

    use kwokka_core::{
        Generation,
        id::{Namespace, Pip},
    };

    use super::*;
    use crate::task::cell::{header::Slot, state::TaskState};

    struct Inert;
    impl Future for Inert {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    /// Future that counts its polls and its drops through shared counters.
    struct Probe<'a> {
        polls: &'a AtomicUsize,
        drops: &'a AtomicUsize,
    }

    impl Future for Probe<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(())
        }
    }

    impl Drop for Probe<'_> {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn seed<F: Future>(slab: &mut Slab<TaskSlot>, pip: Pip, future: F) -> SlabKey {
        let cell = Slot::new(pip, Namespace::ROOT, future).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        key
    }

    fn steal(victim: &Slab<TaskSlot>, key: SlabKey) -> Option<StolenTask> {
        move_out(victim.get(key)?, key)
    }

    fn task_ref(worker: u8, index: u32) -> TaskRef {
        TaskRef::from_slab(worker, SlabKey::new(index, Generation::from_raw(1)))
    }

    #[test]
    fn a_relocated_task_polls_to_completion_on_the_thief() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(
            &mut victim,
            Pip::detached(),
            Probe {
                polls: &polls,
                drops: &drops,
            },
        );
        let Some(stolen) = steal(&victim, key) else {
            panic!("a sleeping task must relocate");
        };
        assert!(victim.retire_slot(stolen.victim_key()));
        let Ok(dest) = thief.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        assert!(
            move_in(&mut thief, dest, stolen).is_none(),
            "install under the promised key must succeed",
        );
        let Some(slot) = thief.get_mut(dest) else {
            panic!("the relocated task must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("the relocated task must wake from Sleeping");
        };
        let Ok(()) = slot.header().state.try_start_poll() else {
            panic!("Woken -> Running must succeed");
        };
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            slot.poll_via_vtable(&mut context),
            Poll::Ready(())
        ));
        let Ok(()) = slot.header().state.complete() else {
            panic!("Running -> Done must succeed");
        };
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_transport_state_resets_to_sleeping_unclaimed() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(stolen) = steal(&victim, key) else {
            panic!("a sleeping task must relocate");
        };
        let header = stolen.cell.header();
        assert_eq!(header.state.load(), TaskState::Sleeping);
        assert!(
            header.state.try_claim(),
            "the transport claim must be released by the copy reset",
        );
    }

    #[test]
    fn identity_survives_the_move() {
        let pip = Pip::issue(3, 7);
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, pip, Inert);
        let Some(stolen) = steal(&victim, key) else {
            panic!("a sleeping task must relocate");
        };
        assert_eq!(stolen.pip(), pip);
        let Ok(dest) = thief.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        assert!(
            move_in(&mut thief, dest, stolen).is_none(),
            "install under the promised key must succeed",
        );
        let Some(slot) = thief.get(dest) else {
            panic!("the relocated task must resolve");
        };
        assert_eq!(slot.header().pip, pip);
    }

    #[test]
    fn an_unpromised_destination_returns_the_transport() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(
            &mut victim,
            Pip::detached(),
            Probe {
                polls: &polls,
                drops: &drops,
            },
        );
        let Some(stolen) = steal(&victim, key) else {
            panic!("a sleeping task must relocate");
        };
        let unpromised = SlabKey::new(0, Generation::from_raw(1));
        let Some(returned) = move_in(&mut thief, unpromised, stolen) else {
            panic!("an unpromised key must reject the install");
        };
        drop(returned);
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "the returned transport must still own the future",
        );
    }

    #[test]
    fn in_flight_io_blocks_the_move() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get_mut(key) else {
            panic!("the task must resolve");
        };
        slot.header_mut().in_flight_ops = 1;
        assert!(steal(&victim, key).is_none());
        let Some(slot) = victim.get(key) else {
            panic!("the task must stay put");
        };
        assert_eq!(slot.header().state.load(), TaskState::Sleeping);
        assert!(
            slot.header().state.try_claim(),
            "an aborted move must release its claim",
        );
    }

    #[test]
    fn a_pinned_task_blocks_the_move() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get_mut(key) else {
            panic!("the task must resolve");
        };
        slot.header_mut().is_pinned = true;
        assert!(steal(&victim, key).is_none());
        let Some(slot) = victim.get(key) else {
            panic!("the task must stay put");
        };
        assert_eq!(slot.header().state.load(), TaskState::Sleeping);
        assert!(
            slot.header().state.try_claim(),
            "an aborted move must release its claim",
        );
    }

    #[test]
    fn linked_children_block_the_move() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get_mut(key) else {
            panic!("the task must resolve");
        };
        slot.header_mut().first_child = Some(task_ref(0, 9));
        assert!(steal(&victim, key).is_none());
        let Some(slot) = victim.get(key) else {
            panic!("the task must stay put");
        };
        assert_eq!(slot.header().state.load(), TaskState::Sleeping);
    }

    #[test]
    fn a_lost_retire_race_moves_no_bytes() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        assert!(steal(&victim, key).is_none());
        let Some(slot) = victim.get(key) else {
            panic!("the task must stay put");
        };
        assert_eq!(slot.header().state.load(), TaskState::Woken);
        assert!(
            slot.header().state.try_claim(),
            "an aborted move must release its claim",
        );
    }

    #[test]
    fn the_woken_transport_state_resets_to_sleeping_unclaimed() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let Some(slot) = victim.get(key) else {
            panic!("the task must still resolve after wake");
        };
        let Some(stolen) = move_out_woken(slot, key) else {
            panic!("a woken task must relocate via move_out_woken");
        };
        let header = stolen.cell.header();
        assert_eq!(header.state.load(), TaskState::Sleeping);
        assert!(
            header.state.try_claim(),
            "the transport claim must be released by the copy reset",
        );
    }

    #[test]
    fn a_held_claim_admits_no_second_thief() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        assert!(slot.header().state.try_claim());
        assert!(steal(&victim, key).is_none());
        let Some(slot) = victim.get(key) else {
            panic!("the task must stay put");
        };
        assert_eq!(slot.header().state.load(), TaskState::Sleeping);
    }

    #[test]
    fn an_uninstalled_transport_drops_the_future_exactly_once() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        {
            let mut victim = Slab::<TaskSlot>::new(1);
            let key = seed(
                &mut victim,
                Pip::detached(),
                Probe {
                    polls: &polls,
                    drops: &drops,
                },
            );
            let Some(stolen) = steal(&victim, key) else {
                panic!("a sleeping task must relocate");
            };
            assert!(victim.retire_slot(stolen.victim_key()));
            drop(stolen);
        }
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "the husk must drop nothing and the transport exactly the future",
        );
    }

    #[test]
    fn the_husk_stays_retired_until_the_victim_releases_it() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(stolen) = steal(&victim, key) else {
            panic!("a sleeping task must relocate");
        };
        let Some(slot) = victim.get(key) else {
            panic!("the husk must still resolve through its live generation");
        };
        assert_eq!(slot.header().state.load(), TaskState::Retired);
        assert!(victim.retire_slot(stolen.victim_key()));
        assert!(victim.get(key).is_none());
        drop(stolen);
        let reused = seed(&mut victim, Pip::detached(), Inert);
        assert_eq!(reused.index(), key.index());
    }
}
