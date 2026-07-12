//! The steal handoff cores -- request preparation, the victim's serve
//! step, and the thief's receive step, free of channel I/O.
//!
//! The run-loop composes these around the per-worker steal channels: a
//! thief prepares a request against its own slab, the victim serves one
//! request against its own slab and forward table, and the thief receives
//! the reply into its promised slot. Keeping the cores channel-free keeps
//! every step testable over plain slabs and keeps this module independent
//! of the worker registry.

use kwokka_core::slab::{Slab, SlabKey};

use crate::{
    scheduler::{
        queue::LocalRunQueue,
        stealing::relocate::{
            ForwardTable, HandoffMsg, StealRequest, move_in, move_out, move_out_woken,
        },
    },
    task::{TaskRef, cell::slot::TaskSlot, state::TaskState},
};

/// Where a relocated resident came from: the victim worker and the husk
/// slot awaiting this worker's settled note.
#[derive(Clone, Copy)]
pub(crate) struct Origin {
    /// Worker whose slab holds the `Retired` husk.
    pub(crate) victim_id: u8,
    /// The husk slot in the victim's slab.
    pub(crate) victim_key: SlabKey,
}

/// Origin records for tasks relocated into this worker's slab, keyed by
/// destination slot index -- the thief-side mirror of the victim's
/// [`ForwardTable`].
///
/// An entry is recorded at install time and taken when the settled note
/// is pushed, so a slot index never carries a stale origin into its next
/// resident: the take precedes the slot's release on every settle path.
pub(crate) struct ForwardOrigin {
    entries: Vec<Option<Origin>>,
}

impl ForwardOrigin {
    /// Empty table sized to the owning slab's capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(None);
        }
        Self { entries }
    }

    /// Records that the resident installed at `index` came from `origin`.
    ///
    /// # Panics
    ///
    /// Panics if `index` lies outside the table -- the thief records only
    /// keys reserved from its own equally-sized slab.
    pub(crate) fn record(&mut self, index: u32, origin: Origin) {
        let Some(entry) = self.entries.get_mut(index as usize) else {
            panic!("an origin record must name a slot inside the thief slab");
        };
        *entry = Some(origin);
    }

    /// Takes the origin recorded for `index`, leaving the slot bare.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed only in test assertions; the settle report path lands in a later PR"
        )
    )]
    pub(crate) fn take(&mut self, index: u32) -> Option<Origin> {
        self.entries.get_mut(index as usize)?.take()
    }

    /// Whether `index` currently hosts a relocated resident.
    pub(crate) fn is_relocated(&self, index: u32) -> bool {
        self.entries
            .get(index as usize)
            .is_some_and(Option::is_some)
    }
}

/// Reserves a destination in the thief's own slab and builds the request
/// to post, or `None` when the slab has no free slot to promise.
///
/// The caller posts the request and signals the victim; a failed post
/// withdraws the reservation through [`Slab::unreserve`], keyed by the
/// returned request's destination.
pub(crate) fn prepare_steal(tasks: &mut Slab<TaskSlot>, thief_id: u8) -> Option<StealRequest> {
    let Ok(promised) = tasks.reserve() else {
        return None;
    };
    Some(StealRequest {
        thief_id,
        dest: TaskRef::from_slab(thief_id, promised),
    })
}

/// Serves one steal request against the victim's own slab: retires the
/// first stealable resident out and records the forwarding route in the
/// same straight-line step.
///
/// Returns the reply for the thief's handoff ring. The husk left behind
/// stays `Retired` under its live generation -- its release belongs to
/// the reap path once the settled note lands, never to this step.
///
/// Pass 1 (Sleeping): sweeps the slab for a sleeping candidate via
/// [`move_out`]. A candidate that loses the `move_out` interlock (a
/// concurrent wake or cancel) is skipped; the sweep continues to the next
/// resident. A resident that already relocated here once is skipped too,
/// keeping every route single-hop until chained forwarding lands.
///
/// Pass 2 (Woken): when pass 1 finds nothing, drains one candidate at a
/// time from `run_queue` and attempts [`move_out_woken`]. A task that fails
/// the stealability guards or loses the retire CAS is pushed back to the
/// run queue so it is not silently dropped; the caller sees a `Declined`
/// and the victim's queue is restored with at most minor FIFO reordering
/// (one rotate per failed candidate), which is correctness-harmless.
pub(crate) fn serve_steal(
    tasks: &mut Slab<TaskSlot>,
    forward: &mut ForwardTable,
    origins: &ForwardOrigin,
    run_queue: &mut LocalRunQueue,
    victim_id: u8,
    request: StealRequest,
) -> HandoffMsg {
    for (key, slot) in tasks.iter() {
        if !is_sleeping_candidate(slot, origins, key) {
            continue;
        }
        let Some(task) = move_out(slot, key) else {
            continue;
        };
        forward.record(task.victim_key(), request.dest);
        return HandoffMsg::Delivered {
            dest: request.dest,
            victim_id,
            task,
        };
    }
    let len = run_queue.len();
    for _ in 0..len {
        let Some(candidate_ref) = run_queue.pop(tasks) else {
            break;
        };
        let key = SlabKey::new(candidate_ref.index(), candidate_ref.generation());
        let qualifies = tasks
            .get(key)
            .is_some_and(|slot| is_woken_candidate(slot, origins, key));
        if !qualifies {
            run_queue.push(candidate_ref, tasks);
            continue;
        }
        let outcome = tasks.get(key).and_then(|slot| move_out_woken(slot, key));
        if let Some(task) = outcome {
            forward.record(task.victim_key(), request.dest);
            return HandoffMsg::Delivered {
                dest: request.dest,
                victim_id,
                task,
            };
        }
        run_queue.push(candidate_ref, tasks);
    }
    HandoffMsg::Declined { dest: request.dest }
}

/// Cheap pre-filter ahead of the `move_out` interlock: sleeping and polled
/// at least once, not pinned, no linked children, no in-flight I/O, and not
/// itself a relocated resident.
///
/// The `has_polled` gate matches `is_woken_candidate`: a task is never stolen
/// before its first poll, so its first poll always runs on the worker that
/// spawned it. This keeps an in-flight op submitted on its issuing worker.
fn is_sleeping_candidate(slot: &TaskSlot, origins: &ForwardOrigin, key: SlabKey) -> bool {
    let header = slot.header();
    header.state.load() == TaskState::Sleeping
        && header.has_polled
        && !header.io_bound
        && !header.is_pinned
        && header.first_child.is_none()
        && header.in_flight_ops == 0
        && !origins.is_relocated(key.index())
}

/// Cheap pre-filter ahead of the `move_out_woken` interlock: woken and
/// polled at least once, not pinned, no linked children, no in-flight I/O,
/// and not itself a relocated resident.
///
/// The `has_polled` gate leaves a freshly woken task that has never run for
/// its owning worker: relocating it before its first poll would move that
/// first poll to the thief, which a future relying on a steal-driven second
/// poll cannot survive.
fn is_woken_candidate(slot: &TaskSlot, origins: &ForwardOrigin, key: SlabKey) -> bool {
    let header = slot.header();
    header.state.load() == TaskState::Woken
        && header.has_polled
        && !header.io_bound
        && !header.is_pinned
        && header.first_child.is_none()
        && header.in_flight_ops == 0
        && !origins.is_relocated(key.index())
}

/// What the receive step resolved a handoff reply into.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Received {
    /// The body landed under its promised key; the caller wakes it.
    Installed(TaskRef),
    /// The promise was withdrawn (a decline, or an unusable delivery);
    /// nothing to wake.
    Withdrawn,
}

/// Receives one handoff reply into the thief's own slab.
///
/// A delivery installs under the promised key and records the origin for
/// the settle report; a decline withdraws the reservation. A delivery
/// whose promise no longer resolves drops the transport -- the carried
/// future releases through the transport's drop -- and reports
/// [`Received::Withdrawn`].
pub(crate) fn receive_handoff(
    tasks: &mut Slab<TaskSlot>,
    origins: &mut ForwardOrigin,
    msg: HandoffMsg,
) -> Received {
    match msg {
        HandoffMsg::Delivered {
            dest,
            victim_id,
            task,
        } => {
            let key = SlabKey::new(dest.index(), dest.generation());
            let victim_key = task.victim_key();
            if let Some(returned) = move_in(tasks, key, task) {
                drop(returned);
                tasks.unreserve(key);
                return Received::Withdrawn;
            }
            origins.record(
                dest.index(),
                Origin {
                    victim_id,
                    victim_key,
                },
            );
            Received::Installed(dest)
        }
        HandoffMsg::Declined { dest } => {
            tasks.unreserve(SlabKey::new(dest.index(), dest.generation()));
            Received::Withdrawn
        }
    }
}

/// Reports every settled relocated resident through `report`, releasing
/// the resident's slot and clearing its origin once the report lands.
///
/// A relocated resident settles by the same criterion as a native child
/// (terminal state or `Done`); its origin names the husk awaiting the
/// settled note. A `report` returning `false` (the victim's note ring is
/// full) keeps the slot and the origin for a later pass -- the note must
/// land before the body's slot recycles, or the husk would never mark
/// settled.
pub(crate) fn report_settled<F>(
    tasks: &mut Slab<TaskSlot>,
    origins: &mut ForwardOrigin,
    mut report: F,
) where
    F: FnMut(Origin) -> bool,
{
    let Ok(capacity) = u32::try_from(origins.entries.len()) else {
        return;
    };
    for index in 0..capacity {
        let Some(origin) = origins.entries[index as usize] else {
            continue;
        };
        if !is_resident_settled(tasks, index) {
            continue;
        }
        if !report(origin) {
            continue;
        }
        origins.entries[index as usize] = None;
        tasks.remove_by_index(index);
    }
}

/// Whether the resident at `index` reached a settled state (terminal or
/// `Done`), mirroring the reap path's criterion for native children.
fn is_resident_settled(tasks: &Slab<TaskSlot>, index: u32) -> bool {
    tasks.get_by_index(index).is_some_and(|slot| {
        let state = slot.header().state.load();
        state.is_terminal() || state == TaskState::Done
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
    use crate::task::{cell::header::Slot, state::TaskState};

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
        // The steal predicates offer only polled tasks; mark the seeded task
        // polled so candidate tests exercise the steal path, not the
        // fresh-task guard.
        if let Some(slot) = slab.get_mut(key) {
            slot.header_mut().has_polled = true;
        }
        key
    }

    fn request_for(thief_id: u8) -> StealRequest {
        StealRequest {
            thief_id,
            dest: TaskRef::from_slab(thief_id, SlabKey::new(0, Generation::from_raw(1))),
        }
    }

    #[test]
    fn an_empty_slab_declines() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut forward = ForwardTable::new(1);
        let origins = ForwardOrigin::new(1);
        let mut run_queue = LocalRunQueue::new();
        let request = request_for(1);
        let HandoffMsg::Declined { dest } = serve_steal(
            &mut victim,
            &mut forward,
            &origins,
            &mut run_queue,
            0,
            request,
        ) else {
            panic!("an empty slab must decline");
        };
        assert_eq!(dest, request.dest);
    }

    #[test]
    fn the_first_stealable_resident_ships() {
        let pip = Pip::issue(3, 7);
        let mut victim = Slab::<TaskSlot>::new(2);
        let key = seed(&mut victim, pip, Inert);
        let mut forward = ForwardTable::new(2);
        let origins = ForwardOrigin::new(2);
        let mut run_queue = LocalRunQueue::new();
        let request = request_for(1);
        let HandoffMsg::Delivered {
            dest,
            victim_id,
            task,
        } = serve_steal(
            &mut victim,
            &mut forward,
            &origins,
            &mut run_queue,
            0,
            request,
        )
        else {
            panic!("a sleeping resident must ship");
        };
        assert_eq!(dest, request.dest);
        assert_eq!(victim_id, 0);
        assert_eq!(task.pip(), pip);
        assert_eq!(
            forward.lookup(key),
            Some(request.dest),
            "the route must be recorded in the serve step itself",
        );
        let Some(husk) = victim.get(key) else {
            panic!("the husk must stay resolvable through its live generation");
        };
        assert_eq!(
            husk.header().state.load(),
            TaskState::Retired,
            "the serve step must leave the husk for the reap path",
        );
    }

    #[test]
    fn unstealable_residents_decline_the_request() {
        let mut victim = Slab::<TaskSlot>::new(3);
        let pinned = seed(&mut victim, Pip::detached(), Inert);
        let in_flight = seed(&mut victim, Pip::detached(), Inert);
        let woken = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get_mut(pinned) else {
            panic!("the pinned resident must resolve");
        };
        slot.header_mut().is_pinned = true;
        let Some(slot) = victim.get_mut(in_flight) else {
            panic!("the in-flight resident must resolve");
        };
        slot.header_mut().in_flight_ops = 1;
        let Some(slot) = victim.get(woken) else {
            panic!("the woken resident must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };

        let mut forward = ForwardTable::new(3);
        let origins = ForwardOrigin::new(3);
        let mut run_queue = LocalRunQueue::new();
        let request = request_for(1);
        assert!(
            matches!(
                serve_steal(
                    &mut victim,
                    &mut forward,
                    &origins,
                    &mut run_queue,
                    0,
                    request
                ),
                HandoffMsg::Declined { .. }
            ),
            "no unstealable resident may ship",
        );
    }

    #[test]
    fn a_relocated_resident_stays_single_hop() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let mut forward = ForwardTable::new(1);
        let mut origins = ForwardOrigin::new(1);
        origins.record(
            key.index(),
            Origin {
                victim_id: 9,
                victim_key: SlabKey::new(5, Generation::from_raw(1)),
            },
        );
        let mut run_queue = LocalRunQueue::new();
        let request = request_for(1);
        assert!(
            matches!(
                serve_steal(
                    &mut victim,
                    &mut forward,
                    &origins,
                    &mut run_queue,
                    0,
                    request
                ),
                HandoffMsg::Declined { .. }
            ),
            "a resident that already relocated once must not ship again",
        );
    }

    #[test]
    fn a_delivery_installs_and_records_the_origin() {
        let pip = Pip::issue(2, 5);
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut thief = Slab::<TaskSlot>::new(1);
        let victim_key = seed(&mut victim, pip, Inert);
        let mut forward = ForwardTable::new(1);
        let victim_origins = ForwardOrigin::new(1);
        let mut origins = ForwardOrigin::new(1);

        let Some(request) = prepare_steal(&mut thief, 1) else {
            panic!("a fresh thief slab must promise a slot");
        };
        let mut victim_run_queue = LocalRunQueue::new();
        let msg = serve_steal(
            &mut victim,
            &mut forward,
            &victim_origins,
            &mut victim_run_queue,
            0,
            request,
        );
        let Received::Installed(task_ref) = receive_handoff(&mut thief, &mut origins, msg) else {
            panic!("a delivery must install");
        };
        assert_eq!(task_ref, request.dest);
        let Some(slot) = thief.get(SlabKey::new(task_ref.index(), task_ref.generation())) else {
            panic!("the resident must resolve under the promised key");
        };
        assert_eq!(slot.header().pip, pip);
        let Some(origin) = origins.take(task_ref.index()) else {
            panic!("the origin must be recorded at install");
        };
        assert_eq!(origin.victim_id, 0);
        assert_eq!(origin.victim_key.index(), victim_key.index());
        assert!(
            !origins.is_relocated(task_ref.index()),
            "a taken origin must leave the slot bare",
        );
    }

    #[test]
    fn a_decline_withdraws_the_promise() {
        let mut thief = Slab::<TaskSlot>::new(1);
        let mut origins = ForwardOrigin::new(1);
        let Some(request) = prepare_steal(&mut thief, 1) else {
            panic!("a fresh thief slab must promise a slot");
        };
        let received = receive_handoff(
            &mut thief,
            &mut origins,
            HandoffMsg::Declined { dest: request.dest },
        );
        assert_eq!(received, Received::Withdrawn);
        let Ok(_promised) = thief.reserve() else {
            panic!("a withdrawn promise must free the slot");
        };
    }

    #[test]
    fn a_full_thief_slab_prepares_nothing() {
        let mut thief = Slab::<TaskSlot>::new(1);
        seed(&mut thief, Pip::detached(), Inert);
        assert!(prepare_steal(&mut thief, 1).is_none());
    }

    /// Drives a seeded resident to `Done` the way the run loop would: a
    /// real poll to `Ready` (which consumes the future in place), then the
    /// completing transition -- the settled criterion the report scans for.
    fn settle(slab: &mut Slab<TaskSlot>, key: SlabKey) {
        let Some(slot) = slab.get_mut(key) else {
            panic!("the resident must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
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
    }

    #[test]
    fn a_settled_resident_reports_frees_and_clears() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(
            &mut thief,
            Pip::detached(),
            Probe {
                polls: &polls,
                drops: &drops,
            },
        );
        settle(&mut thief, key);
        let mut origins = ForwardOrigin::new(1);
        let husk = SlabKey::new(7, Generation::from_raw(1));
        origins.record(
            key.index(),
            Origin {
                victim_id: 4,
                victim_key: husk,
            },
        );
        let mut reported = None;
        report_settled(&mut thief, &mut origins, |origin| {
            reported = Some(origin);
            true
        });
        let Some(origin) = reported else {
            panic!("a settled resident must report");
        };
        assert_eq!(origin.victim_id, 4);
        assert_eq!(origin.victim_key.index(), husk.index());
        assert!(
            !origins.is_relocated(key.index()),
            "a landed report clears the origin",
        );
        assert!(thief.get(key).is_none(), "a landed report frees the slot");
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "the freed cell drops its future exactly once",
        );
    }

    #[test]
    fn an_unsettled_resident_reports_nothing() {
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(&mut thief, Pip::detached(), Inert);
        let mut origins = ForwardOrigin::new(1);
        origins.record(
            key.index(),
            Origin {
                victim_id: 4,
                victim_key: SlabKey::new(7, Generation::from_raw(1)),
            },
        );
        report_settled(&mut thief, &mut origins, |_| {
            panic!("a sleeping resident must not report")
        });
        assert!(
            origins.is_relocated(key.index()),
            "an unsettled resident keeps its origin",
        );
        assert!(
            thief.get(key).is_some(),
            "an unsettled resident keeps its slot",
        );
    }

    #[test]
    fn a_bounced_report_keeps_the_slot_and_origin() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let mut thief = Slab::<TaskSlot>::new(1);
        let key = seed(
            &mut thief,
            Pip::detached(),
            Probe {
                polls: &polls,
                drops: &drops,
            },
        );
        settle(&mut thief, key);
        let mut origins = ForwardOrigin::new(1);
        origins.record(
            key.index(),
            Origin {
                victim_id: 4,
                victim_key: SlabKey::new(7, Generation::from_raw(1)),
            },
        );
        report_settled(&mut thief, &mut origins, |_| false);
        assert!(
            origins.is_relocated(key.index()),
            "a bounced report keeps the origin for a later pass",
        );
        assert!(
            thief.get(key).is_some(),
            "a bounced report keeps the slot until the note lands",
        );
        report_settled(&mut thief, &mut origins, |_| true);
        assert!(!origins.is_relocated(key.index()));
        assert!(thief.get(key).is_none());
    }

    #[test]
    fn a_woken_resident_in_run_queue_ships_when_no_sleeping_candidate() {
        let pip = Pip::issue(5, 9);
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, pip, Inert);
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let mut forward = ForwardTable::new(1);
        let origins = ForwardOrigin::new(1);
        let mut run_queue = LocalRunQueue::new();
        run_queue.push(TaskRef::from_slab(0, key), &mut victim);

        let mut thief = Slab::<TaskSlot>::new(1);
        let Some(request) = prepare_steal(&mut thief, 1) else {
            panic!("reserve must succeed");
        };
        let HandoffMsg::Delivered {
            dest,
            victim_id,
            task,
        } = serve_steal(
            &mut victim,
            &mut forward,
            &origins,
            &mut run_queue,
            0,
            request,
        )
        else {
            panic!("a woken resident in the run queue must ship");
        };
        assert_eq!(victim_id, 0);
        assert_eq!(task.pip(), pip);
        assert_eq!(forward.lookup(key), Some(dest));
    }

    #[test]
    fn a_woken_but_pinned_resident_declines_and_queue_is_restored() {
        let mut victim = Slab::<TaskSlot>::new(1);
        let key = seed(&mut victim, Pip::detached(), Inert);
        let Some(slot) = victim.get(key) else {
            panic!("the task must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let Some(slot) = victim.get_mut(key) else {
            panic!("the task must resolve");
        };
        slot.header_mut().is_pinned = true;
        let mut forward = ForwardTable::new(1);
        let origins = ForwardOrigin::new(1);
        let mut run_queue = LocalRunQueue::new();
        run_queue.push(TaskRef::from_slab(0, key), &mut victim);

        let request = request_for(1);
        assert!(
            matches!(
                serve_steal(
                    &mut victim,
                    &mut forward,
                    &origins,
                    &mut run_queue,
                    0,
                    request
                ),
                HandoffMsg::Declined { .. }
            ),
            "a pinned woken resident must decline",
        );
        assert_eq!(
            run_queue.len(),
            1,
            "the queue is restored after a failed woken steal"
        );
    }

    #[test]
    fn the_protocol_round_trip_polls_on_the_thief() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let mut victim = Slab::<TaskSlot>::new(1);
        let mut thief = Slab::<TaskSlot>::new(1);
        seed(
            &mut victim,
            Pip::detached(),
            Probe {
                polls: &polls,
                drops: &drops,
            },
        );
        let mut forward = ForwardTable::new(1);
        let victim_origins = ForwardOrigin::new(1);
        let mut origins = ForwardOrigin::new(1);

        let Some(request) = prepare_steal(&mut thief, 1) else {
            panic!("a fresh thief slab must promise a slot");
        };
        let mut victim_run_queue = LocalRunQueue::new();
        let msg = serve_steal(
            &mut victim,
            &mut forward,
            &victim_origins,
            &mut victim_run_queue,
            0,
            request,
        );
        let Received::Installed(task_ref) = receive_handoff(&mut thief, &mut origins, msg) else {
            panic!("a delivery must install");
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = thief.get_mut(key) else {
            panic!("the resident must resolve under the promised key");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("the relocated resident must wake from Sleeping");
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
}
