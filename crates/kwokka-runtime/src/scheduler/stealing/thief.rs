//! The thief half of a steal: asking, receiving, and reporting back.
//!
//! A thief prepares a request against its own slab, installs whatever the
//! victim ships into the slot it promised, and later reports each settled
//! resident home so the victim can release the husk it left behind. The
//! victim half is in [`victim`](crate::scheduler::stealing::victim).

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::slab::{Slab, SlabKey};

use crate::{
    scheduler::stealing::{
        origin::{ForwardOrigin, Origin},
        relocate::{HandoffMsg, StealRequest, move_in},
    },
    task::{
        TaskRef,
        cell::{slot::TaskSlot, state::TaskState},
    },
};

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
    for index in 0..origins.capacity() {
        let Some(origin) = origins.peek(index) else {
            continue;
        };
        if !is_resident_settled(tasks, index) {
            continue;
        }
        if !report(origin) {
            continue;
        }
        origins.take(index);
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
        slab::SlabKey,
    };

    use super::*;
    use crate::{
        scheduler::{
            runnable::queue::LocalRunQueue,
            stealing::{forward::ForwardTable, victim::serve_steal},
        },
        task::cell::header::Slot,
    };

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
