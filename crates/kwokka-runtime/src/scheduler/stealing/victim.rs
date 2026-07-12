//! The victim half of a steal: serving one request against your own slab.
//!
//! A worker being stolen from retires the first stealable resident it holds --
//! a sleeping one first, then a woken one off the run queue -- and records the
//! forward route in the same straight-line step. What it ships is built by
//! [`relocate`](crate::scheduler::stealing::relocate); what it refuses is
//! decided here.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::slab::{Slab, SlabKey};

use crate::{
    scheduler::{
        runnable::queue::LocalRunQueue,
        stealing::{
            forward::ForwardTable,
            origin::ForwardOrigin,
            relocate::{HandoffMsg, StealRequest, move_out, move_out_woken},
        },
    },
    task::cell::{slot::TaskSlot, state::TaskState},
};

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

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use kwokka_core::{
        Generation,
        id::{Namespace, Pip},
        slab::SlabKey,
    };

    use super::*;
    use crate::{
        scheduler::stealing::{origin::Origin, thief::prepare_steal},
        task::{TaskRef, cell::header::Slot},
    };

    struct Inert;
    impl Future for Inert {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
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
}
