//! The cooperative scheduler tick: one timer-and-poll pass over a worker's
//! task state.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::ptr::NonNull;

use kwokka_core::{
    id::Pip,
    slab::{Slab, SlabKey},
};
use kwokka_io::{
    DriverType,
    buffer::{inflight::InflightBufSlab, multishot::MultishotSlab},
};

#[cfg(feature = "steal")]
use crate::scheduler::stealing::relocate::ForwardTable;
#[cfg(feature = "steal")]
use crate::worker::park::wake::wake_or_forward;
use crate::{
    scheduler::{dispatch::PollOutcome, queue::LocalRunQueue},
    task::{TaskRef, cell::slot::TaskSlot, join::children::push_child},
    timer::{
        clock::Clock,
        request::{TIMER_INBOX_CAPACITY, TimerInbox},
        wheel::TimerWheel,
    },
    worker::{
        WorkerId,
        park::wake::wake_local,
        poll::polling::poll_one,
        queue::{
            inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
            reap::{REAP_QUEUE_CAPACITY, ReapQueue},
        },
    },
};

/// Whether a [`tick`] advanced any work.
///
/// The blocking run-loop -- which composes the driver completion drain and
/// the park step around this tick -- parks only on
/// [`Tick::Idle`]: an idle tick means the run queue drained empty and no
/// timer fired, so the worker has nothing to do until the next wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tick {
    /// At least one task was woken or polled this pass.
    Worked,
    /// No timer expiry and no runnable task; the worker may park.
    Idle,
}

/// Runs one cooperative pass: advance the timer waking every expired task,
/// then poll each task that was runnable at entry.
///
/// Each poll is delegated to [`poll_one`], which reborrows the slab as a raw
/// pointer for the poll window, derives the exclusive parent slot, installs the
/// frame for `worker_id`, and clears it on exit (including an unwinding poll).
/// The frame lets the polled task request child spawns into `spawn_inbox` -- a
/// field disjoint from `tasks` -- and cancel a disjoint child slot, without
/// re-borrowing the slab the tick holds across the poll. The spawn-inbox drain
/// and the park-on-idle step are composed around the tick by the blocking
/// run-loop, which also drains driver completions before each tick so a task
/// woken by I/O is runnable when the poll step reaches it.
///
/// A task re-queued by a mid-poll reschedule waits for the next pass, so a
/// single tick cannot spin on a self-rescheduling task.
#[expect(
    clippy::too_many_arguments,
    reason = "the shard is destructured into disjoint borrows by design; \
              bundling them would recreate the double-&mut-self conflict"
)]
pub(crate) fn tick<C: Clock>(
    tasks: &mut Slab<TaskSlot>,
    timer: &mut TimerWheel<C>,
    run_queue: &mut LocalRunQueue,
    spawn_inbox: &mut SpawnInbox<SPAWN_INBOX_CAPACITY>,
    reap: &mut ReapQueue<REAP_QUEUE_CAPACITY>,
    timer_requests: &mut TimerInbox<TIMER_INBOX_CAPACITY>,
    worker_id: WorkerId,
    driver: Option<NonNull<DriverType>>,
    inflight_slab: Option<NonNull<InflightBufSlab>>,
    multishot_slab: Option<NonNull<MultishotSlab>>,
    #[cfg(feature = "steal")] forward: &ForwardTable,
) -> Tick {
    let now = timer.now_tick();
    let mut woke_any = false;
    for task_ref in timer.advance_to(now) {
        // A timer armed before its task relocated fires against the husk;
        // the forwarding wake re-routes it to the task's new worker.
        #[cfg(feature = "steal")]
        wake_or_forward(tasks, run_queue, forward, task_ref);
        #[cfg(not(feature = "steal"))]
        wake_local(tasks, run_queue, task_ref);
        woke_any = true;
    }

    let mut polled = 0_usize;
    let mut remaining = run_queue.len();
    while remaining > 0 {
        remaining -= 1;
        let Some(task_ref) = run_queue.pop(tasks) else {
            break;
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        // `poll_one` reborrows the slab as a raw pointer for the poll window,
        // derives the exclusive parent slot, installs the frame, and polls. The
        // run-loop does not touch `tasks` between forming that pointer and the
        // frame clearing, so a child cancel during the poll reaches a disjoint
        // slot through the same pointer. `None` is a stale slot: skip it.
        let Some(outcome) = poll_one(
            NonNull::from(&mut *tasks),
            key,
            worker_id,
            task_ref,
            NonNull::from(&mut *spawn_inbox),
            NonNull::from(&mut *reap),
            driver,
            inflight_slab,
            multishot_slab,
            Some(NonNull::from(&mut *timer_requests)),
        ) else {
            continue;
        };
        if outcome == PollOutcome::Rescheduled {
            run_queue.push(task_ref, tasks);
        }
        polled += 1;
    }

    // Drain timer arms requested during this tick's polls: turn each relative
    // delay into an absolute deadline against this worker's clock and register
    // it on the wheel. A full wheel drops the arm permanently -- the sleeping
    // future then stays pending until its task is cancelled. The wheel is sized
    // to the task slab, so a full wheel means every slot already holds a timer;
    // no panic or UB, but the dropped sleep does not complete on its own.
    while let Some(request) = timer_requests.pop() {
        let deadline = timer.now_tick().saturating_add(request.delay_ticks);
        // IGNORE: a full wheel returns Err; the arm is dropped and its future
        // stays pending until cancelled (a full wheel needs every slot armed).
        let _ = timer.register(request.task_ref, deadline);
    }

    if woke_any || polled > 0 {
        Tick::Worked
    } else {
        Tick::Idle
    }
}

/// Drains the worker spawn inbox: each pending child is stamped with a freshly
/// issued id, inserted into the slab, linked under its parent, and woken onto
/// the run queue.
///
/// Driver-free, like [`tick`]: the blocking run-loop composes it around the
/// tick, after the poll borrow on the inbox has ended. The child cell was built
/// with a detached id at spawn-request time; the issuing worker stamps the real
/// id here from `pip_seq`, the same per-worker counter `WorkerShard::issue_pip`
/// advances, so the issuing worker id stays embedded across a later steal.
pub(crate) fn drain_spawns(
    tasks: &mut Slab<TaskSlot>,
    run_queue: &mut LocalRunQueue,
    spawn_inbox: &mut SpawnInbox<SPAWN_INBOX_CAPACITY>,
    worker_id: WorkerId,
    pip_seq: &mut u64,
) {
    while let Some(mut pending) = spawn_inbox.pop() {
        let pip = Pip::issue(u64::from(worker_id.raw()), *pip_seq);
        *pip_seq += 1;
        pending.cell.header_mut().set_pip(pip);
        let Ok(key) = tasks.insert(pending.cell) else {
            // TODO(pablo): replace this panic with bounded backpressure in 0.2.0 (#116).
            // 0.1.0 treats a full worker task slab as a configuration error and
            // aborts rather than silently dropping the child (a lost-task hazard,
            // not benign backpressure). Unbounded dynamic fan-out is the roadmap, so
            // bounded backpressure -- surfacing to the parent or capping the spawn
            // rate -- is the replacement.
            panic!(
                "worker task slab full: a child spawn exceeded capacity; \
                 increase it via RuntimeBuilder::task_capacity"
            );
        };
        let child = TaskRef::from_slab(worker_id.raw(), key);
        // IGNORE: push_child only fails when the parent or child no longer
        // resolves; both were just established (the parent polled this tick, the
        // child was just inserted), so the link cannot fail.
        let _ = push_child(tasks, pending.parent, child);
        wake_local(tasks, run_queue, child);
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicU64, Ordering},
        task::{Context, Poll},
    };

    use kwokka_core::{
        id::Pip,
        namespace::Namespace,
        slab::{Slab, SlabKey},
    };

    use super::{Tick, drain_spawns, tick};
    use crate::{
        scheduler::{dispatch::spawn_insert, queue::LocalRunQueue},
        task::{
            cell::{header::Slot, slot::TaskSlot},
            join::children::iter_children,
            state::TaskState,
        },
        timer::{
            clock::Clock,
            request::{TIMER_INBOX_CAPACITY, TimerInbox},
            wheel::TimerWheel,
        },
        worker::{
            WorkerId,
            queue::{
                inbox::{PendingSpawn, SPAWN_INBOX_CAPACITY, SpawnInbox},
                reap::{REAP_QUEUE_CAPACITY, ReapQueue},
            },
        },
    };

    /// Builds a [`WorkerId`] for tests, panicking outside the routable range.
    fn worker(id: u8) -> WorkerId {
        let Ok(worker_id) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker_id
    }

    /// Clock whose tick is read from a test-controlled static, so a test can
    /// advance time deterministically after the wheel has taken the clock.
    struct StaticClock(&'static AtomicU64);
    impl Clock for StaticClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    struct Ready;
    impl Future for Ready {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    /// Drives `tick` with test defaults, bridging the steal-gated forward
    /// parameter so the call sites stay feature-agnostic.
    fn tick_for_tests(
        tasks: &mut Slab<TaskSlot>,
        timer: &mut TimerWheel<StaticClock>,
        run_queue: &mut LocalRunQueue,
        spawn_inbox: &mut SpawnInbox<SPAWN_INBOX_CAPACITY>,
    ) -> Tick {
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut timer_requests = TimerInbox::<TIMER_INBOX_CAPACITY>::new();
        #[cfg(feature = "steal")]
        {
            let forward = crate::scheduler::stealing::relocate::ForwardTable::new(1);
            tick(
                tasks,
                timer,
                run_queue,
                spawn_inbox,
                &mut reap,
                &mut timer_requests,
                worker(0),
                None,
                None,
                None,
                &forward,
            )
        }
        #[cfg(not(feature = "steal"))]
        {
            tick(
                tasks,
                timer,
                run_queue,
                spawn_inbox,
                &mut reap,
                &mut timer_requests,
                worker(0),
                None,
                None,
                None,
            )
        }
    }

    #[test]
    fn empty_tick_is_idle() {
        static CLOCK: AtomicU64 = AtomicU64::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut timer = TimerWheel::new(StaticClock(&CLOCK), 1);
        let mut run_queue = LocalRunQueue::new();
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        assert_eq!(
            tick_for_tests(&mut tasks, &mut timer, &mut run_queue, &mut spawn_inbox),
            Tick::Idle
        );
    }

    #[test]
    fn tick_polls_a_queued_task_to_completion() {
        static CLOCK: AtomicU64 = AtomicU64::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut timer = TimerWheel::new(StaticClock(&CLOCK), 1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(task_ref) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Ready)
        else {
            panic!("spawn must succeed");
        };
        // Make the task runnable without going through the timer.
        super::wake_local(&mut tasks, &mut run_queue, task_ref);
        assert_eq!(run_queue.len(), 1);

        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        assert_eq!(
            tick_for_tests(&mut tasks, &mut timer, &mut run_queue, &mut spawn_inbox),
            Tick::Worked
        );

        assert!(run_queue.is_empty(), "a completed task is not re-queued");
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get(key) else {
            panic!("task must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Done);
    }

    #[test]
    fn timer_expiry_wakes_then_polls() {
        static CLOCK: AtomicU64 = AtomicU64::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut timer = TimerWheel::new(StaticClock(&CLOCK), 1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(task_ref) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Ready)
        else {
            panic!("spawn must succeed");
        };
        let Ok(_) = timer.register(task_ref, 5) else {
            panic!("register must succeed");
        };

        // Before the deadline: nothing fires, nothing runnable.
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        assert_eq!(
            tick_for_tests(&mut tasks, &mut timer, &mut run_queue, &mut spawn_inbox),
            Tick::Idle
        );
        assert!(run_queue.is_empty());

        // Advance past the deadline: the timer wakes the task and the same
        // tick polls it to completion.
        CLOCK.store(10, Ordering::Relaxed);
        assert_eq!(
            tick_for_tests(&mut tasks, &mut timer, &mut run_queue, &mut spawn_inbox),
            Tick::Worked
        );
        assert!(run_queue.is_empty());
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get(key) else {
            panic!("task must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Done);
    }

    #[test]
    fn drain_links_and_wakes_a_seeded_child() {
        let mut tasks = Slab::<TaskSlot>::new(4);
        let mut run_queue = LocalRunQueue::new();
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut pip_seq = 1_u64;

        // A parent task plus a pending child spawn that names it as parent.
        let Ok(parent) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Ready)
        else {
            panic!("parent spawn must succeed");
        };
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready).into_erased();
        assert!(
            spawn_inbox.push(PendingSpawn { parent, cell }).is_none(),
            "seed push must enqueue",
        );

        drain_spawns(
            &mut tasks,
            &mut run_queue,
            &mut spawn_inbox,
            worker(0),
            &mut pip_seq,
        );

        assert!(spawn_inbox.is_empty(), "the drain consumes the inbox");
        assert_eq!(run_queue.len(), 1, "the child is woken onto the run queue");
        assert_eq!(
            pip_seq, 2,
            "issuing the child advanced the per-worker counter"
        );

        // The child is linked as the parent's first child and stamped with the
        // issued id, not the detached placeholder it was built with.
        let Some(parent_slot) = tasks.get(SlabKey::new(parent.index(), parent.generation())) else {
            panic!("parent must resolve");
        };
        let Some(child_ref) = parent_slot.header().first_child else {
            panic!("the child must be linked under the parent");
        };
        let Some(child_slot) = tasks.get(SlabKey::new(child_ref.index(), child_ref.generation()))
        else {
            panic!("the child must resolve");
        };
        assert_eq!(
            child_slot.header().pip,
            Pip::issue(0, 1),
            "the child is stamped with the issued id, not the detached placeholder",
        );
    }

    /// Pip issuance and the children tree are two independent axes, asserted
    /// apart here. `drain_spawns` mints a flat, per-worker-unique id for each
    /// child off the shared counter; it does not derive a hierarchical id from
    /// the parent (there is no `Pip::child` on this path). The parent-child
    /// structure lives only in the `first_child` / `next_sibling` list. So pip
    /// is checked for uniqueness, never for relation, and the children list is
    /// checked for structure. There is deliberately no `is_parent_of` claim: a
    /// flat id encodes nothing about its parent.
    #[test]
    fn drain_issues_distinct_child_pips() {
        let mut tasks = Slab::<TaskSlot>::new(8);
        let mut run_queue = LocalRunQueue::new();
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();

        // The parent already carries an issued id; the child counter starts past
        // it, mirroring a live shard that shares one `pip_seq` between the root
        // (issue 1) and the post-poll drain.
        let parent_pip = Pip::issue(0, 1);
        let Ok(parent) = spawn_insert(&mut tasks, 0, parent_pip, Namespace::ROOT, Ready) else {
            panic!("parent spawn must succeed");
        };
        for _ in 0..2 {
            let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready).into_erased();
            assert!(
                spawn_inbox.push(PendingSpawn { parent, cell }).is_none(),
                "each seed push must enqueue",
            );
        }

        let mut pip_seq = 2_u64;
        drain_spawns(
            &mut tasks,
            &mut run_queue,
            &mut spawn_inbox,
            worker(0),
            &mut pip_seq,
        );

        // Axis 2 -- the children list is the tree. Both children link under the
        // parent, and reaching each through the slab also confirms core's `Slab`
        // backs the runtime's live task headers, not dead storage.
        let (first, second) = {
            let mut children = iter_children(&tasks, parent);
            let Some(first) = children.next() else {
                panic!("the first child must be linked under the parent");
            };
            let Some(second) = children.next() else {
                panic!("the second child must be linked under the parent");
            };
            assert!(
                children.next().is_none(),
                "exactly the two spawned children are linked",
            );
            (first, second)
        };

        // Axis 1 -- pip is unique issuance, not relation. The drain stamps each
        // child with `Pip::issue(worker, seq)` off the shared counter, so the two
        // carry exactly issue(0, 2) and issue(0, 3); the children list, not the
        // ids, sets which is which. The check is positive on the issued ids
        // rather than a `Pip::detached()` comparison: detached is a fresh
        // process-global root on every call over `GLOBAL_SEQ`, not a stable
        // sentinel, and it shares the seq field with `issue`, so a re-read
        // detached is both non-deterministic and prone to a seq collision. There
        // is deliberately no `is_parent_of` claim: a flat id encodes nothing
        // about its parent.
        let Some(first_slot) = tasks.get(SlabKey::new(first.index(), first.generation())) else {
            panic!("the first child must resolve in the slab");
        };
        let Some(second_slot) = tasks.get(SlabKey::new(second.index(), second.generation())) else {
            panic!("the second child must resolve in the slab");
        };
        let first_pip = first_slot.header().pip.as_u128();
        let second_pip = second_slot.header().pip.as_u128();
        assert_ne!(first_pip, second_pip, "each child gets a distinct id");
        let issued_low = Pip::issue(0, 2).as_u128();
        let issued_high = Pip::issue(0, 3).as_u128();
        assert!(
            (first_pip == issued_high && second_pip == issued_low)
                || (first_pip == issued_low && second_pip == issued_high),
            "both children carry the two ids the per-worker counter issued",
        );
        assert_eq!(
            first_slot.header().pip.worker_id(),
            0,
            "the issuing worker is stamped in",
        );
        assert_eq!(
            second_slot.header().pip.worker_id(),
            0,
            "the issuing worker is stamped in",
        );
        assert_eq!(
            pip_seq, 4,
            "two children advanced the per-worker counter by two",
        );
    }
}
