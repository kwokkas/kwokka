//! The cooperative scheduler tick: one timer-and-poll pass over a worker's
//! task state.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::ptr::NonNull;

use kwokka_core::slab::{Slab, SlabKey};
use kwokka_io::{
    DriverType,
    buffer::{
        multishot::{MultishotSlab, RecvMultishotSlab},
        oneshot::inflight::InflightBufSlab,
    },
};

#[cfg(feature = "steal")]
use crate::scheduler::stealing::relocate::ForwardTable;
#[cfg(not(feature = "steal"))]
use crate::worker::park::wake::wake_local;
#[cfg(feature = "steal")]
use crate::worker::park::wake::wake_or_forward;
use crate::{
    scheduler::runnable::queue::LocalRunQueue,
    task::cell::{lifecycle::PollOutcome, slot::TaskSlot},
    timer::wheel::{TimerWheel, clock::Clock},
    worker::{
        WorkerId,
        poll::polling::poll_one,
        queue::{
            arm::{TIMER_INBOX_CAPACITY, TimerInbox},
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
    recv_multishot_slab: Option<NonNull<RecvMultishotSlab>>,
    #[cfg(feature = "steal")] forward: &ForwardTable,
) -> Tick {
    let now = timer.now_tick();
    let mut woke_any = false;
    for task_ref in timer.advance_to(now) {
        // A timer armed before its task relocated fires against the husk;
        // the forwarding wake re-routes it to the task's new worker.
        #[cfg(feature = "steal")]
        wake_or_forward(tasks, run_queue, forward, None, task_ref);
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
            recv_multishot_slab,
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
        id::{Namespace, Pip},
        slab::{Slab, SlabKey},
    };

    use super::{Tick, tick};
    use crate::{
        scheduler::runnable::queue::LocalRunQueue,
        task::cell::{lifecycle::spawn_insert, slot::TaskSlot, state::TaskState},
        timer::wheel::{TimerWheel, clock::Clock},
        worker::{
            WorkerId,
            park::wake::wake_local,
            queue::{
                arm::{TIMER_INBOX_CAPACITY, TimerInbox},
                inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
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
        wake_local(&mut tasks, &mut run_queue, task_ref);
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
}
