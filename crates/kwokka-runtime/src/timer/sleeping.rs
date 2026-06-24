//! Timer-backed sleep future.
//!
//! [`sleep`] returns a future that completes after a wall-clock duration. On
//! its first poll it records a timer arm in the worker's timer-request inbox
//! through the active poll frame; the run-loop drains the inbox after the poll
//! and registers the arm on the worker's timer wheel, which wakes the task when
//! the deadline expires. Like the structured scope, this reads the polling
//! task's identity from the runtime waker, so it must be awaited directly --
//! not inside a `select!` / `join!` branch that wraps the waker. Combinator
//! support waits on a later phase.

use core::{
    future::Future,
    pin::Pin,
    ptr,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    task::waker,
    timer::clock::TICK_NS,
    worker::{WorkerId, poll::polling},
};

/// Returns a future that completes after `duration`.
///
/// The future arms a timer on the running worker's wheel on its first poll and
/// resolves once the deadline expires.
///
/// # Examples
///
/// ```rust
/// # async fn run() {
/// use core::time::Duration;
///
/// use kwokka_runtime::task::sleep;
///
/// sleep(Duration::from_millis(10)).await;
/// # }
/// ```
///
/// # Panics
///
/// Awaiting the returned future panics if it is polled under a waker that is
/// not the runtime's task waker (a `select!` / `join!` combinator replaced it),
/// or if that waker carries an out-of-range worker id.
#[must_use = "sleep does nothing unless awaited"]
pub fn sleep(duration: Duration) -> Sleep {
    // One wheel tick is `TICK_NS` nanoseconds; round up so a sub-tick delay
    // still waits at least one tick rather than firing immediately. Saturate
    // rather than truncate for durations beyond the u64 tick range.
    let delay_ticks =
        u64::try_from(duration.as_nanos().div_ceil(u128::from(TICK_NS))).unwrap_or(u64::MAX);
    Sleep {
        delay_ticks,
        armed: false,
    }
}

/// Future returned by [`sleep`]. See the module docs for the arming handshake.
///
/// Carries only the relative delay and a one-bit armed flag, so it is
/// `Send + Sync` regardless of the surrounding task's mode.
#[derive(Debug)]
#[must_use = "futures do nothing unless awaited"]
pub struct Sleep {
    delay_ticks: u64,
    armed: bool,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // A zero delay completes immediately without touching the wheel.
        if this.delay_ticks == 0 {
            return Poll::Ready(());
        }
        // The wheel wakes the armed task at its deadline; the woken re-poll
        // observes `armed` and completes.
        if this.armed {
            return Poll::Ready(());
        }
        // Decode the polling task from the runtime waker, like the scope future.
        // A combinator that wraps the waker hides the task identity, so a sleep
        // inside select!/join! is rejected until that path lands.
        if !ptr::eq(
            ptr::from_ref(cx.waker().vtable()),
            ptr::from_ref(&waker::VTABLE),
        ) {
            foreign_waker_panic();
        }
        let task = waker::data_to_task_ref(cx.waker().data());
        let Ok(worker) = WorkerId::new(task.worker_id()) else {
            panic!("sleep decoded a non-routable worker id from the waker");
        };
        // Arm the timer through the active poll frame. A full inbox (or no
        // installed frame) yields `false`; re-wake to retry on the next poll so
        // the arm is delayed under backpressure rather than lost.
        let armed =
            polling::with_current(worker, |frame| frame.request_timer(task, this.delay_ticks))
                .unwrap_or(false);
        if armed {
            this.armed = true;
        } else {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Aborts a [`sleep`] polled under a wrapped waker.
///
/// Cold path: a combinator replaced the task waker, so the sleeping task
/// cannot be identified to arm the timer against it.
#[cold]
#[inline(never)]
fn foreign_waker_panic() -> ! {
    panic!(
        "sleep() requires the runtime task waker; a combinator wrapped it, so \
         the sleeping task cannot be identified -- await sleep() directly, not \
         inside a select!/join! branch"
    );
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        pin::Pin,
        ptr::NonNull,
        sync::atomic::AtomicU16,
        task::{Context, Waker},
    };

    use kwokka_core::{
        Generation,
        slab::{Slab, SlabKey},
    };

    use super::*;
    use crate::{
        task::{
            TaskRef,
            cell::{header::WakeData, slot::TaskSlot},
            waker::waker_from_task_ref,
        },
        timer::request::{TIMER_INBOX_CAPACITY, TimerInbox},
        worker::{
            WorkerId,
            poll::{
                frame::PollFrame,
                polling::{clear, install},
            },
            queue::{
                inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
                reap::{REAP_QUEUE_CAPACITY, ReapQueue},
            },
        },
    };

    fn worker_id(id: u8) -> WorkerId {
        let Ok(worker) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker
    }

    #[test]
    fn zero_duration_is_ready_without_arming() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut future = sleep(Duration::ZERO);
        // A zero delay short-circuits before the waker check, so any waker works.
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn first_poll_arms_a_request_then_the_armed_repoll_completes() {
        let worker = worker_id(20);
        let task = TaskRef::from_slab(20, SlabKey::new(0, Generation::from_raw(0)));
        let mut slab = Slab::<TaskSlot>::new(1);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let mut timer_requests = TimerInbox::<TIMER_INBOX_CAPACITY>::new();
        // `slab` / `inbox` / `reap` / `timer_requests` outlive `frame`, so its
        // `NonNull` fields stay valid through the asserts.
        let frame = PollFrame {
            current: task,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            first_child: None,
            reap: NonNull::from(&mut reap),
            driver: None,
            wake_data: WakeData::EMPTY,
            submitted_ops: AtomicU16::new(0),
            timer_requests: Some(NonNull::from(&mut timer_requests)),
        };
        install(worker, &frame);
        let waker = waker_from_task_ref(task);
        let mut cx = Context::from_waker(&waker);
        let mut future = sleep(Duration::from_millis(5));
        let first = Pin::new(&mut future).poll(&mut cx);
        // The armed re-poll stands in for the wheel-driven wake.
        let second = Pin::new(&mut future).poll(&mut cx);
        clear(worker);

        assert_eq!(first, Poll::Pending, "the first poll arms and pends");
        assert_eq!(second, Poll::Ready(()), "the armed re-poll completes");
        assert_eq!(
            timer_requests.len(),
            1,
            "arming pushed exactly one timer request",
        );
    }

    #[test]
    #[should_panic(expected = "combinator")]
    fn foreign_waker_panics() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut future = sleep(Duration::from_millis(1));
        let _ = Pin::new(&mut future).poll(&mut cx);
    }

    #[test]
    fn sleep_future_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Sleep>();
    }
}
