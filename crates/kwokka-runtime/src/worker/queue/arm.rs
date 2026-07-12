//! Per-worker timer-request inbox -- the deferred channel a polled task uses
//! to arm a timer without reaching the worker's timer wheel mid-poll.
//!
//! A polled task cannot register on the timer wheel directly: the run-loop
//! owns the wheel, and the [`Clock`](crate::timer::wheel::clock::Clock) generic on
//! [`TimerWheel`](crate::timer::wheel::TimerWheel) does not cross the
//! non-generic poll frame. Instead a sleeping future records a relative delay
//! here -- a worker field disjoint from the task slab -- and the run-loop
//! drains it after the poll, turning each relative delay into an absolute
//! deadline against its own clock and registering it on the wheel. This
//! mirrors the deferral the spawn inbox already uses for child spawns.

use crate::task::TaskRef;

/// Per-worker timer-request inbox capacity. A power of two, sized to absorb a
/// burst of timer arms within one poll before the worker drains the inbox.
pub(crate) const TIMER_INBOX_CAPACITY: usize = 64;

/// One pending timer arm carried across the deferral boundary.
///
/// Records the task to wake and the delay in wheel ticks. The run-loop turns
/// the relative `delay_ticks` into an absolute deadline against its clock at
/// drain time, so the arming future never reads the clock itself.
#[derive(Clone, Copy)]
pub(crate) struct TimerRequest {
    /// The task to wake when the timer expires.
    pub(crate) task_ref: TaskRef,
    /// Delay until expiry in wheel ticks, relative to drain time.
    pub(crate) delay_ticks: u64,
}

/// Fixed-capacity ring of pending timer arms, drained once per tick.
///
/// `N` must be a power of two. The bound caps per-worker memory under an arm
/// storm; a full inbox refuses the arm so the caller retries on its next poll
/// rather than losing the wakeup. No allocation after construction.
pub(crate) struct TimerInbox<const N: usize> {
    slots: [Option<TimerRequest>; N],
    head: usize,
    tail: usize,
}

impl<const N: usize> TimerInbox<N> {
    /// Creates an empty inbox.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is not a power of two or is zero.
    pub(crate) const fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            tail: 0,
        }
    }

    /// Arms a timer request, returning `false` when the inbox is full.
    ///
    /// A full inbox refuses the request rather than dropping it silently: the
    /// arming future observes `false` and retries on its next poll, so a wakeup
    /// is delayed under backpressure but never lost.
    pub(crate) const fn push(&mut self, request: TimerRequest) -> bool {
        if self.tail.wrapping_sub(self.head) >= N {
            return false;
        }
        self.slots[self.tail & (N - 1)] = Some(request);
        self.tail = self.tail.wrapping_add(1);
        true
    }

    /// Pops the oldest pending arm, or `None` when the inbox is empty.
    pub(crate) const fn pop(&mut self) -> Option<TimerRequest> {
        if self.head == self.tail {
            return None;
        }
        let request = self.slots[self.head & (N - 1)].take();
        self.head = self.head.wrapping_add(1);
        request
    }

    /// Number of pending arms.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    /// `true` when no arms are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    fn request(raw: u64, delay: u64) -> TimerRequest {
        TimerRequest {
            task_ref: TaskRef::from_raw(raw),
            delay_ticks: delay,
        }
    }

    #[test]
    fn push_then_pop_is_fifo() {
        let mut inbox = TimerInbox::<4>::new();
        assert!(inbox.push(request(1, 10)));
        assert!(inbox.push(request(2, 20)));
        let Some(first) = inbox.pop() else {
            panic!("pop must yield the first request");
        };
        assert_eq!(first.task_ref.raw(), TaskRef::from_raw(1).raw());
        assert_eq!(first.delay_ticks, 10);
        let Some(second) = inbox.pop() else {
            panic!("pop must yield the second request");
        };
        assert_eq!(second.task_ref.raw(), TaskRef::from_raw(2).raw());
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn push_to_full_is_refused() {
        let mut inbox = TimerInbox::<2>::new();
        assert!(inbox.push(request(1, 1)));
        assert!(inbox.push(request(2, 2)));
        assert!(!inbox.push(request(3, 3)), "a full inbox refuses the arm");
    }

    #[test]
    fn pop_empty_returns_none() {
        let mut inbox = TimerInbox::<2>::new();
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn len_and_empty_reflect_occupancy() {
        let mut inbox = TimerInbox::<4>::new();
        assert!(inbox.is_empty());
        assert_eq!(inbox.len(), 0);
        assert!(inbox.push(request(1, 5)));
        assert_eq!(inbox.len(), 1);
        assert!(!inbox.is_empty());
        assert!(inbox.pop().is_some());
        assert!(inbox.is_empty());
    }

    #[test]
    fn wrap_around_reuses_slots() {
        let mut inbox = TimerInbox::<2>::new();
        assert!(inbox.push(request(1, 1)));
        assert!(inbox.pop().is_some());
        assert!(inbox.push(request(2, 2)));
        assert!(inbox.push(request(3, 3)));
        let Some(second) = inbox.pop() else {
            panic!("pop must yield after wrap");
        };
        assert_eq!(second.task_ref.raw(), TaskRef::from_raw(2).raw());
    }
}
