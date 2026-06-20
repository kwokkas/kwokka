//! Time source abstraction for the timer wheel.

use core::time::Duration;
use std::time::Instant;

/// Tick resolution in nanoseconds (1ms).
pub(crate) const TICK_NS: u64 = 1_000_000;

/// Time source producing monotonic tick counts.
///
/// Each tick represents [`TICK_NS`] nanoseconds. The timer wheel
/// operates on pure `u64` ticks; the [`Clock`] implementation
/// converts wall-clock time internally.
pub(crate) trait Clock: Send + Sync + 'static {
    /// Returns the current tick count since the clock's epoch.
    fn now(&self) -> u64;
}

/// Wall-clock time source backed by [`Instant`].
///
/// Converts elapsed nanoseconds to ticks using [`TICK_NS`].
pub(crate) struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Create a system clock anchored to the current instant.
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Convert a tick count to a [`Duration`].
    #[inline]
    pub(crate) const fn ticks_to_duration(ticks: u64) -> Duration {
        Duration::from_nanos(ticks * TICK_NS)
    }
}

impl Clock for SystemClock {
    #[inline]
    #[allow(
        clippy::cast_possible_truncation,
        reason = "elapsed millis fits u64 for ~585 million years"
    )]
    fn now(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    struct MockClock {
        tick: AtomicU64,
    }

    impl MockClock {
        fn new(initial: u64) -> Self {
            Self {
                tick: AtomicU64::new(initial),
            }
        }

        fn advance(&self, ticks: u64) {
            self.tick.fetch_add(ticks, Ordering::Relaxed);
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> u64 {
            self.tick.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn system_clock_starts_at_zero() {
        let clock = SystemClock::new();
        let tick = clock.now();
        assert!(tick <= 5);
    }

    #[test]
    fn mock_clock_advance() {
        let clock = MockClock::new(0);
        assert_eq!(clock.now(), 0);
        clock.advance(100);
        assert_eq!(clock.now(), 100);
    }

    #[test]
    fn ticks_to_duration_1ms() {
        let duration = SystemClock::ticks_to_duration(1);
        assert_eq!(duration, Duration::from_millis(1));
    }

    #[test]
    fn ticks_to_duration_1s() {
        let duration = SystemClock::ticks_to_duration(1000);
        assert_eq!(duration, Duration::from_secs(1));
    }

    #[test]
    fn tick_ns_is_1ms() {
        assert_eq!(TICK_NS, 1_000_000);
    }
}
