//! Per-worker wake endpoint -- the published signal target for cross-worker
//! wakes.
//!
//! A worker publishes its wake `eventfd` here at bootstrap and brackets each
//! park with the parked flag. A producer that lands a wake in the worker's
//! inbox signals the fd only when the flag shows the worker is parked, so a
//! running worker pays no syscall for an inbox it drains on its next tick
//! anyway. `msg_ring` targeting (kernel 5.18+) replaces the eventfd write on
//! this same endpoint shape when it lands.
//!
//! The parked handshake is the classic store-load race: the consumer stores
//! the flag and re-checks its inbox, the producer pushes and loads the flag.
//! Both sides order the pair through a sequentially consistent fence
//! ([`EndpointCell::set_parked`] and [`EndpointCell::signal_target`]); the
//! loom model in this module pins the no-lost-wake invariant.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

#[cfg(not(loom))]
use core::sync::atomic::{AtomicI32, AtomicU64, Ordering, fence};

#[cfg(loom)]
use loom::sync::atomic::{AtomicI32, AtomicU64, Ordering, fence};

/// Per-worker wake endpoint state.
///
/// The `signal` word holds `event_fd + 1` in bits 32..64 (zero means
/// unpublished, so fd 0 stays representable) and the parked flag in bit 0, so
/// publish, withdraw, park bracketing, and the producer's read are each a
/// single operation on it. `ring_fd` is the worker's own ring, the `msg_ring`
/// wake target, written once at publish and read only after the parked check,
/// so it stays out of the parked handshake.
pub(crate) struct EndpointCell {
    signal: AtomicU64,
    ring_fd: AtomicI32,
}

/// Parked flag -- set by the owning worker around each park.
const PARKED: u64 = 1;
/// Shift of the `event_fd + 1` field.
const FD_SHIFT: u32 = 32;

/// The wake targets a parked, published worker resolves to.
///
/// `event_fd` is the eventfd fallback, always present. `ring_fd` is the
/// worker's own ring, the `msg_ring` wake target, present only on the
/// `io_uring` backend.
pub(crate) struct WakeTarget {
    /// The eventfd to signal.
    pub(crate) event_fd: i32,
    /// The worker's own ring fd, when it has one.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "read by the msg_ring wake branch in a later part of this change"
        )
    )]
    pub(crate) ring_fd: Option<i32>,
}

impl EndpointCell {
    /// Empty, unpublished cell.
    #[cfg(not(loom))]
    pub(crate) const fn new() -> Self {
        Self {
            signal: AtomicU64::new(0),
            ring_fd: AtomicI32::new(-1),
        }
    }

    /// Empty, unpublished cell.
    ///
    /// Non-`const` under loom -- the instrumented atomic offers no
    /// compile-time constructor.
    #[cfg(loom)]
    pub(crate) fn new() -> Self {
        Self {
            signal: AtomicU64::new(0),
            ring_fd: AtomicI32::new(-1),
        }
    }

    /// Publishes `event_fd` and the worker's own `ring_fd` as its wake targets,
    /// unparked.
    ///
    /// The signal store covers the whole word, so it must precede the worker's
    /// first park and every producer's first signal -- the bootstrap publishes
    /// every endpoint before any worker starts looping. A publish racing a park
    /// bracket would clobber the parked flag. `ring_fd` is written first so it
    /// is visible to any producer that later observes the published signal word.
    pub(crate) fn publish(&self, event_fd: i32, ring_fd: Option<i32>) {
        self.ring_fd.store(ring_fd.unwrap_or(-1), Ordering::Release);
        let fd_bits = u32::from_ne_bytes(event_fd.to_ne_bytes());
        let packed = (u64::from(fd_bits) + 1) << FD_SHIFT;
        self.signal.store(packed, Ordering::Release);
    }

    /// Withdraws the endpoint; later signals resolve to nothing.
    #[cfg_attr(
        loom,
        expect(
            dead_code,
            reason = "the loom model exercises the parked handshake; withdraw is consumed through the registry, which the loom build gates out"
        )
    )]
    pub(crate) fn withdraw(&self) {
        self.ring_fd.store(-1, Ordering::Release);
        self.signal.store(0, Ordering::Release);
    }

    /// Brackets a park: the owning worker raises the flag before its final
    /// inbox re-check and clears it on wake-up.
    ///
    /// The raise carries a sequentially consistent fence so the flag store
    /// is ordered before the inbox re-check that follows it -- the
    /// consumer's half of the parked handshake.
    pub(crate) fn set_parked(&self, is_parked: bool) {
        if is_parked {
            self.signal.fetch_or(PARKED, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            return;
        }
        self.signal.fetch_and(!PARKED, Ordering::SeqCst);
    }

    /// The wake targets to signal, when the worker is published and parked.
    ///
    /// Carries a sequentially consistent fence so the producer's preceding
    /// inbox push is ordered before this flag load -- the producer's half
    /// of the parked handshake. A running or unpublished worker resolves to
    /// `None` and costs the producer no syscall.
    pub(crate) fn signal_target(&self) -> Option<WakeTarget> {
        fence(Ordering::SeqCst);
        let packed = self.signal.load(Ordering::SeqCst);
        if packed & PARKED == 0 {
            return None;
        }
        let fd_plus_one = packed >> FD_SHIFT;
        if fd_plus_one == 0 {
            return None;
        }
        #[allow(
            clippy::cast_possible_truncation,
            reason = "the field was packed from an i32 fd in publish"
        )]
        let fd_bits = (fd_plus_one - 1) as u32;
        let event_fd = i32::from_ne_bytes(fd_bits.to_ne_bytes());
        // The ring fd was Released at publish, ordered before this load by the
        // publish-before-signal invariant; -1 marks a worker with no ring.
        let ring = self.ring_fd.load(Ordering::Acquire);
        let ring_fd = if ring >= 0 { Some(ring) } else { None };
        Some(WakeTarget { event_fd, ring_fd })
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn an_unpublished_cell_resolves_to_nothing() {
        let cell = EndpointCell::new();
        assert!(cell.signal_target().is_none());
        cell.set_parked(true);
        assert!(
            cell.signal_target().is_none(),
            "a parked flag without a published fd must stay silent",
        );
    }

    #[test]
    fn a_running_worker_is_never_signaled() {
        let cell = EndpointCell::new();
        cell.publish(5, Some(9));
        assert!(
            cell.signal_target().is_none(),
            "published but unparked resolves to nothing",
        );
    }

    #[test]
    fn a_parked_worker_resolves_to_its_targets() {
        let cell = EndpointCell::new();
        cell.publish(5, Some(9));
        cell.set_parked(true);
        let Some(target) = cell.signal_target() else {
            panic!("a parked, published worker resolves to its targets");
        };
        assert_eq!(target.event_fd, 5);
        assert_eq!(target.ring_fd, Some(9), "the ring fd is carried");
        cell.set_parked(false);
        assert!(cell.signal_target().is_none());
    }

    #[test]
    fn a_worker_without_a_ring_carries_none() {
        let cell = EndpointCell::new();
        cell.publish(5, None);
        cell.set_parked(true);
        let Some(target) = cell.signal_target() else {
            panic!("a parked, published worker resolves to its targets");
        };
        assert_eq!(target.event_fd, 5);
        assert_eq!(
            target.ring_fd, None,
            "a fallback backend has no ring wake target",
        );
    }

    #[test]
    fn fd_zero_is_representable() {
        let cell = EndpointCell::new();
        cell.publish(0, Some(0));
        cell.set_parked(true);
        let Some(target) = cell.signal_target() else {
            panic!("fd zero must resolve");
        };
        assert_eq!(target.event_fd, 0);
        assert_eq!(target.ring_fd, Some(0), "ring fd zero is representable");
    }

    #[test]
    fn withdraw_clears_targets_and_flag() {
        let cell = EndpointCell::new();
        cell.publish(7, Some(3));
        cell.set_parked(true);
        cell.withdraw();
        assert!(cell.signal_target().is_none());
        cell.set_parked(true);
        assert!(
            cell.signal_target().is_none(),
            "a withdrawn endpoint must not resurrect through the flag",
        );
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::thread;

    use super::EndpointCell;
    use crate::sync::mpsc::MpscRing;

    loom::lazy_static! {
        static ref WAKE_RING: MpscRing<u64, 4> = MpscRing::new();
        static ref WAKE_CELL: EndpointCell = EndpointCell::new();
    }

    // The parked handshake's crux: the producer pushes then loads the flag,
    // the consumer raises the flag then re-checks the inbox. Every
    // interleaving must end with the wake either consumed or signaled --
    // a parked worker left with a silent non-empty inbox is the lost wake.
    #[test]
    fn a_parked_worker_with_a_pending_wake_is_always_signaled() {
        loom::model(|| {
            // Bootstrap contract: the endpoint is published before the
            // worker loops or any producer signals (see publish).
            WAKE_CELL.publish(3, None);

            let producer = thread::spawn(|| {
                let Ok(()) = WAKE_RING.push(7) else {
                    panic!("a push into an empty ring must succeed");
                };
                // Read both resolved targets, not just presence: the published
                // event fd and the absent ring fd (published with `None`).
                WAKE_CELL
                    .signal_target()
                    .is_some_and(|target| target.event_fd == 3 && target.ring_fd.is_none())
            });

            // Consumer half: drain, raise the flag, re-check, then park.
            let mut consumed = WAKE_RING.pop().is_some();
            if !consumed {
                WAKE_CELL.set_parked(true);
                consumed = WAKE_RING.pop().is_some();
            }

            let Ok(was_signaled) = producer.join() else {
                panic!("the producer thread must join cleanly");
            };
            assert!(
                consumed || was_signaled,
                "a wake left in the inbox of a parked worker must be signaled",
            );
        });
    }
}
