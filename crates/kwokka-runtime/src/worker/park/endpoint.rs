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
use core::sync::atomic::{AtomicU64, Ordering, fence};

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering, fence};

/// Packed per-worker endpoint state.
///
/// Layout: bits 32..64 hold `event_fd + 1` (zero means unpublished, so fd 0
/// stays representable); bit 0 is the parked flag. One atomic word keeps
/// publish, withdraw, park bracketing, and the producer's read each a single
/// operation.
pub(crate) struct EndpointCell(AtomicU64);

/// Parked flag -- set by the owning worker around each park.
const PARKED: u64 = 1;
/// Shift of the `event_fd + 1` field.
const FD_SHIFT: u32 = 32;

impl EndpointCell {
    /// Empty, unpublished cell.
    #[cfg(not(loom))]
    pub(crate) const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Empty, unpublished cell.
    ///
    /// Non-`const` under loom -- the instrumented atomic offers no
    /// compile-time constructor.
    #[cfg(loom)]
    pub(crate) fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Publishes `event_fd` as this worker's wake target, unparked.
    ///
    /// The store covers the whole word, so it must precede the worker's
    /// first park and every producer's first signal -- the bootstrap
    /// publishes every endpoint before any worker starts looping. A
    /// publish racing a park bracket would clobber the parked flag.
    pub(crate) fn publish(&self, event_fd: i32) {
        let fd_bits = u32::from_ne_bytes(event_fd.to_ne_bytes());
        let packed = (u64::from(fd_bits) + 1) << FD_SHIFT;
        self.0.store(packed, Ordering::Release);
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
        self.0.store(0, Ordering::Release);
    }

    /// Brackets a park: the owning worker raises the flag before its final
    /// inbox re-check and clears it on wake-up.
    ///
    /// The raise carries a sequentially consistent fence so the flag store
    /// is ordered before the inbox re-check that follows it -- the
    /// consumer's half of the parked handshake.
    pub(crate) fn set_parked(&self, is_parked: bool) {
        if is_parked {
            self.0.fetch_or(PARKED, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            return;
        }
        self.0.fetch_and(!PARKED, Ordering::SeqCst);
    }

    /// The fd to signal, when the worker is published and parked.
    ///
    /// Carries a sequentially consistent fence so the producer's preceding
    /// inbox push is ordered before this flag load -- the producer's half
    /// of the parked handshake. A running or unpublished worker resolves to
    /// `None` and costs the producer no syscall.
    pub(crate) fn signal_target(&self) -> Option<i32> {
        fence(Ordering::SeqCst);
        let packed = self.0.load(Ordering::SeqCst);
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
        Some(i32::from_ne_bytes(fd_bits.to_ne_bytes()))
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn an_unpublished_cell_resolves_to_nothing() {
        let cell = EndpointCell::new();
        assert_eq!(cell.signal_target(), None);
        cell.set_parked(true);
        assert_eq!(
            cell.signal_target(),
            None,
            "a parked flag without a published fd must stay silent",
        );
    }

    #[test]
    fn a_running_worker_is_never_signaled() {
        let cell = EndpointCell::new();
        cell.publish(5);
        assert_eq!(
            cell.signal_target(),
            None,
            "published but unparked resolves to nothing",
        );
    }

    #[test]
    fn a_parked_worker_resolves_to_its_fd() {
        let cell = EndpointCell::new();
        cell.publish(5);
        cell.set_parked(true);
        assert_eq!(cell.signal_target(), Some(5));
        cell.set_parked(false);
        assert_eq!(cell.signal_target(), None);
    }

    #[test]
    fn fd_zero_is_representable() {
        let cell = EndpointCell::new();
        cell.publish(0);
        cell.set_parked(true);
        assert_eq!(cell.signal_target(), Some(0));
    }

    #[test]
    fn withdraw_clears_fd_and_flag() {
        let cell = EndpointCell::new();
        cell.publish(7);
        cell.set_parked(true);
        cell.withdraw();
        assert_eq!(cell.signal_target(), None);
        cell.set_parked(true);
        assert_eq!(
            cell.signal_target(),
            None,
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
            WAKE_CELL.publish(3);

            let producer = thread::spawn(|| {
                let Ok(()) = WAKE_RING.push(7) else {
                    panic!("a push into an empty ring must succeed");
                };
                WAKE_CELL.signal_target().is_some()
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
