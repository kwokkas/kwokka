//! Lock-free bounded multi-producer single-consumer ring buffer.
//!
//! Backs the per-worker wake inbox: many worker threads push wakes
//! into one target worker's inbox, and the owning worker drains it each
//! tick. Built on the Dmitry Vyukov bounded queue, restricted to a single
//! consumer so the dequeue side needs no compare-exchange.
//!
//! Each slot carries a sequence number that folds capacity-checking,
//! slot-ownership, and wraparound disambiguation into one atomic, so a full
//! ring is detected before any shared counter is mutated. The affine
//! runtime drives the ring single-producer (the lone worker, where the
//! enqueue compare-exchange never contends); the work-stealing runtime
//! drives it with many producers behind the same API.

#![allow(
    dead_code,
    reason = "MpscRing is wired up by worker::registry, ported in a later runtime PR"
)]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

#[cfg(not(loom))]
use core::sync::atomic::{AtomicUsize, Ordering};
use core::{cell::UnsafeCell, mem::MaybeUninit};

// Loom instruments the position and sequence atomics; the payload cells
// stay `core::cell::UnsafeCell` because the sequence handshake is the
// protocol the model explores, and the cells are checked through the
// popped-value assertions.
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

/// One ring slot: a payload cell plus its Vyukov sequence number.
///
/// `sequence` gates access to `cell`: a producer may write when
/// `sequence` equals the claimed position, a consumer may read when it
/// equals position + 1.
struct Slot<T> {
    sequence: AtomicUsize,
    cell: UnsafeCell<MaybeUninit<T>>,
}

/// Bounded MPSC ring buffer. `N` must be a power of two.
///
/// Producers call [`push`](Self::push) concurrently; the single owning
/// consumer calls [`pop`](Self::pop). Producers are lock-free (a
/// compare-exchange loop on `enqueue_pos`); the consumer is wait-free. No
/// allocation after construction.
///
/// # Safety invariant
///
/// Any number of threads may call `push`, but exactly one thread may call
/// `pop` at any time. Violating the single-consumer rule is undefined
/// behavior.
pub(crate) struct MpscRing<T, const N: usize> {
    /// Next position a producer claims; advanced by compare-exchange.
    enqueue_pos: AtomicUsize,
    /// Next position the consumer reads; advanced by the sole consumer.
    dequeue_pos: AtomicUsize,
    slots: [Slot<T>; N],
}

// SAFETY: Invariant -- concurrent producers are serialized onto disjoint
// claimed positions by the `enqueue_pos` compare-exchange, and each
// per-slot `sequence` Acquire/Release pair both publishes a payload to the
// consumer and frees the slot back to producers, so no two threads ever
// touch one slot's cell at once.
// Precondition: T: Send, because values move from producer threads to the
// consumer thread.
// Failure mode: sending a non-Send T across threads is a data race on T.
unsafe impl<T: Send, const N: usize> Send for MpscRing<T, N> {}

// SAFETY: Invariant -- `&self` access is sound because every cell mutation
// is gated by the per-slot sequence handshake: a producer writes a slot
// only after observing `sequence == claimed position` (the consumer has
// freed it), and the consumer reads only after observing
// `sequence == position + 1` (the producer has published it). Concurrent
// producers hold disjoint positions via the `enqueue_pos` compare-exchange.
// Precondition: at most one thread calls `pop` at any time; T: Send.
// Failure mode: a second concurrent consumer, or two producers writing one
// slot, race on the UnsafeCell.
unsafe impl<T: Send, const N: usize> Sync for MpscRing<T, N> {}

impl<T, const N: usize> MpscRing<T, N> {
    /// Creates an empty ring.
    ///
    /// The seeding loop sets slot `i` to `sequence == i`, the Vyukov
    /// invariant that lets a single counter distinguish empty from full.
    /// It runs at const-eval time, so a `static` array of rings costs
    /// nothing at runtime.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is not a power of two or is zero.
    #[cfg(not(loom))]
    pub(crate) const fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        let mut ring = Self {
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
            slots: [const {
                Slot {
                    sequence: AtomicUsize::new(0),
                    cell: UnsafeCell::new(MaybeUninit::uninit()),
                }
            }; N],
        };
        let mut i = 0;
        while i < N {
            ring.slots[i].sequence = AtomicUsize::new(i);
            i += 1;
        }
        ring
    }

    /// Loom variant -- `loom::sync::atomic::AtomicUsize::new` is not
    /// available in const context, so the const qualifier is dropped
    /// under `--cfg loom`.
    #[cfg(loom)]
    pub(crate) fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        Self {
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
            slots: core::array::from_fn(|i| Slot {
                sequence: AtomicUsize::new(i),
                cell: UnsafeCell::new(MaybeUninit::uninit()),
            }),
        }
    }

    /// Pushes a value. Returns `Err(value)` when the ring is full.
    ///
    /// Lock-free: many producers may call this concurrently, each claiming
    /// a distinct slot through a compare-exchange on `enqueue_pos`.
    ///
    /// # Errors
    ///
    /// Returns the value back when no capacity remains.
    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[pos & (N - 1)];
            let sequence = slot.sequence.load(Ordering::Acquire);
            // Wrapping gap from `pos`: 0 = slot free; past the half-range =
            // `sequence` lags `pos` (ring full); else a producer advanced
            // past `pos`, so reload.
            let gap = sequence.wrapping_sub(pos);
            if gap == 0 {
                match self.enqueue_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => pos = actual,
                }
            } else if gap > usize::MAX / 2 {
                return Err(value);
            } else {
                pos = self.enqueue_pos.load(Ordering::Relaxed);
            }
        }

        let slot = &self.slots[pos & (N - 1)];
        // SAFETY: Invariant -- the winning `enqueue_pos` compare-exchange
        // gives this producer exclusive ownership of `pos`; the matching
        // `sequence == pos` Acquire load (from the iteration that won)
        // synchronizes-with the consumer's Release that freed this slot. No
        // other thread accesses this cell.
        // Precondition: this producer won the compare-exchange for `pos`.
        // Failure mode: writing before the consumer's sequence Release is
        // observed races with the consumer's prior read of the cell.
        unsafe {
            (*slot.cell.get()).write(value);
        }
        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pops the next value, or `None` when the ring is empty.
    ///
    /// Wait-free. Only the single owning consumer may call this.
    pub(crate) fn pop(&self) -> Option<T> {
        let pos = self.dequeue_pos.load(Ordering::Relaxed);
        let slot = &self.slots[pos & (N - 1)];
        let sequence = slot.sequence.load(Ordering::Acquire);
        // `sequence == pos + 1` means the producer published this slot; a
        // wrapped-negative gap means the ring is empty.
        if sequence.wrapping_sub(pos.wrapping_add(1)) > usize::MAX / 2 {
            return None;
        }

        // SAFETY: Invariant -- the single consumer owns `pos`, and the
        // `sequence == pos + 1` Acquire load proved the producer finished
        // writing this slot.
        // Precondition: caller is the sole consumer; the slot holds the
        // value published for `pos`.
        // Failure mode: reading before the producer's Release store observes
        // uninitialized memory.
        let value = unsafe { (*slot.cell.get()).assume_init_read() };
        slot.sequence.store(pos.wrapping_add(N), Ordering::Release);
        self.dequeue_pos
            .store(pos.wrapping_add(1), Ordering::Relaxed);
        Some(value)
    }

    /// Number of elements currently in the ring.
    ///
    /// A racy upper bound under concurrent producers (claimed-but-unwritten
    /// positions count); exact when quiescent. The park bracket consults
    /// occupancy on the steal rings, where over-counting only aborts a park
    /// one pass early -- never under-counts a completed push.
    #[cfg(any(test, feature = "steal"))]
    pub(crate) fn len(&self) -> usize {
        let enqueue = self.enqueue_pos.load(Ordering::Acquire);
        let dequeue = self.dequeue_pos.load(Ordering::Acquire);
        enqueue.wrapping_sub(dequeue)
    }

    /// `true` when the ring contains no elements. See [`MpscRing::len`] for
    /// the racy-bound caveat and its park-bracket consumer.
    #[cfg(any(test, feature = "steal"))]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T, const N: usize> Drop for MpscRing<T, N> {
    fn drop(&mut self) {
        #[cfg(not(loom))]
        let (dequeue, enqueue) = (*self.dequeue_pos.get_mut(), *self.enqueue_pos.get_mut());
        // Loom atomics lack `get_mut`; a Relaxed load through `&mut self`
        // is equivalent because exclusive access excludes every concurrent
        // writer.
        #[cfg(loom)]
        let (dequeue, enqueue) = (
            self.dequeue_pos.load(Ordering::Relaxed),
            self.enqueue_pos.load(Ordering::Relaxed),
        );
        let mut pos = dequeue;
        while pos != enqueue {
            let slot = &mut self.slots[pos & (N - 1)];
            // SAFETY: Invariant -- positions in [dequeue_pos .. enqueue_pos)
            // hold values published by `push` and not yet consumed. `&mut
            // self` excludes every concurrent `&self` borrow, so no producer
            // can be mid-push: each claimed position in the range was fully
            // written before Drop ran.
            // Precondition: Drop holds exclusive ownership of the ring.
            // Failure mode: dropping an uninitialized or already-consumed
            // slot double-frees or reads uninitialized memory.
            unsafe {
                slot.cell.get_mut().assume_init_drop();
            }
            pos = pos.wrapping_add(1);
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    // Compile-time proof that `new` is usable in a `static`: the registry
    // inbox array relies on const construction.
    static _CONST_NEW: MpscRing<u8, 4> = MpscRing::new();

    #[test]
    fn push_to_empty_succeeds() {
        let ring: MpscRing<u32, 4> = MpscRing::new();
        assert!(ring.push(1).is_ok());
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn pop_from_empty_returns_none() {
        let ring: MpscRing<u32, 4> = MpscRing::new();
        assert!(ring.pop().is_none());
    }

    #[test]
    fn push_then_pop_returns_same_value() {
        let ring: MpscRing<u64, 4> = MpscRing::new();
        assert!(ring.push(42).is_ok());
        assert_eq!(ring.pop(), Some(42));
        assert!(ring.is_empty());
    }

    #[test]
    fn push_to_full_returns_err() {
        let ring: MpscRing<u32, 2> = MpscRing::new();
        assert!(ring.push(1).is_ok());
        assert!(ring.push(2).is_ok());
        assert_eq!(ring.push(3), Err(3));
    }

    #[test]
    fn single_producer_fifo_preserved() {
        let ring: MpscRing<u32, 4> = MpscRing::new();
        assert!(ring.push(10).is_ok());
        assert!(ring.push(20).is_ok());
        assert!(ring.push(30).is_ok());
        assert_eq!(ring.pop(), Some(10));
        assert_eq!(ring.pop(), Some(20));
        assert_eq!(ring.pop(), Some(30));
    }

    #[test]
    fn wrap_around_at_capacity_boundary() {
        let ring: MpscRing<u32, 2> = MpscRing::new();
        assert!(ring.push(1).is_ok());
        assert!(ring.push(2).is_ok());
        assert_eq!(ring.pop(), Some(1));
        assert!(ring.push(3).is_ok());
        assert_eq!(ring.pop(), Some(2));
        assert_eq!(ring.pop(), Some(3));
        assert!(ring.is_empty());
    }

    #[test]
    fn len_tracks_occupancy() {
        let ring: MpscRing<u32, 4> = MpscRing::new();
        assert_eq!(ring.len(), 0);
        assert!(ring.push(1).is_ok());
        assert_eq!(ring.len(), 1);
        assert!(ring.push(2).is_ok());
        assert_eq!(ring.len(), 2);
        assert!(ring.pop().is_some());
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn is_empty_reflects_state() {
        let ring: MpscRing<u32, 2> = MpscRing::new();
        assert!(ring.is_empty());
        assert!(ring.push(1).is_ok());
        assert!(!ring.is_empty());
        assert!(ring.pop().is_some());
        assert!(ring.is_empty());
    }

    #[test]
    fn drop_cleans_up_remaining_elements() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct Probe;
        impl Drop for Probe {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }

        DROP_COUNT.store(0, Ordering::Relaxed);
        {
            let ring: MpscRing<Probe, 4> = MpscRing::new();
            assert!(ring.push(Probe).is_ok());
            assert!(ring.push(Probe).is_ok());
            assert!(ring.push(Probe).is_ok());
        }
        assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn concurrent_producers_lose_no_values() {
        const PRODUCERS: usize = 3;
        const PER_PRODUCER: usize = 5_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        let ring: MpscRing<usize, 1024> = MpscRing::new();
        let mut seen = [false; TOTAL];

        std::thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let ring = &ring;
                scope.spawn(move || {
                    for i in 0..PER_PRODUCER {
                        let value = producer * PER_PRODUCER + i;
                        while ring.push(value).is_err() {
                            core::hint::spin_loop();
                        }
                    }
                });
            }

            let mut drained = 0;
            while drained < TOTAL {
                if let Some(value) = ring.pop() {
                    assert!(!seen[value], "value {value} popped twice");
                    seen[value] = true;
                    drained += 1;
                }
            }
        });

        assert!(seen.iter().all(|&was_seen| was_seen), "every value seen");
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::thread;

    use super::MpscRing;

    loom::lazy_static! {
        static ref PAIR_RING: MpscRing<u64, 4> = MpscRing::new();
        static ref SOLO_RING: MpscRing<u64, 4> = MpscRing::new();
    }

    // The multi-producer crux: two producers contend the enqueue
    // compare-exchange while the model's main thread drains. Every value
    // arrives exactly once -- no loss, no duplication -- across every
    // interleaving of the position claims and the slot sequence handshake.
    #[test]
    fn two_producers_and_a_consumer_lose_nothing() {
        loom::model(|| {
            let one = thread::spawn(|| {
                let Ok(()) = PAIR_RING.push(1) else {
                    panic!("a push into a free ring must succeed");
                };
            });
            let two = thread::spawn(|| {
                let Ok(()) = PAIR_RING.push(2) else {
                    panic!("a push into a free ring must succeed");
                };
            });
            let mut seen = [false; 3];
            let mut count = 0;
            while count < 2 {
                match PAIR_RING.pop() {
                    Some(value) => {
                        let idx = value as usize;
                        assert!(!seen[idx], "a value must arrive exactly once");
                        seen[idx] = true;
                        count += 1;
                    }
                    None => thread::yield_now(),
                }
            }
            let Ok(()) = one.join() else {
                panic!("the first producer must join cleanly");
            };
            let Ok(()) = two.join() else {
                panic!("the second producer must join cleanly");
            };
            assert!(seen[1] && seen[2], "both values arrive");
            assert!(PAIR_RING.pop().is_none(), "nothing is duplicated");
        });
    }

    // Smoke model for the loom shim: one producer thread, the model's main
    // thread consuming.
    #[test]
    fn single_producer_values_arrive_in_order() {
        loom::model(|| {
            let handle = thread::spawn(|| {
                let Ok(()) = SOLO_RING.push(7) else {
                    panic!("the first push into an empty ring must succeed");
                };
                let Ok(()) = SOLO_RING.push(8) else {
                    panic!("the second push into a free ring must succeed");
                };
            });
            let mut received = [0u64; 2];
            let mut count = 0;
            while count < 2 {
                match SOLO_RING.pop() {
                    Some(value) => {
                        received[count] = value;
                        count += 1;
                    }
                    None => thread::yield_now(),
                }
            }
            let Ok(()) = handle.join() else {
                panic!("the producer thread must join cleanly");
            };
            assert_eq!(received, [7, 8], "values arrive in push order");
        });
    }
}
