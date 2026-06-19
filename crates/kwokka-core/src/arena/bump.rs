//! [`BumpAllocator`] -- fixed-capacity bump allocator with LIFO drop
//! registry.

use core::{alloc::Layout, fmt, mem::MaybeUninit, ptr::NonNull};

use crate::arena::builder::BumpAllocatorBuilder;
use crate::arena::phase::ArenaPhase;
use crate::flat::FlatLayout;
use crate::generation::Generation;

/// Errors emitted by [`BumpAllocator`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ArenaError {
    /// Builder requested zero bytes.
    ZeroCapacity,
    /// Builder specified an alignment that is not a power of two.
    InvalidAlignment {
        /// The invalid alignment value.
        alignment: usize,
    },
    /// Allocation request exceeds remaining capacity.
    Exhausted {
        /// Bytes the failed call asked for (including alignment padding).
        requested: usize,
        /// Bytes still available before the failure.
        available: usize,
    },
    /// `alloc_with_drop` called but the drop registry is full.
    DropRegistryFull {
        /// Configured drop registry capacity.
        capacity: usize,
    },
    /// Allocation attempted in the Frozen phase.
    WrongPhase {
        /// Phase observed at the call site.
        current: ArenaPhase,
    },
}

impl fmt::Display for ArenaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("arena builder requested zero bytes"),
            Self::InvalidAlignment { alignment } => {
                write!(
                    f,
                    "arena alignment must be a power of two (got {alignment})"
                )
            }
            Self::Exhausted {
                requested,
                available,
            } => write!(
                f,
                "arena exhausted: requested {requested}, available {available}"
            ),
            Self::DropRegistryFull { capacity } => {
                write!(f, "arena drop registry full (capacity {capacity})")
            }
            Self::WrongPhase { current } => write!(
                f,
                "arena allocation requires Build phase (current {current:?})"
            ),
        }
    }
}

impl core::error::Error for ArenaError {}

/// LIFO drop registry entry; `drop_fn` is the monomorphised destructor.
struct DropEntry {
    ptr: *mut u8,
    drop_fn: unsafe fn(*mut u8),
}

/// Fixed-capacity bump allocator with a LIFO drop registry.
///
/// Two allocation paths:
///
/// - `alloc` -- fast path for `T: FlatLayout`; nothing to drop, so `reset()` reclaims the whole
///   region in O(1).
/// - `alloc_with_drop` -- registers a destructor in a fixed-size LIFO, invoked in reverse order on
///   `reset()` and on `Drop`.
///
/// Lifecycle: `Build` -> `freeze()` -> `Frozen` -> `reset()` -> `Build`.
/// Allocation is permitted only in Build; `Frozen` holds every live
/// pointer stable for reads.
///
/// `reset()` reclaims the whole region at once and tracks no outstanding
/// handles: every pointer an `alloc*` call returned dangles afterward.
/// Keeping no handle live across a reset is the caller's contract, which
/// keeps the reset path atomic-free.
pub struct BumpAllocator {
    buffer: NonNull<u8>,
    capacity: usize,
    alignment: usize,
    cursor: usize,
    drops: Vec<DropEntry>,
    drop_capacity: usize,
    phase: ArenaPhase,
    generation: Generation,
}

impl BumpAllocator {
    /// Returns a fresh [`BumpAllocatorBuilder`].
    #[inline]
    #[must_use]
    pub const fn builder() -> BumpAllocatorBuilder {
        BumpAllocatorBuilder::new()
    }

    pub(super) fn from_builder(
        bytes: usize,
        drop_slots: usize,
        alignment: usize,
    ) -> Result<Self, ArenaError> {
        if bytes == 0 {
            return Err(ArenaError::ZeroCapacity);
        }
        if !alignment.is_power_of_two() {
            return Err(ArenaError::InvalidAlignment { alignment });
        }
        let layout = buffer_layout(bytes, alignment)?;
        // SAFETY: layout has nonzero size (bytes > 0 checked above)
        // and a valid power-of-two alignment (validated above). The
        // returned region is owned exclusively by this allocator
        // until Drop. Null return is handled below via NonNull::new.
        let raw = unsafe { std::alloc::alloc(layout) };
        let Some(buffer) = NonNull::new(raw) else {
            std::alloc::handle_alloc_error(layout);
        };
        Ok(Self {
            buffer,
            capacity: bytes,
            alignment,
            cursor: 0,
            drops: Vec::with_capacity(drop_slots),
            drop_capacity: drop_slots,
            phase: ArenaPhase::Build,
            generation: Generation::ZERO,
        })
    }

    fn alloc_raw(&mut self, layout: Layout) -> Result<NonNull<u8>, ArenaError> {
        if self.phase != ArenaPhase::Build {
            return Err(ArenaError::WrongPhase {
                current: self.phase,
            });
        }
        if layout.align() > self.alignment {
            return Err(self.exhausted(layout.size() + layout.align()));
        }
        let aligned = align_up(self.cursor, layout.align());
        let new_cursor = aligned
            .checked_add(layout.size())
            .ok_or_else(|| self.exhausted(layout.size()))?;
        if new_cursor > self.capacity {
            return Err(self.exhausted(new_cursor - self.cursor));
        }
        // SAFETY: new_cursor = aligned + layout.size() <= capacity
        // (checked above), so `aligned` indexes within the backing
        // region; for a zero-size layout `aligned` may equal `capacity`
        // (one-past-the-end), which is valid to form but not to
        // dereference (the zero-size write is a no-op). `buffer` has
        // provenance over the entire std::alloc region; `add` preserves
        // it and `new_unchecked` is sound because `buffer` is NonNull
        // and the offset cannot wrap to null. OOB add or null NonNull
        // would be immediate UB.
        let ptr = unsafe {
            let raw = self.buffer.as_ptr().add(aligned);
            NonNull::new_unchecked(raw)
        };
        self.cursor = new_cursor;
        Ok(ptr)
    }

    #[inline]
    const fn exhausted(&self, requested: usize) -> ArenaError {
        ArenaError::Exhausted {
            requested,
            available: self.capacity - self.cursor,
        }
    }

    /// Allocates a `T: FlatLayout` value, returning a typed pointer.
    ///
    /// The pointer is valid until the next [`reset`] or until the arena
    /// drops; it dangles afterward. Reading or writing through it is the
    /// caller's `unsafe` obligation.
    ///
    /// # Errors
    ///
    /// - [`ArenaError::WrongPhase`] when called outside Build.
    /// - [`ArenaError::Exhausted`] when remaining capacity cannot fit the value (including
    ///   alignment padding).
    ///
    /// [`reset`]: BumpAllocator::reset
    pub fn alloc<T: FlatLayout>(&mut self, value: T) -> Result<NonNull<T>, ArenaError> {
        let layout = Layout::new::<T>();
        let raw = self.alloc_raw(layout)?;
        let typed = raw.cast::<T>();
        // SAFETY: `alloc_raw` returned a writable, aligned pointer
        // with backing storage valid for `size_of::<T>()` bytes.
        // Writing out of bounds would corrupt adjacent allocations.
        unsafe { typed.as_ptr().write(value) };
        Ok(typed)
    }

    /// Allocates a value of any `T` and registers its destructor.
    ///
    /// The drop function is invoked in LIFO order on the next `reset()`
    /// and on `Drop`.
    ///
    /// The returned pointer is valid until the next [`reset`] or until
    /// the arena drops; it dangles afterward, and `reset` also runs the
    /// registered destructor. Reading or writing through it is the
    /// caller's `unsafe` obligation.
    ///
    /// # Errors
    ///
    /// - [`ArenaError::WrongPhase`] when called outside Build.
    /// - [`ArenaError::Exhausted`] when the buffer cannot fit `T`.
    /// - [`ArenaError::DropRegistryFull`] when the drop registry is at capacity.
    ///
    /// [`reset`]: BumpAllocator::reset
    pub fn alloc_with_drop<T>(&mut self, value: T) -> Result<NonNull<T>, ArenaError> {
        if self.drops.len() >= self.drop_capacity {
            return Err(ArenaError::DropRegistryFull {
                capacity: self.drop_capacity,
            });
        }
        let layout = Layout::new::<T>();
        let raw = self.alloc_raw(layout)?;
        let typed = raw.cast::<T>();
        // SAFETY: same as `alloc` -- aligned, owned, big enough for T.
        // Writing out of bounds would corrupt adjacent allocations.
        unsafe { typed.as_ptr().write(value) };
        self.drops.push(DropEntry {
            ptr: typed.as_ptr().cast::<u8>(),
            drop_fn: drop_in_place_for::<T>,
        });
        Ok(typed)
    }

    /// Allocates an uninitialized slice of `count` `T: FlatLayout`.
    ///
    /// The returned pointer is valid until the next [`reset`] or until
    /// the arena drops; it dangles afterward. Initializing and reading
    /// the slice is the caller's `unsafe` obligation.
    ///
    /// # Errors
    ///
    /// - [`ArenaError::WrongPhase`] when called outside Build.
    /// - [`ArenaError::Exhausted`] when the buffer cannot fit the slice or `count * size_of::<T>()`
    ///   overflows.
    ///
    /// [`reset`]: BumpAllocator::reset
    pub fn alloc_slice<T: FlatLayout>(
        &mut self,
        count: usize,
    ) -> Result<NonNull<[MaybeUninit<T>]>, ArenaError> {
        let bytes = core::mem::size_of::<T>()
            .checked_mul(count)
            .ok_or_else(|| self.exhausted(usize::MAX))?;
        let layout = Layout::from_size_align(bytes, core::mem::align_of::<T>())
            .map_err(|_| self.exhausted(bytes))?;
        let raw = self.alloc_raw(layout)?;
        let typed = raw.cast::<MaybeUninit<T>>();
        Ok(NonNull::slice_from_raw_parts(typed, count))
    }

    /// Transitions Build -> Frozen.
    ///
    /// Subsequent `alloc*` calls return [`ArenaError::WrongPhase`].
    pub const fn freeze(&mut self) {
        self.phase = ArenaPhase::Frozen;
    }

    /// Transitions Frozen -> Build, runs LIFO drops, and bumps the generation.
    ///
    /// Every pointer a prior `alloc*` call returned dangles after this
    /// call. The arena keeps no outstanding-handle count, so ensuring
    /// none is live at reset is the caller's responsibility, which keeps
    /// the reset path free of atomics. A higher-level coordinator that
    /// owns the allocator is expected to enforce that guard.
    pub fn reset(&mut self) {
        self.run_drops();
        self.cursor = 0;
        self.phase = ArenaPhase::Build;
        self.generation = self.generation.next();
    }

    /// Returns the current lifecycle phase.
    #[inline]
    #[must_use]
    pub const fn phase(&self) -> ArenaPhase {
        self.phase
    }

    /// Returns the current generation.
    #[inline]
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Returns the number of bytes consumed in the active phase.
    #[inline]
    #[must_use]
    pub const fn used(&self) -> usize {
        self.cursor
    }

    /// Returns the total backing capacity in bytes.
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns bytes still available in the active phase.
    #[inline]
    #[must_use]
    pub const fn available(&self) -> usize {
        self.capacity - self.cursor
    }

    #[cfg(test)]
    fn buffer_addr(&self) -> usize {
        self.buffer.as_ptr() as usize
    }

    fn run_drops(&mut self) {
        while let Some(entry) = self.drops.pop() {
            // SAFETY: each entry was registered by `alloc_with_drop`
            // for a value of the type whose monomorphised
            // `drop_in_place_for` is stored in `entry.drop_fn`. The
            // pointer is valid until consumed here. Double-drop would
            // cause UB on the value's fields.
            unsafe { (entry.drop_fn)(entry.ptr) };
        }
    }
}

fn drop_in_place_for<T>(ptr: *mut u8) {
    // SAFETY: caller guarantees `ptr` was produced by
    // `alloc_with_drop::<T>` and has not been read out since
    // registration. Mismatched T would cause UB.
    unsafe { ptr.cast::<T>().drop_in_place() };
}

impl Drop for BumpAllocator {
    fn drop(&mut self) {
        self.run_drops();
        let Ok(layout) = Layout::from_size_align(self.capacity, self.alignment) else {
            return;
        };
        // SAFETY: `buffer` was allocated by `std::alloc::alloc` with
        // this exact layout in `from_builder` and has not been freed
        // since; the allocator owns it exclusively. Double-free or
        // mismatched layout causes heap corruption.
        unsafe { std::alloc::dealloc(self.buffer.as_ptr(), layout) };
    }
}

#[inline]
const fn align_up(addr: usize, align: usize) -> usize {
    (addr + align - 1) & !(align - 1)
}

#[inline]
fn buffer_layout(bytes: usize, alignment: usize) -> Result<Layout, ArenaError> {
    Layout::from_size_align(bytes, alignment).map_err(|_| ArenaError::Exhausted {
        requested: bytes,
        available: 0,
    })
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    fn build_or_panic(bytes: usize, drops: usize) -> BumpAllocator {
        let Ok(arena) = BumpAllocator::builder()
            .bytes(bytes)
            .drop_slots(drops)
            .build()
        else {
            panic!("builder must succeed for valid bytes/drops")
        };
        arena
    }

    #[test]
    fn builder_default_rejects_alloc_with_drop() {
        let mut arena = build_or_panic(1024, 0);
        assert_eq!(
            arena.alloc_with_drop(42u32).err(),
            Some(ArenaError::DropRegistryFull { capacity: 0 })
        );
    }

    #[test]
    fn builder_zero_bytes_returns_zero_capacity() {
        assert_eq!(
            BumpAllocator::builder().bytes(0).build().err(),
            Some(ArenaError::ZeroCapacity)
        );
    }

    #[test]
    fn alloc_flatlayout_roundtrip() {
        let mut arena = build_or_panic(64, 0);
        let Ok(ptr) = arena.alloc::<u32>(0xDEAD_BEEF) else {
            panic!("alloc must succeed in 64-byte arena")
        };
        // SAFETY: pointer was just written and lives until arena drops.
        let value = unsafe { ptr.as_ptr().read() };
        assert_eq!(value, 0xDEAD_BEEF);
    }

    #[test]
    fn alloc_with_drop_invokes_drop_in_lifo_on_reset() {
        use core::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

        struct Order {
            cursor: AtomicUsize,
            slots: [AtomicU32; 3],
        }
        struct Recorder<'a> {
            order: &'a Order,
            tag: u32,
        }
        impl Drop for Recorder<'_> {
            fn drop(&mut self) {
                let slot = self.order.cursor.fetch_add(1, Ordering::Relaxed);
                self.order.slots[slot].store(self.tag, Ordering::Relaxed);
            }
        }
        let order = Order {
            cursor: AtomicUsize::new(0),
            slots: [AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0)],
        };
        let mut arena = build_or_panic(256, 4);
        let Ok(_) = arena.alloc_with_drop(Recorder {
            order: &order,
            tag: 1,
        }) else {
            panic!("first alloc_with_drop must succeed")
        };
        let Ok(_) = arena.alloc_with_drop(Recorder {
            order: &order,
            tag: 2,
        }) else {
            panic!("second alloc_with_drop must succeed")
        };
        let Ok(_) = arena.alloc_with_drop(Recorder {
            order: &order,
            tag: 3,
        }) else {
            panic!("third alloc_with_drop must succeed")
        };
        arena.freeze();
        arena.reset();
        let recorded = [
            order.slots[0].load(Ordering::Relaxed),
            order.slots[1].load(Ordering::Relaxed),
            order.slots[2].load(Ordering::Relaxed),
        ];
        assert_eq!(recorded, [3, 2, 1]);
    }

    #[test]
    fn alloc_in_frozen_returns_wrong_phase() {
        let mut arena = build_or_panic(64, 0);
        arena.freeze();
        assert_eq!(
            arena.alloc::<u32>(7).err(),
            Some(ArenaError::WrongPhase {
                current: ArenaPhase::Frozen
            })
        );
    }

    #[test]
    fn reset_returns_to_build() {
        let mut arena = build_or_panic(64, 0);
        arena.freeze();
        arena.reset();
        assert_eq!(arena.phase(), ArenaPhase::Build);
    }

    #[test]
    fn reset_bumps_generation() {
        let mut arena = build_or_panic(64, 0);
        let g0 = arena.generation();
        arena.freeze();
        arena.reset();
        assert_ne!(g0, arena.generation());
    }

    #[test]
    fn exhausted_returns_error_with_sizes() {
        let mut arena = build_or_panic(8, 0);
        let Ok(_) = arena.alloc::<u64>(1) else {
            panic!("first u64 must fit in 8-byte arena")
        };
        let Err(err) = arena.alloc::<u32>(2) else {
            panic!("second alloc must exhaust the arena")
        };
        assert!(matches!(err, ArenaError::Exhausted { .. }));
    }

    #[test]
    fn drop_slot_capacity_enforced() {
        let mut arena = build_or_panic(256, 1);
        let Ok(_) = arena.alloc_with_drop(0u32) else {
            panic!("first alloc_with_drop must succeed")
        };
        assert_eq!(
            arena.alloc_with_drop(1u32).err(),
            Some(ArenaError::DropRegistryFull { capacity: 1 })
        );
    }

    #[test]
    fn drop_invokes_pending_drops_on_drop() {
        use core::sync::atomic::{AtomicUsize, Ordering};

        struct Bomb<'a>(&'a AtomicUsize);
        impl Drop for Bomb<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drop_count = AtomicUsize::new(0);
        {
            let mut arena = build_or_panic(64, 2);
            let Ok(_) = arena.alloc_with_drop(Bomb(&drop_count)) else {
                panic!("first alloc_with_drop must succeed")
            };
            let Ok(_) = arena.alloc_with_drop(Bomb(&drop_count)) else {
                panic!("second alloc_with_drop must succeed")
            };
        }
        assert_eq!(drop_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn used_and_available_track_cursor() {
        let mut arena = build_or_panic(16, 0);
        assert_eq!(arena.used(), 0);
        assert_eq!(arena.available(), 16);
        let Ok(_) = arena.alloc::<u32>(0) else {
            panic!("u32 must fit in 16-byte arena")
        };
        assert_eq!(arena.used(), 4);
        assert_eq!(arena.available(), 12);
    }

    #[test]
    fn arenaerror_display_messages() {
        assert_eq!(
            ArenaError::ZeroCapacity.to_string(),
            "arena builder requested zero bytes"
        );
        assert_eq!(
            ArenaError::DropRegistryFull { capacity: 4 }.to_string(),
            "arena drop registry full (capacity 4)"
        );
        assert_eq!(
            ArenaError::InvalidAlignment { alignment: 3 }.to_string(),
            "arena alignment must be a power of two (got 3)"
        );
    }

    #[test]
    fn alignment_8_buffer_is_8_aligned() {
        let Ok(arena) = BumpAllocator::builder().bytes(64).alignment(8).build() else {
            panic!("builder must succeed for alignment 8")
        };
        assert_eq!(arena.buffer_addr() % 8, 0);
    }

    #[test]
    fn alignment_64_buffer_is_64_aligned() {
        let Ok(arena) = BumpAllocator::builder().bytes(128).alignment(64).build() else {
            panic!("builder must succeed for alignment 64")
        };
        assert_eq!(arena.buffer_addr() % 64, 0);
    }

    #[test]
    fn alignment_4096_buffer_is_4k_aligned() {
        let Ok(arena) = BumpAllocator::builder().bytes(8192).alignment(4096).build() else {
            panic!("builder must succeed for alignment 4096")
        };
        assert_eq!(arena.buffer_addr() % 4096, 0);
    }

    #[test]
    fn alignment_non_power_of_two_rejected() {
        assert_eq!(
            BumpAllocator::builder()
                .bytes(64)
                .alignment(3)
                .build()
                .err(),
            Some(ArenaError::InvalidAlignment { alignment: 3 })
        );
    }

    #[test]
    fn alignment_zero_rejected() {
        assert_eq!(
            BumpAllocator::builder()
                .bytes(64)
                .alignment(0)
                .build()
                .err(),
            Some(ArenaError::InvalidAlignment { alignment: 0 })
        );
    }

    #[test]
    fn alignment_preserved_across_reset() {
        let Ok(mut arena) = BumpAllocator::builder().bytes(8192).alignment(4096).build() else {
            panic!("builder must succeed")
        };
        let addr_before = arena.buffer_addr();
        arena.freeze();
        arena.reset();
        assert_eq!(arena.buffer_addr(), addr_before);
        assert_eq!(arena.buffer_addr() % 4096, 0);
    }
}
