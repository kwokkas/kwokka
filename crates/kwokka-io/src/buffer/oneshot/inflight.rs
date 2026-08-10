//! Per-worker in-flight buffer registry for drop-safe completion futures.
//!
//! Completion-based I/O hands the kernel a pointer into a buffer that must
//! outlive the submitting future. If the future is dropped before its
//! completion arrives, inline storage would free under an in-flight kernel
//! write -- undefined behavior. This registry owns the bytes instead: a
//! future holds a Copy [`InflightSlotKey`], and the bytes live in
//! fixed-capacity mmap storage freed only after the completion is harvested,
//! never on the drop.
//!
//! Zero-heap: the backing is one `MmapRegion` allocated at construction and
//! never grown.
//!
//! Generational: each slot carries a generation bumped on free, so a stale
//! handle for a reused slot is rejected. A retire-pending bit alone cannot
//! tell one occupant from the next.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) restricts slab internals inside the now-pub inflight module"
)]

use std::{
    io,
    os::fd::{AsRawFd, OwnedFd},
};

use crate::{buffer::storage::mmap::MmapRegion, operation::OpCode};

/// Per-slot byte capacity; a buffered future's `CAP` must not exceed this.
pub(crate) const INFLIGHT_BUF_STRIDE: u32 = 4096;

/// Public capacity ceiling for a single buffered op's `CAP`.
///
/// Mirrors the per-slot in-flight stride as a `usize`. The in-flight slot
/// registry backs every buffered `recv`/`send`/`send_zc` future regardless of
/// buffer type, so a `CAP` past this stride cannot submit and the driver rejects it
/// with `-EINVAL` at runtime. A `CAP`-generic convenience method can compare
/// its own `CAP` against this constant in a `const` block to turn an oversized
/// buffer into a compile error instead.
pub const MAX_INLINE_CAP: usize = INFLIGHT_BUF_STRIDE as usize;

/// Default in-flight slots per worker.
pub const DEFAULT_INFLIGHT_CAP: u16 = 256;

/// Slot-count ceiling that sizes the inline bitmaps and generation table.
const MAX_INFLIGHT_SLOTS: usize = DEFAULT_INFLIGHT_CAP as usize;

/// Bitmap words covering [`MAX_INFLIGHT_SLOTS`].
const BITMAP_WORDS: usize = MAX_INFLIGHT_SLOTS / 64;

/// A Copy handle into the in-flight buffer registry.
///
/// Held by a completion future in place of inline byte storage. The
/// `generation` guards slot reuse: once the slot is freed its generation is
/// bumped, so a stale future or a stale cancel that still names the old
/// generation is rejected rather than touching a reallocated slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InflightSlotKey {
    /// Slot index in the owning worker's registry.
    pub(crate) slot: u16,
    /// Generation captured at allocation.
    pub(crate) generation: u64,
    /// Worker whose registry owns the slot.
    pub(crate) worker_id: u8,
    /// Submitted op `user_data`; the cancel target when the future drops.
    pub(crate) op_token: u64,
}

/// A readiness operation deferred on a live in-flight slot.
pub(crate) struct DeferredOp {
    /// Duplicate retained until the retry completes or the slot is reclaimed.
    pub(crate) fd: OwnedFd,
    /// Bytes to send, or capacity available for a receive.
    pub(crate) len: u32,
    /// The retry direction, restricted to `Send` and `Recv`.
    pub(crate) opcode: OpCode,
}

/// The outcome of reclaiming a slot by its operation token.
pub(crate) struct Reclaimed {
    /// Whether the matching retire-pending slot was freed.
    pub(crate) freed: bool,
    /// The record removed from that slot, if it owned one.
    pub(crate) deferred: Option<DeferredOp>,
}

/// Per-worker fixed-capacity in-flight buffer registry.
///
/// Owns the bytes for every buffered op in flight on its worker. A slot is
/// freed only by the completion drain after the kernel signals it is done,
/// never by the future's drop, so the kernel never writes freed memory.
pub struct InflightBufSlab {
    storage: MmapRegion,
    occupied: [u64; BITMAP_WORDS],
    retire_pending: [u64; BITMAP_WORDS],
    /// Per-slot flag: a `SEND_ZC` primary CQE arrived with `IORING_CQE_F_MORE`,
    /// so a notification CQE releasing the buffer is still expected. Gates the
    /// `-ENOENT` cancel free so a slot the kernel may still read is not freed
    /// before its NOTIF lands.
    notif_expected: [u64; BITMAP_WORDS],
    /// Per-slot flag: the `SEND_ZC` NOTIF CQE arrived while the owning future
    /// was still live (nothing retire-pending for its op), so the kernel has
    /// released the buffer. The live future reads this by key on its next poll
    /// and frees its own slot.
    notif_ready: [u64; BITMAP_WORDS],
    /// Per-slot generation, bumped on free. A `u64` makes the ABA window
    /// effectively unbounded: a slot would need 2^64 reuses to wrap.
    generation: [u64; MAX_INFLIGHT_SLOTS],
    /// Per-slot submitted op `user_data`, matched when the op's own completion
    /// frees the slot. The runtime holds at most one outstanding buffered
    /// completion per task (one `wake_data` slot per task header), so an
    /// `op_token` is unique across live slots; a multishot op must never route
    /// through this slab, since one token would then post several CQEs.
    op_token: [u64; MAX_INFLIGHT_SLOTS],
    /// Per-slot deferred readiness operation. Its presence is the one source
    /// of truth for whether this slot owns a retry descriptor.
    deferred: [Option<DeferredOp>; MAX_INFLIGHT_SLOTS],
    worker_id: u8,
    cap: u16,
    stride: u32,
}

impl InflightBufSlab {
    /// Builds a registry of `cap` slots (clamped to [`DEFAULT_INFLIGHT_CAP`])
    /// for `worker_id`, each `INFLIGHT_BUF_STRIDE` bytes wide.
    ///
    /// # Errors
    ///
    /// Returns the `mmap` error when backing allocation fails.
    pub fn new(worker_id: u8, cap: u16) -> io::Result<Self> {
        let cap = cap.min(DEFAULT_INFLIGHT_CAP);
        let stride = INFLIGHT_BUF_STRIDE;
        let storage = MmapRegion::new(cap as usize * stride as usize)?;
        Ok(Self {
            storage,
            occupied: [0; BITMAP_WORDS],
            retire_pending: [0; BITMAP_WORDS],
            notif_expected: [0; BITMAP_WORDS],
            notif_ready: [0; BITMAP_WORDS],
            generation: [0; MAX_INFLIGHT_SLOTS],
            op_token: [0; MAX_INFLIGHT_SLOTS],
            deferred: [const { None }; MAX_INFLIGHT_SLOTS],
            worker_id,
            cap,
            stride,
        })
    }

    /// Allocates the first free slot for an op, returning its handle.
    ///
    /// Returns `None` when every slot is occupied; the caller surfaces that
    /// as an `io::Error` rather than blocking.
    pub(crate) fn allocate(&mut self, op_token: u64) -> Option<InflightSlotKey> {
        let slot = self.first_free()?;
        let (word, bit) = word_bit(slot);
        self.occupied[word] |= 1u64 << bit;
        self.op_token[slot as usize] = op_token;
        Some(InflightSlotKey {
            slot,
            generation: self.generation[slot as usize],
            worker_id: self.worker_id,
            op_token,
        })
    }

    /// Frees `key`'s slot, bumping its generation so later stale handles are
    /// rejected. A stale handle (the slot was already freed and reused) is a
    /// no-op. Returns its deferred record when present; dropping that record is
    /// the ordinary case.
    pub(crate) const fn free(&mut self, key: InflightSlotKey) -> Option<DeferredOp> {
        if !self.is_live(key) {
            return None;
        }
        let (word, bit) = word_bit(key.slot);
        self.occupied[word] &= !(1u64 << bit);
        self.retire_pending[word] &= !(1u64 << bit);
        self.notif_expected[word] &= !(1u64 << bit);
        self.notif_ready[word] &= !(1u64 << bit);
        let deferred = self.clear_deferred(key.slot);
        let generation = &mut self.generation[key.slot as usize];
        *generation = generation.wrapping_add(1);
        deferred
    }

    /// Marks `key`'s slot retire-pending. A stale handle is a no-op.
    pub(crate) const fn mark_retire_pending(&mut self, key: InflightSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.retire_pending[word] |= 1u64 << bit;
    }

    /// Returns whether `slot` is currently marked retire-pending.
    #[cfg(test)]
    pub(crate) const fn is_retire_pending(&self, slot: u16) -> bool {
        if slot >= self.cap {
            return false;
        }
        let (word, bit) = word_bit(slot);
        self.retire_pending[word] & (1u64 << bit) != 0
    }

    /// Frees the retire-pending slot whose op matches `op_token`, returning
    /// whether a slot was freed.
    ///
    /// Called from the completion drain on the original buffered op's own
    /// completion (keyed by the task token it was submitted with). That CQE is
    /// the kernel's signal it is done with the slot's bytes, for every cancel
    /// outcome, so freeing here -- not on the cancel op's own CQE -- keeps a
    /// still-in-flight write from landing in a reused slot.
    ///
    /// Only retire-pending slots are eligible: a slot still owned by a live
    /// future frees through its own `harvest_into` or `free_slot`, never here.
    /// The scan is a no-op when no slot is retire-pending, the common case. The
    /// `freed` lets the `SEND_ZC` NOTIF path tell a dropped-future free (`true`)
    /// from a still-live future (`false`, which then marks `notif_ready`). The
    /// returned deferred record is ordinarily dropped.
    pub(crate) fn free_by_op_token(&mut self, op_token: u64) -> Reclaimed {
        for word in 0..BITMAP_WORDS {
            let mut pending = self.retire_pending[word];
            while pending != 0 {
                let bit = pending.trailing_zeros() as usize;
                pending &= pending - 1;
                let slot = word * 64 + bit;
                if self.op_token[slot] == op_token && self.occupied[word] & (1u64 << bit) != 0 {
                    let Ok(slot) = u16::try_from(slot) else {
                        return Reclaimed {
                            freed: false,
                            deferred: None,
                        };
                    };
                    self.occupied[word] &= !(1u64 << bit);
                    self.retire_pending[word] &= !(1u64 << bit);
                    self.notif_expected[word] &= !(1u64 << bit);
                    self.notif_ready[word] &= !(1u64 << bit);
                    let deferred = self.clear_deferred(slot);
                    self.generation[slot as usize] = self.generation[slot as usize].wrapping_add(1);
                    return Reclaimed {
                        freed: true,
                        deferred,
                    };
                }
            }
        }
        Reclaimed {
            freed: false,
            deferred: None,
        }
    }

    /// Frees `slot` when it is retire-pending, occupied, at the matching
    /// truncated generation, and not awaiting a `SEND_ZC` NOTIF.
    ///
    /// Called from the completion drain on a cancel completion that reported
    /// `-ENOENT`: the target op already completed and posted its one CQE before
    /// the cancel, so no op-token completion will arrive to free the slot. The
    /// cancel sentinel carries the slot's low 16 generation bits, matched here
    /// so a stale cancel completion cannot free a slot the same op token has
    /// since reused at a later generation.
    ///
    /// A `SEND_ZC` slot is the one `-ENOENT` exception: its primary CQE
    /// completing (hence the `-ENOENT` on the cancel) does not mean the kernel
    /// is done with the buffer -- the NOTIF has not landed. `notif_expected`
    /// without `notif_ready` therefore refuses the free, leaving the slot for
    /// the NOTIF path; once `notif_ready` is set the buffer is released and the
    /// free proceeds.
    /// Returns the deferred record from the reclaimed slot, if any; dropping it
    /// is the ordinary case.
    pub(crate) fn free_if_retire_pending(
        &mut self,
        slot: u16,
        generation_low16: u16,
    ) -> Option<DeferredOp> {
        if slot >= self.cap {
            return None;
        }
        let (word, bit) = word_bit(slot);
        let is_occupied = self.occupied[word] & (1u64 << bit) != 0;
        let is_pending = self.retire_pending[word] & (1u64 << bit) != 0;
        let matches_generation =
            self.generation[slot as usize] & 0xFFFF == u64::from(generation_low16);
        let is_notif_expected = self.notif_expected[word] & (1u64 << bit) != 0;
        let is_notif_ready = self.notif_ready[word] & (1u64 << bit) != 0;
        if !is_occupied
            || !is_pending
            || !matches_generation
            || (is_notif_expected && !is_notif_ready)
        {
            return None;
        }
        self.occupied[word] &= !(1u64 << bit);
        self.retire_pending[word] &= !(1u64 << bit);
        self.notif_expected[word] &= !(1u64 << bit);
        self.notif_ready[word] &= !(1u64 << bit);
        let deferred = self.clear_deferred(slot);
        self.generation[slot as usize] = self.generation[slot as usize].wrapping_add(1);
        deferred
    }

    /// Records `op` on the occupied slot named by `op_token`.
    ///
    /// Returns `false` when no live slot carries the token, leaving `op` to
    /// drop with its descriptor. The runtime gives each live buffered op a
    /// unique token, so the first match is the sole slot that may own it.
    pub(crate) fn mark_deferred_by_op_token(&mut self, op_token: u64, op: DeferredOp) -> bool {
        for word in 0..BITMAP_WORDS {
            let mut occupied = self.occupied[word];
            while occupied != 0 {
                let bit = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                let slot = word * 64 + bit;
                if self.op_token[slot] == op_token {
                    self.deferred[slot] = Some(op);
                    return true;
                }
            }
        }
        false
    }

    /// Returns the deferred operation belonging to `key`, if it is still live.
    #[cfg(test)]
    pub(crate) fn deferred(&self, key: InflightSlotKey) -> Option<&DeferredOp> {
        self.is_live(key)
            .then(|| self.deferred[key.slot as usize].as_ref())
            .flatten()
    }

    /// Returns the live deferred slot whose record owns `fd`.
    ///
    /// Each live record owns a distinct duplicate, so the first matching
    /// descriptor is its only matching slot. `None` means no live record owns
    /// the descriptor, including after its slot was reclaimed.
    pub(crate) fn deferred_key_by_fd(&self, fd: i32) -> Option<InflightSlotKey> {
        for word in 0..BITMAP_WORDS {
            let mut occupied = self.occupied[word];
            while occupied != 0 {
                let bit = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                let slot = word * 64 + bit;
                let Some(deferred) = self.deferred[slot].as_ref() else {
                    continue;
                };
                if deferred.fd.as_raw_fd() == fd {
                    let Ok(slot) = u16::try_from(slot) else {
                        return None;
                    };
                    return Some(InflightSlotKey {
                        slot,
                        generation: self.generation[slot as usize],
                        worker_id: self.worker_id,
                        op_token: self.op_token[slot as usize],
                    });
                }
            }
        }
        None
    }

    /// Returns the retained descriptor, slot bytes, and opcode for a retry.
    pub(crate) fn retry_parts(
        &mut self,
        key: InflightSlotKey,
    ) -> Option<(&OwnedFd, &mut [u8], OpCode)> {
        if !self.is_live(key) {
            return None;
        }
        let deferred = self.deferred[key.slot as usize].as_ref()?;
        let len = deferred.len.min(self.stride) as usize;
        let offset = key.slot as usize * self.stride as usize;
        let bytes = &mut self.storage.as_mut_slice()[offset..offset + len];
        Some((&deferred.fd, bytes, deferred.opcode))
    }

    /// Removes a deferred record without reclaiming its slot or bytes.
    ///
    /// Returns the record when present; dropping it is the ordinary case.
    pub(crate) const fn retire_deferred(&mut self, key: InflightSlotKey) -> Option<DeferredOp> {
        if !self.is_live(key) {
            return None;
        }
        self.deferred[key.slot as usize].take()
    }

    /// Marks the live slot for `op_token` as awaiting its `SEND_ZC` NOTIF.
    ///
    /// Called from the completion drain when a primary CQE carries
    /// `IORING_CQE_F_MORE`: a notification CQE releasing the buffer is still
    /// coming, so the slot must not be freed by a racing `-ENOENT` cancel until
    /// then. This is the op's first event, so exactly one occupied slot carries
    /// this token; the scan sets that slot's flag and stops. A no-op when no
    /// occupied slot matches.
    pub(crate) fn mark_notif_expected_by_op_token(&mut self, op_token: u64) {
        for word in 0..BITMAP_WORDS {
            let mut occupied = self.occupied[word];
            while occupied != 0 {
                let bit = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                if self.op_token[word * 64 + bit] == op_token {
                    self.notif_expected[word] |= 1u64 << bit;
                    return;
                }
            }
        }
    }

    /// Marks the live slot for `op_token` as `SEND_ZC` NOTIF-released.
    ///
    /// Called from the completion drain on a NOTIF CQE only after
    /// [`free_by_op_token`](Self::free_by_op_token) reported no retire-pending
    /// match, which means the owning future is still live. The future reads the
    /// flag by key on its next poll (see [`is_notif_ready`](Self::is_notif_ready))
    /// and frees its own slot. A no-op when no occupied slot matches, e.g. the
    /// future already freed it.
    pub(crate) fn mark_notif_ready_by_op_token(&mut self, op_token: u64) {
        for word in 0..BITMAP_WORDS {
            let mut occupied = self.occupied[word];
            while occupied != 0 {
                let bit = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                if self.op_token[word * 64 + bit] == op_token {
                    self.notif_ready[word] |= 1u64 << bit;
                    return;
                }
            }
        }
    }

    /// Whether `key`'s live slot has seen its `SEND_ZC` NOTIF.
    ///
    /// The owning future polls this by key; `true` means the kernel released
    /// the buffer, so the future may resolve and free the slot. A stale or
    /// cross-worker key reads `false` through the generation guard.
    pub(crate) const fn is_notif_ready(&self, key: InflightSlotKey) -> bool {
        if !self.is_live(key) {
            return false;
        }
        let (word, bit) = word_bit(key.slot);
        self.notif_ready[word] & (1u64 << bit) != 0
    }

    /// Returns a writable pointer to `key`'s slot, or `None` if stale.
    ///
    /// The pointer is valid for [`INFLIGHT_BUF_STRIDE`] bytes while `self`
    /// lives and the slot stays occupied at `key`'s generation. This method
    /// performs no dereference: the future hands the pointer to the kernel
    /// through `InlineBuf`, and that unsafe contract lives at the call site.
    ///
    /// The caller must not also read the slot via
    /// [`slot_slice`](Self::slot_slice) while the pointer is submitted to the
    /// kernel: a shared read aliasing an in-flight kernel write is a data race.
    pub(crate) const fn slot_ptr(&self, key: InflightSlotKey) -> Option<*mut u8> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        Some(self.storage.as_ptr().cast_mut().wrapping_add(offset))
    }

    /// Returns `key`'s slot bytes truncated to `len`, or `None` if stale.
    ///
    /// Must be called only after the CQE for this slot's op is received;
    /// reading the slot while the kernel write is still in flight is a data
    /// race. `len` must not exceed the CQE-confirmed byte count -- bytes
    /// beyond it are zero (the initial mmap fill) or hold a prior op's data.
    /// The slice is clamped to the stride to keep the read inside its slot.
    pub(crate) fn slot_slice(&self, key: InflightSlotKey, len: usize) -> Option<&[u8]> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        let end = offset + len.min(self.stride as usize);
        Some(&self.storage.as_slice()[offset..end])
    }

    /// Returns `key`'s slot as a fixed-size mutable array, or `None` if stale.
    ///
    /// The `sendmsg`/`recvmsg` seam lays the msghdr/iovec/addr/payload structure
    /// over the whole slot, which needs a sized `&mut [u8; INFLIGHT_BUF_STRIDE]`
    /// rather than the raw pointer or length-truncated slice the plain buffered
    /// ops use. The slot is exactly `INFLIGHT_BUF_STRIDE` bytes wide, so the
    /// conversion never fails on a live key.
    pub(crate) fn slot_array_mut(
        &mut self,
        key: InflightSlotKey,
    ) -> Option<&mut [u8; INFLIGHT_BUF_STRIDE as usize]> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        let end = offset + self.stride as usize;
        let slot = &mut self.storage.as_mut_slice()[offset..end];
        slot.try_into().ok()
    }

    /// Returns `key`'s slot as a fixed-size shared array, or `None` if stale.
    ///
    /// The read side of [`slot_array_mut`](Self::slot_array_mut): the `recvmsg`
    /// seam reads the kernel-written sender address out of the slot. Call only
    /// after the CQE for this slot's op arrives.
    pub(crate) fn slot_array(
        &self,
        key: InflightSlotKey,
    ) -> Option<&[u8; INFLIGHT_BUF_STRIDE as usize]> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        let end = offset + self.stride as usize;
        self.storage.as_slice()[offset..end].try_into().ok()
    }

    /// First free slot within `cap`, by inverted-bitmap scan.
    fn first_free(&self) -> Option<u16> {
        let limit = self.cap as usize;
        let last_word = limit / 64;
        let last_bit = limit % 64;
        for (word_idx, word) in self.occupied.iter().enumerate() {
            let mask = if word_idx < last_word {
                u64::MAX
            } else if word_idx == last_word && last_bit > 0 {
                (1u64 << last_bit) - 1
            } else {
                break;
            };
            let available = !*word & mask;
            if available != 0 {
                let raw = word_idx * 64 + available.trailing_zeros() as usize;
                return u16::try_from(raw).ok();
            }
        }
        None
    }

    /// Removes a deferred record before a slot is made available again.
    const fn clear_deferred(&mut self, slot: u16) -> Option<DeferredOp> {
        self.deferred[slot as usize].take()
    }

    /// Whether `key` names a live slot this registry owns at the matching
    /// generation.
    ///
    /// Checks worker ownership and slot occupancy alongside the generation, so
    /// a cross-worker handle or a fabricated key for an unallocated slot is
    /// rejected, not just a stale one. This generation guard is what lets
    /// [`is_retire_pending`](Self::is_retire_pending) skip a generation
    /// parameter: [`free`](Self::free) clears the retire-pending bit and bumps
    /// the generation together, so any reuse of the slot begins with the bit
    /// clear and a stale mark cannot survive across a free.
    const fn is_live(&self, key: InflightSlotKey) -> bool {
        if key.worker_id != self.worker_id {
            return false;
        }
        if key.slot >= self.cap {
            return false;
        }
        let (word, bit) = word_bit(key.slot);
        self.occupied[word] & (1u64 << bit) != 0
            && self.generation[key.slot as usize] == key.generation
    }
}

/// Bitmap word index and bit offset for `slot`.
const fn word_bit(slot: u16) -> (usize, usize) {
    (slot as usize / 64, slot as usize % 64)
}

#[cfg(test)]
mod tests {
    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    use std::{fs::File, os::unix::net::UnixStream};

    use super::*;
    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    use crate::boundary::seam::socket::duplicate_fd;

    fn slab(cap: u16) -> InflightBufSlab {
        let Ok(slab) = InflightBufSlab::new(3, cap) else {
            panic!("mmap must succeed for the test registry");
        };
        slab
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    fn deferred_op() -> DeferredOp {
        let Ok(file) = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")) else {
            panic!("the crate manifest must open");
        };
        let Ok(duplicated) = duplicate_fd(file.as_raw_fd()) else {
            panic!("an open descriptor must duplicate");
        };
        DeferredOp {
            fd: duplicated.into_owned(),
            len: 11,
            opcode: OpCode::Recv,
        }
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_record_is_found_by_occupied_token() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        assert!(registry.mark_deferred_by_op_token(0xABCD, deferred_op()));
        let Some(deferred) = registry.deferred(key) else {
            panic!("the occupied slot must own the record");
        };
        assert_eq!(deferred.len, 11);
        assert_eq!(deferred.opcode, OpCode::Recv);
        assert!(deferred.fd.as_raw_fd() >= 0);
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_finds_a_live_record() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));

        let Some(found) = registry.deferred_key_by_fd(fd) else {
            panic!("the record's descriptor must resolve to its slot");
        };
        assert_eq!(found, key);
        assert!(registry.deferred(found).is_some());
        assert!(registry.slot_ptr(found).is_some());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_distinguishes_duplicate_records() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(1) else {
            panic!("first allocation must succeed");
        };
        let Some(second) = registry.allocate(2) else {
            panic!("second allocation must succeed");
        };
        let Ok(file) = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")) else {
            panic!("the crate manifest must open");
        };
        let Ok(first_fd) = duplicate_fd(file.as_raw_fd()) else {
            panic!("the first descriptor must duplicate");
        };
        let Ok(second_fd) = duplicate_fd(file.as_raw_fd()) else {
            panic!("the second descriptor must duplicate");
        };
        let first_op = DeferredOp {
            fd: first_fd.into_owned(),
            len: 11,
            opcode: OpCode::Recv,
        };
        let first_fd = first_op.fd.as_raw_fd();
        let second_op = DeferredOp {
            fd: second_fd.into_owned(),
            len: 11,
            opcode: OpCode::Recv,
        };
        let second_fd = second_op.fd.as_raw_fd();
        assert_ne!(first_fd, second_fd);
        assert!(registry.mark_deferred_by_op_token(first.op_token, first_op));
        assert!(registry.mark_deferred_by_op_token(second.op_token, second_op));

        assert_eq!(registry.deferred_key_by_fd(first_fd), Some(first));
        assert_eq!(registry.deferred_key_by_fd(second_fd), Some(second));
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_ignores_unknown_and_reclaimed_records() {
        let mut registry = slab(8);
        let unrecorded = deferred_op();
        assert!(
            registry
                .deferred_key_by_fd(unrecorded.fd.as_raw_fd())
                .is_none()
        );

        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));
        let _ = registry.free(key);
        assert!(registry.deferred_key_by_fd(fd).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_follows_the_current_occupant() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(1) else {
            panic!("first allocation must succeed");
        };
        let first_op = deferred_op();
        let first_fd = first_op.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(first.op_token, first_op));
        let _ = registry.free(first);
        assert!(registry.deferred_key_by_fd(first_fd).is_none());

        let Some(second) = registry.allocate(2) else {
            panic!("second allocation must succeed");
        };
        let second_op = deferred_op();
        let second_fd = second_op.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(second.op_token, second_op));
        assert_eq!(registry.deferred_key_by_fd(second_fd), Some(second));

        let prior = registry.deferred_key_by_fd(first_fd);
        assert_ne!(prior, Some(first));
        assert!(prior.is_none() || prior == Some(second));
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_is_local_to_its_worker() {
        let mut owner = slab(8);
        let other = InflightBufSlab::new(4, 8);
        let Ok(other) = other else {
            panic!("mmap must succeed for the other worker registry");
        };
        let Some(key) = owner.allocate(1) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(owner.mark_deferred_by_op_token(key.op_token, deferred));

        assert_eq!(owner.deferred_key_by_fd(fd), Some(key));
        assert!(other.deferred_key_by_fd(fd).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn deferred_key_by_fd_keeps_the_record_live() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));

        assert_eq!(registry.deferred_key_by_fd(fd), Some(key));
        assert!(registry.deferred(key).is_some());
        assert_eq!(registry.deferred_key_by_fd(fd), Some(key));
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn retry_parts_borrows_the_record_and_truncates_the_slot() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let mut deferred = deferred_op();
        deferred.len = u32::MAX;
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));
        let Some((fd, bytes, opcode)) = registry.retry_parts(key) else {
            panic!("a live deferred slot supplies retry parts");
        };
        assert!(fd.as_raw_fd() >= 0);
        assert_eq!(bytes.len(), 4096);
        assert_eq!(opcode, OpCode::Recv);
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn retire_deferred_keeps_the_slot_and_bytes_live() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let Some(bytes) = registry.slot_array_mut(key) else {
            panic!("a live slot supplies its bytes");
        };
        bytes[0] = b'x';
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));
        let Some(retired) = registry.retire_deferred(key) else {
            panic!("the live slot must yield its deferred record");
        };
        assert_eq!(retired.fd.as_raw_fd(), fd);
        assert_eq!(retired.len, 11);
        assert_eq!(retired.opcode, OpCode::Recv);
        assert!(registry.deferred_key_by_fd(fd).is_none());
        assert!(registry.slot_array(key).is_some());
        assert_eq!(registry.slot_array(key).map(|bytes| bytes[0]), Some(b'x'));
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn unknown_token_drops_the_deferred_record() {
        let mut registry = slab(8);
        assert!(!registry.mark_deferred_by_op_token(0xABCD, deferred_op()));
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        assert!(registry.deferred(key).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn free_yields_the_deferred_record() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(1, deferred));
        let Some(deferred) = registry.free(first) else {
            panic!("the reclaimed slot must yield its deferred record");
        };
        assert_eq!(deferred.fd.as_raw_fd(), fd);
        assert_eq!(deferred.len, 11);
        assert_eq!(deferred.opcode, OpCode::Recv);
        let Some(second) = registry.allocate(2) else {
            panic!("allocate must succeed");
        };
        assert!(registry.deferred(second).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn free_by_op_token_clears_the_deferred_record() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(2) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(2, deferred));
        registry.mark_retire_pending(first);
        let reclaimed = registry.free_by_op_token(2);
        assert!(reclaimed.freed);
        let Some(deferred) = reclaimed.deferred else {
            panic!("the reclaimed slot must yield its deferred record");
        };
        assert_eq!(deferred.fd.as_raw_fd(), fd);
        assert_eq!(deferred.len, 11);
        assert_eq!(deferred.opcode, OpCode::Recv);
        let Some(second) = registry.allocate(3) else {
            panic!("allocate must succeed");
        };
        assert!(registry.deferred(second).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn free_if_retire_pending_clears_the_deferred_record() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(3) else {
            panic!("allocate must succeed");
        };
        let deferred = deferred_op();
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(3, deferred));
        registry.mark_retire_pending(first);
        let generation_low16 = (first.generation & 0xFFFF) as u16;
        let Some(deferred) = registry.free_if_retire_pending(first.slot, generation_low16) else {
            panic!("the reclaimed slot must yield its deferred record");
        };
        assert_eq!(deferred.fd.as_raw_fd(), fd);
        assert_eq!(deferred.len, 11);
        assert_eq!(deferred.opcode, OpCode::Recv);
        let Some(second) = registry.allocate(4) else {
            panic!("allocate must succeed");
        };
        assert!(registry.deferred(second).is_none());
    }

    #[test]
    fn reclaiming_nothing_yields_nothing() {
        let mut registry = slab(8);
        let stale = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 3,
            op_token: 1,
        };
        assert!(registry.free(stale).is_none(), "a stale key yields nothing");

        let reclaimed = registry.free_by_op_token(1);
        assert!(!reclaimed.freed, "an unknown token frees no slot");
        assert!(
            reclaimed.deferred.is_none(),
            "an unknown token yields no record"
        );

        let Some(key) = registry.allocate(2) else {
            panic!("allocate must succeed");
        };
        registry.mark_retire_pending(key);
        assert_eq!(key.generation, 0, "a new slot has generation zero");
        assert!(
            registry.free_if_retire_pending(key.slot, 1).is_none(),
            "a generation mismatch yields nothing",
        );
        assert!(
            registry.free_if_retire_pending(key.slot, 0).is_none(),
            "a reclaimed slot with no record yields nothing",
        );
    }

    #[test]
    fn allocate_is_sequential() {
        let mut registry = slab(8);
        assert_eq!(registry.allocate(0).map(|key| key.slot), Some(0));
        assert_eq!(registry.allocate(0).map(|key| key.slot), Some(1));
    }

    #[test]
    fn allocate_carries_worker_token_and_generation() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        assert_eq!(key.worker_id, 3);
        assert_eq!(key.op_token, 0xABCD);
        assert_eq!(key.generation, 0);
    }

    #[test]
    fn exhaustion_returns_none() {
        let mut registry = slab(2);
        assert!(registry.allocate(0).is_some());
        assert!(registry.allocate(0).is_some());
        assert!(registry.allocate(0).is_none());
    }

    #[test]
    fn free_reuses_slot_and_bumps_generation() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let _ = registry.free(first);
        let Some(second) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert_eq!(second.slot, 0, "the freed slot is reused");
        assert_eq!(second.generation, 1, "the generation bumped on free");
    }

    #[test]
    fn stale_handle_is_rejected_everywhere() {
        let mut registry = slab(8);
        let Some(stale) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let _ = registry.free(stale);
        let Some(fresh) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert_eq!(fresh.slot, stale.slot, "the slot is reused");
        assert!(registry.slot_ptr(stale).is_none(), "stale ptr is rejected");
        assert!(
            registry.slot_slice(stale, 4).is_none(),
            "stale slice is rejected"
        );
        let _ = registry.free(stale);
        registry.mark_retire_pending(stale);
        assert!(
            !registry.is_retire_pending(fresh.slot),
            "the stale mark did not apply to the fresh occupant",
        );
        assert!(
            registry.slot_ptr(fresh).is_some(),
            "the fresh slot survives a stale free",
        );
    }

    #[test]
    fn retire_pending_marks_and_clears_on_free() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert!(!registry.is_retire_pending(key.slot));
        registry.mark_retire_pending(key);
        assert!(registry.is_retire_pending(key.slot));
        let _ = registry.free(key);
        assert!(!registry.is_retire_pending(key.slot), "free clears the bit");
    }

    #[test]
    fn slot_ptr_is_distinct_and_stride_apart() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(second) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let (Some(first_ptr), Some(second_ptr)) =
            (registry.slot_ptr(first), registry.slot_ptr(second))
        else {
            panic!("a live handle yields a pointer");
        };
        let gap = second_ptr as usize - first_ptr as usize;
        assert_eq!(gap, INFLIGHT_BUF_STRIDE as usize);
    }

    #[test]
    fn slot_slice_clamps_to_len() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(bytes) = registry.slot_slice(key, 4) else {
            panic!("a live handle yields a slice");
        };
        assert_eq!(bytes.len(), 4);
        assert!(bytes.iter().all(|&byte| byte == 0), "mmap is zero-filled");
    }

    #[test]
    fn slot_array_spans_the_stride_and_round_trips() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(slot) = registry.slot_array_mut(key) else {
            panic!("a live handle yields the mutable slot array");
        };
        assert_eq!(slot.len(), INFLIGHT_BUF_STRIDE as usize);
        slot[0] = 0xAB;
        slot[INFLIGHT_BUF_STRIDE as usize - 1] = 0xCD;
        let Some(read) = registry.slot_array(key) else {
            panic!("a live handle yields the shared slot array");
        };
        assert_eq!(read[0], 0xAB);
        assert_eq!(read[INFLIGHT_BUF_STRIDE as usize - 1], 0xCD);
    }

    #[test]
    fn slot_array_rejects_a_stale_key() {
        let registry = slab(8);
        let stale = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 0,
            op_token: 0,
        };
        assert!(
            registry.slot_array(stale).is_none(),
            "a fabricated generation-0 key is not live",
        );
    }

    #[test]
    fn slot_slice_len_is_capped_at_stride() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(bytes) = registry.slot_slice(key, usize::MAX) else {
            panic!("a live handle yields a slice");
        };
        assert_eq!(bytes.len(), INFLIGHT_BUF_STRIDE as usize);
    }

    #[test]
    fn unallocated_slot_is_not_live() {
        let registry = slab(8);
        let fabricated = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 3,
            op_token: 0,
        };
        assert!(
            registry.slot_ptr(fabricated).is_none(),
            "a fabricated key for an unallocated slot is rejected",
        );
    }

    #[test]
    fn foreign_worker_key_is_rejected() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let foreign = InflightSlotKey {
            worker_id: key.worker_id + 1,
            ..key
        };
        assert!(
            registry.slot_ptr(foreign).is_none(),
            "a key from another worker is rejected",
        );
    }

    #[test]
    fn op_token_frees_marked_slot() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        registry.mark_retire_pending(key);
        let _ = registry.free_by_op_token(0xABCD);
        let Some(next) = registry.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(next.slot, key.slot, "the slot is reused");
        assert_eq!(
            next.generation,
            key.generation + 1,
            "the op-token free bumped the generation",
        );
    }

    #[test]
    fn op_token_skips_live_slot() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        // Not retire-pending: a live future still owns the slot, so its op
        // completion must not free it here -- only its own harvest/free may.
        let _ = registry.free_by_op_token(0xABCD);
        assert!(
            registry.slot_ptr(key).is_some(),
            "a slot not yet retire-pending survives its op completion",
        );
    }

    #[test]
    fn op_token_ignores_mismatch() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        registry.mark_retire_pending(key);
        let _ = registry.free_by_op_token(0x1234);
        assert!(
            registry.is_retire_pending(key.slot),
            "a foreign op token leaves the slot marked, not freed",
        );
        assert!(registry.slot_ptr(key).is_some(), "the marked slot survives");
    }

    #[test]
    fn op_token_free_is_idempotent() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        registry.mark_retire_pending(key);
        let _ = registry.free_by_op_token(0xABCD);
        // The op's CQE cannot arrive twice, but a defensive second call finds
        // the slot cleared (bit gone), so it is a no-op.
        let _ = registry.free_by_op_token(0xABCD);
        let Some(next) = registry.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.generation,
            key.generation + 1,
            "the slot was freed once, not twice",
        );
    }

    #[test]
    fn free_by_op_token_reports_whether_it_freed() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        // Live (not retire-pending): nothing to free, so it reports false.
        let reclaimed = registry.free_by_op_token(0xABCD);
        assert!(!reclaimed.freed, "a live slot is not freed");
        assert!(reclaimed.deferred.is_none(), "a live slot yields no record");
        registry.mark_retire_pending(key);
        let reclaimed = registry.free_by_op_token(0xABCD);
        assert!(
            reclaimed.freed,
            "a retire-pending slot is freed and reported"
        );
        assert!(
            reclaimed.deferred.is_none(),
            "a freed slot without a record yields none",
        );
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn dropping_a_reclaimed_record_closes_its_descriptor() {
        let Ok((socket, _peer)) = UnixStream::pair() else {
            panic!("a socket pair must open");
        };
        let mut registry = slab(8);
        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let Ok(duplicated) = duplicate_fd(socket.as_raw_fd()) else {
            panic!("the socket descriptor must duplicate");
        };
        let deferred = DeferredOp {
            fd: duplicated.into_owned(),
            len: 11,
            opcode: OpCode::Recv,
        };
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));
        let _ = registry.free(key);
        assert!(
            duplicate_fd(fd).is_err(),
            "dropping the reclaimed record closes its descriptor",
        );
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn holding_a_reclaimed_record_keeps_its_descriptor_open() {
        let Ok((socket, _peer)) = UnixStream::pair() else {
            panic!("a socket pair must open");
        };
        let mut registry = slab(8);
        let Some(key) = registry.allocate(1) else {
            panic!("allocate must succeed");
        };
        let Ok(duplicated) = duplicate_fd(socket.as_raw_fd()) else {
            panic!("the socket descriptor must duplicate");
        };
        let deferred = DeferredOp {
            fd: duplicated.into_owned(),
            len: 11,
            opcode: OpCode::Recv,
        };
        let fd = deferred.fd.as_raw_fd();
        assert!(registry.mark_deferred_by_op_token(key.op_token, deferred));
        let Some(deferred) = registry.free(key) else {
            panic!("the reclaimed slot must yield its deferred record");
        };
        assert!(
            registry.allocate(2).is_some(),
            "the reclaimed slot is reusable"
        );
        assert!(
            duplicate_fd(fd).is_ok(),
            "the held record keeps its socket descriptor open",
        );
        drop(deferred);
        assert!(
            duplicate_fd(fd).is_err(),
            "giving up the record closes its descriptor",
        );
    }

    #[test]
    fn notif_ready_roundtrips_by_key_and_clears_on_free() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        assert!(
            !registry.is_notif_ready(key),
            "a fresh slot is not notif-ready",
        );
        registry.mark_notif_ready_by_op_token(0xABCD);
        assert!(
            registry.is_notif_ready(key),
            "the notif mark is visible by key",
        );
        let _ = registry.free(key);
        assert!(
            !registry.is_notif_ready(key),
            "free clears the notif flag and the stale key reads false",
        );
    }

    // Drop order 1: the future drops before its NOTIF arrives, so the NOTIF's
    // op-token free reclaims the retire-pending slot.
    #[test]
    fn notif_after_drop_frees_via_op_token() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        // Primary CQE carried F_MORE.
        registry.mark_notif_expected_by_op_token(0xABCD);
        // Future drops -> its cancel marks the slot retire-pending.
        registry.mark_retire_pending(key);
        // NOTIF arrives: the dropped future's slot frees by op token.
        assert!(
            registry.free_by_op_token(0xABCD).freed,
            "the NOTIF frees the slot"
        );
        let Some(next) = registry.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(next.slot, key.slot, "the slot is reused after the NOTIF");
    }

    // Drop order 2: primary done, an -ENOENT cancel races ahead of the NOTIF.
    // The kernel may still read the buffer, so the free must be refused.
    #[test]
    fn enoent_refuses_free_before_notif() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        registry.mark_notif_expected_by_op_token(0xABCD);
        registry.mark_retire_pending(key);
        let generation_low16 = (key.generation & 0xFFFF) as u16;
        let _ = registry.free_if_retire_pending(key.slot, generation_low16);
        assert!(
            registry.slot_ptr(key).is_some(),
            "the slot survives an -ENOENT before its NOTIF",
        );
    }

    // Drop order 3 (the leak-fix corner): the NOTIF lands on a still-live
    // future (marking ready), then the future drops and its cancel returns
    // -ENOENT. Guarding on notif_expected alone would leak; notif_ready must
    // permit the -ENOENT free to proceed.
    #[test]
    fn enoent_frees_after_notif_ready_then_drop() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        registry.mark_notif_expected_by_op_token(0xABCD);
        // NOTIF on a live future: not retire-pending, so it marks ready.
        assert!(
            !registry.free_by_op_token(0xABCD).freed,
            "a live future frees nothing here",
        );
        registry.mark_notif_ready_by_op_token(0xABCD);
        // Future drops now; the cancel marks retire-pending and returns -ENOENT.
        registry.mark_retire_pending(key);
        let generation_low16 = (key.generation & 0xFFFF) as u16;
        let _ = registry.free_if_retire_pending(key.slot, generation_low16);
        let Some(next) = registry.allocate(0) else {
            panic!("the freed slot reallocates once notif-ready");
        };
        assert_eq!(
            next.slot, key.slot,
            "notif_ready lets the -ENOENT free proceed",
        );
    }
}
