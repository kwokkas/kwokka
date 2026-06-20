//! Type-erased task slot for per-worker slab storage.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{cell::UnsafeCell, mem::MaybeUninit};

/// Fixed-size, type-erased cell holding a `Slot<F>` for slab storage.
///
/// The slab is homogeneous in its element type, so a heterogeneous
/// `Slot<F>` (header plus the future cell, sized per `F`) cannot be stored
/// directly. Instead every task occupies a fixed [`Self::CELL_BYTES`]-byte,
/// [`Self::CELL_ALIGN`]-aligned cell into which the concrete `Slot<F>` is
/// written by [`Slot::into_erased`](crate::task::header::Slot::into_erased),
/// header at offset 0. Type-erased `poll` and `drop` reach the future
/// through the header vtable.
///
/// `repr(C, align(16))` puts `bytes` at offset 0 and gives the cell base a
/// 16-byte alignment, so a `*const TaskSlot` reinterprets as the leading
/// `TaskHeader` with correct alignment and no offset math. All pointer
/// reinterpretation lives in the `header` module (an already-permitted-unsafe
/// site), so this file stays free of the `unsafe` keyword.
///
/// The cell is wrapped in [`UnsafeCell`] so a shared `&TaskSlot` carries
/// interior-mutability provenance: the leading `TaskHeader` holds an
/// `AtomicTaskState`, so forming a `&TaskHeader` from a plain `&[u8; N]`
/// would be a Stacked/Tree Borrows violation the moment the atomic is read.
/// `UnsafeCell` is `repr(transparent)`, so layout, size, and alignment are
/// unchanged.
#[repr(C, align(16))]
pub(crate) struct TaskSlot {
    bytes: UnsafeCell<[MaybeUninit<u8>; Self::CELL_BYTES]>,
}

impl TaskSlot {
    /// Byte capacity of the erased cell. Matches the `Slot<F>` size cap.
    pub(crate) const CELL_BYTES: usize = 512;
    /// Alignment of the erased cell. Covers `TaskHeader` (align 16 via the
    /// 128-bit `Pip`) and every future whose alignment does not exceed it.
    pub(crate) const CELL_ALIGN: usize = 16;

    /// Uninitialized cell.
    ///
    /// The returned value must be filled via
    /// [`Slot::into_erased`](crate::task::header::Slot::into_erased) before it
    /// is dropped or read; dropping an unfilled cell would invoke the vtable
    /// on garbage. `into_erased` is the sole caller and fills it with no
    /// intervening panic point.
    pub(crate) const fn uninit() -> Self {
        Self {
            bytes: UnsafeCell::new([MaybeUninit::uninit(); Self::CELL_BYTES]),
        }
    }
}

impl Drop for TaskSlot {
    fn drop(&mut self) {
        self.drop_via_vtable();
    }
}
