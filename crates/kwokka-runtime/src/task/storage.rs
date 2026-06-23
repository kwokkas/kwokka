//! [`TaskStorage`] over the per-worker [`Slab`], resolving a [`TaskRef`] to its
//! [`TaskHeader`] so the children-list helpers drive the production slab.
//!
//! The trait lives in [`children`](crate::task::children) as pure list logic;
//! this is its one concrete production implementor.

use kwokka_core::slab::{Slab, SlabKey};

use crate::task::{TaskRef, children::TaskStorage, header::TaskHeader, slot::TaskSlot};

impl TaskStorage for Slab<TaskSlot> {
    fn get(&self, task_ref: TaskRef) -> Option<&TaskHeader> {
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        // SlabKey arg resolves to the inherent `Slab::get`, not this trait method.
        Some(self.get(key)?.header())
    }

    fn get_mut(&mut self, task_ref: TaskRef) -> Option<&mut TaskHeader> {
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        Some(self.get_mut(key)?.header_mut())
    }

    fn get_slot_ptr(&self, task_ref: TaskRef) -> Option<*const u8> {
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        Some(self.slot_ptr(key)?.as_ptr().cast::<u8>().cast_const())
    }
}
