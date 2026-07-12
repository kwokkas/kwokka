//! Task slab-cell representation: erased layout, header, slot, storage, and
//! the lifecycle that drives a cell from insertion through each poll.

pub(crate) mod header;
pub(crate) mod layout;
pub(crate) mod lifecycle;
pub(crate) mod slot;
mod storage;
