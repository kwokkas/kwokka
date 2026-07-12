//! Task slab-cell representation: erased layout, header, slot, storage, the
//! state machine the header carries, and the lifecycle that drives a cell from
//! insertion through each poll.

pub(crate) mod header;
pub(crate) mod layout;
pub(crate) mod lifecycle;
pub(crate) mod slot;
pub(crate) mod state;
mod storage;
