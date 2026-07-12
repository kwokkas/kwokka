//! The hierarchical timing wheel: its time source, levels, slots, entries, and
//! handles.

pub(crate) mod clock;
pub(crate) mod entry;
pub(crate) mod handle;
pub(crate) mod hierarchy;
pub(crate) mod slot;

pub(crate) use hierarchy::TimerWheel;
