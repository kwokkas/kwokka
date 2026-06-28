//! Buffer types: mmap regions, slot handles, vectored I/O, provided rings, and registries.

pub mod inflight;
pub(crate) mod mmap;
pub(crate) mod registration;
pub(crate) mod ring;
pub(crate) mod slot;
pub(crate) mod vectored;
