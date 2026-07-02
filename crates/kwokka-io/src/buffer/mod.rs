//! Buffer types: mmap regions, slot handles, vectored I/O, provided rings, and registries.

pub mod inflight;
pub(crate) mod mmap;
pub mod multishot;
pub(crate) mod registration;
pub mod ring;
pub(crate) mod slot;
pub(crate) mod vectored;
