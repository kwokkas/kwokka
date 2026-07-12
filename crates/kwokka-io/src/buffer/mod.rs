//! Buffer storage, and the per-op-category registries built over it.

pub mod multishot;
pub mod oneshot;
pub(crate) mod registration;
pub(crate) mod ring;
pub(crate) mod storage;

pub use oneshot::MAX_INLINE_CAP;
