//! The per-worker registry for single-shot ops that carry a caller buffer.
//!
//! The counterpart to [`multishot`](crate::buffer::multishot): where that
//! registry tracks streams the kernel keeps feeding, this one owns the bytes
//! of one op at a time, from submit until the completion drain reclaims them.

pub mod inflight;

pub use inflight::MAX_INLINE_CAP;
