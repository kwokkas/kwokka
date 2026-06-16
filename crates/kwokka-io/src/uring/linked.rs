//! Linked SQE chaining -- deferred to 0.2.0.
//!
//! Use cases (`accept`->`recv`, `write`->`fsync`, op->timeout) are
//! covered in 0.1.0 by:
//! - multishot `accept` + `buf_ring` (`accept`->`recv`)
//! - sequential await (`write`->`fsync`)
//! - runtime `TimerWheel` + `IORING_OP_ASYNC_CANCEL` (op->timeout); the SQE-native `LINK_TIMEOUT`
//!   is the 0.2.0 mechanism
//!
//! 0.2.0 evaluation: measured benchmark vs current mechanisms.
