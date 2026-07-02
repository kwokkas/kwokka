//! Per-worker registries for in-flight multishot operations.
//!
//! Each variant owns a small FIFO per in-flight multishot op: the completion
//! drain pushes each CQE result into the op's slot and wakes the owning task,
//! which drains the FIFO on its next poll. [`accept`] carries `i32` results (an
//! accepted fd or a negative errno) in inline storage, sized for the handful of
//! listeners a worker runs. [`recv`] carries `(count, buf_id)` results in
//! mmap-backed storage, sized for per-connection scale.

pub mod accept;
pub mod recv;

pub(crate) use accept::MultishotPush;
pub use accept::{DEFAULT_MULTISHOT_CAP, MULTISHOT_FIFO_DEPTH, MultishotSlab, MultishotSlotKey};
pub use recv::{DEFAULT_RECV_MULTISHOT_CAP, RecvMultishotSlab, RecvMultishotSlotKey};
pub(crate) use recv::{NO_BUFFER, RecvMultishotPush};
