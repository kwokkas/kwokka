//! `io_uring` backend for Linux 6.0+.

pub(crate) mod cancel;
pub(crate) mod completion;
pub(crate) mod driver;
pub(crate) mod fixed;
pub(crate) mod linked;
pub(crate) mod msg;
pub(crate) mod multishot;
pub(crate) mod opcode;
pub(crate) mod setup;
pub(crate) mod submission;
pub(crate) mod wake;

pub use driver::UringDriver;
