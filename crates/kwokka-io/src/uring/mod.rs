//! `io_uring` backend for Linux 6.0+.

pub(crate) mod backend;
pub(crate) mod cqe;
pub(crate) mod opcode;
pub(crate) mod setup;

pub use backend::UringDriver;
