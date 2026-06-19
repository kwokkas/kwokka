//! Completion-based I/O driver layer.
//!
//! Provides the [`IoDriver`] trait, backend dispatch enum, operation
//! types, buffer management, and platform-specific backends (`io_uring`,
//! `epoll`, `kqueue`, `IOCP`). The completion model delivers a
//! [`SubmitToken`] naming the in-flight operation; the runtime wakes
//! the associated task on arrival.
//!
//! [`SubmitToken`]: operation::SubmitToken

pub mod addr;
pub mod buffer;
pub mod capability;
mod driver;
pub mod operation;
#[cfg(target_os = "linux")]
pub mod uring;

pub use addr::{AddrError, AddressFamily, SockAddr, UnixAddr};
pub use capability::{CapabilityMatrix, KernelVersion};
pub use driver::{CancelError, IoDriver, RegisterError};
