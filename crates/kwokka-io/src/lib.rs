//! Completion-based I/O driver, buffer pool, and operation types.

pub mod addr;
pub mod capability;

pub use addr::{AddrError, AddressFamily, SockAddr, UnixAddr};
pub use capability::{CapabilityMatrix, KernelVersion};
