//! Backend abstraction, enum dispatch, and the cross-thread wake surface.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) is required for the seam-only type behind this private module"
)]

mod backend;
mod dispatch;
pub mod wake;

pub use backend::{CancelError, IoDriver, RegisterError};
pub use dispatch::DriverType;
pub(crate) use dispatch::SlotSubmit;
