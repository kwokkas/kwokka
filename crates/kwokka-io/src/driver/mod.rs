//! Backend abstraction, enum dispatch, and the cross-thread wake surface.

mod backend;
mod dispatch;
pub mod wake;

pub use backend::{CancelError, IoDriver, RegisterError};
pub use dispatch::DriverType;
