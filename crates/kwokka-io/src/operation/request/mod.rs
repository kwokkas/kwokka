//! The submit payload a backend receives, grouped by what the op acts on.

mod control;
mod fd;
mod net;
mod payload;
mod read;
mod splice;
mod write;

pub use payload::{CommonFields, ControlPayload, IoRequest, OpPayload};
