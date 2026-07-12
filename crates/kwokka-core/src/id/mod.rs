//! Tree-structured 128-bit task identifier ([`Pip`]).

mod display;
mod error;
mod generate;
mod layout;
mod namespace;
mod pip;
mod relation;

pub use error::PipError;
pub use namespace::Namespace;
pub use pip::Pip;
