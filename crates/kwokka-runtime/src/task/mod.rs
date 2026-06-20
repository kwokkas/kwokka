//! Task identity, lifecycle state, and mode markers.

mod identity;
mod marker;
pub(crate) mod state;

pub use identity::TaskRef;
pub use marker::{Affine, Mode, Stealing};
