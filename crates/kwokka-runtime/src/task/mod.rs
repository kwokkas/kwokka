//! Task identity, state, mode markers, waker, the slab cell, and the join surface.

pub(crate) mod cell;
mod identity;
pub(crate) mod join;
mod marker;
pub(crate) mod state;
pub(crate) mod waker;
mod yielding;

pub use identity::TaskRef;
pub use join::{
    handle::{JoinError, TaskHandle},
    scope::{Scope, SpawnError, scope, scope_send},
};
pub use marker::{Affine, Mode, Stealing};
pub use yielding::{YieldNow, yield_now};
