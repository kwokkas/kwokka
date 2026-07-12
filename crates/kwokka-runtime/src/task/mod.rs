//! What a task is: the cell it lives in, what names it, how it migrates,
//! how it yields, and the join surface a caller waits on.

pub(crate) mod cell;
mod cooperative;
pub(crate) mod join;
pub(crate) mod migration;
pub(crate) mod reference;

pub use cooperative::{YieldNow, yield_now};
pub use join::{
    handle::{JoinError, TaskHandle},
    scope::{Scope, SpawnError, scope, scope_send},
};
pub use migration::marker::{Affine, Mode, Stealing};
pub use reference::identity::TaskRef;
