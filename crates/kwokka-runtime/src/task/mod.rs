//! Task identity, lifecycle state, header layout, mode markers, waker, handle, and children.

pub(crate) mod children;
mod handle;
pub(crate) mod header;
mod identity;
pub(crate) mod io;
pub(crate) mod lifecycle;
mod marker;
mod scope;
mod sleeping;
pub(crate) mod slot;
pub(crate) mod state;
pub(crate) mod storage;
pub(crate) mod waker;
mod yielding;

pub use handle::{JoinError, TaskHandle};
pub use identity::TaskRef;
pub use marker::{Affine, Mode, Stealing};
pub use scope::{Scope, SpawnError, scope, scope_send};
pub use sleeping::{Sleep, sleep};
pub use yielding::{YieldNow, yield_now};
