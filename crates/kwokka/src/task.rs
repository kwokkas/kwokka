//! Task surface -- structured scopes, scheduler markers, and yielding.
//!
//! [`scope`] fans a task out into children that all settle before the
//! scope resolves; [`scope_send`] is its `Send`-bounded twin whose
//! children may migrate across the stealing crew. [`yield_now`] hands the
//! worker back to the scheduler for one pass. The [`Affine`] and
//! [`Stealing`] markers select the scheduler discipline at the type level
//! through the [`Mode`] bound. Wall-clock sleeping lives in
//! [`crate::time`].

pub use kwokka_runtime::task::{
    Affine, Mode, Scope, SpawnError, Stealing, YieldNow, scope, scope_send, yield_now,
};
