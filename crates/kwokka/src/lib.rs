//! Completion-based async runtime with integrated orchestration.
//!
//! This crate is the sole user entry point: every item re-exported here
//! is the supported surface over the workspace crates, and nothing
//! outside it is a stability promise. The runtime is scheduler-explicit
//! by design -- construct [`runtime::Runtime::affine`] for a pinned
//! thread-per-core worker or [`runtime::Runtime::stealing`] for a
//! work-stealing crew; there is no default scheduler.
//!
//! Tasks fan out through structured scopes ([`task::scope`] and the
//! `Send`-bounded [`task::scope_send`]) rather than a free-standing
//! spawn, so every child settles before its scope resolves.
//! [`time::sleep`] suspends a task for a wall-clock duration.
//!
//! Network and filesystem endpoints live under the `net` and `fs`
//! modules, each gated behind its own feature (`net`, `fs`, or `full`)
//! so a minimal build pulls in neither. [`Pip`] is the tree-structured
//! task identity every runtime task carries; runtime-issued ids surface
//! through task introspection in a later release, so today the type is
//! constructible but not yet handed back by the runtime.
//!
//! # Examples
//!
//! ```rust
//! let mut runtime = kwokka::runtime::Runtime::affine()?;
//! let value = runtime.block_on(async { 41 + 1 });
//! assert_eq!(value, 42);
//! # Ok::<(), std::io::Error>(())
//! ```

#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "net")]
pub mod net;
pub mod runtime;
pub mod task;
pub mod time;

pub use kwokka_core::id::{Pip, PipError};
pub use kwokka_macros::main;
