//! Language-neutral typed intermediate representation for kwokka
//! orchestration specs.
//!
//! `kwokka-ir` is the stable contract between conductor macro lowering
//! and runtime execution: `#[kwokka::conductor]` and the imperative
//! builder lower a DAG to a flat IR blob, and the runtime consumes that
//! blob rather than the user's AST. The crate is a zero-dependency leaf
//! carrying no workspace or external dependency, so the IR is portable
//! to and consumable from other languages.
//!
//! The wire format is a relative-offset, little-endian flat layout: the
//! bytes written into the consumer's arena are the wire format itself,
//! so in-process reads and cross-language interchange share one
//! representation.
#![no_std]

pub mod conductor;
pub mod error;
pub mod flat;
pub mod node;
pub mod policy;

pub use conductor::{ConductorView, EdgeView, StageView};
pub use error::IrError;
pub use flat::{StageSpec, WriteError, validate, write_conductor};
pub use node::{KwokkaIr, NodeTag};
pub use policy::{BreakerView, LimiterView, PolicyKind, RetryView, TimeoutView};
