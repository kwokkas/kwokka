//! Orchestration layer for the kwokka runtime.
//!
//! `kwokka-ensemble` composes runtime tasks into higher-level workflows: a
//! Stage is the smallest unit of work; Chain and Quantum compose stages
//! linearly or in bulk; a Conductor wires stages into a DAG; and an Advisor
//! decorates execution with retry, breaker, limiter, and timeout policies. The
//! crate consumes the typed IR from `kwokka-ir` and drives it on the kwokka
//! runtime.

pub mod error;

pub use error::EnsembleError;
