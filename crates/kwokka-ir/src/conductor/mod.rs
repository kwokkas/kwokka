//! Conductor DAG views over a validated IR blob.

mod edge;
mod spec;
mod stage;

pub use edge::EdgeView;
pub use spec::ConductorView;
pub use stage::StageView;
