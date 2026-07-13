//! The worker crew: sibling threads brought up per scheduler discipline, and
//! the shutdown broadcast every sibling watches.

pub(crate) mod affine;
pub(crate) mod kind;
pub(crate) mod stealing;
