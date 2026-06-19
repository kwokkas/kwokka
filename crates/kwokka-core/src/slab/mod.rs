//! Generational slab allocator.

mod key;
#[allow(
    clippy::module_inception,
    reason = "Slab<T> impl colocated with the slab module"
)]
mod slab;

pub use key::SlabKey;
pub use slab::{Slab, SlabError};
