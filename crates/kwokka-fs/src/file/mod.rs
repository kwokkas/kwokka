//! File primitives -- the owned handle and its pinned futures.

mod handle;

pub use handle::File;
pub use kwokka_io::operation::{FileReadFuture, FileWriteFuture};
