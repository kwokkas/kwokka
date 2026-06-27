//! Wire codec: the 16-byte header, the validating reader, and the
//! single-pass writer.

pub mod header;
pub mod reader;
pub mod writer;

pub use header::{HEADER_LEN, MAGIC, VERSION};
pub use reader::validate;
pub use writer::{StageSpec, WriteError, write_conductor};
