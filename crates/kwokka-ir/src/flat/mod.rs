//! Wire codec. The 16-byte header and the validating reader land here;
//! the writer follows in a later step.

pub mod header;
pub mod reader;

pub use header::{HEADER_LEN, MAGIC, VERSION};
pub use reader::validate;
