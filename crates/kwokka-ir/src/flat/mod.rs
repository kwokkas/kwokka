//! Wire codec. The 16-byte header lands here now; the reader and writer
//! follow in later steps.

mod header;

pub use header::{HEADER_LEN, MAGIC, VERSION};
