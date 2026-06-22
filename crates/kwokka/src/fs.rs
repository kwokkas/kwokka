//! Filesystem endpoints -- files over the completion backend.
//!
//! Gated behind the `fs` feature. [`File`] opens or creates a handle and
//! hands out the [`read`](File::read) and [`write`](File::write) futures
//! that drive the descriptor at an offset through the stream's own pinned
//! inline buffer. Opening is async-shaped today over a one-shot blocking
//! syscall; the ring-lowered open swaps the body with no caller-visible
//! change.
//!
//! # Examples
//!
//! ```no_run
//! # // no_run: opens a real file and drives io_uring, which a doctest host may lack.
//! use kwokka::{fs::File, runtime::Runtime};
//!
//! let mut runtime = Runtime::affine()?;
//! let file = runtime.block_on(File::open("Cargo.toml"))?;
//! let (result, _buf) = runtime.block_on(file.read::<64>(0));
//! let _read = result?;
//! # Ok::<(), std::io::Error>(())
//! ```

pub use kwokka_fs::file::{File, FileReadFuture, FileWriteFuture};
