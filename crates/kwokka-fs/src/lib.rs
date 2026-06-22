#![doc(html_logo_url = "https://cdn.kwokka.dev/images/icon-light.png")]
#![doc(html_favicon_url = "https://cdn.kwokka.dev/images/icon-light.png")]
//! Asynchronous file I/O for the kwokka runtime.
//!
//! Filesystem endpoints live here, speaking through the pinned
//! completion futures. Opening is async-shaped from the start --
//! internally a one-shot blocking syscall until the ring-lowered open
//! lands -- so the lowering arrives without a breaking change. Reads
//! and writes travel the ring through the futures the open hands out.
//!
//! The first resident is [`file::File`] -- the owned handle whose
//! descriptor feeds the read and write ops. The crate starts with
//! `file`; directories, paths, and pipes arrive in follow-on releases.

pub mod file;
