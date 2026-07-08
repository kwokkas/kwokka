//! TCP primitives -- the listening endpoint, the connected stream, and
//! the accept stream.
//!
//! The socket futures stay unnamed: every entry point returns an opaque
//! future the caller only awaits. Two named buffer types cross the surface:
//! [`ProvidedBuf`], the borrowed zero-copy view a provided-buffer recv
//! resolves into, and [`FixedBuf`], the owned partial-length source the
//! buffer-generic `send_buf` accepts.

pub mod accept;
mod connect;
mod listener;
mod recv;
mod stream;

pub(crate) use accept::AcceptFuture;
pub use accept::AcceptStream;
pub use kwokka_io::operation::{FixedBuf, ProvidedBuf};
pub use listener::TcpListener;
pub use recv::RecvStream;
pub use stream::TcpStream;
