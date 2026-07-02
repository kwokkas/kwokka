//! TCP primitives -- the listening endpoint, the connected stream, and
//! the accept stream. The socket futures stay unnamed: every entry point
//! returns an opaque future the caller only awaits.

mod accept;
mod connect;
mod listener;
mod stream;

pub use accept::AcceptStream;
pub use listener::TcpListener;
pub use stream::TcpStream;
