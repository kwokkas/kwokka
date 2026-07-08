//! Unix-domain socket primitives -- the listening endpoint and the connected
//! stream.
//!
//! [`UnixListener`] binds a stream endpoint at a filesystem path whose raw fd
//! feeds the accept op; the accepted [`UnixStream`] converses through the
//! [`recv_buf`](UnixStream::recv_buf) and [`send_buf`](UnixStream::send_buf)
//! futures. Binding and connecting are synchronous one-shot syscalls; the
//! accept and the byte transfers are futures driven through the completion
//! backend, reusing the TCP accept / recv / send ops over the Unix fd.

mod listener;
mod stream;

pub use listener::UnixListener;
pub use stream::UnixStream;
