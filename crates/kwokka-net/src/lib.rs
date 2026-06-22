#![doc(html_logo_url = "https://cdn.kwokka.dev/images/icon-light.png")]
#![doc(html_favicon_url = "https://cdn.kwokka.dev/images/icon-light.png")]
//! TCP, UDP, and Unix sockets for the kwokka runtime.
//!
//! Network endpoints live here; the completion futures that drive them
//! migrate in from the runtime as the crate grows. Construction calls
//! (`bind`, `listen`) are synchronous one-shot syscalls; everything that
//! waits on a peer (`accept`, `connect`, `recv`, `send`) is a future.
//!
//! The first resident is [`tcp::TcpListener`] -- the bound endpoint whose
//! raw fd feeds the accept op.

pub mod tcp;
