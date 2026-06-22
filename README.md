<p align="center">
  <img src=".github/images/banner.png" alt="kwokka" />
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka.svg)](https://crates.io/crates/kwokka)
[![docs.rs](https://docs.rs/kwokka/badge.svg)](https://docs.rs/kwokka)
[![CI](https://github.com/kwokkas/kwokka/actions/workflows/test.yml/badge.svg)](https://github.com/kwokkas/kwokka/actions/workflows/test.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85.0-blue.svg)](#supported-rust-versions)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## What is Kwokka?

Kwokka is a completion-based async runtime for Rust, built on io_uring.
Instead of polling file descriptors for readiness, it hands operations
to the kernel and reacts when they complete. On Linux that maps onto
io_uring, with an epoll fallback and a kqueue backend for macOS and the
BSDs behind the same API.

You choose the scheduler explicitly. `affine` is thread-per-core with
tasks pinned to their thread, and `stealing` is work-stealing with tasks
that move toward idle workers. A phantom `Mode` type makes mixing the
two a compile error rather than a runtime panic.

## Features

- Completion-based I/O on io_uring, with epoll and kqueue behind the
  same API.
- Two schedulers you choose explicitly: thread-per-core (`affine`) and
  work-stealing (`stealing`).
- Zero-cost dispatch. The runtime is enum-based, with no trait objects
  or vtables.
- Zero-copy reads and writes through pinned inline buffers, with no
  per-call heap allocation.
- Index-based ownership. Tasks live in per-worker generational slabs and
  carry no reference counting.
- Structured concurrency through scopes, so a scope waits for its
  children before it resolves.
- TCP and file I/O behind the `net` and `fs` features.

## Examples

Thread-per-core (`affine`):

```rust
#[kwokka::main(affine)]
async fn main() {}
```

Work-stealing (`stealing`):

```rust
#[kwokka::main(stealing)]
async fn main() {}
```

To embed the runtime yourself, build `Runtime::affine()` or
`Runtime::stealing()` and drive it with `block_on`.

## Supported Rust Versions

Kwokka supports Rust 1.85.0 and later on edition 2024. Raising the
minimum supported version is treated as a minor-version change.

## Supported Linux kernels

io_uring is the primary backend and needs Linux 5.11 or newer. On older
kernels Kwokka falls back to epoll. Some io_uring features, such as
provided buffers and zero-copy send, need newer kernels and turn on only
when the running kernel supports them.

## Contributing

Contributions are welcome. Open an issue to discuss a change, or send a
pull request. A fuller contributor guide arrives with the public
release.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
