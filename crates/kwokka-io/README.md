<p align="center">
  <img src="../../.github/images/banner.png" alt="kwokka" />
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-io.svg)](https://crates.io/crates/kwokka-io)
[![docs.rs](https://docs.rs/kwokka-io/badge.svg)](https://docs.rs/kwokka-io)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-io

The completion-based I/O layer behind kwokka. It exposes one `IoDriver`
trait over io_uring on Linux, with epoll and kqueue fallbacks, alongside
the registered buffer pool and the operation types. `kwokka-net` and
`kwokka-fs` build their endpoints on top of it.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Depend on the `kwokka` facade rather than this crate
directly.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
