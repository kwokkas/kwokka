<p align="center">
  <img src="../../.github/images/banner.png" alt="kwokka" />
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-core.svg)](https://crates.io/crates/kwokka-core)
[![docs.rs](https://docs.rs/kwokka-core/badge.svg)](https://docs.rs/kwokka-core)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-core

Foundational types shared across the kwokka workspace: the `Pip` task
identity, the scheduling and cancellation vocabulary, the generational
slab, and the bump arena.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Depend on the `kwokka` facade rather than this crate
directly.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
