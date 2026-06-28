<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.kwokka.dev/images/banner-dark.png">
    <img src="https://cdn.kwokka.dev/images/banner-light.png" alt="kwokka">
  </picture>
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-ir.svg)](https://crates.io/crates/kwokka-ir)
[![docs.rs](https://docs.rs/kwokka-ir/badge.svg)](https://docs.rs/kwokka-ir)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-ir

A language-neutral, typed intermediate representation for orchestration
specs. The wire format is a portable flat layout: relative-offset,
little-endian, 8-aligned, and zero-dependency `#![no_std]`. The in-memory
bytes are the wire format itself, so what a producer writes is exactly
what crosses a process or language boundary, with no separate
serialization pass. A spec encoded by one language can be decoded by
another.

Two-tier trust keeps reads safe. Bytes built in-process are read
directly; bytes from an untrusted source pass through `validate`, which
bounds-checks every offset, table, and ordinal first. Framing and bounds
are the crate's guarantee. Value-level and graph-level semantics belong
to the consumer.

Within kwokka, this IR sits between the macro layer and the runtime:
`#[kwokka::conductor]` and the imperative builder lower a DAG into the
blob, and the runtime consumes that blob rather than the user's AST. The
IR pulls in no runtime or workspace dependency of its own. It is a
standalone data model that any language can implement against.

This crate is part of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. From Rust, reach it through the `kwokka` facade rather
than depending on it directly.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
