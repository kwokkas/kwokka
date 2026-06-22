<p align="center">
  <img src=".github/images/banner.png" alt="kwokka" />
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka.svg)](https://crates.io/crates/kwokka)
[![docs.rs](https://docs.rs/kwokka/badge.svg)](https://docs.rs/kwokka)
[![CI](https://github.com/kwokkas/kwokka/actions/workflows/test.yml/badge.svg)](https://github.com/kwokkas/kwokka/actions/workflows/test.yml)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Kwokka is a completion-based async runtime for Rust, built on io_uring.
It hands I/O to the kernel and waits for completion instead of polling
for readiness. There are two schedulers, thread-per-core and
work-stealing, and you pick one explicitly.

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

`affine` pins `!Send` tasks one thread per core. `stealing` migrates
`Send` tasks toward idle workers. A phantom `Mode` type makes mixing the
two a compile error. For manual control, build `Runtime::affine()` or
`Runtime::stealing()` and drive it with `block_on`.

## Design

- Completion-based on every platform. epoll and kqueue are adapted to
  completions internally.
- No reference counting. Tasks live in per-worker generational slabs,
  addressed by index.
- Zero-copy I/O through pinned inline buffers, with no per-call heap.
- Enum dispatch only. No trait objects, no vtables in the runtime.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
