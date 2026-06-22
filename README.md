<p align="center">
  <img src=".github/images/banner.png" alt="kwokka" />
</p>

<p align="center">
  <a href="https://crates.io/crates/kwokka"><img src="https://img.shields.io/crates/v/kwokka.svg" alt="crates.io"></a>
  <a href="https://docs.rs/kwokka"><img src="https://docs.rs/kwokka/badge.svg" alt="docs.rs"></a>
  <a href="https://github.com/kwokkas/kwokka/actions/workflows/test.yml"><img src="https://github.com/kwokkas/kwokka/actions/workflows/test.yml/badge.svg" alt="CI"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
  <a href="#minimum-supported-rust-version"><img src="https://img.shields.io/badge/MSRV-1.85.0-blue.svg" alt="MSRV"></a>
</p>

<p align="center">
  A completion-based async runtime for Rust, built on io_uring.
</p>

Kwokka hands I/O to the kernel and waits for completion instead of
polling for readiness. There are two schedulers, thread-per-core and
work-stealing, and you pick one explicitly.

## Quick start

```toml
[dependencies]
kwokka = "0.1"
```

```rust
#[kwokka::main(affine)] // or `stealing`
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

## Platform support

| Platform      | Backend  | Status           |
| ------------- | -------- | ---------------- |
| Linux 5.11+   | io_uring | Primary          |
| Linux (older) | epoll    | Fallback         |
| macOS, BSD    | kqueue   | Supported        |
| Windows       | IOCP     | Planned (0.2.0+) |

## Cargo features

| Feature    | Enables                              |
| ---------- | ------------------------------------ |
| `net`      | TCP listener and stream              |
| `fs`       | file open, read, and write           |
| `stealing` | task migration in work-stealing mode |
| `full`     | `net`, `fs`, and `stealing`          |

## Minimum supported Rust version

Rust 1.85.0, edition 2024.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
