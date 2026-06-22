<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.kwokka.dev/images/banner-dark.png">
    <img src="https://cdn.kwokka.dev/images/banner-light.png" alt="kwokka">
  </picture>
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka.svg)](https://crates.io/crates/kwokka)
[![docs.rs](https://docs.rs/kwokka/badge.svg)](https://docs.rs/kwokka)
[![CI](https://github.com/kwokkas/kwokka/actions/workflows/test.yml/badge.svg)](https://github.com/kwokkas/kwokka/actions/workflows/test.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85.0-blue.svg)](#supported-rust-versions)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> [!WARNING]
> Kwokka is at 0.1.0. The public API is settled but pre-1.0 and can
> still change before 1.0. Orchestration and a Tokio compatibility layer
> arrive in later releases.

## Installation

Add Kwokka to your `Cargo.toml`:

```toml
[dependencies]
kwokka = "0.1"
```

The runtime and structured concurrency come by default. TCP, files, and
work-stealing migration are cargo features, so a minimal build skips
them:

```toml
[dependencies]
kwokka = { version = "0.1", features = ["full"] }
```

`full` enables `net`, `fs`, and `stealing`. Pick them one at a time if
you only need some.

## What is Kwokka?

Kwokka is a completion-native async framework for Rust, built on
io_uring. Instead of polling file descriptors for readiness, it hands
operations to the kernel and reacts when they complete. io_uring is the
Linux backend, with epoll as a fallback, kqueue for macOS and the BSDs,
and IOCP for Windows planned. They all sit behind one completion API.

You choose the scheduler explicitly. `affine` is thread-per-core with
tasks pinned to their thread, and `stealing` is work-stealing with tasks
that move toward idle workers.

> [!TIP]
> A phantom `Mode` type makes mixing `affine` and `stealing` a compile
> error rather than a runtime panic. The dual-scheduler runtime is the
> foundation, and an optional orchestration layer for pipelines, batches,
> and DAGs builds on top of it in later releases.

## Features

- Completion-native I/O on io_uring, with epoll and kqueue behind the
  same API.
- Two schedulers you pick explicitly: thread-per-core (`affine`) and
  work-stealing (`stealing`).
- Enum-based dispatch, with no trait objects or vtables in the runtime.
- Zero-copy reads and writes through pinned inline buffers, with no
  per-call heap allocation.
- An allocation-free hot path: task poll, wake, and I/O submission touch
  no heap in steady state.
- Index-addressed tasks in per-worker generational slabs, with no
  reference counting.
- Structured concurrency through scopes, so a scope waits for its
  children.
- TCP and file I/O behind the `net` and `fs` features.

## Examples

The full API reference lives on [docs.rs](https://docs.rs/kwokka).

A thread-per-core echo server on `affine`:

```rust
use kwokka::net::TcpListener;

#[kwokka::main(affine)]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    let stream = listener.accept().await?;

    let (read, buf) = stream.recv::<1024>().await;
    let len = read?;
    stream.send(buf, len).await?;

    Ok(())
}
```

A work-stealing fan-out on `stealing`:

```rust
use kwokka::task::scope_send;

#[kwokka::main(stealing)]
async fn main() {
    scope_send(|crew| {
        for _ in 0..8 {
            crew.spawn(async {
                // a unit of work the crew can relocate toward an idle core
            }).ok();
        }
    })
    .await;
}
```

> [!NOTE]
> The `stealing` cargo feature turns on task migration. Without it,
> `stealing` mode still runs but keeps each task on its starting worker.

To embed the runtime yourself, build `Runtime::affine()` or
`Runtime::stealing()` and drive it with `block_on`.

## Supported Rust Versions

Kwokka supports Rust 1.85.0 and later on edition 2024. Raising the
minimum supported version is treated as a minor-version change.

## Supported Linux kernels

> [!IMPORTANT]
> io_uring needs Linux 5.11 or newer. On older kernels Kwokka falls back
> to epoll automatically.

Some io_uring features, such as provided buffers and zero-copy send, need
newer kernels and turn on only when the running kernel supports them.

## Contributing

Contributions are welcome. Open an issue to discuss a change, or send a
pull request. See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for the full
contributor guide.

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
