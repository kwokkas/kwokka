<p align="center">
  <img src="../../.github/images/banner.png" alt="kwokka" />
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-runtime.svg)](https://crates.io/crates/kwokka-runtime)
[![docs.rs](https://docs.rs/kwokka-runtime/badge.svg)](https://docs.rs/kwokka-runtime)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-runtime

The runtime behind kwokka: the work-stealing and thread-per-core
schedulers, the workers and tasks, the timer wheel, and the lock-free
sync primitives. It surfaces through `kwokka::runtime` and `kwokka::task`.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Depend on the `kwokka` facade rather than this crate
directly.

## Example

```rust
fn main() -> std::io::Result<()> {
    // affine() is thread-per-core; stealing() is work-stealing
    let mut runtime = kwokka::runtime::Runtime::affine()?;
    runtime.block_on(async {
        // your async code
    });
    Ok(())
}
```

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
