<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.kwokka.dev/images/banner-dark.png">
    <img src="https://cdn.kwokka.dev/images/banner-light.png" alt="kwokka">
  </picture>
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-macros.svg)](https://crates.io/crates/kwokka-macros)
[![docs.rs](https://docs.rs/kwokka-macros/badge.svg)](https://docs.rs/kwokka-macros)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-macros

The procedural macros for kwokka. In 0.1.0 this is the `#[kwokka::main]`
attribute, which boots a runtime and runs an async `main` on the
scheduler you name. It is re-exported through the facade as
`kwokka::main`.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Use these macros through the `kwokka` facade rather than
depending on this crate directly.

## Example

Thread-per-core:

```rust
#[kwokka::main(affine)]
async fn main() {}
```

Work-stealing:

```rust
#[kwokka::main(stealing)]
async fn main() {}
```

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
