//! Procedural macros lowering onto the `kwokka` facade.
//!
//! The generated code imports only from the facade, never from the
//! workspace internals, so the macros track the stable surface. Every
//! internal binding carries the `__kwokka_` prefix and every emitted token
//! keeps the span of the source it came from, so compiler errors point
//! at the user's code rather than the expansion.

use proc_macro::TokenStream;

mod entry;

/// The async entry-point attribute.
///
/// Wraps an `async fn` in a synchronous entry point that builds the
/// runtime and drives the body to completion with `block_on`. The
/// scheduler is never defaulted: pass `affine` for a pinned
/// thread-per-core worker or `stealing` for a work-stealing crew.
///
/// The annotated function must be `async` and take no arguments; its
/// name, visibility, other attributes, and return type are preserved.
/// Under `stealing` the body must be `Send`, the same admission bound
/// the runtime itself enforces.
///
/// # Panics
///
/// The generated entry point panics when the runtime fails to build
/// (for example when the backend setup fails), carrying the build
/// error in the panic message.
///
/// # Examples
///
/// ```
/// #[kwokka::main(affine)]
/// async fn main() {
///     let answer = 41 + 1;
///     assert_eq!(answer, 42);
/// }
/// ```
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    entry::expand_main(args.into(), item.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}
