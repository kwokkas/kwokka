//! Attribute-argument parsing for `#[kwokka::main]`.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use proc_macro2::Span;
use syn::{
    Ident, Token,
    parse::{Parse, ParseStream},
};

/// The scheduler the entry point builds, named by a bare identifier
/// (`affine` or `stealing`) in the first macro argument.
#[derive(Clone, Copy)]
pub(crate) enum Scheduler {
    /// `Runtime::affine()` -- one worker pinned to the calling thread.
    Affine,
    /// `Runtime::stealing()` -- a work-stealing crew.
    Stealing,
}

/// Parsed `#[kwokka::main(...)]` arguments.
pub(crate) struct MainArgs {
    /// The scheduler choice; never defaulted.
    pub(crate) scheduler: Scheduler,
}

impl Parse for MainArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Err(syn::Error::new(
                Span::call_site(),
                "the scheduler is never defaulted; write \
                 #[kwokka::main(affine)] or #[kwokka::main(stealing)]",
            ));
        }
        let first: Ident = input.parse()?;
        // A `key = value` first argument is either the retired
        // `scheduler = "..."` spelling or a 0.2.0 parameter arriving early.
        if input.peek(Token![=]) {
            if first == "scheduler" {
                return Err(syn::Error::new(
                    first.span(),
                    "the `scheduler = \"...\"` form was replaced; write \
                     #[kwokka::main(affine)] or #[kwokka::main(stealing)]",
                ));
            }
            return Err(syn::Error::new(
                first.span(),
                format!("argument `{first}` is not supported in 0.1.0"),
            ));
        }
        let scheduler = if first == "affine" {
            Scheduler::Affine
        } else if first == "stealing" {
            Scheduler::Stealing
        } else {
            return Err(syn::Error::new(
                first.span(),
                format!("unknown scheduler `{first}`, expected `affine` or `stealing`"),
            ));
        };
        // The grammar reserves a trailing `, key = value` tail for 0.2.0
        // (workers, placement); 0.1.0 admits the scheduler alone.
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            return Err(syn::Error::new(
                key.span(),
                format!("argument `{key}` is not supported in 0.1.0"),
            ));
        }
        Ok(Self { scheduler })
    }
}
