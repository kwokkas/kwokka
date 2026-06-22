//! Code generation for `#[kwokka::main]`.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

use super::parse::{MainArgs, Scheduler};

/// Expands `#[kwokka::main]` into a synchronous entry point that builds
/// the chosen runtime and drives the async body with `block_on`.
///
/// The user's attributes, visibility, name, generics, and return type
/// pass through unchanged; the body keeps its spans, so errors inside
/// it point at the source.
///
/// # Errors
///
/// Returns the parse or validation error -- rendered as a compile error
/// at the offending span -- when the arguments are malformed, the
/// function is not `async`, or it takes arguments.
pub(crate) fn expand_main(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: MainArgs = syn::parse2(args)?;
    let function: ItemFn = syn::parse2(item)?;
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "#[kwokka::main] wraps an async fn; mark the entry point async",
        ));
    }
    if !function.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "the #[kwokka::main] entry point takes no arguments",
        ));
    }
    let (constructor, scheduler_name) = match args.scheduler {
        Scheduler::Affine => (quote!(::kwokka::runtime::Runtime::affine()), "affine"),
        Scheduler::Stealing => (quote!(::kwokka::runtime::Runtime::stealing()), "stealing"),
    };
    let attrs = &function.attrs;
    let vis = &function.vis;
    let ident = &function.sig.ident;
    let generics = &function.sig.generics;
    let where_clause = &function.sig.generics.where_clause;
    let output = &function.sig.output;
    let body = &function.block;
    Ok(quote! {
        #(#attrs)*
        #vis fn #ident #generics () #output #where_clause {
            let mut __kwokka_runtime = match #constructor {
                ::core::result::Result::Ok(__kwokka_runtime) => __kwokka_runtime,
                ::core::result::Result::Err(__kwokka_error) => ::core::panic!(
                    "#[kwokka::main] failed to build the {} runtime: {}",
                    #scheduler_name,
                    __kwokka_error,
                ),
            };
            __kwokka_runtime.block_on(async move #body)
        }
    })
}
