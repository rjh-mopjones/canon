use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ItemStruct;

use crate::util::AggregateVersionArgs;

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: AggregateVersionArgs = syn::parse2(attr)?;
    let mut input: ItemStruct = syn::parse2(item)?;

    let event_name = &input.ident;
    let aggregate_name = &args.aggregate;
    let version = args.version;
    let aggregate_name_str = aggregate_name.to_string();
    let event_name_str = event_name.to_string();

    // Marker trait for compile-time enforcement: every event must have a combiner
    let marker_trait_name = format_ident!("{}V{}HasCombiner", event_name, version);

    // Strip existing derives
    input.attrs.retain(|attr| !attr.path().is_ident("derive"));

    Ok(quote! {
        #[derive(Debug, Clone, ::canon_core::__Serialize, ::canon_core::__Deserialize)]
        #input

        /// Marker trait: compile error at ServiceBuilder call site if no `#[event_combiner]` satisfies this.
        #[doc(hidden)]
        pub trait #marker_trait_name {}

        ::canon_core::__submit! {
            ::canon_core::EventRegistration {
                aggregate_type_name: #aggregate_name_str,
                event_type_name: #event_name_str,
                event_version: #version,
            }
        }
    })
}
