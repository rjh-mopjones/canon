use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use syn::ItemStruct;

use crate::util::CommandArgs;

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: CommandArgs = syn::parse2(attr)?;
    let mut input: ItemStruct = syn::parse2(item)?;

    let command_name = &input.ident;
    let aggregate_name = &args.aggregate;
    let version = args.version;
    let aggregate_name_str = aggregate_name.to_string();
    let command_name_str = command_name.to_string();

    // Generate the per-command event enum
    let event_enum_name = format_ident!("{}Event", command_name);
    let produces = &args.produces;
    let enum_variants: Vec<_> = produces
        .iter()
        .map(|p| {
            quote! { #p(#p) }
        })
        .collect();

    // Marker trait for compile-time enforcement: every command must have a handler
    let marker_trait_name = format_ident!("{}V{}HasHandler", command_name, version);

    // Strip existing derives
    input.attrs.retain(|attr| !attr.path().is_ident("derive"));

    Ok(quote! {
        #[derive(Debug, Clone, ::canon_core::__Serialize, ::canon_core::__Deserialize)]
        #input

        #[derive(Debug, Clone)]
        pub enum #event_enum_name {
            #(#enum_variants),*
        }

        /// Marker trait: compile error at ServiceBuilder call site if no `#[command_handler]` satisfies this.
        #[doc(hidden)]
        pub trait #marker_trait_name {}

        ::canon_core::__submit! {
            ::canon_core::CommandRegistration {
                aggregate_type_name: #aggregate_name_str,
                command_type_name: #command_name_str,
                command_version: #version,
            }
        }
    })
}
