use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

use crate::util::AggregateArgs;

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: AggregateArgs = syn::parse2(attr)?;
    let mut input: ItemStruct = syn::parse2(item)?;

    let name = &input.ident;

    let snapshot_const = match args.snapshot_every {
        Some(n) => quote! { Some(#n) },
        None => quote! { None },
    };

    // Strip any existing derives to avoid duplicates
    input.attrs.retain(|attr| !attr.path().is_ident("derive"));

    Ok(quote! {
        #[derive(Debug, Clone, Default, ::canon_core::__Serialize, ::canon_core::__Deserialize)]
        #input

        impl ::canon_core::Aggregate for #name {
            type State = #name;
            type Error = ::canon_core::MacroError;

            fn hydrate(
                state: &mut Self::State,
                events: impl Iterator<Item = ::canon_core::EventEnvelope>,
            ) -> Result<(), Self::Error> {
                let target_id = ::std::any::TypeId::of::<#name>();
                for envelope in events {
                    ::canon_core::__apply_event_combiner(
                        target_id,
                        &envelope,
                        state as &mut dyn ::std::any::Any,
                    ).map_err(|e| ::canon_core::MacroError(e.to_string()))?;
                }
                Ok(())
            }
        }

        impl #name {
            /// Snapshot cadence: take a snapshot every N events, or None for no snapshotting.
            pub const SNAPSHOT_EVERY: ::std::option::Option<u64> = #snapshot_const;
        }
    })
}
