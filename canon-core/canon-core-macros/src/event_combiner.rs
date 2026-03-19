use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use syn::{ImplItem, ItemImpl};

use crate::util::AggregateVersionArgs;

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: AggregateVersionArgs = syn::parse2(attr)?;
    let input: ItemImpl = syn::parse2(item)?;

    let aggregate_name = &args.aggregate;
    let version = args.version;

    // Extract the event type from `impl EventType { ... }`
    let event_type = &input.self_ty;

    // Find the `combine` method
    let combine_method = input
        .items
        .iter()
        .find_map(|item| {
            if let ImplItem::Fn(method) = item {
                if method.sig.ident == "combine" {
                    return Some(method);
                }
            }
            None
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(&input, "event_combiner impl must contain a `combine` method")
        })?;

    let combine_body = &combine_method.block;

    // Extract event type name as a string for inventory registration
    let event_type_str = match event_type.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => {
            return Err(syn::Error::new_spanned(
                event_type,
                "expected a simple type path",
            ))
        }
    };

    // Marker trait satisfaction: satisfies the HasCombiner marker from #[event]
    let marker_trait_name = format_ident!("{}V{}HasCombiner", event_type_str, version);

    // Unique function name for the apply_fn
    let apply_fn_name = format_ident!(
        "__canon_apply_{}_v{}",
        event_type_str.to_lowercase(),
        version
    );

    Ok(quote! {
        // EventCombiner trait impl
        impl ::canon_core::EventCombiner<#aggregate_name> for #event_type {
            fn combine(&self, state: &mut #aggregate_name) #combine_body
        }

        // Satisfy the marker trait
        impl #marker_trait_name for #event_type {}

        // Type-erased apply function for inventory registration
        fn #apply_fn_name(
            payload: &[u8],
            state: &mut dyn ::std::any::Any,
        ) -> ::std::result::Result<(), Box<dyn ::std::error::Error + Send + Sync>> {
            let event: #event_type = ::canon_core::__deserialize(payload)?;
            let state = state
                .downcast_mut::<#aggregate_name>()
                .ok_or_else(|| -> Box<dyn ::std::error::Error + Send + Sync> {
                    "aggregate state type mismatch in event combiner".into()
                })?;
            <#event_type as ::canon_core::EventCombiner<#aggregate_name>>::combine(&event, state);
            Ok(())
        }

        ::canon_core::__submit! {
            ::canon_core::EventCombinerRegistration {
                aggregate_type_id: ::std::any::TypeId::of::<#aggregate_name>(),
                event_type_name: #event_type_str,
                event_version: #version,
                apply_fn: #apply_fn_name,
            }
        }
    })
}
