use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{ImplItem, ItemImpl};

use crate::util::{replace_self_in_tokens, AggregateVersionArgs};

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
            syn::Error::new_spanned(
                &input,
                "event_combiner impl must contain a `combine` method",
            )
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

    // Unique function names
    let combine_fn_name = format_ident!(
        "__canon_combine_{}_v{}",
        event_type_str.to_lowercase(),
        version
    );
    let apply_fn_name = format_ident!(
        "__canon_apply_{}_v{}",
        event_type_str.to_lowercase(),
        version
    );

    // Rewrite the combine body: replace `self` with `__canon_self` so the
    // logic can live in a standalone function instead of a trait impl.
    // This avoids orphan-rule violations when the event type is defined in
    // a different crate from the aggregate.
    let modified_body = replace_self_in_tokens(combine_body.to_token_stream());

    Ok(quote! {
        // Standalone combine function — avoids orphan rules because no trait
        // is implemented on a foreign type.
        fn #combine_fn_name(__canon_self: &#event_type, state: &mut #aggregate_name)
            #modified_body

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
            #combine_fn_name(&event, state);
            Ok(())
        }

        // Satisfy the marker trait — only possible when the event type is local.
        // When the event is defined in another crate, users must ensure
        // exhaustiveness via inventory registrations (runtime) instead.
        #[allow(dead_code)]
        const _: () = {
            // Ensure the marker trait exists (compile error if #[event] is missing).
            fn __check_marker<T: #marker_trait_name>() {}
        };

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
