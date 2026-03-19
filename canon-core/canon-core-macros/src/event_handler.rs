use proc_macro2::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl};

use crate::util::{EventHandlerArgs, HandlesArgs, parse_duration_to_secs};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: EventHandlerArgs = syn::parse2(attr)?;
    let input: ItemImpl = syn::parse2(item)?;

    let handler_type = &input.self_ty;
    let handler_type_str = match handler_type.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => {
            return Err(syn::Error::new_spanned(
                handler_type,
                "expected a simple type path",
            ))
        }
    };

    // Find the handle method and its #[handles] attribute
    let handle_method = input
        .items
        .iter()
        .find_map(|item| {
            if let ImplItem::Fn(method) = item {
                if method.sig.ident == "handle" {
                    return Some(method);
                }
            }
            None
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(&input, "event_handler impl must contain a `handle` method")
        })?;

    // Parse #[handles(EventType, version = N)] attribute
    let handles_attr = handle_method
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("handles"))
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &handle_method.sig,
                "handle method must have a #[handles(EventType, version = N)] attribute",
            )
        })?;

    let handles_args: HandlesArgs = handles_attr.parse_args()?;
    let event_type = &handles_args.event_type;
    let event_version = handles_args.version;
    let event_type_str = event_type.to_string();

    // Check for oversight method
    let has_oversight = input.items.iter().any(|item| {
        if let ImplItem::Fn(method) = item {
            method.sig.ident == "oversight"
        } else {
            false
        }
    });

    // Enforce: window_ttl requires oversight
    if args.window_ttl.is_some() && !has_oversight {
        return Err(syn::Error::new_spanned(
            &input,
            "event_handler with `window_ttl` requires an `oversight` method",
        ));
    }

    let window_ttl_secs = match &args.window_ttl {
        Some(ttl) => {
            let secs = parse_duration_to_secs(ttl)?;
            quote! { ::std::option::Option::Some(#secs) }
        }
        None => quote! { ::std::option::Option::None },
    };

    // Extract handle method body (strip #[handles] attribute)
    let handle_body = &handle_method.block;
    let handle_params = &handle_method.sig.inputs;

    // Extract oversight method body if present
    let oversight_impl = if let Some(oversight_method) = input.items.iter().find_map(|item| {
        if let ImplItem::Fn(method) = item {
            if method.sig.ident == "oversight" {
                return Some(method);
            }
        }
        None
    }) {
        let oversight_body = &oversight_method.block;
        quote! {
            fn oversight(&self, accumulated: &[::canon_core::IncomingMessage]) -> ::canon_core::Oversight
                #oversight_body
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        pub struct #handler_type;

        // Inherent method with the user's handle body (returns Option, not Result)
        impl #handler_type {
            #[doc(hidden)]
            fn __canon_handle(#handle_params) -> ::std::option::Option<::canon_core::CommandEnvelope>
                #handle_body
        }

        #[::canon_core::__async_trait]
        impl ::canon_core::EventHandler for #handler_type {
            type Event = #event_type;
            type Error = ::canon_core::MacroError;

            async fn handle(
                &self,
                events: Vec<#event_type>,
            ) -> ::std::result::Result<::std::option::Option<::canon_core::CommandEnvelope>, Self::Error> {
                ::std::result::Result::Ok(self.__canon_handle(events))
            }

            #oversight_impl
        }

        ::canon_core::__submit! {
            ::canon_core::EventHandlerRegistration {
                handler_type_name: #handler_type_str,
                event_type_name: #event_type_str,
                event_version: #event_version,
                window_ttl_secs: #window_ttl_secs,
            }
        }
    })
}

