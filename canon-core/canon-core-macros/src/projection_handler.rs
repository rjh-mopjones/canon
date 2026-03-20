use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{FnArg, Ident, ImplItem, ItemImpl};

struct ProjectionHandlerArgs {
    projection: Ident,
}

impl Parse for ProjectionHandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let projection: Ident = input.parse()?;
        Ok(ProjectionHandlerArgs { projection })
    }
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: ProjectionHandlerArgs = syn::parse2(attr)?;
    let input: ItemImpl = syn::parse2(item)?;

    let projection_name = &args.projection;
    let projection_name_str = projection_name.to_string();

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

    // Find the apply method
    let apply_method = input
        .items
        .iter()
        .find_map(|item| {
            if let ImplItem::Fn(method) = item {
                if method.sig.ident == "apply" {
                    return Some(method);
                }
            }
            None
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &input,
                "projection_handler impl must contain an `apply` method",
            )
        })?;

    // Extract event type from the apply method's second parameter: event: &EventType
    // Parameters: &self, event: &EventType, store: &mut ProjectionType
    let event_param = apply_method.sig.inputs.iter().nth(1).ok_or_else(|| {
        syn::Error::new_spanned(
            &apply_method.sig,
            "apply method must have 3 parameters: &self, event, store",
        )
    })?;

    let event_type = match event_param {
        FnArg::Typed(pat_type) => {
            // The type is &EventType — extract the inner type
            match pat_type.ty.as_ref() {
                syn::Type::Reference(type_ref) => &type_ref.elem,
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "event parameter should be a reference (&EventType)",
                    ))
                }
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                event_param,
                "expected typed parameter for event",
            ))
        }
    };

    let apply_body = &apply_method.block;

    Ok(quote! {
        pub struct #handler_type;

        impl ::canon_core::ProjectionHandler<#projection_name> for #handler_type {
            type Event = #event_type;

            fn apply(&self, event: &Self::Event, store: &mut #projection_name) #apply_body
        }

        ::canon_core::__submit! {
            ::canon_core::ProjectionHandlerRegistration {
                projection_type_name: #projection_name_str,
                handler_type_name: #handler_type_str,
            }
        }
    })
}
