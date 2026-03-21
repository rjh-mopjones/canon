use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, GenericArgument, ImplItem, ItemImpl, PathArguments, ReturnType, Type};

use crate::util::AggregateVersionArgs;

/// Extract the event type from `Result<EventType, Error>` return type.
fn extract_event_type(return_type: &ReturnType) -> syn::Result<&Type> {
    let ty = match return_type {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                return_type,
                "handle method must have a return type",
            ))
        }
    };

    // Expect Result<EventType, Error>
    let result_path = match ty {
        Type::Path(tp) => tp,
        _ => {
            return Err(syn::Error::new_spanned(
                ty,
                "expected Result<EventType, Error> return type",
            ))
        }
    };

    let result_seg = result_path
        .path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(ty, "expected Result type"))?;

    let result_args = match &result_seg.arguments {
        PathArguments::AngleBracketed(args) => args,
        _ => {
            return Err(syn::Error::new_spanned(
                ty,
                "expected Result<EventType, Error>",
            ))
        }
    };

    // First generic arg is EventType
    match result_args.args.first() {
        Some(GenericArgument::Type(event_type)) => Ok(event_type),
        _ => Err(syn::Error::new_spanned(
            ty,
            "expected Result<EventType, Error>",
        )),
    }
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: AggregateVersionArgs = syn::parse2(attr)?;
    let input: ItemImpl = syn::parse2(item)?;

    let aggregate_name = &args.aggregate;
    let version = args.version;

    // Extract the handler type from `impl HandlerType { ... }`
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

    let aggregate_name_str = aggregate_name.to_string();

    // Find the `type Error = ...` associated type
    let error_type = input
        .items
        .iter()
        .find_map(|item| {
            if let ImplItem::Type(ty) = item {
                if ty.ident == "Error" {
                    return Some(&ty.ty);
                }
            }
            None
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &input,
                "command_handler impl must contain `type Error = ...`",
            )
        })?;

    // Find the `handle` method
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
            syn::Error::new_spanned(
                &input,
                "command_handler impl must contain a `handle` method",
            )
        })?;

    // Extract command type from the handle method's third parameter: cmd: CommandType
    // Parameters: &self, state: &Aggregate, cmd: CommandType
    let cmd_param = handle_method.sig.inputs.iter().nth(2).ok_or_else(|| {
        syn::Error::new_spanned(
            &handle_method.sig,
            "handle method must have 3 parameters: &self, state, command",
        )
    })?;

    let command_type = match cmd_param {
        FnArg::Typed(pat_type) => &pat_type.ty,
        _ => {
            return Err(syn::Error::new_spanned(
                cmd_param,
                "expected typed parameter for command",
            ))
        }
    };

    let command_type_str = match command_type.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    };

    // Extract event type from return type: Result<Vec<EventType>, Error>
    let event_type = extract_event_type(&handle_method.sig.output)?;

    // Get the full handle method signature elements
    let handle_params = &handle_method.sig.inputs;
    let handle_body = &handle_method.block;
    let return_type = &handle_method.sig.output;

    // Marker trait satisfaction
    let marker_trait_name = format_ident!("{}V{}HasHandler", command_type_str, version);

    // Unique dispatch function name for this handler
    let dispatch_fn_name = format_ident!(
        "__canon_dispatch_{}_v{}",
        command_type_str.to_lowercase(),
        version
    );

    // Extract the event type name as a string for the dispatch result
    let event_type_str = match event_type {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    };

    Ok(quote! {
        pub struct #handler_type;

        // Inherent method with the user's sync body
        impl #handler_type {
            #[doc(hidden)]
            fn __canon_handle(#handle_params) #return_type #handle_body
        }

        // Async trait impl delegates to the sync inherent method
        #[::canon_core::__async_trait]
        impl ::canon_core::CommandHandler<#aggregate_name> for #handler_type {
            type Command = #command_type;
            type Event = #event_type;
            type Error = #error_type;

            async fn handle(
                &self,
                state: &#aggregate_name,
                command: #command_type,
            ) -> ::std::result::Result<Self::Event, Self::Error> {
                self.__canon_handle(state, command)
            }
        }

        // Satisfy the marker trait
        impl #marker_trait_name for #handler_type {}

        // Type-erased dispatch function for the Dispatcher.
        // Hydrates aggregate state from events, deserializes the command,
        // runs the handler, and serializes the resulting event.
        fn #dispatch_fn_name(
            command_payload: &[u8],
            events: &[::canon_core::EventEnvelope],
            aggregate_type_id: ::std::any::TypeId,
        ) -> ::std::result::Result<
            ::canon_core::HandlerDispatchResult,
            Box<dyn ::std::error::Error + Send + Sync>,
        > {
            // 1. Create default aggregate state and hydrate from events
            let mut state = <#aggregate_name as ::std::default::Default>::default();
            for envelope in events {
                ::canon_core::__apply_event_combiner(
                    aggregate_type_id,
                    envelope,
                    &mut state as &mut dyn ::std::any::Any,
                )?;
            }

            // 2. Deserialize the command
            let command: #command_type = ::canon_core::__deserialize(command_payload)?;

            // 3. Run the handler
            let handler = #handler_type;
            let event = handler.__canon_handle(&state, command)
                .map_err(|e| -> Box<dyn ::std::error::Error + Send + Sync> { Box::new(e) })?;

            // 4. Serialize the resulting event
            let event_payload = ::canon_core::__serialize(&event)?;

            Ok(::canon_core::HandlerDispatchResult {
                event_payload,
                event_type: #event_type_str,
                event_version: #version,
            })
        }

        ::canon_core::__submit! {
            ::canon_core::CommandHandlerRegistration {
                aggregate_type_name: #aggregate_name_str,
                command_type_name: #command_type_str,
                command_version: #version,
                handler_type_name: #handler_type_str,
                dispatch_fn: #dispatch_fn_name,
                produces_event_type: #event_type_str,
                produces_event_version: #version,
            }
        }
    })
}
