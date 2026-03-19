use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

use crate::util::to_snake_case;

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "#[projection] takes no arguments",
        ));
    }

    let mut input: ItemStruct = syn::parse2(item)?;
    let name = &input.ident;
    let projection_id = to_snake_case(&name.to_string());
    let name_str = name.to_string();

    // Strip existing derives
    input.attrs.retain(|attr| !attr.path().is_ident("derive"));

    Ok(quote! {
        #[derive(Debug, Clone, ::canon_core::__Serialize, ::canon_core::__Deserialize)]
        #input

        impl #name {
            /// Returns the snake_case projection identifier for checkpoint tracking.
            pub fn projection_id(&self) -> &str {
                #projection_id
            }
        }

        ::canon_core::__submit! {
            ::canon_core::ProjectionRegistration {
                projection_type_name: #name_str,
                projection_id: #projection_id,
            }
        }
    })
}
