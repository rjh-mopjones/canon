use syn::{Ident, Token, punctuated::Punctuated};
use syn::parse::{Parse, ParseStream};

/// Convert a PascalCase identifier to snake_case.
pub fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap_or(ch));
        } else {
            result.push(ch);
        }
    }
    result
}

/// Arguments: `AggregateName, version = N`
pub struct AggregateVersionArgs {
    pub aggregate: Ident,
    pub version: u32,
}

impl Parse for AggregateVersionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let aggregate: Ident = input.parse()?;
        let mut version = 1u32;

        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if key == "version" {
                input.parse::<Token![=]>()?;
                let lit: syn::LitInt = input.parse()?;
                version = lit.base10_parse()?;
            }
        }

        Ok(AggregateVersionArgs { aggregate, version })
    }
}

/// Arguments: `AggregateName, version = N, produces = [Event1, Event2]`
pub struct CommandArgs {
    pub aggregate: Ident,
    pub version: u32,
    pub produces: Vec<Ident>,
}

impl Parse for CommandArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let aggregate: Ident = input.parse()?;
        let mut version = 1u32;
        let mut produces = Vec::new();

        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if key == "version" {
                input.parse::<Token![=]>()?;
                let lit: syn::LitInt = input.parse()?;
                version = lit.base10_parse()?;
            } else if key == "produces" {
                input.parse::<Token![=]>()?;
                let content;
                syn::bracketed!(content in input);
                let items: Punctuated<Ident, Token![,]> =
                    content.parse_terminated(Ident::parse, Token![,])?;
                produces = items.into_iter().collect();
            }
        }

        Ok(CommandArgs { aggregate, version, produces })
    }
}

/// Arguments: `snapshot_every = N`
pub struct AggregateArgs {
    pub snapshot_every: Option<u64>,
}

impl Parse for AggregateArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut snapshot_every = None;

        if !input.is_empty() {
            let key: Ident = input.parse()?;
            if key == "snapshot_every" {
                input.parse::<Token![=]>()?;
                let lit: syn::LitInt = input.parse()?;
                snapshot_every = Some(lit.base10_parse()?);
            }
        }

        Ok(AggregateArgs { snapshot_every })
    }
}

/// Arguments for #[event_handler]: optional `window_ttl = "30m"`
pub struct EventHandlerArgs {
    pub window_ttl: Option<String>,
}

impl Parse for EventHandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut window_ttl = None;

        if !input.is_empty() {
            let key: Ident = input.parse()?;
            if key == "window_ttl" {
                input.parse::<Token![=]>()?;
                let lit: syn::LitStr = input.parse()?;
                window_ttl = Some(lit.value());
            }
        }

        Ok(EventHandlerArgs { window_ttl })
    }
}

/// Parse a duration string like "30m", "1h", "90s" into seconds.
pub fn parse_duration_to_secs(s: &str) -> syn::Result<u64> {
    let s = s.trim();
    if let Some(mins) = s.strip_suffix('m') {
        let n: u64 = mins.parse().map_err(|_| {
            syn::Error::new(proc_macro2::Span::call_site(), format!("invalid duration: {s}"))
        })?;
        Ok(n * 60)
    } else if let Some(hours) = s.strip_suffix('h') {
        let n: u64 = hours.parse().map_err(|_| {
            syn::Error::new(proc_macro2::Span::call_site(), format!("invalid duration: {s}"))
        })?;
        Ok(n * 3600)
    } else if let Some(secs) = s.strip_suffix('s') {
        let n: u64 = secs.parse().map_err(|_| {
            syn::Error::new(proc_macro2::Span::call_site(), format!("invalid duration: {s}"))
        })?;
        Ok(n)
    } else {
        Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("invalid duration format '{s}': expected suffix m, h, or s"),
        ))
    }
}

/// Arguments for `#[handles(EventType, version = N)]`
pub struct HandlesArgs {
    pub event_type: Ident,
    pub version: u32,
}

impl Parse for HandlesArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let event_type: Ident = input.parse()?;
        let mut version = 1u32;

        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if key == "version" {
                input.parse::<Token![=]>()?;
                let lit: syn::LitInt = input.parse()?;
                version = lit.base10_parse()?;
            }
        }

        Ok(HandlesArgs { event_type, version })
    }
}
