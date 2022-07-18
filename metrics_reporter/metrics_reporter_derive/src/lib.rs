use {
    proc_macro::TokenStream,
    quote::quote,
    syn::{parse_macro_input, Attribute, DeriveInput, FieldsNamed, Ident},
};

extern crate proc_macro;

#[proc_macro_derive(
    ReportMetrics,
    attributes(report_log_level, report_name, report_interval, last_report_time)
)]
pub fn report_metrics_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let report_level_attr = find_report_level_attr(&input.attrs);
    let default_log_level = quote! {log::Level::Info};
    let report_level = match report_level_attr {
        Some(level_attr) => &level_attr.tokens,
        None => &default_log_level,
    };

    let report_name = find_report_metrics_name(&input.attrs)
        .expect("Struct must have a report_name attribute to derive ReportMetrics");
    let interval = &find_report_metrics_interval_attr(&input.attrs)
        .expect("Struct must have a report_interval attribute to derive ReportMetrics")
        .tokens;

    let name = &input.ident;
    let fields = match &input.data {
        syn::Data::Struct(data_struct) => match &data_struct.fields {
            syn::Fields::Named(fields) => fields,
            _ => panic!("Can only derive ReportMetrics for named fields"),
        },
        _ => panic!("Can only derive ReportMetrics for structs"),
    };

    let last_report_time_ident = find_last_report_time_field_ident(fields)
        .expect("Must have a field with the last_report_time attribute");

    let metric_fields: Vec<_> = fields
        .named
        .iter()
        .filter(|field| field.ident.as_ref() != Some(&last_report_time_ident))
        .collect();

    let metric_field_names: Vec<_> = metric_fields
        .iter()
        .map(|field| field.ident.as_ref().expect("Field must have a name"))
        .collect();
    let metric_field_name_strs: Vec<_> = metric_field_names
        .iter()
        .map(|name| name.to_string())
        .collect();

    let metric_field_types: Vec<_> = metric_fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(type_path) => {
                let last_segment = type_path
                    .path
                    .segments
                    .last()
                    .expect("type should have segments");
                last_segment.ident.clone()
            }
            unexpected_field_type => panic!("Unexpected field type: {unexpected_field_type:?}"),
        })
        .collect();

    let field_loaders: Vec<_> = metric_field_types
        .iter()
        .zip(metric_field_names.iter())
        .map(|(field_type, name)| match field_type.to_string().as_str() {
            "i64" | "i32" | "i16" | "i8" | "usize" | "u64" | "u32" | "u16" | "u8" | "f64"
            | "f32" | "bool" => return quote! { let #name = self.#name; },
            "AtomicI64" | "AtomicI32" | "AtomicI16" | "AtomicI8" | "AtomicUSize" | "AtomicU64"
            | "AtomicU32" | "AtomicU16" | "AtomicU8" | "AtomicF32" | "AtomicF64" | "AtomicBool" => {
                return quote! { let #name = self.#name.load(std::sync::atomic::Ordering::Relaxed); }
            }
            unexpected_type => panic!("Unexpected type: {unexpected_type}"),
        })
        .collect();
    let mapped_types: Vec<_> = metric_field_types
        .iter()
        .map(|field_type| match field_type.to_string().as_str() {
            "i64" | "i32" | "i16" | "i8" | "usize" | "u64" | "u32" | "u16" | "u8" | "AtomicI64"
            | "AtomicI32" | "AtomicI16" | "AtomicI8" | "AtomicUSize" | "AtomicU64"
            | "AtomicU32" | "AtomicU16" | "AtomicU8" => quote! {i64},
            "f64" | "f32" | "AtomicF64" | "AtomicF32" => quote! {f64},
            "bool" | "AtomicBool" => quote! {bool},
            unexpected_type => panic!("Unexpected type: {unexpected_type}"),
        })
        .collect();

    let expanded = quote! {
        impl ReportMetrics for #name {
            fn report(&mut self) {
                if self.#last_report_time_ident.elapsed() >= #interval {
                    // Store as temporaries - this takes care of loading from atomics
                    #(
                        #field_loaders
                    )*

                    // Publish datapoints
                    datapoint!(
                        #report_level,
                        &#report_name,
                        #(
                            (&#metric_field_name_strs, #metric_field_names as #mapped_types, #mapped_types),
                        )*
                    );

                    // Reset all metrics fields to default state (should be 0)
                    #(
                        self.#metric_field_names = Default::default();
                    )*
                    // Reset the last reported time to now
                    self.#last_report_time_ident = std::time::Instant::now();
                }
            }
        }
    };

    TokenStream::from(expanded)
}

fn find_report_level_attr(attrs: &Vec<Attribute>) -> Option<&Attribute> {
    attrs.iter().find(|a| a.path.is_ident("report_level"))
}

fn find_report_metrics_name(attrs: &Vec<Attribute>) -> Option<String> {
    attrs
        .iter()
        .find(|a| a.path.is_ident("report_name"))
        .map(|a| {
            let literal: syn::LitStr = a
                .parse_args()
                .expect("\"report_name\" value should be a str");
            literal.value()
        })
}

fn find_report_metrics_interval_attr(attrs: &Vec<Attribute>) -> Option<&Attribute> {
    attrs.iter().find(|a| a.path.is_ident("report_interval"))
}

fn find_last_report_time_field_ident(fields: &FieldsNamed) -> Option<Ident> {
    fields
        .named
        .iter()
        .find(|field| {
            field
                .attrs
                .iter()
                .find(|a| a.path.is_ident("last_report_time"))
                .is_some()
        })
        .map(|field| {
            match &field.ty {
                syn::Type::Path(p) => {
                    assert!(&p.path.segments.last().unwrap().ident.to_string() == "Instant")
                }
                _ => panic!("Type of last_report_time field must be Instant"),
            }
            field
                .ident
                .to_owned()
                .expect("last_report_time must have an ident")
        })
}
