// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//! The model of Apache Kafka's JSON message descriptors.
//!
//! `tansu-sans-io`'s build script reads every descriptor into a [`Message`]
//! and generates the protocol types from it; [`MessageMeta`] and [`FieldMeta`]
//! are what the generated code keeps of them at run time.

pub mod error;
pub mod wv;

use std::fmt;
use std::str::FromStr;

use convert_case::{Case, Casing as _};
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use regex::Regex;
use serde_json::Value;
use syn::{Expr, Type};
use tracing::debug;

pub use self::error::Error;
use self::wv::{As as _, AsOption as _, Wv};

/// `$e`'s name behind this crate's, as the generated code spells a path.
macro_rules! prefix_crate {
    ($e:ident) => {{
        let krate = env!("CARGO_CRATE_NAME");
        format!("{krate}::{}", stringify!($e))
    }};
}

/// The `Debug` of `$e` behind this crate's name, and `$f`'s behind `$e` when
/// `$e` names the enum `$f` is a variant of.
macro_rules! with_crate {
    ($e:expr_2021) => {{
        let krate = env!("CARGO_CRATE_NAME");
        format!("{krate}::{:?}", $e)
    }};

    ($e:expr_2021, $f:expr_2021) => {{
        let krate = env!("CARGO_CRATE_NAME");
        format!("{krate}::{}::{:?}", $e, $f)
    }};
}

/// The expression `spelled`, as tokens.
///
/// # Panics
///
/// When `spelled` is not an expression, which only a bug in this crate's own
/// `Debug` output could cause.
fn expression(spelled: &str) -> Expr {
    let parsed = syn::parse_str::<Expr>(spelled);
    parsed.unwrap_or_else(|_| panic!("an expression: {spelled}"))
}

/// Every value of `values`, parsed by `parse`; the first failure, if any.
fn parsed_all<T>(values: &[Value], parse: impl Fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    values.iter().map(parse).collect()
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn type_mapping(kind: &str) -> String {
    if let Some(element) = kind.strip_prefix("[]") {
        return type_mapping(element);
    }
    let rust = match kind {
        "bytes" => "bytes::Bytes",
        "float64" => "f64",
        "int8" => "i8",
        "int16" => "i16",
        "int32" => "i32",
        "int64" => "i64",
        "uint16" => "u16",
        "string" => "String",
        "uuid" => "[u8; 16]",
        "records" => "crate::RecordBatch",
        named => named,
    };
    rust.to_owned()
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The Kafka field kind (type).
pub struct Kind(String);

impl ToTokens for Kind {
    #![allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let spelled = with_crate!(self);
        expression(&spelled).to_tokens(tokens);
    }
}

impl From<Type> for Kind {
    fn from(value: Type) -> Self {
        let spelled = value.to_token_stream().to_string();
        Kind(spelled)
    }
}

/// The kinds the protocol defines itself, as opposed to the structures a
/// message declares.
const PRIMITIVE: &[&str] = &[
    "bool", "int8", "int16", "int32", "int64", "uint16", "float64", "string", "bytes", "uuid",
    "records",
];

impl Kind {
    #[must_use]
    pub fn new(kind: &str) -> Self {
        Kind(String::from(kind))
    }

    #[must_use]
    pub fn name(&self) -> &str {
        let Kind(spelled) = self;
        spelled.as_str()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    /// The Rust type name of this kind.
    pub fn type_name(&self) -> Type {
        let mapped = type_mapping(self.name());
        let parsed = syn::parse_str::<Type>(&mapped);
        parsed.unwrap_or_else(|_| panic!("not a type: {self:?}"))
    }

    #[must_use]
    /// Returns true if this kind is a sequence (array)
    pub fn is_sequence(&self) -> bool {
        self.kind_of_sequence().is_some()
    }

    #[must_use]
    /// Whether this kind is one the protocol defines itself.
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.name())
    }

    #[must_use]
    /// Whether this kind is a `float64`.
    pub fn is_float(&self) -> bool {
        self.name() == "float64"
    }

    #[must_use]
    /// Whether this kind is a sequence of a kind the protocol defines itself.
    pub fn is_sequence_of_primitive(&self) -> bool {
        let element = self.kind_of_sequence();
        element.is_some_and(|element| element.is_primitive())
    }

    #[must_use]
    /// The kind of each element, when this kind is a sequence.
    pub fn kind_of_sequence(&self) -> Option<Self> {
        let element = self.name().strip_prefix("[]")?;
        Some(Kind::new(element))
    }
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Kind::new(s))
    }
}

impl TryFrom<&Value> for Kind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let spelled = as_str(value, "type")?;
        spelled.parse()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The listener for this message type.
pub enum Listener {
    ZkBroker,
    #[default]
    Broker,
    Controller,
}

impl TryFrom<&Value> for Listener {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let Some(spelled) = value.as_str() else {
            let why = "expecting string for listener";
            return Err(Error::Message(why.to_owned()));
        };
        spelled.parse()
    }
}

impl FromStr for Listener {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let listener = match s {
            "zkBroker" => Listener::ZkBroker,
            "broker" => Listener::Broker,
            "controller" => Listener::Controller,
            unknown => return Err(Error::Message(unknown.to_owned())),
        };
        Ok(listener)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Model request and response types only
pub enum MessageKind {
    #[default]
    Request,
    Response,
}

impl ToTokens for MessageKind {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let spelled = with_crate!("MessageKind", self);
        expression(&spelled).to_tokens(tokens);
    }
}

impl TryFrom<&Value> for MessageKind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let spelled = as_str(value, "type")?;
        spelled.parse()
    }
}

impl FromStr for MessageKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "request" => MessageKind::Request,
            "response" => MessageKind::Response,
            unknown => return Err(Error::Message(unknown.to_owned())),
        };
        Ok(kind)
    }
}

#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// A range of versions.
pub struct VersionRange {
    pub start: i16,
    pub end: i16,
}

impl fmt::Debug for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let VersionRange { start, end } = self;
        let name = prefix_crate!(VersionRange);
        let mut range = f.debug_struct(&name);
        range.field("start", start).field("end", end).finish()
    }
}

impl VersionRange {
    #[must_use]
    pub fn within(&self, version: i16) -> bool {
        (self.start..=self.end).contains(&version)
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        let Some(VersionRange { start, end }) = parent else {
            return self.start == 0 && self.end == i16::MAX;
        };
        let ends_with_parent = self.end == end;
        if start > self.start {
            ends_with_parent
        } else {
            self.start == start && ends_with_parent
        }
    }
}

impl ToTokens for VersionRange {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let spelled = format!("{self:?}");
        expression(&spelled).to_tokens(tokens);
    }
}

impl FromStr for VersionRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        /// `N`, `N-M` or `N+`.
        static RANGE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
            let pattern = r"^(?<start>\d+)(-(?<end>\d+)|(?<infinite>\+))?$";
            Regex::new(pattern).unwrap_or_else(|_| panic!("regex"))
        });

        if s == "none" {
            return Ok(VersionRange { start: -2, end: -1 });
        }
        let Some(captures) = RANGE.captures(s) else {
            let why = format!("invalid version range format: {s}");
            return Err(Error::Message(why));
        };

        let start = match captures.name("start") {
            Some(given) => given.as_str().parse()?,
            None => 0,
        };
        let end = match (captures.name("end"), captures.name("infinite")) {
            (Some(given), _) => given.as_str().parse()?,
            (None, Some(_)) => i16::MAX,
            (None, None) => start,
        };
        Ok(VersionRange { start, end })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The validity, deprecation and flexible version ranges of a Kafka API message.
pub struct Version {
    /// The valid version ranges of this Kafka message.
    pub valid: VersionRange,
    /// The deprecated version range of this Kafka message.
    pub deprecated: Option<VersionRange>,
    /// The range of versions where this message uses flexible encoding.
    pub flexible: VersionRange,
}

impl ToTokens for Version {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let spelled = with_crate!(self);
        expression(&spelled).to_tokens(tokens);
    }
}

impl Version {
    #[must_use]
    pub fn valid(&self) -> VersionRange {
        let Version { valid, .. } = self;
        *valid
    }

    #[must_use]
    pub fn deprecated(&self) -> Option<VersionRange> {
        let Version { deprecated, .. } = self;
        *deprecated
    }

    #[must_use]
    pub fn flexible(&self) -> VersionRange {
        let Version { flexible, .. } = self;
        *flexible
    }
}

impl<'a> TryFrom<&Wv<'a>> for Version {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        debug!("value: {value:?}");
        let valid = value.as_a("validVersions")?;
        let deprecated = value.as_option("deprecatedVersions")?;
        let flexible = value.as_a("flexibleVersions")?;
        Ok(Version {
            valid,
            deprecated,
            flexible,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka message field schema
pub struct Field {
    /// The name of this field.
    name: String,
    /// The Kafka type of this field.
    kind: Kind,
    /// A comment about this field.
    about: Option<String>,
    /// The version range for this field.
    versions: VersionRange,
    /// Whether this field can be used as the key in a map.
    map_key: Option<bool>,
    /// Whether this field can be null.
    nullable: Option<VersionRange>,
    /// The tag ID for this field
    tag: Option<u32>,
    /// The version range in which this field can be tagged.
    tagged: Option<VersionRange>,
    /// The entity type of this field.
    entity_type: Option<String>,
    /// The default value of this field.
    default: Option<String>,
    /// Any fields this field contains.
    fields: Option<Vec<Field>>,
}

impl Field {
    #[must_use]
    pub fn ident(&self) -> Ident {
        let snake = self.name.to_case(Case::Snake);
        let here = Span::call_site();
        if is_reserved_keyword(&snake) {
            Ident::new_raw(&snake, here)
        } else {
            Ident::new(&snake, here)
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        let Field { name, .. } = self;
        name.as_str()
    }

    #[must_use]
    pub fn kind(&self) -> &Kind {
        let Field { kind, .. } = self;
        kind
    }

    #[must_use]
    pub fn fields(&self) -> Option<&[Field]> {
        let Field { fields, .. } = self;
        fields.as_deref()
    }

    #[must_use]
    pub fn versions(&self) -> VersionRange {
        let Field { versions, .. } = self;
        *versions
    }

    #[must_use]
    pub fn nullable(&self) -> Option<VersionRange> {
        let Field { nullable, .. } = self;
        *nullable
    }

    #[must_use]
    pub fn tagged(&self) -> Option<VersionRange> {
        let Field { tagged, .. } = self;
        *tagged
    }

    #[must_use]
    pub fn tag(&self) -> Option<u32> {
        let Field { tag, .. } = self;
        *tag
    }

    #[must_use]
    pub fn about(&self) -> Option<&str> {
        let Field { about, .. } = self;
        about.as_deref()
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        self.tagged().is_some()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        let nested = self.fields().unwrap_or_default();
        self.kind.name() == "records" || nested.iter().any(Field::has_records)
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        let nested = self.fields().unwrap_or_default();
        self.kind.is_float() || nested.iter().any(Field::has_float)
    }
}

impl<'a> TryFrom<&Wv<'a>> for Field {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let mut field = Field {
            name: value.as_a("name")?,
            kind: value.as_a("type")?,
            about: value.as_option("about")?,
            versions: value.as_a("versions")?,
            map_key: value.as_option("mapKey")?,
            nullable: value.as_option("nullableVersions")?,
            tag: value.as_option("tag")?,
            tagged: value.as_option("taggedVersions")?,
            entity_type: value.as_option("entityType")?,
            default: value.as_option("default")?,
            fields: None,
        };
        let nested: Option<&[Value]> = value.as_option("fields")?;
        if let Some(values) = nested {
            let parse = |nested: &Value| Field::try_from(&Wv::from(nested));
            field.fields = Some(parsed_all(values, parse)?);
        }
        Ok(field)
    }
}

fn is_reserved_keyword(s: &str) -> bool {
    /// Rust's strict keywords, the ones reserved for later, and those the 2018
    /// edition added to both: a field named after one becomes a raw identifier.
    const RESERVED: &[&str] = &[
        "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum",
        "extern", "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move",
        "mut", "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait",
        "true", "type", "unsafe", "use", "where", "while", "abstract", "become", "box", "do",
        "final", "macro", "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
    ];
    RESERVED.contains(&s)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Message {
    api_key: i16,
    kind: MessageKind,
    listeners: Option<Vec<Listener>>,
    name: String,
    versions: Version,
    fields: Vec<Field>,
    common_structs: Option<Vec<CommonStruct>>,
}

impl Message {
    #[must_use]
    pub fn api_key(&self) -> i16 {
        let Message { api_key, .. } = self;
        *api_key
    }

    #[must_use]
    pub fn kind(&self) -> MessageKind {
        let Message { kind, .. } = self;
        *kind
    }

    #[must_use]
    pub fn name(&self) -> &str {
        let Message { name, .. } = self;
        name.as_str()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc, clippy::unwrap_used)]
    pub fn type_name(&self) -> Type {
        let parsed = syn::parse_str::<Type>(self.name());
        parsed.unwrap()
    }

    #[must_use]
    pub fn version(&self) -> Version {
        let Message { versions, .. } = self;
        *versions
    }

    #[must_use]
    pub fn listeners(&self) -> Option<&[Listener]> {
        let Message { listeners, .. } = self;
        listeners.as_deref()
    }

    #[must_use]
    pub fn fields(&self) -> &[Field] {
        let Message { fields, .. } = self;
        fields.as_slice()
    }

    #[must_use]
    pub fn common_structs(&self) -> Option<&[CommonStruct]> {
        let Message { common_structs, .. } = self;
        common_structs.as_deref()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        let fields = self.fields();
        fields.iter().any(Field::has_records)
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        let fields = self.fields();
        fields.iter().any(Field::has_tags)
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        let floating = |field: &Field| field.kind().is_float();
        let shared = self.common_structs().unwrap_or_default();
        self.fields.iter().any(floating) || shared.iter().any(CommonStruct::has_float)
    }
}

impl<'a> TryFrom<&Wv<'a>> for Message {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let api_key = value.as_a("apiKey")?;
        let name = value.as_a("name")?;
        let kind = value.as_a("type")?;
        let listed: Option<&[Value]> = value.as_option("listeners")?;
        let listeners = match listed {
            Some(values) => Some(parsed_all(values, |listener| Listener::try_from(listener))?),
            None => None,
        };
        let declared: &[Value] = value.as_a("fields")?;
        let fields = parsed_all(declared, |field| Field::try_from(&Wv::from(field)))?;
        let versions = Version::try_from(value)?;
        let shared: Option<&[Value]> = value.as_option("commonStructs")?;
        let common_structs = match shared {
            Some(values) => {
                let parse = |value: &Value| CommonStruct::try_from(&Wv::from(value));
                Some(parsed_all(values, parse)?)
            }
            None => None,
        };

        Ok(Message {
            api_key,
            kind,
            listeners,
            name,
            versions,
            fields,
            common_structs,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommonStruct {
    name: String,
    fields: Vec<Field>,
}

impl CommonStruct {
    #[must_use]
    pub fn name(&self) -> &str {
        let CommonStruct { name, .. } = self;
        name
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn type_name(&self) -> Type {
        let parsed = syn::parse_str::<Type>(self.name());
        parsed.unwrap_or_else(|_| panic!("not a type: {self:?}"))
    }

    #[must_use]
    pub fn fields(&self) -> &Vec<Field> {
        let CommonStruct { fields, .. } = self;
        fields
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        let floating = |field: &Field| field.kind().is_float();
        self.fields().iter().any(floating)
    }
}

impl<'a> TryFrom<&Wv<'a>> for CommonStruct {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let name = value.as_a("name")?;
        let declared: &[Value] = value.as_a("fields")?;
        let fields = parsed_all(declared, |field| Field::try_from(&Wv::from(field)))?;
        Ok(CommonStruct { name, fields })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message metadata.
pub struct MessageMeta {
    /// The name of the Kafka API message.
    pub name: &'static str,
    /// The API key used by this message.
    pub api_key: i16,
    /// The version ranges for this message.
    pub version: Version,
    /// The message kind of this message.
    pub message_kind: MessageKind,
    /// The fields that this message describes.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

impl MessageMeta {
    #[must_use]
    pub fn is_flexible(&self, version: i16) -> bool {
        let flexible = self.version.flexible();
        flexible.within(version)
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        let mut found = Vec::new();
        for (name, meta) in self.fields.iter().filter(|(_, meta)| meta.is_structure()) {
            debug!(name = self.name, field = ?name, kind = ?meta.kind.name());
            found.extend(meta.structures());
            match meta.kind.kind_of_sequence() {
                Some(element) if element.is_primitive() => {}
                Some(element) => found.push((element.name(), *meta)),
                None => found.push((meta.kind.name(), *meta)),
            }
        }
        found
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        let (_, meta) = self.fields.iter().find(|(named, _)| *named == name)?;
        Some(*meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message field metadata
pub struct FieldMeta {
    /// The version range of this field.
    pub version: VersionRange,
    /// The version range where this field may be null.
    pub nullable: Option<VersionRange>,
    /// The kind (type) metadata of this field.
    pub kind: KindMeta,
    /// When present the tag ID of this field.
    pub tag: Option<u32>,
    /// The range of versions where this field is tagged.
    pub tagged: Option<VersionRange>,
    /// The fields contained within this structure.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

impl FieldMeta {
    #[must_use]
    pub fn is_nullable(&self, version: i16) -> bool {
        let nullable = self.nullable;
        nullable.is_some_and(|range| range.within(version))
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        let version = self.version;
        version.is_mandatory(parent)
    }

    #[must_use]
    pub fn is_structure(&self) -> bool {
        let element = self.kind.kind_of_sequence();
        element.is_some_and(|element| !element.is_primitive()) || !self.fields.is_empty()
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        let mut found = Vec::new();
        for (_, meta) in self.fields.iter().filter(|(_, meta)| meta.is_structure()) {
            found.extend(meta.structures());
            let element = meta.kind.kind_of_sequence();
            if let Some(element) = element.filter(|element| !element.is_primitive()) {
                found.push((element.name(), *meta));
            }
        }
        found
    }

    pub fn field(&self, name: &str) -> Option<&FieldMeta> {
        let (_, meta) = self.fields.iter().find(|(named, _)| *named == name)?;
        Some(*meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KindMeta(pub &'static str);

impl KindMeta {
    #[must_use]
    pub fn name(&self) -> &'static str {
        let KindMeta(spelled) = *self;
        spelled
    }

    #[must_use]
    pub fn is_sequence(&self) -> bool {
        self.kind_of_sequence().is_some()
    }

    #[must_use]
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.name())
    }

    #[must_use]
    pub fn is_string(&self) -> bool {
        self.name() == "string"
    }

    #[must_use]
    pub fn is_records(&self) -> bool {
        self.name() == "records"
    }

    #[must_use]
    pub fn kind_of_sequence(&self) -> Option<Self> {
        let element = self.name().strip_prefix("[]")?;
        Some(KindMeta(element))
    }
}

fn as_str<'v>(value: &'v Value, name: &str) -> Result<&'v str> {
    let Some(spelled) = value[name].as_str() else {
        return Err(Error::Message(name.to_owned()));
    };
    Ok(spelled)
}

#[cfg(test)]
mod tests {
    use std::any::type_name_of_val;
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    /// Every kind the protocol defines that is not `records`.
    const PRIMITIVES: [&str; 10] = [
        "bool", "int8", "int16", "int32", "int64", "uint16", "float64", "string", "bytes", "uuid",
    ];

    /// A message header descriptor, which this crate does not model itself.
    #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct Header {
        name: String,
        valid: VersionRange,
        flexible: VersionRange,
        fields: Vec<Field>,
    }

    impl<'a> TryFrom<&Wv<'a>> for Header {
        type Error = Error;

        fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
            let declared: &[Value] = value.as_a("fields")?;
            let fields = parsed_all(declared, |field| Field::try_from(&Wv::from(field)))?;
            Ok(Header {
                name: value.as_a("name")?,
                valid: value.as_a("validVersions")?,
                flexible: value.as_a("flexibleVersions")?,
                fields,
            })
        }
    }

    /// A field called `name` of `kind` over `versions`, documented as `about`,
    /// with nothing else set.
    fn field(name: &str, kind: &str, versions: &str, about: &str) -> Result<Field> {
        Ok(Field {
            name: name.to_owned(),
            kind: Kind::new(kind),
            about: Some(about.to_owned()),
            versions: versions.parse()?,
            ..Field::default()
        })
    }

    /// `descriptor` read as a message.
    fn message(descriptor: &Value) -> Result<Message> {
        Message::try_from(&Wv::from(descriptor))
    }

    /// A range from `start` up to the newest version.
    fn from(start: i16) -> VersionRange {
        VersionRange {
            start,
            end: i16::MAX,
        }
    }

    #[test]
    fn type_mapping_test() {
        let mapped = [
            ("bytes", "bytes::Bytes"),
            ("float64", "f64"),
            ("int8", "i8"),
            ("int16", "i16"),
            ("int32", "i32"),
            ("int64", "i64"),
            ("uint16", "u16"),
            ("string", "String"),
            ("uuid", "[u8; 16]"),
            ("[]int16", "i16"),
            ("[]SomeType", "SomeType"),
            ("SomeType", "SomeType"),
        ];
        for (kafka, rust) in mapped {
            assert_eq!(rust, type_mapping(kafka), "{kafka}");
        }
    }

    #[test]
    fn kind_is_sequence() {
        assert!(
            PRIMITIVES
                .iter()
                .all(|&kind| !Kind::new(kind).is_sequence())
        );
        let sequences = ["[]int16", "[]string", "[]SomeType"];
        assert!(sequences.iter().all(|&kind| Kind::new(kind).is_sequence()));
    }

    #[test]
    fn kind_is_sequence_of_primitive() {
        let of_primitive = |kind: &str| Kind::new(kind).is_sequence_of_primitive();
        assert!(PRIMITIVES.iter().all(|&kind| !of_primitive(kind)));
        assert!(
            PRIMITIVES
                .iter()
                .all(|kind| of_primitive(&format!("[]{kind}")))
        );
        assert!(
            ["[]SomeType", "[]OtherType"]
                .iter()
                .all(|&kind| !of_primitive(kind))
        );
    }

    #[test]
    fn kind_is_primitive() {
        let primitive = |kind: &str| Kind::new(kind).is_primitive();
        assert!(PRIMITIVES.iter().all(|&kind| primitive(kind)));
        assert!(
            PRIMITIVES
                .iter()
                .all(|kind| !primitive(&format!("[]{kind}")))
        );
    }

    #[test]
    fn listener_from_value() -> Result<()> {
        for (spelled, expected) in [
            ("zkBroker", Listener::ZkBroker),
            ("broker", Listener::Broker),
            ("controller", Listener::Controller),
        ] {
            assert_eq!(expected, Listener::try_from(&json!(spelled))?);
        }
        Ok(())
    }

    #[test]
    fn message_kind_from_value() -> Result<()> {
        for (spelled, expected) in [
            ("request", MessageKind::Request),
            ("response", MessageKind::Response),
        ] {
            assert_eq!(expected, MessageKind::try_from(&json!({"type": spelled}))?);
        }
        Ok(())
    }

    #[test]
    fn version_range_from_str() -> Result<()> {
        for (spelled, start, end) in [
            ("none", -2, -1),
            ("3+", 3, i16::MAX),
            ("6-9", 6, 9),
            ("1", 1, 1),
        ] {
            assert_eq!(VersionRange { start, end }, spelled.parse()?, "{spelled}");
        }
        Ok(())
    }

    #[test]
    fn version_range_within() {
        let cases: [(VersionRange, &[i16], &[i16]); 3] = [
            (
                VersionRange { start: 0, end: 0 },
                &[0],
                &[i16::MIN, 1, i16::MAX],
            ),
            (from(3), &[3, i16::MAX], &[i16::MIN, 2]),
            (
                VersionRange { start: 6, end: 9 },
                &[6, 7, 8, 9],
                &[i16::MIN, 5, 10, i16::MAX],
            ),
        ];
        for (range, inside, outside) in cases {
            assert!(
                inside.iter().all(|&version| range.within(version)),
                "{range:?}"
            );
            assert!(
                outside.iter().all(|&version| !range.within(version)),
                "{range:?}"
            );
        }
    }

    #[test]
    fn field_from_value() -> Result<()> {
        let name = Field {
            map_key: Some(true),
            entity_type: Some("topicName".to_owned()),
            ..field("Name", "string", "0+", "The topic name.")?
        };
        let expected = Field {
            fields: Some(vec![name]),
            ..field("Topics", "[]CreatableTopic", "0+", "The topics to create.")?
        };

        let descriptor = json!({
            "name": "Topics", "type": "[]CreatableTopic", "versions": "0+",
            "about": "The topics to create.",
            "fields": [{
                "name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                "entityType": "topicName", "about": "The topic name."
            }]
        });
        assert_eq!(expected, Field::try_from(&Wv::from(&descriptor))?);
        Ok(())
    }

    #[test]
    fn tagged_field_from_value() -> Result<()> {
        let name = Field {
            map_key: Some(true),
            ..field("Name", "string", "3+", "The name of the feature.")?
        };
        let min = field(
            "MinVersion",
            "int16",
            "3+",
            "The minimum supported version for the feature.",
        )?;
        let max = field(
            "MaxVersion",
            "int16",
            "3+",
            "The maximum supported version for the feature.",
        )?;
        let expected = Field {
            tag: Some(0),
            tagged: Some(from(3)),
            fields: Some(vec![name, min, max]),
            ..field(
                "SupportedFeatures",
                "[]SupportedFeatureKey",
                "3+",
                "Features supported by the broker.",
            )?
        };

        let descriptor = json!({
            "name": "SupportedFeatures", "type": "[]SupportedFeatureKey", "ignorable": true,
            "versions": "3+", "tag": 0, "taggedVersions": "3+",
            "about": "Features supported by the broker.",
            "fields": [
                {"name": "Name", "type": "string", "versions": "3+", "mapKey": true,
                 "about": "The name of the feature."},
                {"name": "MinVersion", "type": "int16", "versions": "3+",
                 "about": "The minimum supported version for the feature."},
                {"name": "MaxVersion", "type": "int16", "versions": "3+",
                 "about": "The maximum supported version for the feature."}
            ]
        });
        assert_eq!(expected, Field::try_from(&Wv::from(&descriptor))?);
        Ok(())
    }

    #[test]
    fn untagged_message() -> Result<()> {
        let add_offsets = message(&json!({
            "apiKey": 25, "type": "request", "listeners": ["zkBroker", "broker"],
            "name": "AddOffsetsToTxnRequest", "validVersions": "0-3", "flexibleVersions": "3+",
            "fields": [
                {"name": "TransactionalId", "type": "string", "versions": "0+",
                 "entityType": "transactionalId",
                 "about": "The transactional id corresponding to the transaction."},
                {"name": "ProducerId", "type": "int64", "versions": "0+", "entityType": "producerId",
                 "about": "Current producer id in use by the transactional id."},
                {"name": "ProducerEpoch", "type": "int16", "versions": "0+",
                 "about": "Current epoch associated with the producer id."},
                {"name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
                 "about": "The unique group identifier."}
            ]
        }))?;
        assert_eq!(
            (MessageKind::Request, false),
            (add_offsets.kind(), add_offsets.has_tags())
        );
        Ok(())
    }

    /// The `tag` here is the string `"0"`, as in the descriptor this came from:
    /// it is `taggedVersions` that makes the message tagged.
    #[test]
    fn tagged_message() -> Result<()> {
        let heartbeat = message(&json!({
            "apiKey": 63, "type": "request", "listeners": ["controller"],
            "name": "BrokerHeartbeatRequest", "validVersions": "0-1", "flexibleVersions": "0+",
            "fields": [
                {"name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
                 "about": "The broker ID."},
                {"name": "BrokerEpoch", "type": "int64", "versions": "0+", "default": "-1",
                 "about": "The broker epoch."},
                {"name": "CurrentMetadataOffset", "type": "int64", "versions": "0+",
                 "about": "The highest metadata offset which the broker has reached."},
                {"name": "WantFence", "type": "bool", "versions": "0+",
                 "about": "True if the broker wants to be fenced, false otherwise."},
                {"name": "WantShutDown", "type": "bool", "versions": "0+",
                 "about": "True if the broker wants to be shut down, false otherwise."},
                {"name": "OfflineLogDirs", "type": "[]uuid", "versions": "1+",
                 "taggedVersions": "1+", "tag": "0",
                 "about": "Log directories that failed and went offline."}
            ]
        }))?;
        assert_eq!(
            (MessageKind::Request, true),
            (heartbeat.kind(), heartbeat.has_tags())
        );
        Ok(())
    }

    #[test]
    fn message_from_value() -> Result<()> {
        let topic_name = Field {
            map_key: Some(true),
            entity_type: Some("topicName".to_owned()),
            ..field("Name", "string", "0+", "The name of the topic.")?
        };
        let partitions = field(
            "Partitions",
            "[]int32",
            "0+",
            "The partition indexes to add to the transaction",
        )?;
        let created_name = Field {
            map_key: Some(true),
            entity_type: Some("topicName".to_owned()),
            ..field("Name", "string", "0+", "The topic name.")?
        };
        let topics = Field {
            fields: Some(vec![created_name]),
            ..field("Topics", "[]CreatableTopic", "0+", "The topics to create.")?
        };
        let expected = Message {
            api_key: 19,
            kind: MessageKind::Request,
            listeners: Some(vec![
                Listener::ZkBroker,
                Listener::Broker,
                Listener::Controller,
            ]),
            name: "CreateTopicsRequest".to_owned(),
            versions: Version {
                valid: "0-7".parse()?,
                deprecated: Some("0-1".parse()?),
                flexible: "5+".parse()?,
            },
            fields: vec![topics],
            common_structs: Some(vec![CommonStruct {
                name: "AddPartitionsToTxnTopic".to_owned(),
                fields: vec![topic_name, partitions],
            }]),
        };

        let descriptor = json!({
            "apiKey": 19, "type": "request", "listeners": ["zkBroker", "broker", "controller"],
            "name": "CreateTopicsRequest", "validVersions": "0-7", "deprecatedVersions": "0-1",
            "flexibleVersions": "5+",
            "fields": [{
                "name": "Topics", "type": "[]CreatableTopic", "versions": "0+",
                "about": "The topics to create.",
                "fields": [{
                    "name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                    "entityType": "topicName", "about": "The topic name."
                }]
            }],
            "commonStructs": [{
                "name": "AddPartitionsToTxnTopic", "versions": "0+",
                "fields": [
                    {"name": "Name", "type": "string", "versions": "0+", "mapKey": true,
                     "entityType": "topicName", "about": "The name of the topic."},
                    {"name": "Partitions", "type": "[]int32", "versions": "0+",
                     "about": "The partition indexes to add to the transaction"}
                ]
            }]
        });
        assert_eq!(expected, message(&descriptor)?);
        Ok(())
    }

    #[test]
    fn header_from_value() -> Result<()> {
        let client_id = Field {
            nullable: Some(from(1)),
            ..field("ClientId", "string", "1+", "The client ID string.")?
        };
        let expected = Header {
            name: "RequestHeader".to_owned(),
            valid: VersionRange { start: 0, end: 2 },
            flexible: from(2),
            fields: vec![
                field(
                    "RequestApiKey",
                    "int16",
                    "0+",
                    "The API key of this request.",
                )?,
                field(
                    "RequestApiVersion",
                    "int16",
                    "0+",
                    "The API version of this request.",
                )?,
                field(
                    "CorrelationId",
                    "int32",
                    "0+",
                    "The correlation ID of this request.",
                )?,
                client_id,
            ],
        };

        let descriptor = json!({
            "type": "header", "name": "RequestHeader", "validVersions": "0-2",
            "flexibleVersions": "2+",
            "fields": [
                {"name": "RequestApiKey", "type": "int16", "versions": "0+",
                 "about": "The API key of this request."},
                {"name": "RequestApiVersion", "type": "int16", "versions": "0+",
                 "about": "The API version of this request."},
                {"name": "CorrelationId", "type": "int32", "versions": "0+",
                 "about": "The correlation ID of this request."},
                {"name": "ClientId", "type": "string", "versions": "1+", "nullableVersions": "1+",
                 "ignorable": true, "flexibleVersions": "none", "about": "The client ID string."}
            ]
        });
        assert_eq!(expected, Header::try_from(&Wv::from(&descriptor))?);
        Ok(())
    }

    /// The shape the generator reads a list of `(ident, expression)` pairs in.
    #[test]
    fn parse_expression() -> Result<()> {
        let listed = r#"[(one, "a/b/c"), (abc, 123), (pqr, a::b::c)]"#;
        let refused = |why: &str| Error::Message(why.to_owned());
        let Expr::Array(array) = syn::parse_str::<Expr>(listed)? else {
            return Err(refused("expecting an array"));
        };

        let mut mappings = HashMap::new();
        for element in array.elems {
            let Expr::Tuple(pair) = element else {
                return Err(refused("expecting a tuple"));
            };
            assert_eq!(2, pair.elems.len());
            println!(
                "i: {}, ty: {}",
                pair.to_token_stream(),
                type_name_of_val(&pair)
            );

            let Expr::Path(ref left) = pair.elems[0] else {
                return Err(refused("lhs expecting a path expression"));
            };
            let Some(ident) = left.path.get_ident() else {
                return Err(refused("lhs expecting a path ident expression"));
            };
            let right = pair.elems[1].clone();
            println!("lhs: {}", ident.to_token_stream());
            println!("rhs: {}", right.to_token_stream());
            _ = mappings.insert(ident.clone(), right);
        }

        assert!(mappings.contains_key(&syn::parse_str::<Ident>("one")?));
        Ok(())
    }

    #[test]
    fn find_coordinator_request() -> Result<()> {
        let find = message(&json!({
            "apiKey": 10, "type": "request", "listeners": ["zkBroker", "broker"],
            "name": "FindCoordinatorRequest", "validVersions": "0-4", "deprecatedVersions": "0",
            "flexibleVersions": "3+",
            "fields": [
                {"name": "Key", "type": "string", "versions": "0-3", "about": "The coordinator key."},
                {"name": "KeyType", "type": "int8", "versions": "1+", "default": "0",
                 "ignorable": false, "about": "The coordinator key type. (Group, transaction, etc.)"},
                {"name": "CoordinatorKeys", "type": "[]string", "versions": "4+",
                 "about": "The coordinator keys."}
            ]
        }))?;

        assert!(!find.has_records());
        assert_eq!(MessageKind::Request, find.kind());
        assert_eq!("FindCoordinatorRequest", find.name());
        let version = Version {
            valid: VersionRange { start: 0, end: 4 },
            deprecated: Some(VersionRange { start: 0, end: 0 }),
            flexible: from(3),
        };
        assert_eq!(version, find.version());

        let key = &find.fields()[0];
        assert_eq!("Key", key.name());
        assert_eq!(&Kind::new("string"), key.kind());
        assert_eq!(VersionRange { start: 0, end: 3 }, key.versions());
        assert_eq!(Some("The coordinator key."), key.about());
        Ok(())
    }

    #[test]
    fn fetch_response() -> Result<()> {
        let fetch = message(&json!({
            "apiKey": 1, "type": "response", "name": "FetchResponse", "validVersions": "0-16",
            "flexibleVersions": "12+",
            "fields": [{
                "name": "NodeEndpoints", "type": "[]NodeEndpoint", "versions": "16+",
                "taggedVersions": "16+", "tag": 0,
                "about": "Endpoints for all current-leaders enumerated in PartitionData.",
                "fields": [
                    {"name": "NodeId", "type": "int32", "versions": "16+", "mapKey": true,
                     "entityType": "brokerId", "about": "The ID of the associated node."},
                    {"name": "Host", "type": "string", "versions": "16+",
                     "about": "The node's hostname."},
                    {"name": "Port", "type": "int32", "versions": "16+", "about": "The node's port."},
                    {"name": "Rack", "type": "string", "versions": "16+", "nullableVersions": "16+",
                     "default": "null",
                     "about": "The rack of the node, or null if it has not been assigned to a rack."}
                ]
            }]
        }))?;

        assert_eq!(MessageKind::Response, fetch.kind());
        assert_eq!("FetchResponse", fetch.name());
        let version = Version {
            valid: VersionRange { start: 0, end: 16 },
            deprecated: None,
            flexible: from(12),
        };
        assert_eq!(version, fetch.version());

        let endpoints = &fetch.fields()[0];
        assert_eq!("NodeEndpoints", endpoints.name());
        assert_eq!(&Kind::new("[]NodeEndpoint"), endpoints.kind());
        assert_eq!(from(16), endpoints.versions());

        let node_id = &endpoints.fields().unwrap_or_default()[0];
        assert_eq!("NodeId", node_id.name());
        assert_eq!(&Kind::new("int32"), node_id.kind());
        assert_eq!(from(16), node_id.versions());
        Ok(())
    }
}
