// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use crate::{Error, Result, RootMessageMeta};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{
    Deserializer,
    de::{DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
};
use std::{
    any::{type_name, type_name_of_val},
    collections::VecDeque,
    fmt,
    io::Read,
    str::from_utf8,
};
use tansu_model::{FieldMeta, MessageMeta};
use tracing::{debug, warn};

const MESSAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;

const PARSE_DEPTH: usize = 6;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Container {
    Struct {
        name: &'static str,
        fields: &'static [&'static str],
    },

    Enum {
        name: &'static str,
        variants: &'static [&'static str],
    },
}

impl Container {
    fn name(&self) -> &'static str {
        match self {
            Self::Struct { name, .. } | Self::Enum { name, .. } => name,
        }
    }
}

/// Deserialize the Kafka protocol into the serde data model.
pub struct Decoder<'de> {
    reader: &'de mut dyn Read,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
    length: Option<usize>,
    in_seq_of_primitive: bool,
    path: VecDeque<&'static str>,
    in_records: bool,
    message_max_size: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FieldLookup(&'static [(&'static str, &'static FieldMeta)]);

impl From<&'static [(&'static str, &'static FieldMeta)]> for FieldLookup {
    fn from(value: &'static [(&'static str, &'static FieldMeta)]) -> Self {
        Self(value)
    }
}

impl FieldLookup {
    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.0
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Meta {
    message: Option<&'static MessageMeta>,
    structures: Option<Vec<(&'static str, &'static FieldMeta)>>,
    field: Option<&'static FieldMeta>,
    parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            structures: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}

impl<'de> Decoder<'de> {
    pub fn new(reader: &'de mut dyn Read) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size: None,
        }
    }

    pub(crate) fn request(reader: &'de mut dyn Read) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: Some(Kind::Request),
            api_key: None,
            api_version: None,
            meta: Meta::default(),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size: None,
        }
    }

    pub(crate) fn response(reader: &'de mut dyn Read, api_key: i16, api_version: i16) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: Some(Kind::Response),
            api_key: Some(api_key),
            api_version: Some(api_version),
            meta: RootMessageMeta::messages()
                .responses()
                .get(&api_key)
                .map_or_else(Meta::default, |meta| {
                    let mut parse = VecDeque::with_capacity(PARSE_DEPTH);
                    parse.push_front(meta.fields.into());

                    Meta {
                        message: Some(*meta),
                        structures: Some(meta.structures()),
                        parse,
                        ..Default::default()
                    }
                }),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size: None,
        }
    }

    #[must_use]
    fn field_name(&self) -> String {
        self.path.iter().fold(String::new(), |acc, step| {
            if acc.is_empty() {
                (*step).to_string()
            } else {
                format!("{step}.{acc}")
            }
        })
    }

    fn in_header(&self) -> bool {
        self.containers
            .front()
            .is_some_and(|c| c.name() == "HeaderMezzanine")
    }

    #[must_use]
    fn is_flexible(&self) -> bool {
        if self.in_header()
            && ((self.kind.is_some_and(|kind| kind == Kind::Request)
                && self.field.is_some_and(|field| field == "client_id"))
                || (self.kind.is_some_and(|kind| kind == Kind::Response)
                    && self.api_key.is_some_and(|api_key| api_key == 18)))
        {
            false
        } else {
            self.meta.message.is_some_and(|meta| {
                self.api_version
                    .is_some_and(|api_version| meta.is_flexible(api_version))
            })
        }
    }

    #[must_use]
    fn is_valid(&self) -> bool {
        self.api_version.is_none_or(|api_version| {
            self.meta
                .field
                .is_none_or(|field| field.version.within(api_version))
        })
    }

    fn is_nullable(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.is_nullable(api_version))
        })
    }

    #[must_use]
    fn is_sequence(&self) -> bool {
        self.meta
            .field
            .is_some_and(|field| field.kind.is_sequence())
    }

    #[must_use]
    fn is_structure(&self) -> bool {
        self.meta.field.is_some_and(|field| field.is_structure())
    }

    fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
    }

    #[must_use]
    fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    fn read_mandatory_non_nullable_length(&mut self) -> Result<()> {
        debug!(
            "header mezzanine: {}, nullable: {}, valid: {}",
            self.in_header(),
            self.is_nullable(),
            self.is_valid(),
        );

        if self.in_header() || self.is_nullable() || !self.is_valid() {
            debug!(
                "field: {} is not a mandatory non nullable length",
                self.field_name()
            );
            return Ok(());
        }

        debug!(
            "read_non_nullable_length, field: {}, flexible: {}, string: {}",
            self.field_name(),
            self.is_flexible(),
            self.is_string(),
        );

        if self.is_flexible() {
            self.length = self
                .unsigned_varint()
                .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))
                .and_then(|length| TryInto::try_into(length).map_err(Into::into))
                .map(Some)?;
        } else if self.is_string()
            || (self.in_seq_of_primitive
                && self.meta.field.is_some_and(|field| {
                    field
                        .kind
                        .kind_of_sequence()
                        .is_some_and(|sk| sk.is_string())
                }))
        {
            let mut buf = [0u8; 2];
            self.reader.read_exact(&mut buf)?;

            let length = i16::from_be_bytes(buf);
            debug!("length: {length}");
            self.length = Some(length.try_into()?);
        } else {
            let mut buf = [0u8; 4];
            self.reader.read_exact(&mut buf)?;

            let length = i32::from_be_bytes(buf);
            debug!("length: {length}");
            self.length = Some(length.try_into()?);
        }

        Ok(())
    }

    fn unsigned_varint(&mut self) -> Result<u32> {
        const CONTINUATION: u8 = 0b1000_0000;
        const MASK: u8 = 0b0111_1111;
        let mut shift = 0u8;
        let mut accumulator = 0u32;
        let mut done = false;

        let mut buf = [0u8; 1];

        while !done {
            self.reader.read_exact(&mut buf)?;

            if buf[0] & CONTINUATION == CONTINUATION {
                accumulator = u32::from(buf[0] & MASK)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                shift = shift.checked_add(7).ok_or(Error::Overflow)?;
            } else {
                accumulator = u32::from(buf[0])
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                done = true;
            }
        }

        Ok(accumulator)
    }
}

impl fmt::Debug for Decoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl<'de> Deserializer<'de> for &mut Decoder<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = buf[0] != 0;

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_bool(v)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = i8::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = i16::from_be_bytes(buf);

        match (self.containers.front(), self.field) {
            (
                Some(Container::Struct {
                    name: "HeaderMezzanine",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.structures = Some(meta.structures());
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::Struct {
                    name: "HeaderMezzanine",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        debug!(
            field = self.field_name(),
            value = v,
            type_name = type_name::<V::Value>(),
        );
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = i32::from_be_bytes(buf);

        debug!(
            field = self.field_name(),
            v,
            type_name = type_name::<V::Value>(),
        );
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = i64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = u8::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u8(v)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = u16::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = u32::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = u64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = f32::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_f32(v)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = f64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_f64(v)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.read_mandatory_non_nullable_length()?;

        self.length
            .ok_or(Error::StringWithoutLength)
            .and_then(|length| {
                if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(length));
                }

                let mut buf = vec![0u8; length];
                self.reader.read_exact(&mut buf)?;
                from_utf8(buf.as_slice())
                    .map_err(Into::into)
                    .inspect(|v| debug!("visitor: {}, v: {v}", type_name_of_val(&visitor)))
                    .and_then(|s| visitor.visit_str(s))
            })
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            field = self.field_name(),
            is_nullable = self.meta.field.is_some_and(|field| self
                .api_version
                .is_some_and(|api_version| field.is_nullable(api_version)))
        );

        if self.length.is_none() {
            self.read_mandatory_non_nullable_length()?;
        }

        if let Some(length) = self.length.take() {
            if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                return Err(Error::MessageMaxSizeExceeded(length));
            }

            let mut buf = vec![0u8; length];
            self.reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map_err(Into::into)
                .inspect(|v| {
                    debug!(field = self.field_name(), value = v);
                })
                .and_then(|s| visitor.visit_string(s))
        } else {
            Err(Error::StringWithoutLength)
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }

        let length = if self.is_flexible() {
            self.unsigned_varint()
                .and_then(|length| usize::try_from(length - 1).map_err(Into::into))?
        } else {
            let mut buf = [0u8; 4];

            self.reader.read_exact(&mut buf)?;
            usize::try_from(u32::from_be_bytes(buf))?
        };

        let mut buf = vec![0u8; length];
        self.reader.read_exact(&mut buf)?;
        visitor.visit_bytes(&buf[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }

        let length = if self.is_flexible() {
            self.unsigned_varint()
                .and_then(|length| usize::try_from(length).map_err(Into::into))
                .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))?
        } else {
            let mut buf = [0u8; 4];

            self.reader.read_exact(&mut buf)?;
            usize::try_from(u32::from_be_bytes(buf))?
        };

        if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
            return Err(Error::MessageMaxSizeExceeded(length));
        }

        let mut buf = vec![0u8; length];
        self.reader.read_exact(&mut buf)?;
        visitor.visit_byte_buf(buf)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            field = self.field_name(),
            is_flexible = self.is_flexible(),
            is_valid = self.is_valid(),
            is_string = self.is_string(),
            is_sequence = self.is_sequence(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_records = self.is_records(),
        );

        if self.is_valid() {
            if self.field.is_some_and(|field| field == "tag_buffer") {
                if self.is_flexible() {
                    self.length = None;
                    visitor.visit_some(self)
                } else {
                    visitor.visit_none()
                }
            } else if self.is_records() {
                let length = if self.is_flexible() {
                    self.unsigned_varint()
                        .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))?
                } else {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;

                    u32::from_be_bytes(buf)
                };

                debug!(length);

                if length == 0 {
                    visitor.visit_none()
                } else {
                    self.length = Some(length.try_into()?);
                    self.in_records = true;
                    visitor.visit_some(self)
                }
            } else if self.is_sequence() {
                if self.is_flexible() {
                    self.unsigned_varint().and_then(|length| {
                        if length == 0 {
                            self.length = None;
                            visitor.visit_none()
                        } else {
                            self.length = Some((length - 1).try_into()?);
                            visitor.visit_some(self)
                        }
                    })
                } else {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;

                    let length = i32::from_be_bytes(buf);
                    debug!(length);

                    if length == -1 {
                        self.length = None;
                        visitor.visit_none()
                    } else {
                        self.length = Some(length.try_into()?);
                        visitor.visit_some(self)
                    }
                }
            } else if self.is_string() {
                if self.is_flexible() {
                    self.unsigned_varint().and_then(|length| {
                        if length == 0 {
                            self.length = None;
                            visitor.visit_none()
                        } else {
                            self.length = Some((length - 1).try_into()?);
                            visitor.visit_some(self)
                        }
                    })
                } else {
                    let mut buf = [0u8; 2];
                    self.reader.read_exact(&mut buf)?;

                    let length = i16::from_be_bytes(buf);
                    debug!(length);

                    if length == -1 {
                        self.length = None;
                        visitor.visit_none()
                    } else {
                        self.length = Some(length.try_into()?);
                        visitor.visit_some(self)
                    }
                }
            } else if self.is_nullable() && self.is_structure() {
                let mut buf = [0u8; 1];
                self.reader.read_exact(&mut buf)?;

                if (buf[0] as i8) < 0 {
                    visitor.visit_none()
                } else {
                    self.length = None;
                    visitor.visit_some(self)
                }
            } else {
                self.length = None;
                visitor.visit_some(self)
            }
        } else {
            self.length = None;
            visitor.visit_none()
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            visitor = type_name_of_val(&visitor),
            type_name = type_name::<V::Value>(),
        );
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(name, visitor = type_name_of_val(&visitor));
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(name, visitor = type_name_of_val(&visitor));
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            visitor = type_name::<V>(),
            type_name = type_name::<V::Value>(),
            length = self.length,
            meta_field = self.meta.field.is_some(),
            is_seq_of_primitive = self.meta.field.is_some_and(|field| field
                .kind
                .kind_of_sequence()
                .is_some_and(|seq| seq.is_primitive())),
            is_records = self.is_records(),
        );

        self.in_seq_of_primitive = self.meta.field.is_some_and(|field| {
            field
                .kind
                .kind_of_sequence()
                .is_some_and(|seq| seq.is_primitive())
        });

        match self.length.take() {
            Some(size_in_bytes) if self.in_records => {
                debug!(size_in_bytes);

                if size_in_bytes > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(size_in_bytes));
                }

                let mut buf = vec![0u8; size_in_bytes];
                self.reader.read_exact(&mut buf)?;
                let outcome = visitor.visit_seq(Batch::new(Bytes::from(buf)));
                self.in_seq_of_primitive = false;
                self.in_records = false;
                outcome
            }

            otherwise => {
                let outcome = visitor.visit_seq(Seq::new(self, otherwise));
                self.in_seq_of_primitive = false;
                outcome
            }
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(len, visitor = type_name_of_val(&visitor));
        visitor.visit_seq(Seq::new(self, Some(len)))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(name, len, visitor = type_name_of_val(&visitor));
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(visitor = type_name_of_val(&visitor));
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(r#struct = name, ?fields);

        self.containers
            .push_front(Container::Struct { name, fields });

        let outcome = if let Some(mm) = self.meta.message {
            if let Some((_, fm)) = self
                .meta
                .structures
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|(found, _)| name == *found)
            {
                debug!(r#struct = name);

                _ = self.meta.field.replace(*fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else if name.ends_with("Request") || name.ends_with("Response") {
                self.meta.parse.push_front(mm.fields.into());
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                outcome
            } else {
                if !["Frame", "HeaderMezzanine"].contains(&name) {
                    warn!("deserialize_struct, no field meta for struct, name: {name}");
                }
                _ = self.meta.field.take();
                self.meta.parse.push_front(FieldLookup(&[]));
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                outcome
            }
        } else {
            if !["Frame", "HeaderMezzanine"].contains(&name) {
                debug!("deserialize_struct, no message meta for struct, name: {name}");
            }

            visitor.visit_seq(Struct::new(self, name, fields))
        };

        _ = self.containers.pop_front();

        outcome
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(r#enum = name);

        self.containers
            .push_front(Container::Enum { name, variants });

        let outcome = visitor.visit_enum(Enum::new(self, name));

        _ = self.containers.pop_front();
        outcome
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            front = ?self.containers.front().map(Container::name),
            message_name = self.meta.message.map(|message| message.name)
        );

        visitor.visit_str(match (self.containers.front(), self.meta.message) {
            (
                Some(Container::Enum {
                    name: "HeaderMezzanine",
                    ..
                }),
                _,
            ) if self.kind.is_some_and(|kind| kind == Kind::Request) => Ok("Request"),

            (
                Some(Container::Enum {
                    name: "HeaderMezzanine",
                    ..
                }),
                _,
            ) if self.kind.is_some_and(|kind| kind == Kind::Response) => Ok("Response"),

            (Some(Container::Enum { name: "Body", .. }), Some(meta)) => Ok(meta.name),

            (_, None) => Err(Error::UnknownContainer),

            container => Err(Error::UnexpectedType(format!(
                "{container:?}:{}",
                type_name::<V::Value>()
            ))),
        }?)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }
}

struct Batch {
    encoded: Bytes,
}

impl Batch {
    fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> SeqAccess<'de> for Batch {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            seed = type_name::<T>(),
            value = type_name::<T::Value>(),
            encoded = ?&self.encoded[..]
        );

        if self.encoded.has_remaining() {
            let base_offset = self.encoded.try_get_i64()?;
            let batch_length = self.encoded.try_get_i32()?;
            debug!(base_offset, batch_length);

            let mut batch = BytesMut::with_capacity(batch_length as usize);
            batch.put_i64(base_offset);
            batch.put_i32(batch_length);

            if (batch_length as usize) > self.encoded.len() {
                return Err(Error::Overflow);
            }

            batch.put(self.encoded.split_to(batch_length as usize));

            let decoder = BatchDecoder {
                encoded: batch.freeze(),
            };

            seed.deserialize(decoder).map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct BatchDecoder {
    encoded: Bytes,
}

impl BatchDecoder {
    pub fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> Deserializer<'de> for BatchDecoder {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(&self.encoded[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(Vec::from(self.encoded))
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_tuple<V>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{fields:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{variants:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }
}

#[derive(Debug)]
struct Seq<'de, 'a> {
    de: &'a mut Decoder<'de>,
    length: Option<usize>,
}

impl<'de, 'a> Seq<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, length: Option<usize>) -> Self {
        Self { de, length }
    }
}

impl<'de> SeqAccess<'de> for Seq<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            "seq, next seed: {}, length: {:?}",
            type_name_of_val(&seed),
            self.length
        );

        match self.length {
            Some(0) => Ok(None),

            Some(length) => {
                _ = self.length.replace(length - 1);
                seed.deserialize(&mut *self.de).map(Some)
            }

            None => seed.deserialize(&mut *self.de).map(Some),
        }
    }
}

#[derive(Debug)]
struct Struct<'de, 'a> {
    de: &'a mut Decoder<'de>,
    name: &'static str,
    fields: &'static [&'static str],
    index: usize,
}

impl<'de, 'a> Struct<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str, fields: &'static [&'static str]) -> Self {
        Self {
            de,
            name,
            fields,
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for Struct<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let field = self.fields[self.index];
        self.de.field = Some(field);
        self.index += 1;

        if let Some(fl) = self.de.meta.parse.front() {
            self.de.meta.field = fl.field(field);
        }

        debug!(
            struct = self.name,
            field,
            is_flexible = self.de.is_flexible(),
            type_name = type_name::<T::Value>(),
            meta_field = self.de.meta.field.is_some(),
            is_records = self.de.is_records(),
        );

        self.de.path.push_front(field);
        let outcome = seed.deserialize(&mut *self.de).map(Some);
        _ = self.de.path.pop_front();
        outcome
    }
}

#[derive(Debug)]
struct Enum<'de, 'a> {
    de: &'a mut Decoder<'de>,
    name: &'static str,
}

impl<'de, 'a> Enum<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str) -> Self {
        Self { de, name }
    }
}

impl<'de> EnumAccess<'de> for Enum<'de, '_> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}

impl<'de> VariantAccess<'de> for Enum<'de, '_> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Err(Error::Message(String::from("expecting string")))
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(name = self.name, seed = type_name_of_val(&seed));
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            name = self.name,
            ?fields,
            visitor = type_name_of_val(&visitor)
        );
        Deserializer::deserialize_struct(self.de, self.name, fields, visitor)
    }
}

#[cfg(test)]
mod accepted_forms {
    //! What the two deserializers in this file accept, and what they refuse
    //! (#556).
    //!
    //! Nothing on the wire says what a value is. The frame's API key and
    //! version name a schema, and everything after that is position and width
    //! — so a form outside the set a decoder reads cannot be guessed at from
    //! the bytes. Refusing is the only answer that does not fabricate a value,
    //! which is the same argument #351 makes for the encoder.

    use super::*;
    use bytes::Bytes;
    use serde::{Deserialize, de::DeserializeOwned};
    use std::{collections::BTreeMap, fmt::Debug, io::Cursor};

    /// Reads a value with no message metadata in play, which is the decoder as
    /// [`Decoder::new`] hands it out: no API key, no version, so every field is
    /// valid, nothing is nullable and nothing is flexible.
    fn decoded<T>(encoded: &[u8]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut reader = Cursor::new(encoded);
        let mut decoder = Decoder::new(&mut reader);
        T::deserialize(&mut decoder)
    }

    /// Reads a value out of a record batch's bytes.
    fn from_batch<T>(encoded: &[u8]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        T::deserialize(BatchDecoder::new(Bytes::copy_from_slice(encoded)))
    }

    fn assert_unexpected_type(error: Error) {
        assert!(
            matches!(error, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {error:?}"
        );
    }

    /// A visitor that answers nothing, for the arms that refuse before they
    /// reach one.
    struct Nothing;

    impl<'de> Visitor<'de> for Nothing {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("nothing")
        }
    }

    /// A value asked for by a method a generated type never asks for.
    ///
    /// Each of these exists because `serde`'s own impls route elsewhere: `&str`
    /// refuses a transient string, `Bytes` asks for a `byte_buf`, and there is
    /// no `Deserialize` at all that calls `deserialize_identifier` outside a
    /// derived enum.
    macro_rules! asks_for {
        ($name:ident, $method:ident) => {
            #[derive(Debug)]
            struct $name;

            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    deserializer.$method(Nothing).map(|()| Self)
                }
            }
        };
    }

    asks_for!(Any, deserialize_any);
    asks_for!(Identifier, deserialize_identifier);
    asks_for!(IgnoredAny, deserialize_ignored_any);

    /// A visitor that accepts a byte array, for the two arms that hand one
    /// over.
    struct Octets;

    impl<'de> Visitor<'de> for Octets {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a byte array")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }
    }

    /// A byte array asked for by the borrowed arm, which `Bytes` does not use:
    /// it needs an owned value, so it asks for a `byte_buf`.
    #[derive(Debug)]
    struct AsBytes(Vec<u8>);

    impl<'de> Deserialize<'de> for AsBytes {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_bytes(Octets).map(AsBytes)
        }
    }

    /// A byte array asked for by the owned arm.
    #[derive(Debug)]
    struct AsByteBuf(Vec<u8>);

    impl<'de> Deserialize<'de> for AsByteBuf {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_byte_buf(Octets).map(AsByteBuf)
        }
    }

    /// A string asked for by the borrowed arm.
    #[derive(Debug)]
    struct Str(String);

    impl<'de> Deserialize<'de> for Str {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct V;

            impl<'de> Visitor<'de> for V {
                type Value = String;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a string")
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(v.to_owned())
                }
            }

            deserializer.deserialize_str(V).map(Str)
        }
    }

    /// Every fixed-width number is big-endian.
    ///
    /// The unsigned widths and the floats are answered for even though the
    /// generated types do not use them: Kafka's schema has `int8` through
    /// `int64`, `uint16`, `uint32` and `float64`, and no `uint64` or `float32`
    /// at all.
    #[test]
    fn every_fixed_width_number_is_big_endian() -> Result<()> {
        assert!(decoded::<bool>(&[1])?);
        assert!(!decoded::<bool>(&[0])?);
        assert_eq!(-1i8, decoded::<i8>(&[255])?);
        assert_eq!(-2i16, decoded::<i16>(&[255, 254])?);
        assert_eq!(-3i32, decoded::<i32>(&[255, 255, 255, 253])?);
        assert_eq!(
            -4i64,
            decoded::<i64>(&[255, 255, 255, 255, 255, 255, 255, 252])?
        );
        assert_eq!(7u8, decoded::<u8>(&[7])?);
        assert_eq!(8u16, decoded::<u16>(&[0, 8])?);
        assert_eq!(9u32, decoded::<u32>(&[0, 0, 0, 9])?);
        assert_eq!(10u64, decoded::<u64>(&[0, 0, 0, 0, 0, 0, 0, 10])?);
        assert_eq!(1.5f32, decoded::<f32>(&1.5f32.to_be_bytes())?);
        assert_eq!(2.5f64, decoded::<f64>(&2.5f64.to_be_bytes())?);

        Ok(())
    }

    /// Without a version, a string and a byte array carry a four-byte length:
    /// the non-flexible encoding, which is what a decoder with no message
    /// metadata falls back to.
    #[test]
    fn a_string_and_a_byte_array_carry_a_four_byte_length() -> Result<()> {
        assert_eq!("abc", decoded::<String>(&[0, 0, 0, 3, b'a', b'b', b'c'])?);
        assert_eq!("abc", decoded::<Str>(&[0, 0, 0, 3, b'a', b'b', b'c'])?.0);
        assert_eq!(
            Bytes::from_static(&[1, 2]),
            decoded::<Bytes>(&[0, 0, 0, 2, 1, 2])?
        );
        assert_eq!(vec![1, 2], decoded::<AsBytes>(&[0, 0, 0, 2, 1, 2])?.0);
        assert_eq!(vec![1, 2], decoded::<AsByteBuf>(&[0, 0, 0, 2, 1, 2])?.0);

        Ok(())
    }

    /// A newtype struct is its inner value, with nothing around it.
    ///
    /// Which is what makes the generated wrappers — `Uuid`, the varint types,
    /// the mezzanine newtypes — free on the wire.
    #[test]
    fn a_newtype_struct_is_its_inner_value() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Wrapper(i32);

        assert_eq!(Wrapper(7), decoded::<Wrapper>(&[0, 0, 0, 7])?);

        Ok(())
    }

    /// A length past the maximum message size is refused before it is
    /// allocated.
    ///
    /// The length is four bytes the peer chose, so a twelve-byte frame can ask
    /// for four gigabytes. The guard is what stops `vec![0u8; length]` running
    /// first and the read failing afterwards — which is a `Vec` allocation, not
    /// a read, and so not something the frame size bounds. All three string and
    /// bytes arms carry it: until #556 the borrowed one did not.
    #[test]
    fn a_length_past_the_maximum_message_size_is_refused_before_it_is_allocated() {
        let hostile = [0x40, 0x00, 0x00, 0x01];

        for error in [
            decoded::<String>(&hostile).expect_err("a gigabyte string"),
            decoded::<Str>(&hostile).expect_err("a gigabyte string"),
            decoded::<AsByteBuf>(&hostile).expect_err("a gigabyte byte array"),
        ] {
            assert!(
                matches!(error, Error::MessageMaxSizeExceeded(length)
                    if length == MESSAGE_MAX_SIZE + 1),
                "expected MessageMaxSizeExceeded, got {error:?}"
            );
        }
    }

    /// The protocol decoder refuses the forms the wire format cannot carry.
    ///
    /// `deserialize_any` is the one worth naming: a self-describing format
    /// answers it, and this one cannot, so a `Deserialize` written against
    /// `serde_json`'s data model fails here rather than reading the next bytes
    /// as whatever it hoped for.
    #[test]
    fn the_protocol_decoder_refuses_what_the_wire_format_cannot_carry() {
        let payload = [0, 0, 0, 1, 0, 0, 0, 0];

        for error in [
            decoded::<Any>(&payload).expect_err("a self-describing value"),
            decoded::<char>(&payload).expect_err("a char"),
            decoded::<()>(&payload).expect_err("a unit"),
            decoded::<BTreeMap<i32, i32>>(&payload).expect_err("a map"),
            decoded::<IgnoredAny>(&payload).expect_err("an ignored value"),
        ] {
            assert_unexpected_type(error);
        }
    }

    /// A unit struct and a tuple struct of two fields are refused.
    ///
    /// Apart because a one-field tuple struct is a newtype struct, which the
    /// decoder does support, and because both of these carry their name into
    /// the error.
    #[test]
    fn a_unit_struct_and_a_tuple_struct_are_refused() {
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct UnitStruct;

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TupleStruct(i32, i32);

        for error in [
            decoded::<UnitStruct>(&[]).expect_err("a unit struct"),
            decoded::<TupleStruct>(&[0, 0, 0, 1, 0, 0, 0, 2]).expect_err("a tuple struct"),
        ] {
            assert_unexpected_type(error);
        }
    }

    /// Asked which variant to read with no message metadata to answer from, the
    /// decoder says so.
    ///
    /// [`Body`](crate::Body) and [`Header`](crate::Header) are enums, and which
    /// variant to read comes from the API key — so a decoder that was handed
    /// neither an API key nor a container it recognises has nothing to answer
    /// with. `UnknownContainer` rather than a guess is what makes
    /// `Frame::response_from_bytes` require the caller to supply the key.
    #[test]
    fn an_enum_with_no_message_metadata_is_an_unknown_container() {
        let error = decoded::<Identifier>(&[]).expect_err("no container");

        assert!(
            matches!(error, Error::UnknownContainer),
            "expected UnknownContainer, got {error:?}"
        );
    }

    /// A record batch is read as bytes, and nothing else.
    ///
    /// [`BatchDecoder`] hands the whole of one batch to whoever asked for it,
    /// which is `deflated::Batch`'s own `Decode` — it parses the fixed
    /// forty-nine byte header itself, because the batch layout is not
    /// version-negotiated and does not go through the message metadata.
    #[test]
    fn a_record_batch_is_read_as_bytes_and_nothing_else() -> Result<()> {
        assert_eq!(
            Bytes::from_static(&[1, 2, 3]),
            from_batch::<Bytes>(&[1, 2, 3])?
        );
        assert_eq!(b"abc".to_vec(), from_batch::<AsBytes>(b"abc")?.0);

        let payload = [0u8, 0, 0, 1, 0, 0, 0, 0];

        for error in [
            from_batch::<Any>(&payload).expect_err("a self-describing value"),
            from_batch::<bool>(&payload).expect_err("a bool"),
            from_batch::<i8>(&payload).expect_err("an i8"),
            from_batch::<i16>(&payload).expect_err("an i16"),
            from_batch::<i32>(&payload).expect_err("an i32"),
            from_batch::<i64>(&payload).expect_err("an i64"),
            from_batch::<u8>(&payload).expect_err("a u8"),
            from_batch::<u16>(&payload).expect_err("a u16"),
            from_batch::<u32>(&payload).expect_err("a u32"),
            from_batch::<u64>(&payload).expect_err("a u64"),
            from_batch::<f32>(&payload).expect_err("an f32"),
            from_batch::<f64>(&payload).expect_err("an f64"),
            from_batch::<char>(&payload).expect_err("a char"),
            from_batch::<String>(&payload).expect_err("a string"),
            from_batch::<Str>(&payload).expect_err("a borrowed string"),
            from_batch::<Option<i32>>(&payload).expect_err("an option"),
            from_batch::<()>(&payload).expect_err("a unit"),
            from_batch::<Vec<i32>>(&payload).expect_err("a sequence"),
            from_batch::<(i32, i32)>(&payload).expect_err("a tuple"),
            from_batch::<BTreeMap<i32, i32>>(&payload).expect_err("a map"),
            from_batch::<Identifier>(&payload).expect_err("an identifier"),
            from_batch::<IgnoredAny>(&payload).expect_err("an ignored value"),
        ] {
            assert_unexpected_type(error);
        }

        Ok(())
    }

    /// A struct, a unit struct, a tuple struct, a newtype struct and an enum
    /// are refused by the record-batch decoder too.
    ///
    /// Named separately because each carries its own name into the error, so
    /// they cannot share the loop above.
    #[test]
    fn a_record_batch_refuses_every_named_form() {
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct UnitStruct;

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TupleStruct(i32, i32);

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct Newtype(i32);

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct Pair {
            first: i32,
        }

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        enum Variants {
            Unit,
        }

        let payload = [0u8, 0, 0, 1];

        for error in [
            from_batch::<UnitStruct>(&payload).expect_err("a unit struct"),
            from_batch::<TupleStruct>(&payload).expect_err("a tuple struct"),
            from_batch::<Newtype>(&payload).expect_err("a newtype struct"),
            from_batch::<Pair>(&payload).expect_err("a struct"),
            from_batch::<Variants>(&payload).expect_err("an enum"),
        ] {
            assert_unexpected_type(error);
        }
    }

    /// A batch whose declared length runs past the records it was read from is
    /// refused.
    ///
    /// The records field is a length and then that many bytes of batches, each
    /// of which declares its own length again. The two can disagree — the outer
    /// length is what the frame supplied and the inner one is what the batch
    /// header claims — and splitting on the claim is what would panic.
    #[test]
    fn a_batch_declaring_more_bytes_than_it_has_is_refused() {
        let mut batch = Batch::new(Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 231]));

        let error = batch
            .next_element::<Bytes>()
            .expect_err("a batch claiming 999 bytes of nothing");

        assert!(
            matches!(error, Error::Overflow),
            "expected Overflow, got {error:?}"
        );
    }

    /// A records field with nothing left in it is the end of the sequence.
    #[test]
    fn a_records_field_with_no_batches_left_ends_the_sequence() -> Result<()> {
        assert_eq!(None, Batch::new(Bytes::new()).next_element::<Bytes>()?);

        Ok(())
    }
}
