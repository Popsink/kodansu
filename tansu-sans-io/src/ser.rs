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

use std::{
    any::{type_name, type_name_of_val},
    collections::VecDeque,
    fmt,
};

use bytes::{BufMut, Bytes, BytesMut};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use tansu_model::{FieldMeta, MessageMeta};
use tracing::{debug, instrument};

use crate::{Encode, Error, Result, RootMessageMeta, primitive::varint::UnsignedVarInt};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Container {
    Struct {
        name: &'static str,
        len: usize,
    },

    StructVariant {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    },
}

impl Container {
    fn name(&self) -> String {
        match self {
            Self::Struct { name, .. } => (*name).to_string(),
            Self::StructVariant { name, variant, .. } => format!("{name}::{variant}"),
        }
    }
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

const PARSE_DEPTH: usize = 6;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Meta {
    message: Option<&'static MessageMeta>,
    field: Option<&'static FieldMeta>,
    parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}

/// Serialize the serde data model into the Kafka protocol.
pub struct Encoder {
    working: BytesMut,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
}

impl From<Encoder> for BytesMut {
    fn from(encoder: Encoder) -> Self {
        encoder.working
    }
}

impl From<Encoder> for Bytes {
    fn from(encoder: Encoder) -> Self {
        BytesMut::from(encoder).freeze()
    }
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl Encoder {
    pub(crate) fn request(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Request),
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    pub(crate) fn response(working: BytesMut, api_key: i16, api_version: i16) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Response),
            field: None,
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
                        parse,
                        ..Default::default()
                    }
                }),
        }
    }

    pub fn new(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: None,
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    #[instrument(skip(self))]
    fn field_meta(&self, name: &str) -> Option<&'static FieldMeta> {
        debug!(
            parse_front = ?self.meta.parse.front().and_then(|front| front.field(name)),
            meta = ?self.meta.message.and_then(|mm| mm.field(name))
        );

        self.meta
            .parse
            .front()
            .and_then(|front| front.field(name))
            .or(self.meta.message.and_then(|mm| mm.field(name)))
    }

    fn field_name(&self) -> String {
        self.containers.iter().fold(
            self.field.map_or_else(String::new, str::to_owned),
            |acc, container| {
                if acc.is_empty() {
                    container.name()
                } else {
                    format!("{}.{acc}", container.name())
                }
            },
        )
    }

    fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        const CONTINUATION: u8 = 0b1000_0000;

        while v >= u32::from(CONTINUATION) {
            self.working.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        self.working.put_u8(v as u8);
        Ok(())
    }

    fn in_header(&self) -> bool {
        matches!(
            self.containers.front(),
            Some(Container::StructVariant {
                name: "HeaderMezzanine",
                ..
            })
        )
    }

    #[must_use]
    fn is_flexible(&self) -> bool {
        debug!(
            "api_key: {:?}, api_version: {:?}, in_header: {}, is_client_id: {}",
            self.api_key,
            self.api_version,
            self.in_header(),
            self.field.is_some_and(|field| field == "client_id")
        );

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

    fn is_nullable(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.is_nullable(api_version))
        })
    }

    #[must_use]
    fn is_valid(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.version.within(api_version))
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

    #[must_use]
    fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    #[must_use]
    fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
    }
}

impl Serializer for &mut Encoder {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[instrument(skip(self))]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        match (self.containers.front(), self.field) {
            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{v}")))
    }

    #[instrument(skip(self))]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            v.len()
                .try_into()
                .map_err(Into::into)
                .and_then(|len| self.serialize_i16(len))?;

            self.working.put_slice(v.as_bytes());
        } else if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_i16(len))?;
            }

            self.working.put_slice(v.as_bytes());
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        debug!(
            ?v,
            is_valid = self.is_valid(),
            is_flexible = self.is_flexible()
        );

        if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_u32(len))?;
            }

            self.working.put_slice(v);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        debug!(
            name = self.field_name(),
            is_valid = self.is_valid(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_sequence = self.is_sequence(),
            is_flexible = self.is_flexible(),
        );

        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            self.serialize_i16(-1)
        } else if self.is_valid() && self.is_records() {
            if self.is_flexible() {
                self.unsigned_varint(1)
            } else {
                self.serialize_i32(0)
            }
        } else if self.is_valid() && self.is_nullable() {
            if self.is_structure() && !self.is_sequence() {
                self.serialize_i8(-1)
            } else if self.is_flexible() {
                self.unsigned_varint(0)
            } else if self.is_sequence() {
                self.serialize_i32(-1)
            } else if self.is_string() {
                self.serialize_i16(-1)
            } else {
                Ok(())
            }
        } else if self.is_valid() {
            // Valid at this version and not nullable: there is no encoding for
            // "absent". Writing nothing would not shorten the message, it would
            // shift every following field left, and the peer would read the
            // next bytes as this one — decoding into fabricated values as often
            // as it fails outright (#351).
            Err(Error::OmittedNonNullableField {
                field: self.field_name(),
                api_version: self.api_version,
            })
        } else {
            // Not valid at this version: the field genuinely has no bytes here,
            // and writing nothing is the whole point.
            Ok(())
        }
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        debug!(
            is_tag_buffer = self.field.is_some_and(|field| field == "tag_buffer"),
            is_flexible = self.is_flexible(),
            is_records = self.is_records(),
        );

        if self.field.is_some_and(|field| field == "tag_buffer") && !self.is_flexible() {
            Ok(())
        } else if self.is_records() {
            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len()
            );

            let records = {
                // `split_off` past `len` borrows from spare capacity, and panics
                // outright if the offset runs past `capacity`. So the buffer's
                // size — computed once by `size_in_bytes()` in
                // `Frame::response`, never grown — was load-bearing for memory
                // safety and not merely for efficiency, and an under-count by as
                // little as three bytes took down a `tokio-rt-worker` instead of
                // reallocating (#312, observed twice in 12 hours on a
                // `FetchResponse`).
                //
                // Reserve the length prefix rather than trust the estimate. This
                // makes a wrong estimate cost what a wrong estimate should cost
                // anywhere else: a reallocation.
                self.working.reserve(size_of::<u32>());

                let records = self
                    .working
                    .split_off(self.working.len() + size_of::<u32>());
                debug!(
                    records_capacity = records.capacity(),
                    records_len = records.len(),
                );

                let mut e = RecordBatchEncoder::new(records);
                value.serialize(&mut e)?;
                BytesMut::from(e)
            };

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
                records_capacity = records.capacity(),
                records_len = records.len(),
            );

            let length = u32::try_from(records.len())?;

            if self.is_flexible() {
                let encoded = UnsignedVarInt(length + 1).encode()?;
                self.working.put(encoded);
            } else {
                self.working.put_u32(length);
            }

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            self.working.unsplit(records);

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            Ok(())
        } else {
            value.serialize(self)
        }
    }

    #[instrument(skip_all)]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(name.into()))
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!(
            "{name}:{variant_index}:{variant}"
        )))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self))]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if self.is_valid()
            && let Some(len) = len
        {
            if self.is_flexible() {
                (len + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.unsigned_varint(l))?;
            } else {
                len.try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.serialize_i32(l))?;
            }
        }

        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.containers.push_front(Container::Struct { name, len });

        if let Some(fm) = self.field_meta(name) {
            self.meta.field = Some(fm);
            self.meta.parse.push_front(fm.fields.into());
        }

        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.containers.push_front(Container::StructVariant {
            name,
            variant_index,
            variant,
            len,
        });

        Ok(self)
    }
}

impl SerializeSeq for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeTupleVariant for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeMap for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, key), fields(key = type_name::<T>()))]
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(key).into()))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeStruct for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            debug!(field = self.field_name(), meta = ?fm, is_valid = self.is_valid());

            _ = self.meta.field.replace(fm);
            self.meta.parse.push_front(fm.fields.into());
            let outcome = if self.is_valid() {
                value.serialize(&mut **self)
            } else {
                Ok(())
            };
            _ = self.meta.parse.pop_front();
            _ = self.meta.field.take();
            outcome
        } else {
            debug!(field = self.field_name());

            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            if self
                .api_version
                .is_some_and(|api_version| fm.version.within(api_version))
            {
                debug!(field_name = self.field_name(), meta = ?fm);

                _ = self.meta.field.replace(fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = value.serialize(&mut **self);
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else {
                debug!(
                    field_name = self.field_name(),
                    api_version = self.api_version
                );
                Ok(())
            }
        } else {
            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}

pub struct RecordBatchEncoder {
    working: BytesMut,
}

impl RecordBatchEncoder {
    pub fn new(working: BytesMut) -> Self {
        Self { working }
    }
}

impl From<RecordBatchEncoder> for BytesMut {
    fn from(value: RecordBatchEncoder) -> Self {
        value.working
    }
}

impl From<RecordBatchEncoder> for Bytes {
    fn from(value: RecordBatchEncoder) -> Self {
        BytesMut::from(value).into()
    }
}

impl Serializer for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant = Self;

    type SerializeMap = Self;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

    #[instrument(skip(self))]
    fn serialize_bool(self, v: bool) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{v}")))
    }

    #[instrument(skip(self))]
    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(v.into()))
    }

    #[instrument(skip(self))]
    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    #[instrument(skip_all, fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    #[instrument(skip_all)]
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(String::from("unit")))
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(
        self,
        name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(name.into()))
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!(
            "{name}, {variant_index}, {variant}"
        )))
    }

    #[instrument(skip_all, fields(name, value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(format!(
            "{}, {name}",
            type_name_of_val(value)
        )))
    }

    #[instrument(skip_all, fields(name, variant_index, variant, value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(format!(
            "{}, {name}, {variant_index}, {variant}",
            type_name_of_val(value)
        )))
    }

    #[instrument(skip(self))]
    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        Err(Error::UnexpectedType(format!("{len}")))
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::UnexpectedType(format!("{name}, {len}")))
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::UnexpectedType(format!(
            "{name}, {variant_index}, {variant}, {len}"
        )))
    }

    #[instrument(skip(self))]
    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        Err(Error::UnexpectedType(format!("{len:?}")))
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
    }
}

impl SerializeSeq for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(type_name = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(type_name::<Self>().into()))
    }
}

impl SerializeTupleVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(type_name::<Self>().into()))
    }
}

impl SerializeMap for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(key).into()))
    }

    fn serialize_value<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(type_name::<Self>().into()))
    }
}

impl SerializeStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(type_name::<Self>().into()))
    }
}

impl SerializeStructVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod encode_allocation {
    use crate::{
        ApiKey as _, Body, FetchResponse, Frame, Header, MaximumAllocationSize as _, Result,
    };

    /// A real `FetchResponse` v16 carrying a record batch, taken from the worked
    /// example in `record.rs`.
    fn fetch_response_v16_bytes() -> Vec<u8> {
        vec![
            0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142,
            19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0,
            74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117,
            121, 0, 3, 0, 13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 0, 0, 0,
        ]
    }

    /// Encoding a `FetchResponse` must not depend on `size_in_bytes()` being
    /// exact (#312).
    ///
    /// `Frame::response` allocates once from that estimate and never grows the
    /// buffer, and the records field then splits **past `len`** into the reserved
    /// capacity. `BytesMut::split_off` panics when the offset runs past
    /// `capacity`, so an estimate three bytes short took down a
    /// `tokio-rt-worker` — twice in twelve hours of production — where anywhere
    /// else a wrong estimate would have cost a reallocation.
    ///
    /// This asserts the encode completes. It cannot assert the estimate is
    /// correct, which is the point: correctness of the estimate must stop being
    /// a liveness requirement.
    #[test]
    fn encoding_a_fetch_response_does_not_depend_on_an_exact_estimate() -> Result<()> {
        let api_key = FetchResponse::KEY;
        let api_version = 16;

        let bytes = fetch_response_v16_bytes();
        let frame = Frame::response_from_bytes(&bytes[..], api_key, api_version)?;

        let encoded = Frame::response(frame.header, frame.body, api_key, api_version)?;

        assert!(!encoded.is_empty());

        Ok(())
    }

    /// The estimate must cover what is produced (#312).
    ///
    /// With the reservation in place an under-count is a reallocation rather than
    /// a panic, so it becomes *testable* — which is why this assertion can exist
    /// at all. It failing is a diagnosis, not an outage: the delta names how many
    /// bytes `ByteSize` is missing.
    #[test]
    fn the_size_estimate_covers_the_encoded_frame() -> Result<()> {
        let api_key = FetchResponse::KEY;
        let api_version = 16;

        let bytes = fetch_response_v16_bytes();
        let frame = Frame::response_from_bytes(&bytes[..], api_key, api_version)?;

        let estimate = frame.maximum_allocation_size()?;
        let encoded = Frame::response(frame.header, frame.body, api_key, api_version)?;

        assert!(
            estimate >= encoded.len(),
            "maximum_allocation_size() said {estimate} but the frame encoded to {} — short by \
             {} bytes. \
             Before #312 this shortfall was a panic in the records `split_off` rather than \
             an assertion.",
            encoded.len(),
            encoded.len().saturating_sub(estimate),
        );

        Ok(())
    }

    /// The estimate covers the frame at **every** version, not just the one
    /// vector that happened to be captured (#312).
    ///
    /// Worth being precise about what this one does: it did **not** find the
    /// under-count, and still passes with the tag-buffer allowance removed. What
    /// it shows is where the margin goes — the slack narrows steadily as versions
    /// add fields, from 86 bytes at v0 to 3 from v13 on, where the flexible
    /// encoding starts paying for tag buffers. That is the warning the sweep
    /// below turns into a failure.
    #[test]
    fn the_size_estimate_covers_every_version() -> Result<()> {
        let api_key = FetchResponse::KEY;

        let bytes = fetch_response_v16_bytes();
        let decoded = Frame::response_from_bytes(&bytes[..], api_key, 16)?;
        let body = FetchResponse::try_from(decoded.body)?;

        // The captured vector is v16, which identifies a topic by `topic_id`
        // and carries no name. Encoding it below v13 needs `topic`, which is
        // not nullable there — so name it, or the sweep is asking for a frame
        // that cannot legally exist (#351).
        let named = body.responses.clone().map(|responses| {
            responses
                .into_iter()
                .map(|response| response.topic(Some("t".into())))
                .collect()
        });
        let body = body.responses(named);

        for api_version in 0..=17i16 {
            let header = Header::Response { correlation_id: 8 };

            let estimate = Frame {
                size: 0,
                header: header.clone(),
                body: Body::from(body.clone()),
            }
            .maximum_allocation_size()?;

            let encoded = Frame::response(header, Body::from(body.clone()), api_key, api_version)?;

            assert!(
                estimate >= encoded.len(),
                "v{api_version}: maximum_allocation_size() said {estimate} but the frame encoded \
                 to {} — short by {} bytes",
                encoded.len(),
                encoded.len().saturating_sub(estimate),
            );
        }

        Ok(())
    }

    /// The estimate covers the frame as the **number of structs** grows (#312).
    ///
    /// The sweep that localises the defect rather than merely detecting it. A
    /// tag buffer costs a byte per struct and was counted nowhere, so the
    /// shortfall is linear in the struct count while the slack that hid it —
    /// a compact array counted as 4 bytes, a null field counted as 4 — is
    /// per-array, not per-struct. Measured before the fix, at v16, one
    /// partition per topic:
    ///
    /// | topics | estimate | encoded | slack |
    /// |---|---|---|---|
    /// | 1 | 193 | 190 | +3 |
    /// | 2 | 360 | 359 | +1 |
    /// | 4 | 694 | 697 | **-3** |
    /// | 1024 | 171 034 | 173 078 | **-2044** |
    ///
    /// Two bytes per topic — one for the topic's tag buffer, one for its
    /// partition's. The production panic was a `FetchResponse` three bytes over
    /// its 8 KiB allocation, which is this at four topics.
    #[test]
    fn the_size_estimate_covers_a_frame_of_many_structs() -> Result<()> {
        let api_key = FetchResponse::KEY;
        let api_version = 16;

        let bytes = fetch_response_v16_bytes();
        let decoded = Frame::response_from_bytes(&bytes[..], api_key, api_version)?;
        let body = FetchResponse::try_from(decoded.body)?;
        let one_topic = body.responses.clone().unwrap_or_default();

        // The precondition: a single topic is what the captured vector holds, so
        // without more than one struct this test is the assertion above.
        assert_eq!(1, one_topic.len());

        for topics in [1usize, 2, 4, 8, 16, 64, 256, 1024] {
            let body = body.clone().responses(Some(
                one_topic.iter().cycle().take(topics).cloned().collect(),
            ));

            let header = Header::Response { correlation_id: 8 };

            let estimate = Frame {
                size: 0,
                header: header.clone(),
                body: Body::from(body.clone()),
            }
            .maximum_allocation_size()?;

            let encoded = Frame::response(header, Body::from(body), api_key, api_version)?;

            assert!(
                estimate >= encoded.len(),
                "{topics} topics: maximum_allocation_size() said {estimate} but the frame encoded \
                 to {} — short by {} bytes, {:.2} per topic",
                encoded.len(),
                encoded.len().saturating_sub(estimate),
                encoded.len().saturating_sub(estimate) as f64 / topics as f64,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod accepted_forms {
    //! What the two serializers in this file accept, and what they refuse
    //! (#556).
    //!
    //! Neither writes anything that says what a value *is* — the protocol is
    //! positional and a record batch is a fixed layout — so a form outside the
    //! set each one encodes cannot be skipped or approximated. It has to be an
    //! error, and until #556 the record-batch encoder made it a panic instead:
    //! twenty-two `todo!()` and `unimplemented!()` arms in a serializer whose
    //! every other arm returns `Result`. Nothing in the tree reaches them,
    //! which is exactly what was said about the allocation estimate in
    //! `encode_allocation` before it took down two `tokio-rt-worker` threads
    //! (#312).

    use super::*;
    use serde::Serialize;
    use std::collections::BTreeMap;

    #[derive(Serialize)]
    struct UnitStruct;

    #[derive(Serialize)]
    enum Variants {
        Unit,
        Newtype(i32),
        Tuple(i32, i32),
    }

    #[derive(Serialize)]
    struct TupleStruct(i32, i32);

    #[derive(Serialize)]
    struct Newtype(i32);

    /// What the protocol encoder writes for a value, with no message metadata
    /// in play.
    fn encoded<T>(value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized,
    {
        let mut encoder = Encoder::new(BytesMut::new());
        value.serialize(&mut encoder)?;
        Ok(Vec::from(&BytesMut::from(encoder)[..]))
    }

    /// What the record-batch encoder writes for a value.
    fn in_batch<T>(value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized,
    {
        let mut encoder = RecordBatchEncoder::new(BytesMut::new());
        value.serialize(&mut encoder)?;
        Ok(Vec::from(&BytesMut::from(encoder)[..]))
    }

    fn assert_unexpected_type(error: Error) {
        assert!(
            matches!(error, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {error:?}"
        );
    }

    /// Every fixed-width number is big-endian, in both encoders.
    ///
    /// The unsigned widths and the floats are here because the encoders answer
    /// for them, not because Kafka's schema has them: the generated types use
    /// the signed widths and `u8`/`u32` for record counts. An arm that exists
    /// and is never exercised is an arm that can be wrong for as long as it
    /// takes something to start using it.
    #[test]
    fn every_fixed_width_number_is_big_endian_in_both_encoders() -> Result<()> {
        for encode in [encoded as fn(&u64) -> Result<Vec<u8>>, in_batch] {
            assert_eq!(vec![0, 0, 0, 0, 0, 0, 0, 10], encode(&10u64)?);
        }

        assert_eq!(1.5f32.to_be_bytes().to_vec(), encoded(&1.5f32)?);
        assert_eq!(2.5f64.to_be_bytes().to_vec(), encoded(&2.5f64)?);
        assert_eq!(1.5f32.to_be_bytes().to_vec(), in_batch(&1.5f32)?);
        assert_eq!(2.5f64.to_be_bytes().to_vec(), in_batch(&2.5f64)?);

        assert_eq!(vec![1], in_batch(&true)?);
        assert_eq!(vec![0, 8], in_batch(&8u16)?);

        Ok(())
    }

    /// The protocol encoder refuses the forms the wire format has no encoding
    /// for.
    ///
    /// A tuple struct and a tuple variant are refused at their first field
    /// rather than when they are opened, and a map at its first key, because
    /// the serializer's associated types are all `Self` — there is nowhere for
    /// `serialize_map` itself to fail. An empty map is refused by `end`.
    #[test]
    fn the_protocol_encoder_refuses_what_the_wire_format_cannot_carry() {
        for value in [
            encoded(&'a'),
            encoded(&()),
            encoded(&UnitStruct),
            encoded(&Variants::Unit),
            encoded(&TupleStruct(1, 2)),
            encoded(&Variants::Tuple(1, 2)),
            encoded(&BTreeMap::from([(1i32, 2i32)])),
            encoded(&BTreeMap::<i32, i32>::new()),
        ] {
            assert_unexpected_type(value.expect_err("a form the wire format cannot carry"));
        }
    }

    /// A map's value is refused as well as its key, for a caller driving the
    /// serializer by hand.
    ///
    /// A derived `Serialize` never gets past the key, which is why the value
    /// has to refuse too rather than trusting that it cannot be reached.
    #[test]
    fn the_protocol_encoder_refuses_a_map_value_as_well_as_a_key() -> Result<()> {
        let mut encoder = Encoder::new(BytesMut::new());
        let mut map = (&mut encoder).serialize_map(Some(1))?;

        assert_unexpected_type(
            map.serialize_value(&1i32)
                .expect_err("a map value has no encoding"),
        );

        Ok(())
    }

    /// A record batch is numbers and bytes, and nothing else.
    ///
    /// `bytes` is the record data, written raw with no length in front of it —
    /// the batch header already said how long it is. Everything on this list
    /// was a `todo!()` or an `unimplemented!()` until #556, including `str`:
    /// a record batch has no string field at any version, and asking it to
    /// write one aborted the process rather than returning the error every
    /// caller of this serializer is already handling.
    #[test]
    fn a_record_batch_is_numbers_and_bytes_and_nothing_else() -> Result<()> {
        assert_eq!(vec![1, 2, 3], in_batch(&Bytes::from_static(&[1, 2, 3]))?);

        for value in [
            in_batch(&'a'),
            in_batch("abc"),
            in_batch(&Some(1i32)),
            in_batch(&()),
            in_batch(&UnitStruct),
            in_batch(&Variants::Unit),
            in_batch(&Variants::Newtype(1)),
            in_batch(&(1i32, 2i32)),
            in_batch(&Newtype(7)),
            in_batch(&TupleStruct(1, 2)),
            in_batch(&Variants::Tuple(1, 2)),
            in_batch(&BTreeMap::from([(1i32, 2i32)])),
        ] {
            assert_unexpected_type(value.expect_err("a form a record batch cannot carry"));
        }

        Ok(())
    }

    /// An absent field writes nothing in a record batch, where a present one is
    /// refused.
    ///
    /// The asymmetry is deliberate and it is load-bearing: `deflated::Batch`
    /// has no optional field, so `None` writing nothing is what lets an
    /// `Option` in a *containing* struct pass through, while `Some` has no
    /// layout to write into.
    #[test]
    fn an_absent_field_writes_nothing_in_a_record_batch() -> Result<()> {
        assert!(in_batch(&Option::<i32>::None)?.is_empty());

        Ok(())
    }

    /// The record-batch encoder's sequence and map drivers refuse their
    /// elements, reached by hand because nothing hands them out.
    ///
    /// `serialize_tuple`, `serialize_tuple_struct`, `serialize_tuple_variant`
    /// and `serialize_map` all refuse before returning one of these, so their
    /// `serialize_*`/`end` pairs are unreachable through `Serialize`. They are
    /// asserted rather than deleted because the trait requires them, and a
    /// `todo!()` in a method the compiler insists exists is a panic waiting for
    /// the day something calls it.
    #[test]
    fn the_record_batch_element_drivers_refuse_everything() {
        let mut encoder = RecordBatchEncoder::new(BytesMut::new());

        let mut tuple: &mut RecordBatchEncoder = &mut encoder;
        assert_unexpected_type(
            SerializeTuple::serialize_element(&mut tuple, &1i32).expect_err("a tuple element"),
        );
        assert_unexpected_type(SerializeTuple::end(tuple).expect_err("a tuple"));

        let mut tuple_struct: &mut RecordBatchEncoder = &mut encoder;
        assert_unexpected_type(
            SerializeTupleStruct::serialize_field(&mut tuple_struct, &1i32)
                .expect_err("a tuple struct field"),
        );
        assert_unexpected_type(
            SerializeTupleStruct::end(tuple_struct).expect_err("a tuple struct"),
        );

        let mut tuple_variant: &mut RecordBatchEncoder = &mut encoder;
        assert_unexpected_type(
            SerializeTupleVariant::serialize_field(&mut tuple_variant, &1i32)
                .expect_err("a tuple variant field"),
        );
        assert_unexpected_type(
            SerializeTupleVariant::end(tuple_variant).expect_err("a tuple variant"),
        );

        let mut map: &mut RecordBatchEncoder = &mut encoder;
        assert_unexpected_type(map.serialize_key(&1i32).expect_err("a map key"));
        assert_unexpected_type(map.serialize_value(&1i32).expect_err("a map value"));
        assert_unexpected_type(SerializeMap::end(map).expect_err("a map"));
    }

    /// The protocol encoder's tuple drivers refuse their `end` as well as
    /// their fields, reached by hand because a derived `Serialize` stops at the
    /// first field.
    #[test]
    fn the_protocol_tuple_drivers_refuse_their_end() {
        let mut encoder = Encoder::new(BytesMut::new());

        let tuple_struct = (&mut encoder).serialize_tuple_struct("TupleStruct", 2);
        assert_unexpected_type(
            tuple_struct
                .and_then(SerializeTupleStruct::end)
                .expect_err("a tuple struct"),
        );

        let tuple_variant = (&mut encoder).serialize_tuple_variant("Variants", 0, "Tuple", 2);
        assert_unexpected_type(
            tuple_variant
                .and_then(SerializeTupleVariant::end)
                .expect_err("a tuple variant"),
        );
    }

    /// A struct variant is written as its fields, in both encoders, with
    /// nothing saying which variant it was.
    ///
    /// That is how the `Body` enum is encoded — the API key in the header is
    /// what names the variant — and it is why the record-batch encoder's
    /// struct-variant arm is `Ok` rather than a refusal.
    #[test]
    fn a_struct_variant_is_written_as_its_fields() -> Result<()> {
        #[derive(Serialize)]
        enum Variants {
            Struct { value: i16 },
        }

        assert_eq!(vec![0, 6], encoded(&Variants::Struct { value: 6 })?);
        assert_eq!(vec![0, 6], in_batch(&Variants::Struct { value: 6 })?);

        Ok(())
    }

    /// A newtype struct is its inner value, in both encoders.
    #[test]
    fn a_newtype_struct_is_its_inner_value() -> Result<()> {
        #[derive(Serialize)]
        struct Wrapper(i32);

        assert_eq!(vec![0, 0, 0, 7], encoded(&Wrapper(7))?);

        Ok(())
    }
}
