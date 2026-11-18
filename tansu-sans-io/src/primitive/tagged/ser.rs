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

use std::any::{type_name, type_name_of_val};
use std::fmt;
use std::io::Write;

use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant};
use serde::ser::{SerializeTuple, SerializeTupleStruct, SerializeTupleVariant};
use serde::{Serialize, Serializer};

use crate::Error;
use crate::Result;

/// The error for a serde form that has no encoding in a tag's payload.
fn refused<T>(form: String) -> Result<T> {
    Err(Error::UnexpectedType(form))
}

/// `serializer`, ready for the elements of the compound it has just opened.
fn opened<S>(serializer: S) -> Result<S> {
    Ok(serializer)
}

pub(crate) struct Encoder<'a> {
    writer: &'a mut dyn Write,
}

impl fmt::Debug for Encoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Self")
    }
}

impl<'a> Encoder<'a> {
    pub(crate) fn new(writer: &'a mut dyn Write) -> Self {
        Encoder { writer }
    }

    #[tracing::instrument(skip_all, fields(decoded = type_name::<T>()))]
    pub(crate) fn encode<T>(decoded: &T) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        let mut bytes = Vec::with_capacity(512);
        T::serialize(decoded, &mut Encoder::new(&mut bytes))?;
        Ok(bytes)
    }

    fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        while v > 0x7f {
            let [low, ..] = v.to_le_bytes();
            self.put(&[low | 0x80])?;
            v /= 128;
        }
        let [last, ..] = v.to_le_bytes();
        self.put(&[last])
    }

    /// `encoded`, written as it is.
    fn put(&mut self, encoded: &[u8]) -> Result<()> {
        self.writer.write_all(encoded)?;
        Ok(())
    }

    /// A compact length: `len + 1`, as an unsigned varint.
    fn length(&mut self, len: usize) -> Result<()> {
        let plus_one = u32::try_from(len + 1)?;
        self.unsigned_varint(plus_one)
    }
}

impl Serializer for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[tracing::instrument(skip(self))]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.put(&[u8::from(v)])
    }

    #[tracing::instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        tracing::debug!(v = ?v);
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.put(&v.to_be_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        refused(format!("{v:?}"))
    }

    #[tracing::instrument(skip(self))]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.length(v.len())?;
        self.put(v.as_bytes())
    }

    #[tracing::instrument(skip(self))]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.length(v.len())?;
        self.put(v)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Encoder::unsigned_varint(self, 0)
    }

    #[tracing::instrument(skip_all, fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        T::serialize(value, self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        refused(format!("{self:?}"))
    }

    #[tracing::instrument(skip(self))]
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        refused(String::from(name))
    }

    #[tracing::instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        refused(format!("{name}:{variant_index}:{variant}"))
    }

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        T::serialize(value, self)
    }

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
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
        _ = value;
        refused(format!("{name}:{variant_index}:{variant}"))
    }

    #[tracing::instrument(skip(self))]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if let Some(count) = len {
            self.length(count)?;
        }
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        opened(self)
    }

    #[tracing::instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        opened(self)
    }
}

impl SerializeSeq for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        T::serialize(value, &mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        T::serialize(value, &mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        refused(type_name_of_val(value).into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        refused(format!("{self:?}"))
    }
}

impl SerializeTupleVariant for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        refused(type_name_of_val(value).into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        refused(format!("{self:?}"))
    }
}

impl SerializeMap for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[tracing::instrument(skip(self, key), fields(key = type_name::<T>()))]
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        refused(type_name_of_val(key).into())
    }

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        refused(type_name_of_val(value).into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        refused(type_name_of_val(self).into())
    }
}

impl SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = key;
        T::serialize(value, &mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    #[tracing::instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = key;
        T::serialize(value, &mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::collections::BTreeMap;

    /// What a tag's value encodes to, which is the whole of this serializer's
    /// contract: it is handed one value and returns the bytes that go in a
    /// [`TagField`](super::super::TagField)'s payload.
    fn encoded<T>(value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized,
    {
        Encoder::encode(&value)
    }

    /// The error a form outside the Kafka primitive set produces.
    fn refused<T>(value: &T) -> Error
    where
        T: Serialize + ?Sized,
    {
        encoded(value).expect_err("a form this codec has no encoding for")
    }

    /// Fixed-width numbers are big-endian and unprefixed.
    ///
    /// Unprefixed is the part that matters: a tag's payload length is written by
    /// the [`TagField`](super::super::TagField) around it, so nothing here
    /// describes its own size and the decoder recovers a value only by knowing
    /// the type it asked for.
    #[test]
    fn fixed_width_numbers_are_big_endian_and_carry_no_length() -> Result<()> {
        assert_eq!(vec![1], encoded(&true)?);
        assert_eq!(vec![0], encoded(&false)?);

        assert_eq!(vec![255], encoded(&-1i8)?);
        assert_eq!(vec![255, 254], encoded(&-2i16)?);
        assert_eq!(vec![255, 255, 255, 253], encoded(&-3i32)?);
        assert_eq!(
            vec![255, 255, 255, 255, 255, 255, 255, 252],
            encoded(&-4i64)?
        );

        assert_eq!(vec![7], encoded(&7u8)?);
        assert_eq!(vec![0, 8], encoded(&8u16)?);
        assert_eq!(vec![0, 0, 0, 9], encoded(&9u32)?);
        assert_eq!(vec![0, 0, 0, 0, 0, 0, 0, 10], encoded(&10u64)?);

        assert_eq!(1.5f32.to_be_bytes().to_vec(), encoded(&1.5f32)?);
        assert_eq!(2.5f64.to_be_bytes().to_vec(), encoded(&2.5f64)?);

        Ok(())
    }

    /// A string is a compact string: an unsigned varint of `len + 1`, then the
    /// bytes.
    ///
    /// The 200-byte case is the one worth having: it is the only value here
    /// whose length does not fit in a single varint byte, so it is what
    /// exercises the continuation loop the other cases never enter.
    #[test]
    fn a_string_is_a_varint_of_length_plus_one_then_the_bytes() -> Result<()> {
        assert_eq!(vec![1], encoded("")?);
        assert_eq!(vec![4, b'a', b'b', b'c'], encoded("abc")?);

        let long = "z".repeat(200);
        let mut expected = vec![0xc9, 0x01];
        expected.extend_from_slice(long.as_bytes());
        assert_eq!(expected, encoded(long.as_str())?);

        Ok(())
    }

    /// `None` is a zero varint, and `Some` is the value with nothing in front
    /// of it.
    ///
    /// So `Some(0u8)` and `None` are both a single zero byte, and which one the
    /// decoder recovers depends entirely on the type it deserializes into.
    #[test]
    fn none_is_a_zero_varint_and_some_is_the_value_alone() -> Result<()> {
        assert_eq!(vec![0], encoded(&Option::<i32>::None)?);
        assert_eq!(vec![0, 0, 0, 5], encoded(&Some(5i32))?);
        assert_eq!(encoded(&Option::<u8>::None)?, encoded(&Some(0u8))?);

        Ok(())
    }

    /// A sequence is a compact array: a varint of `len + 1`, then the elements.
    #[test]
    fn a_sequence_is_a_varint_of_length_plus_one_then_the_elements() -> Result<()> {
        assert_eq!(vec![1], encoded(&Vec::<i32>::new())?);
        assert_eq!(vec![3, 0, 0, 0, 1, 0, 0, 0, 2], encoded(&vec![1i32, 2])?);

        Ok(())
    }

    /// A struct is its fields in declaration order, with no field names, no
    /// count and no tag buffer of its own.
    #[test]
    fn a_struct_is_its_fields_in_order_and_nothing_else() -> Result<()> {
        #[derive(Serialize)]
        struct Pair {
            first: i16,
            second: bool,
        }

        assert_eq!(
            vec![0, 6, 1],
            encoded(&Pair {
                first: 6,
                second: true
            })?
        );

        Ok(())
    }

    /// A tuple is its elements, which makes it indistinguishable from a struct
    /// of the same fields.
    #[test]
    fn a_tuple_is_its_elements_like_a_struct() -> Result<()> {
        assert_eq!(vec![0, 6, 1], encoded(&(6i16, true))?);

        Ok(())
    }

    /// Bytes are length-prefixed like a string — and cannot be read back.
    ///
    /// The deserializer refuses `bytes` and `byte_buf` outright (see
    /// `de::tests::the_forms_this_codec_cannot_read`), so this arm is
    /// write-only. Nothing in the tree reaches it today; it is asserted here so
    /// that a tag payload typed as bytes fails in the decoder, where the
    /// asymmetry is visible, rather than encoding into something no reader
    /// accepts.
    #[test]
    fn bytes_encode_like_a_string_even_though_nothing_can_decode_them() -> Result<()> {
        struct Octets<'a>(&'a [u8]);

        impl Serialize for Octets<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_bytes(self.0)
            }
        }

        assert_eq!(vec![3, 1, 2], encoded(&Octets(&[1, 2]))?);

        Ok(())
    }

    /// The forms Kafka has no encoding for are refused rather than skipped.
    ///
    /// Every one of these would otherwise write nothing and return `Ok`, and a
    /// tag whose payload is empty is not distinguishable from a tag that was
    /// never set — so the failure would land on whoever read it back.
    #[test]
    fn the_forms_kafka_has_no_encoding_for_are_refused() {
        #[derive(Serialize)]
        struct UnitStruct;

        #[derive(Serialize)]
        enum Variants {
            Unit,
            Newtype(i32),
        }

        for error in [
            refused(&'a'),
            refused(&()),
            refused(&UnitStruct),
            refused(&Variants::Unit),
            refused(&Variants::Newtype(1)),
        ] {
            assert!(
                matches!(error, Error::UnexpectedType(_)),
                "expected UnexpectedType, got {error:?}"
            );
        }
    }

    /// A map is refused at its first key, not when it is opened.
    ///
    /// `serialize_map` answers `Ok` — there is nowhere else for it to fail,
    /// since the serializer's associated types are all `Self` — so the refusal
    /// has to come from the entries. An empty map has no entries, and is
    /// refused by `end` instead.
    #[test]
    fn a_map_is_refused_at_its_first_entry_or_at_its_end() {
        let one = refused(&BTreeMap::from([(1i32, 2i32)]));
        assert!(
            matches!(one, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {one:?}"
        );

        let empty = refused(&BTreeMap::<i32, i32>::new());
        assert!(
            matches!(empty, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {empty:?}"
        );
    }

    /// A map's value is refused too, for a caller driving the serializer by
    /// hand rather than through a `Serialize` impl.
    ///
    /// `serialize_key` is what a derived impl hits first, so `serialize_value`
    /// is unreachable through one — which is exactly why it has to refuse as
    /// well.
    #[test]
    fn a_map_refuses_a_value_as_well_as_a_key() -> Result<()> {
        let mut encoded = vec![];
        let mut encoder = Encoder::new(&mut encoded);
        let mut map = (&mut encoder).serialize_map(Some(1))?;

        let error = map
            .serialize_value(&1i32)
            .expect_err("a map value has no encoding");

        assert!(
            matches!(error, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {error:?}"
        );

        Ok(())
    }

    /// A tuple struct and a tuple variant open, then refuse their first field.
    ///
    /// Same shape as the map: the refusal cannot be in `serialize_tuple_struct`
    /// itself, so it is in the field. Worth pinning because the plain tuple
    /// above *is* supported, and the three arrive at the same associated type.
    #[test]
    fn a_tuple_struct_and_a_tuple_variant_refuse_their_fields() {
        #[derive(Serialize)]
        struct TupleStruct(i32, i32);

        #[derive(Serialize)]
        enum Variants {
            Tuple(i32, i32),
        }

        for error in [refused(&TupleStruct(1, 2)), refused(&Variants::Tuple(1, 2))] {
            assert!(
                matches!(error, Error::UnexpectedType(_)),
                "expected UnexpectedType, got {error:?}"
            );
        }
    }

    /// A struct variant encodes as its fields, with nothing saying which
    /// variant it was.
    ///
    /// That is not a useful encoding — the decoder refuses `enum` outright, so
    /// this can be written and never read — but it is what the serializer does,
    /// and a variant silently encoding as a bare struct is worth having written
    /// down.
    #[test]
    fn a_struct_variant_loses_which_variant_it_was() -> Result<()> {
        #[derive(Serialize)]
        enum Variants {
            Struct { value: i16 },
        }

        assert_eq!(vec![0, 6], encoded(&Variants::Struct { value: 6 })?);

        Ok(())
    }
}
