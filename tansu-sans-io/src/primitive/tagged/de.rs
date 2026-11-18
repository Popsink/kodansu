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
use std::io::Read;

use serde::Deserializer;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};

use crate::Error;
use crate::Result;

/// The error for a serde form that has no encoding in a tag's payload.
fn refused<T>(form: String) -> Result<T> {
    Err(Error::UnexpectedType(form))
}

/// Logs `value`, decoded for a visitor that produces `Produced`.
fn traced<Produced>(value: &dyn fmt::Display) {
    tracing::debug!("value: {value}:{}", type_name::<Produced>());
}

const MESSAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;

pub struct Decoder<'de> {
    reader: &'de mut dyn Read,
    length: Option<usize>,
    message_max_size: Option<usize>,
}

impl<'de> Decoder<'de> {
    pub fn new(reader: &'de mut dyn Read) -> Self {
        let (length, message_max_size) = (None, None);
        Decoder {
            length,
            message_max_size,
            reader,
        }
    }

    pub fn unsigned_varint(&mut self) -> Result<u32> {
        let mut value = 0u32;
        let mut shift = 0u32;
        loop {
            let [byte] = self.read()?;
            let bits = u32::from(byte & 0x7f);
            let shifted = bits.checked_shl(shift).ok_or(Error::Overflow)?;
            value = value.checked_add(shifted).ok_or(Error::Overflow)?;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    /// The next `N` bytes.
    fn read<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0u8; N];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    /// The length of the next string: the one an option already read, or a
    /// compact length, `len + 1` as an unsigned varint.
    fn length(&mut self) -> Result<usize> {
        if let Some(known) = self.length.take() {
            return Ok(known);
        }
        let plus_one = self.unsigned_varint()?;
        let length = plus_one.checked_sub(1).ok_or(Error::Overflow)?;
        Ok(usize::try_from(length)?)
    }

    /// The next `length` bytes, refused past the largest message allowed.
    fn payload(&mut self, length: usize) -> Result<Vec<u8>> {
        let largest = self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE);
        if length > largest {
            let too_large = Error::MessageMaxSizeExceeded(length);
            return Err(too_large);
        }
        let mut bytes = vec![0u8; length];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

impl fmt::Debug for Decoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Self")
    }
}

impl<'de> Deserializer<'de> for &mut Decoder<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let [byte] = self.read()?;
        let decoded = byte != 0;
        traced::<V::Value>(&decoded);
        visitor.visit_bool(decoded)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = i8::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_i8(decoded)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = i16::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_i16(decoded)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = i32::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_i32(decoded)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = i64::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_i64(decoded)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = u8::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_u8(decoded)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = u16::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_u16(decoded)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = u32::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_u32(decoded)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = u64::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_u64(decoded)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = f32::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_f32(decoded)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let decoded = f64::from_be_bytes(self.read()?);
        traced::<V::Value>(&decoded);
        visitor.visit_f64(decoded)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let length = self.length()?;
        let bytes = self.payload(length)?;
        let text = std::str::from_utf8(&bytes)?;
        traced::<V::Value>(&text);
        visitor.visit_str(text)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let length = self.length()?;
        let text = String::from_utf8(self.payload(length)?)?;
        traced::<V::Value>(&text);
        visitor.visit_string(text)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let plus_one = self.unsigned_varint()?;
        let Some(length) = plus_one.checked_sub(1) else {
            return visitor.visit_none();
        };
        self.length = Some(usize::try_from(length)?);
        V::visit_some(visitor, self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(format!("{name}:{}", type_name::<V::Value>()))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        _ = name;
        V::visit_newtype_struct(visitor, self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let plus_one = self.unsigned_varint()?;
        let count = plus_one.checked_sub(1).ok_or(Error::Overflow)?;
        let elements = Seq::new(self, Some(usize::try_from(count)?));
        visitor.visit_seq(elements)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(format!("{len}:{}", type_name::<V::Value>()))
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
        refused(format!("{name}:{len}:{}", type_name::<V::Value>()))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
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
        tracing::debug!("name: {name}, fields: {fields:?}");
        let in_order = Struct::new(self);
        visitor.visit_seq(in_order)
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
        refused(format!("{name}:{variants:?}:{}", type_name::<V::Value>()))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        refused(type_name::<V::Value>().into())
    }
}

#[derive(Debug)]
struct Seq<'de, 'a> {
    de: &'a mut Decoder<'de>,
    length: Option<usize>,
}

impl<'de, 'a> Seq<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, length: Option<usize>) -> Self {
        Seq { de, length }
    }
}

impl<'de> SeqAccess<'de> for Seq<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let next = type_name_of_val(&seed);
        tracing::debug!("seq, next seed: {next}, length: {:?}", self.length);
        if let Some(left) = self.length {
            let Some(after) = left.checked_sub(1) else {
                return Ok(None);
            };
            self.length = Some(after);
        }
        T::deserialize(seed, &mut *self.de).map(Some)
    }
}

#[derive(Debug)]
struct Struct<'de, 'a> {
    de: &'a mut Decoder<'de>,
}

impl<'de, 'a> Struct<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>) -> Self {
        Struct { de }
    }
}

impl<'de> SeqAccess<'de> for Struct<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let next = type_name_of_val(&seed);
        tracing::debug!("seed: {next}");
        T::deserialize(seed, &mut *self.de).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::tagged::ser::Encoder;
    use serde::{Deserialize, Serialize, de::DeserializeOwned};
    use std::{collections::BTreeMap, fmt::Debug, io::Cursor};

    /// Reads a value back out of a tag's payload, which is the whole of this
    /// deserializer's contract.
    fn decoded<T>(encoded: &[u8]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut reader = Cursor::new(encoded);
        let mut decoder = Decoder::new(&mut reader);
        T::deserialize(&mut decoder)
    }

    /// Encodes and reads back, which is how a tag is actually used: written by
    /// `TagField::encode` and read by `TagBuffer::decode`.
    fn round_trip<T>(value: T) -> Result<()>
    where
        T: Serialize + DeserializeOwned + Debug + PartialEq,
    {
        let encoded = Encoder::encode(&value)?;
        assert_eq!(value, decoded::<T>(&encoded)?);
        Ok(())
    }

    /// The error a form outside the Kafka primitive set produces.
    fn refused<T>(encoded: &[u8]) -> Error
    where
        T: DeserializeOwned + Debug,
    {
        decoded::<T>(encoded).expect_err("a form this codec cannot read")
    }

    /// A visitor that answers nothing, for the arms that refuse before they
    /// ever reach one.
    struct Nothing;

    impl<'de> Visitor<'de> for Nothing {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("nothing")
        }
    }

    /// A string asked for by the borrowed arm.
    ///
    /// `serde`'s own `&str` impl refuses a transient string, so reaching
    /// `deserialize_str` at all takes a visitor that accepts one.
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

    #[test]
    fn fixed_width_numbers_round_trip_big_endian() -> Result<()> {
        round_trip(true)?;
        round_trip(false)?;
        round_trip(-1i8)?;
        round_trip(-2i16)?;
        round_trip(-3i32)?;
        round_trip(-4i64)?;
        round_trip(7u8)?;
        round_trip(8u16)?;
        round_trip(9u32)?;
        round_trip(10u64)?;
        round_trip(1.5f32)?;
        round_trip(2.5f64)?;

        assert_eq!(-3i32, decoded::<i32>(&[255, 255, 255, 253])?);

        Ok(())
    }

    /// A varint of `len + 1` and then the bytes, over the whole varint range.
    ///
    /// The 200-byte case is the continuation loop, which no other case here
    /// enters.
    #[test]
    fn a_compact_string_round_trips_at_any_length() -> Result<()> {
        round_trip(String::new())?;
        round_trip("abc".to_owned())?;
        round_trip("z".repeat(200))?;

        Ok(())
    }

    /// The borrowed arm reads the same compact string as the owned one.
    ///
    /// Nothing in the tree deserializes a tag payload as `&str` — every field
    /// the generator emits is a `String` — so `deserialize_str` is reachable
    /// only by a caller asking for it, which is what this does.
    #[test]
    fn the_borrowed_arm_reads_the_same_string_as_the_owned_one() -> Result<()> {
        assert_eq!("abc", decoded::<Str>(&Encoder::encode(&"abc")?)?.0);

        Ok(())
    }

    /// A length past the maximum message size is refused before it is
    /// allocated.
    ///
    /// The length is a varint the peer chose, so a five-byte tag payload can
    /// ask for four gigabytes. The guard is what stops `vec![0u8; length]`
    /// running first and the read failing afterwards. Both string arms carry
    /// it: until #556 only the owned one did.
    #[test]
    fn a_length_past_the_maximum_message_size_is_refused_before_it_is_allocated() {
        let hostile = [0x82, 0x80, 0x80, 0x80, 0x04];

        for error in [refused::<String>(&hostile), refused::<Str>(&hostile)] {
            assert!(
                matches!(error, Error::MessageMaxSizeExceeded(length)
                    if length == MESSAGE_MAX_SIZE + 1),
                "expected MessageMaxSizeExceeded, got {error:?}"
            );
        }
    }

    /// A nullable string round trips; a nullable number cannot.
    ///
    /// The presence marker *is* the compact length — zero for null, `len + 1`
    /// otherwise — so it only exists for the forms that carry one: strings,
    /// bytes and arrays. Kafka has no nullable number, and this is what asking
    /// for one does: the serializer writes the four bytes of the `i32` with no
    /// marker in front, and the deserializer reads the first of them as the
    /// marker. `Some(0)` comes back as `None`, and nothing errors.
    #[test]
    fn a_nullable_string_round_trips_but_a_nullable_number_reads_as_null() -> Result<()> {
        round_trip(Some("abc".to_owned()))?;
        round_trip(Option::<String>::None)?;

        assert_eq!(
            None,
            decoded::<Option<i32>>(&Encoder::encode(&Some(0i32))?)?
        );

        Ok(())
    }

    /// A compact array: a varint of `len + 1`, then the elements.
    #[test]
    fn a_sequence_round_trips() -> Result<()> {
        round_trip(Vec::<i32>::new())?;
        round_trip(vec![1i32, 2, 3])?;
        round_trip(vec!["a".to_owned(), "bc".to_owned()])?;

        Ok(())
    }

    /// A struct is read positionally, and a newtype struct is its inner value
    /// with nothing around it.
    #[test]
    fn a_struct_is_read_positionally() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Serialize)]
        struct Pair {
            first: i16,
            second: String,
        }

        #[derive(Debug, Deserialize, PartialEq, Serialize)]
        struct Wrapper(i32);

        round_trip(Pair {
            first: 6,
            second: "abc".to_owned(),
        })?;
        round_trip(Wrapper(7))?;

        assert_eq!(Wrapper(7), decoded::<Wrapper>(&[0, 0, 0, 7])?);

        Ok(())
    }

    /// The forms this codec cannot read are refused rather than guessed at.
    ///
    /// A tag payload is bytes with no self-description, so there is no arm here
    /// that could ask the input what it is. Refusing is the only answer that
    /// does not fabricate a value — and `bytes` is on this list while the
    /// serializer's `serialize_bytes` writes one, which makes that arm
    /// write-only.
    #[test]
    fn the_forms_this_codec_cannot_read() {
        #[derive(Debug, Deserialize)]
        struct UnitStruct;

        #[derive(Debug, Deserialize)]
        enum Variants {
            Unit,
        }

        #[derive(Debug)]
        struct Bytes;

        impl<'de> Deserialize<'de> for Bytes {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_bytes(Nothing).map(|()| Bytes)
            }
        }

        #[derive(Debug)]
        struct ByteBuf;

        impl<'de> Deserialize<'de> for ByteBuf {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_byte_buf(Nothing).map(|()| ByteBuf)
            }
        }

        #[derive(Debug)]
        struct Any;

        impl<'de> Deserialize<'de> for Any {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(Nothing).map(|()| Any)
            }
        }

        #[derive(Debug)]
        struct Identifier;

        impl<'de> Deserialize<'de> for Identifier {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_identifier(Nothing).map(|()| Self)
            }
        }

        #[derive(Debug)]
        struct IgnoredAny;

        impl<'de> Deserialize<'de> for IgnoredAny {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_ignored_any(Nothing).map(|()| Self)
            }
        }

        let payload = [1, 0, 0, 0, 0];

        for error in [
            refused::<Any>(&payload),
            refused::<char>(&payload),
            refused::<Bytes>(&payload),
            refused::<ByteBuf>(&payload),
            refused::<()>(&payload),
            refused::<UnitStruct>(&payload),
            refused::<(i32, i32)>(&payload),
            refused::<BTreeMap<i32, i32>>(&payload),
            refused::<Variants>(&payload),
            refused::<Identifier>(&payload),
            refused::<IgnoredAny>(&payload),
        ] {
            assert!(
                matches!(error, Error::UnexpectedType(_)),
                "expected UnexpectedType, got {error:?}"
            );
        }
    }

    /// A tuple struct of more than one field is refused, where a newtype struct
    /// of one is not.
    ///
    /// Kept apart from the list above because `serde`'s derive sends a
    /// one-field tuple struct to `deserialize_newtype_struct`, which this codec
    /// supports — so the refusal only shows up at two fields.
    #[test]
    fn a_tuple_struct_of_two_fields_is_refused() {
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TupleStruct(i32, i32);

        let error = refused::<TupleStruct>(&[0, 0, 0, 1, 0, 0, 0, 2]);

        assert!(
            matches!(error, Error::UnexpectedType(_)),
            "expected UnexpectedType, got {error:?}"
        );
    }

    /// An unbounded sequence reads until its elements run out.
    ///
    /// The bounded case is every compact array; this is the arm that serves a
    /// [`TagBuffer`](super::super::TagBuffer), whose count is its own first
    /// element rather than a length the codec consumed.
    #[test]
    fn a_sequence_with_no_length_reads_until_the_input_ends() -> Result<()> {
        let mut reader = Cursor::new(&[0u8, 1, 0, 2][..]);
        let mut decoder = Decoder::new(&mut reader);

        let mut seq = Seq::new(&mut decoder, None);

        assert_eq!(Some(1i16), seq.next_element::<i16>()?);
        assert_eq!(Some(2i16), seq.next_element::<i16>()?);
        assert!(seq.next_element::<i16>().is_err());

        Ok(())
    }
}
