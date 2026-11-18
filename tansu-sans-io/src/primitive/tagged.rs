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

mod de;
mod ser;

use std::any::{type_name, type_name_of_val};
use std::fmt::{self, Formatter};
use std::io::Cursor;
use std::ops::Deref;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ByteSize;
use crate::Result;
use crate::primitive::varint::UnsignedVarInt;

const MAXIMUM_TAGGED_FIELDS: usize = 128;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagField(pub u32, pub Vec<u8>);

/// The error for a length too big for an unsigned varint.
fn too_big<E: serde::ser::Error>(error: impl fmt::Debug) -> E {
    E::custom(format!("length too big: {error:?}"))
}

/// The next element of `seq`, an error naming `what` where there is none.
fn required<'de, A, T>(seq: &mut A, what: &'static str) -> Result<T, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    seq.next_element()?
        .ok_or_else(|| serde::de::Error::custom(what))
}

/// `count`, refused past [`MAXIMUM_TAGGED_FIELDS`].
fn bounded<E: serde::de::Error>(count: usize) -> Result<usize, E> {
    if count > MAXIMUM_TAGGED_FIELDS {
        let exceeded = format!("maximum tagged fields exceeded {count}");
        return Err(E::custom(exceeded));
    }
    Ok(count)
}

impl TagField {
    pub(crate) fn tag(&self) -> u32 {
        let TagField(tag, _) = self;
        *tag
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.1.as_slice()
    }

    #[tracing::instrument(skip(field))]
    pub fn encode(tag: u32, field: &impl Serialize) -> Result<Self> {
        let encoded = ser::Encoder::encode(field)?;
        Ok(TagField(tag, encoded))
    }
}

impl ByteSize for TagField {
    fn size_in_bytes(&self) -> Result<usize> {
        let payload = self.data().len();
        let length = UnsignedVarInt::try_from(payload)?;
        let tag = UnsignedVarInt(self.tag()).size_in_bytes()?;
        Ok(payload + tag + length.size_in_bytes()?)
    }
}

impl Serialize for TagField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        tracing::debug!(self = ?self);
        let mut elements = serializer.serialize_seq(None)?;
        elements.serialize_element(&UnsignedVarInt::from(self.tag()))?;
        let length = UnsignedVarInt::try_from(self.data().len()).map_err(too_big)?;
        elements.serialize_element(&length)?;
        for octet in self.data() {
            elements.serialize_element(octet)?;
        }
        elements.end()
    }
}

impl<'de> Deserialize<'de> for TagField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagField;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("Tag")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let named = type_name_of_val(&seq);
                tracing::debug!("seq={named}");
                let tag = u32::from(required::<A, UnsignedVarInt>(&mut seq, "tag")?);
                let length = usize::from(required::<A, UnsignedVarInt>(&mut seq, "length")?);
                let length = bounded(length)?;
                let mut data = Vec::with_capacity(length);
                for _ in 0..length {
                    data.push(required::<A, u8>(&mut seq, "byte")?);
                }
                tracing::debug!(?tag, ?data);
                Ok(TagField(tag, data))
            }
        }

        let named = type_name_of_val(&deserializer);
        tracing::debug!("deserializer={named}");
        D::deserialize_seq(deserializer, V)
    }
}

impl From<(u32, Vec<u8>)> for TagField {
    fn from(value: (u32, Vec<u8>)) -> Self {
        let (tag, data) = value;
        TagField(tag, data)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBuffer(pub Vec<TagField>);

impl TagBuffer {
    #[must_use]
    pub fn empty() -> Self {
        TagBuffer(Vec::new())
    }

    #[must_use]
    pub fn builder() -> TagBufferBuilder {
        TagBufferBuilder { tags: Vec::new() }
    }

    pub fn decode<T>(&self, tag: &u32) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        tracing::debug!("tag={tag} T={}", type_name::<T>());
        let wanted = self.0.iter().find(|field| field.tag() == *tag);
        let Some(TagField(_, encoded)) = wanted else {
            return Ok(None);
        };
        let mut reader = Cursor::new(encoded);
        T::deserialize(&mut de::Decoder::new(&mut reader)).map(Some)
    }

    pub fn encode(tags: &[(u32, impl Serialize)]) -> Result<Self> {
        let mut fields = Vec::with_capacity(tags.len());
        for (tag, field) in tags {
            let encoded = ser::Encoder::encode(field)?;
            fields.push(TagField(*tag, encoded));
        }
        Ok(TagBuffer(fields))
    }
}

impl Deref for TagBuffer {
    type Target = Vec<TagField>;

    fn deref(&self) -> &Self::Target {
        let TagBuffer(fields) = self;
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBufferBuilder {
    tags: Vec<TagField>,
}

impl TagBufferBuilder {
    #[must_use]
    pub fn tag(mut self, tag: u32, data: Vec<u8>) -> Self {
        let field = TagField(tag, data);
        self.tags.push(field);
        self
    }

    #[must_use]
    pub fn build(self) -> TagBuffer {
        let TagBufferBuilder { tags } = self;
        TagBuffer(tags)
    }
}

impl From<Vec<TagField>> for TagBuffer {
    fn from(value: Vec<TagField>) -> Self {
        TagBuffer(value)
    }
}

impl ByteSize for TagBuffer {
    fn size_in_bytes(&self) -> Result<usize> {
        let count = UnsignedVarInt::try_from(self.0.len())?;
        let mut total = count.size_in_bytes()?;
        for field in &self.0 {
            total += field.size_in_bytes()?;
        }
        Ok(total)
    }
}

impl Serialize for TagBuffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut elements = serializer.serialize_seq(None)?;
        let length = UnsignedVarInt::try_from(self.0.len()).map_err(too_big)?;
        tracing::debug!(?length);
        elements.serialize_element(&length)?;
        for field in &self.0 {
            elements.serialize_element(field)?;
        }
        elements.end()
    }
}

impl<'de> Deserialize<'de> for TagBuffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagBuffer;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("TagBuffer")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let named = type_name_of_val(&seq);
                tracing::debug!("seq={named}");
                let counted = required::<A, UnsignedVarInt>(&mut seq, "tag")?;
                let number_of_tagged_fields = usize::from(counted);
                tracing::debug!(?number_of_tagged_fields);
                let count = bounded(number_of_tagged_fields)?;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    let tag: TagField = required(&mut seq, "tagged field")?;
                    tracing::debug!(?tag);
                    fields.push(tag);
                }
                Ok(TagBuffer(fields))
            }
        }

        let named = type_name_of_val(&deserializer);
        tracing::debug!("deserializer={named}");
        D::deserialize_seq(deserializer, V)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::io::Cursor;

    use bytes::BytesMut;
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};

    use super::de::Decoder;
    use super::ser::Encoder;
    use super::{TagBuffer, TagField};
    use crate::{ByteSize as _, Error, Result, de, ser};

    /// `buffer` as the protocol serializer writes it into a frame.
    fn framed(buffer: &TagBuffer) -> Result<Vec<u8>> {
        let mut encoder = ser::Encoder::new(BytesMut::new());
        buffer.serialize(&mut encoder)?;
        Ok(BytesMut::from(encoder).to_vec())
    }

    /// `value` encodes to `wire` as a tag's payload, and decodes back from it.
    fn payload<T>(value: &T, wire: &[u8]) -> Result<()>
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let mut encoded = Vec::new();
        value.serialize(&mut Encoder::new(&mut encoded))?;
        assert_eq!(wire, &encoded[..]);
        let mut reader = Cursor::new(&encoded);
        assert_eq!(*value, T::deserialize(&mut Decoder::new(&mut reader))?);
        Ok(())
    }

    /// One field, `value`, as the payload tests encode it.
    #[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
    struct One<T> {
        value: T,
    }

    /// The `Metadata` broker shape: a struct inside an array.
    #[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
    struct Broker {
        node: i32,
        host_name: String,
        port_number: i32,
        rack_name: String,
    }

    /// The `metadata.version` feature, versions `min` to `max`, as an
    /// `ApiVersionsResponse` v3 carries it in a tag.
    fn metadata_version(min: u8, max: u8) -> Vec<u8> {
        let mut feature = vec![2, 17];
        feature.extend_from_slice(b"metadata.version");
        feature.extend_from_slice(&[0, min, 0, max, 0]);
        feature
    }

    /// A tag buffer as it appears in a frame: written by the protocol
    /// serializer and read back by the protocol deserializer.
    ///
    /// That pairing is the point. A [`TagBuffer`] states its own field count as
    /// its first element, so it needs a `deserialize_seq` that hands the
    /// visitor an *unbounded* sequence — which the protocol decoder does and
    /// the tag-value decoder in [`de`](super::de) does not, since there a
    /// sequence is a compact array whose length the codec consumes itself.
    /// Until #556 these four fixtures were `#[ignore]`d and paired with the
    /// wrong one of the two, having also encoded and decoded through a single
    /// `Cursor` without rewinding it, so all four failed on the first byte they
    /// read.
    fn round_trip(expected: &TagBuffer) -> Result<()> {
        let encoded = framed(expected)?;

        assert_eq!(encoded.len(), expected.size_in_bytes()?);

        let mut reader = Cursor::new(&encoded[..]);
        let mut decoder = de::Decoder::new(&mut reader);

        assert_eq!(*expected, TagBuffer::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn an_empty_tag_buffer_is_a_single_zero_byte() -> Result<()> {
        let expected = TagBuffer::builder().build();

        assert_eq!(TagBuffer::empty(), expected);
        assert!(expected.is_empty());
        assert_eq!(1, expected.size_in_bytes()?);

        round_trip(&expected)
    }

    #[test]
    fn a_tag_field_is_its_tag_its_length_and_its_payload() -> Result<()> {
        let expected = TagBuffer::builder().tag(55, b"pqr".to_vec()).build();
        assert_eq!(b"\x01\x37\x03pqr".to_vec(), framed(&expected)?);

        assert_eq!(TagField::from((55, b"pqr".to_vec())), expected[0]);

        round_trip(&expected)
    }

    #[test]
    fn a_tag_buffer_of_two_fields_round_trips() -> Result<()> {
        let builder = TagBuffer::builder().tag(66, (1..=5).rev().collect());
        round_trip(&builder.tag(88, b"abc".to_vec()).build())
    }

    /// The tag buffer of a real `ApiVersionsResponse` v3, as a broker sends it.
    #[test]
    fn an_api_versions_v3_tag_buffer_round_trips() -> Result<()> {
        let finalized_epoch = 76i64.to_be_bytes().to_vec();
        let builder = TagBuffer::builder().tag(0, metadata_version(1, 14));
        let builder = builder.tag(1, finalized_epoch);
        round_trip(&builder.tag(2, metadata_version(14, 14)).build())
    }

    /// A tag's value is encoded by [`TagBuffer::encode`] and read back by tag.
    ///
    /// This is the pair every caller of tagged fields uses. Note what `decode`
    /// answers for a tag that is not there: `Ok(None)`, the same as a tag
    /// carrying a null — the buffer carries no schema, so "absent" and "present
    /// and null" are the same answer.
    #[test]
    fn a_tag_is_encoded_by_tag_and_read_back_by_tag() -> Result<()> {
        let buffer = TagBuffer::encode(&[(0u32, "a-cluster".to_owned())])?;

        assert_eq!(Some("a-cluster".to_owned()), buffer.decode::<String>(&0)?);
        assert_eq!(None, buffer.decode::<String>(&1)?);

        assert_eq!(
            TagField::encode(0, &"a-cluster".to_owned())?,
            buffer.first().cloned().expect("one tag")
        );

        Ok(())
    }

    /// More than 128 tagged fields in one buffer is refused.
    ///
    /// The bound is a decision this fork inherited rather than a protocol
    /// limit: nothing in the Kafka messages says 128. What it buys is that the
    /// count — an unsigned varint a peer chose, so up to four billion — cannot
    /// be turned into a four-billion-element `Vec::with_capacity` by a frame
    /// that then supplies two bytes.
    #[test]
    fn more_than_128_tagged_fields_is_refused() {
        let mut reader = Cursor::new(vec![0x81, 0x01]);
        let mut decoder = de::Decoder::new(&mut reader);

        let error = TagBuffer::deserialize(&mut decoder).expect_err("129 tagged fields");

        assert!(
            matches!(&error, Error::Message(message)
                if message.contains("maximum tagged fields exceeded 129")),
            "expected the field-count bound, got {error:?}"
        );
    }

    /// A tag whose payload is longer than 128 bytes is refused too — by the
    /// same constant, which is the defect.
    ///
    /// `MAXIMUM_TAGGED_FIELDS` bounds two unrelated things: how many fields a
    /// buffer may carry, and how long any one of them may be. The second is
    /// wrong. A tagged field's payload is an arbitrary encoded value, and the
    /// `ApiVersionsResponse` fixture above already carries a 23-byte one, so a
    /// broker advertising a handful more features sends a frame this decoder
    /// refuses. Asserted here as the behaviour that ships, not as the behaviour
    /// that is wanted: raising the payload bound changes what frames this fork
    /// accepts, which is its own change and not a test's to make (#556).
    #[test]
    fn a_tag_payload_longer_than_128_bytes_is_refused_by_the_same_constant() {
        let mut reader = Cursor::new(vec![1, 0, 0x81, 0x01]);
        let mut decoder = de::Decoder::new(&mut reader);

        let error = TagBuffer::deserialize(&mut decoder).expect_err("a 129-byte payload");

        assert!(
            matches!(&error, Error::Message(message)
                if message.contains("maximum tagged fields exceeded 129")),
            "expected the payload-length bound, got {error:?}"
        );
    }

    #[test]
    fn a_compact_string_is_its_length_plus_one_then_its_bytes() -> Result<()> {
        let hello = One {
            value: String::from("Hello World!"),
        };
        payload(&hello, b"\x0dHello World!")?;
        payload(
            &One {
                value: String::new(),
            },
            &[1],
        )
    }

    #[test]
    fn a_compact_nullable_string_is_zero_for_none() -> Result<()> {
        payload(&One::<Option<String>> { value: None }, &[0])?;
        let hello = One {
            value: Some(String::from("Hello World!")),
        };
        payload(&hello, b"\x0dHello World!")
    }

    #[test]
    fn a_compact_array_is_its_length_plus_one_then_its_elements() -> Result<()> {
        let three = One {
            value: vec![12321i32, 54345, 78987],
        };
        let mut wire = vec![4];
        for element in &three.value {
            wire.extend_from_slice(&element.to_be_bytes());
        }
        payload(&three, &wire)?;
        payload(&One::<Vec<i32>> { value: vec![] }, &[1])
    }

    #[test]
    fn a_compact_array_of_structs_is_each_struct_in_turn() -> Result<()> {
        let (host_name, rack_name) = (String::from("abc"), String::from("pqr"));
        let (node, port_number) = (32123, 98789);
        let broker = Broker {
            node,
            host_name,
            port_number,
            rack_name,
        };
        let mut wire = vec![2];
        wire.extend_from_slice(&32123i32.to_be_bytes());
        wire.extend_from_slice(b"\x04abc");
        wire.extend_from_slice(&98789i32.to_be_bytes());
        wire.extend_from_slice(b"\x04pqr");
        payload(
            &One {
                value: vec![broker],
            },
            &wire,
        )
    }
}
