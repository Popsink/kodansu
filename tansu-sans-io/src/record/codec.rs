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

use std::fmt::{self, Formatter};
use std::marker::PhantomData;

use bytes::BufMut;
use bytes::{Bytes, BytesMut};
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::{self, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::primitive::varint::{UnsignedVarInt, VarInt};
use crate::{ByteSize, Decode, Encode, Result};

/// The serializer's error for a length that does not fit the wire's type.
fn too_long<E: ser::Error>(error: impl fmt::Display) -> E {
    E::custom(error.to_string())
}

/// The next element of `seq`, an error naming `what` where there is none.
fn required<'de, A, T>(seq: &mut A, what: &'static str) -> Result<T, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    seq.next_element()?.ok_or_else(|| de::Error::custom(what))
}

/// The next `length` elements of `seq`, after the count that announced them.
fn elements<'de, A, T>(seq: &mut A, length: i32) -> Result<Vec<T>, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    let capacity = usize::try_from(length).map_err(|error| {
        let caused = format!("length: {length}, caused: {error:?}");
        <A::Error as de::Error>::custom(caused)
    })?;
    let mut items = Vec::with_capacity(capacity);
    for _ in 0..length {
        items.push(required(seq, "item")?);
    }
    Ok(items)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Octets(pub Option<Bytes>);

impl ByteSize for Octets {
    fn size_in_bytes(&self) -> Result<usize> {
        let Some(present) = &self.0 else {
            return VarInt(-1).size_in_bytes();
        };
        let prefix = VarInt::try_from(present.len())?.size_in_bytes()?;
        Ok(prefix + present.len())
    }
}

impl Encode for Octets {
    #[tracing::instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let Some(data) = &self.0 else {
            return VarInt::from(-1).encode();
        };
        let mut encoded = BytesMut::with_capacity(self.size_in_bytes()?);
        encoded.put(VarInt::try_from(data.len())?.encode()?);
        encoded.put(data.clone());
        Ok(encoded.freeze())
    }
}

impl Decode for Octets {
    #[tracing::instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let VarInt(length) = VarInt::decode(encoded)?;
        if length == -1 {
            return Ok(Octets(None));
        }
        let taken = encoded.split_to(length as usize);
        Ok(Octets(Some(taken)))
    }
}

impl Octets {
    #[must_use]
    pub fn empty() -> Self {
        Octets(None)
    }

    pub fn serialize<S>(i: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(bytes) = i else {
            let mut absent = serializer.serialize_struct("Octets", 1)?;
            absent.serialize_field("length", &VarInt(-1))?;
            return absent.end();
        };

        let mut present = serializer.serialize_struct("Octets", 2)?;
        let length = i32::try_from(bytes.len()).map_err(too_long);
        let wrote_length = length.and_then(|n| present.serialize_field("length", &VarInt(n)));
        let wrote_data = present.serialize_field("data", &bytes);
        let ended = present.end();
        wrote_length.and(wrote_data).and(ended)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<Bytes>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let VarInt(length) = required(&mut seq, "length")?;
                tracing::debug!(length = ?length);
                if length == -1 {
                    return Ok(None);
                }

                let capacity = usize::try_from(length)
                    .map_err(|error| de::Error::custom(format!("{error:?}")))?;
                let mut collected = BytesMut::with_capacity(capacity);
                for _ in 0..length {
                    collected.put_u8(required(&mut seq, "byte")?);
                }
                tracing::debug!(r = ?collected);
                Ok(Some(collected.freeze()))
            }
        }

        D::deserialize_seq(deserializer, V)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        let Octets(bytes) = value;
        bytes
    }
}

impl From<Bytes> for Octets {
    fn from(value: Bytes) -> Self {
        Octets(Some(value))
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Octets(value)
    }
}

impl From<Option<Vec<u8>>> for Octets {
    fn from(value: Option<Vec<u8>>) -> Self {
        Octets(value.map(Bytes::from))
    }
}

impl From<Vec<u8>> for Octets {
    fn from(value: Vec<u8>) -> Self {
        Octets(Some(Bytes::from(value)))
    }
}

impl From<&[u8]> for Octets {
    fn from(value: &[u8]) -> Self {
        let copied = Bytes::copy_from_slice(value);
        Octets(Some(copied))
    }
}

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Octets::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Octets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Octets::deserialize(deserializer).map(Octets)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VarIntSequence<T>(pub Vec<T>);

impl<T> IntoIterator for VarIntSequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let VarIntSequence(items) = self;
        items.into_iter()
    }
}

impl<T> FromIterator<T> for VarIntSequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        VarIntSequence(iter.into_iter().collect())
    }
}

impl<T> From<VarIntSequence<T>> for Vec<T> {
    fn from(value: VarIntSequence<T>) -> Self {
        let VarIntSequence(items) = value;
        items
    }
}

impl<T> From<Vec<T>> for VarIntSequence<T> {
    fn from(value: Vec<T>) -> Self {
        VarIntSequence(value)
    }
}

impl<T> ByteSize for VarIntSequence<T>
where
    T: ByteSize,
{
    #[tracing::instrument(skip_all, ret)]
    fn size_in_bytes(&self) -> Result<usize> {
        let count = i32::try_from(self.0.len())?;
        let mut total = VarInt(count).size_in_bytes()?;
        for item in &self.0 {
            total += item.size_in_bytes()?;
        }
        Ok(total)
    }
}

impl<T> Encode for VarIntSequence<T>
where
    T: Encode + ByteSize,
{
    #[tracing::instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = BytesMut::with_capacity(self.size_in_bytes()?);
        encoded.put(VarInt::try_from(self.0.len())?.encode()?);
        for item in &self.0 {
            encoded.put(item.encode()?);
        }
        Ok(encoded.freeze())
    }
}

impl<T> Decode for VarIntSequence<T>
where
    T: Decode,
{
    #[tracing::instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        tracing::debug!(encoded = ?&encoded[..]);
        let VarInt(count) = VarInt::decode(encoded)?;
        let length = count as usize;
        tracing::debug!(length);

        let mut decoded = Vec::with_capacity(length);
        for _ in 0..length {
            decoded.push(T::decode(encoded)?);
        }
        Ok(VarIntSequence(decoded))
    }
}

impl<T> VarIntSequence<T> {
    pub(crate) fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        let mut sequence = serializer.serialize_seq(Some(v.len()))?;
        let length = i32::try_from(v.len()).map_err(too_long)?;
        sequence.serialize_element(&VarInt(length))?;
        for item in v {
            sequence.serialize_element(&item)?;
        }
        sequence.end()
    }
}

impl<T> VarIntSequence<T>
where
    T: Serialize,
{
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let VarInt(length) = required(&mut seq, "length")?;
                tracing::debug!("length: {length}");
                elements(&mut seq, length)
            }
        }

        D::deserialize_seq(deserializer, V(PhantomData))
    }
}

impl<T: Serialize> Serialize for VarIntSequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        VarIntSequence::<T>::serialize(&self.0, serializer)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sequence<T>(pub Vec<T>);

impl<T> IntoIterator for Sequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let Sequence(items) = self;
        items.into_iter()
    }
}

impl<T> FromIterator<T> for Sequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Sequence(iter.into_iter().collect())
    }
}

impl<T> From<Vec<T>> for Sequence<T> {
    fn from(value: Vec<T>) -> Self {
        Sequence(value)
    }
}

impl<T: Serialize> Sequence<T> {
    pub(crate) fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(v.len()))?;
        let length = u32::try_from(v.len()).map_err(too_long)?;
        sequence.serialize_element(&length)?;
        for item in v {
            sequence.serialize_element(&item)?;
        }
        sequence.end()
    }
}

impl<T> Sequence<T> {
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let length: i32 = required(&mut seq, "length")?;
                tracing::debug!("length: {length}");
                elements(&mut seq, length)
            }
        }

        D::deserialize_seq(deserializer, V(PhantomData))
    }
}

impl<T> ByteSize for Sequence<T>
where
    T: ByteSize,
{
    fn size_in_bytes(&self) -> Result<usize> {
        let mut total = size_of::<u32>();
        for item in &self.0 {
            total += UnsignedVarInt::size_inclusive(item)?;
        }
        Ok(total)
    }
}

impl<T> Extend<T> for Sequence<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let Sequence(items) = self;
        items.extend(iter);
    }
}

impl<T: Serialize> Serialize for Sequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Sequence::<T>::serialize(&self.0, serializer)
    }
}

impl<'de, T> Deserialize<'de> for Sequence<T>
where
    T: Deserialize<'de>,
    T: Default,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Sequence::<T>::deserialize(deserializer).map(Sequence)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use bytes::Bytes;
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::{Octets, VarIntSequence};
    use crate::{ByteSize, Decode, Encode, Error, Result};

    /// Logs this test at `debug` for this crate, spans included, on top of
    /// `RUST_LOG`, into `../logs/<package>/<thread>.log`: one file per test.
    fn init_tracing() -> Result<tracing::subscriber::DefaultGuard> {
        let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
        let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

        let unnamed = || Error::Message(String::from("unnamed thread"));
        let current = std::thread::current();
        let test = current.name().ok_or_else(unnamed)?;
        let log = std::fs::File::create(format!("../logs/{}/{test}.log", env!("CARGO_PKG_NAME")))?;

        let subscriber = tracing_subscriber::fmt().with_level(true);
        let subscriber = subscriber.with_line_number(true).with_thread_names(false);
        let subscriber = subscriber.with_span_events(FmtSpan::FULL);
        let subscriber = subscriber.with_env_filter(filter);
        let subscriber = subscriber.with_writer(std::sync::Arc::new(log));
        Ok(tracing::subscriber::set_default(subscriber.finish()))
    }

    /// `value` takes `size` bytes, and decodes back to itself from what it
    /// encodes to, leaving nothing over.
    fn round_trip<T>(value: T, size: usize) -> Result<()>
    where
        T: Encode + Decode + ByteSize + Debug + PartialEq,
    {
        assert_eq!(size, value.size_in_bytes()?);
        let mut encoded = value.encode()?;
        tracing::debug!(encoded = ?&encoded[..]);
        let decoded = T::decode(&mut encoded)?;
        assert_eq!((value, 0), (decoded, encoded.len()));
        Ok(())
    }

    /// Octets holding `content`.
    fn octets(content: &'static [u8]) -> Octets {
        Octets(Some(Bytes::from_static(content)))
    }

    /// Absent octets are a length of minus one and nothing after it.
    #[test]
    fn absent_octets_are_a_length_of_minus_one() -> Result<()> {
        let _guard = init_tracing()?;
        round_trip(Octets(None), 1)
    }

    /// Present octets are their length, then their bytes.
    #[test]
    fn present_octets_are_their_length_then_their_bytes() -> Result<()> {
        let _guard = init_tracing()?;
        round_trip(octets(b"34543"), 6)
    }

    /// A varint sequence is its count, then each item: none, one and two.
    #[test]
    fn a_varint_sequence_is_its_count_then_its_items() -> Result<()> {
        let _guard = init_tracing()?;
        round_trip(VarIntSequence(Vec::<Octets>::new()), 1)?;
        round_trip(VarIntSequence(vec![octets(b"34543")]), 7)?;
        let two = vec![octets(b"34543"), octets(b"32123")];
        round_trip(VarIntSequence(two), 13)
    }
}
