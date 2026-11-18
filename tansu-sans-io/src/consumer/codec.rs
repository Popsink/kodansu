// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
// Copyright ⓒ 2026 Popsink SAS
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

use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::num::TryFromIntError;

use bytes::Bytes;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::{self, SerializeSeq, SerializeStruct as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/// A decoding error saying `why`.
fn refused<E: de::Error>(why: impl Display) -> E {
    E::custom(why)
}

/// `found`, or an error saying `missing` when the sequence had run out.
fn required<T, E: de::Error>(found: Option<T>, missing: &str) -> Result<T, E> {
    found.ok_or_else(|| refused(missing))
}

/// The next `length` bytes of `seq`, none when `length` is negative.
fn octets<'de, A>(seq: &mut A, length: i32) -> Result<Vec<u8>, A::Error>
where
    A: SeqAccess<'de>,
{
    let mut read = Vec::new();
    for _ in 0..length {
        read.push(required(seq.next_element::<u8>()?, "byte")?);
    }
    Ok(read)
}

/// `length` as the length prefix `P`, an error when it does not fit.
fn prefix<P, E>(length: usize) -> Result<P, E>
where
    P: TryFrom<usize, Error = TryFromIntError>,
    E: ser::Error,
{
    P::try_from(length).map_err(|overflow| E::custom(overflow.to_string()))
}

/// `bytes` written into `seq` one element each, after their `P` length.
fn written<P, S>(seq: &mut S, bytes: &[u8]) -> Result<(), S::Error>
where
    P: Serialize + TryFrom<usize, Error = TryFromIntError>,
    S: SerializeSeq,
{
    let length: P = prefix(bytes.len())?;
    seq.serialize_element(&length)?;
    for byte in bytes {
        seq.serialize_element(byte)?;
    }
    Ok(())
}

#[derive(Clone, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub(crate) struct MemberMetadata(ConsumerProtocolSubscription);

impl From<MemberMetadata> for super::MemberMetadata {
    fn from(value: MemberMetadata) -> Self {
        let MemberMetadata(subscription) = value;
        let named = *AsRef::<i16>::as_ref(&subscription);
        let subscription = super::ConsumerProtocolSubscription::from(subscription);
        super::MemberMetadata {
            subscription,
            version: named,
        }
    }
}

/// The version a member's own subscription names, refused rather than
/// abandoned when it is one this fork does not encode (#556).
///
/// `serde`'s `into` attribute takes an infallible conversion, so for as long as
/// the public type serialized through it the only thing this could do with a
/// version outside `0..=3` was `todo!()` — an abort, in a broker, on a value a
/// caller supplies. [`Error::InvalidConsumerProtocolSubscriptionVersion`] has
/// been declared for it since the type was written; this is what constructs it.
impl TryFrom<super::MemberMetadata> for MemberMetadata {
    type Error = Error;

    fn try_from(value: super::MemberMetadata) -> Result<Self, Self::Error> {
        let super::MemberMetadata {
            version,
            subscription,
        } = value;
        let super::ConsumerProtocolSubscription {
            topics,
            user_data,
            owned_partitions,
            generation_id,
            rack_id,
        } = subscription;

        let topics = Sequence::from(topics);
        let user_data = Octets::from(user_data);
        let owned = || Sequence::from(owned_partitions.unwrap_or_default());
        let generation = generation_id.unwrap_or(-1);

        let chosen = match version {
            0 => {
                let v0 = ConsumerProtocolSubscriptionV0 { topics, user_data };
                ConsumerProtocolSubscription::V0(v0)
            }
            1 => {
                let v1 = ConsumerProtocolSubscriptionV1 {
                    owned_partitions: owned(),
                    topics,
                    user_data,
                };
                ConsumerProtocolSubscription::V1(v1)
            }
            2 => {
                let v2 = ConsumerProtocolSubscriptionV2 {
                    generation_id: generation,
                    owned_partitions: owned(),
                    topics,
                    user_data,
                };
                ConsumerProtocolSubscription::V2(v2)
            }
            3 => {
                let v3 = ConsumerProtocolSubscriptionV3 {
                    generation_id: generation,
                    owned_partitions: owned(),
                    rack_id: NullableU16String::from(rack_id),
                    topics,
                    user_data,
                };
                ConsumerProtocolSubscription::V3(v3)
            }
            unknown => return Err(Error::InvalidConsumerProtocolSubscriptionVersion(unknown)),
        };
        Ok(MemberMetadata(chosen))
    }
}

impl<'de> Deserialize<'de> for MemberMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = MemberMetadata;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let version: i16 = required(seq.next_element()?, "length")?;
                let chosen = match version {
                    0 => ConsumerProtocolSubscription::V0(required(seq.next_element()?, "v0")?),
                    1 => ConsumerProtocolSubscription::V1(required(seq.next_element()?, "v1")?),
                    2 => ConsumerProtocolSubscription::V2(required(seq.next_element()?, "v2")?),
                    3 => ConsumerProtocolSubscription::V3(required(seq.next_element()?, "v3")?),
                    unknown => return Err(refused(format!("unsupported: {unknown}"))),
                };
                Ok(MemberMetadata(chosen))
            }
        }

        Deserializer::deserialize_seq(deserializer, V)
    }
}

impl Serialize for MemberMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let MemberMetadata(subscription) = self;
        let mut fields = serializer.serialize_struct("MemberMetadata", 2)?;
        fields.serialize_field("version", AsRef::<i16>::as_ref(subscription))?;
        match subscription {
            ConsumerProtocolSubscription::V0(inner) => {
                fields.serialize_field("subscription", inner)
            }
            ConsumerProtocolSubscription::V1(inner) => {
                fields.serialize_field("subscription", inner)
            }
            ConsumerProtocolSubscription::V2(inner) => {
                fields.serialize_field("subscription", inner)
            }
            ConsumerProtocolSubscription::V3(inner) => {
                fields.serialize_field("subscription", inner)
            }
        }?;
        fields.end()
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum ConsumerProtocolSubscription {
    V0(ConsumerProtocolSubscriptionV0),
    V1(ConsumerProtocolSubscriptionV1),
    V2(ConsumerProtocolSubscriptionV2),
    V3(ConsumerProtocolSubscriptionV3),
}

impl AsRef<i16> for ConsumerProtocolSubscription {
    fn as_ref(&self) -> &i16 {
        match self {
            Self::V0(_) => &super::ConsumerProtocolSubscription::V0,
            Self::V1(_) => &super::ConsumerProtocolSubscription::V1,
            Self::V2(_) => &super::ConsumerProtocolSubscription::V2,
            Self::V3(_) => &super::ConsumerProtocolSubscription::V3,
        }
    }
}

impl From<ConsumerProtocolSubscription> for super::ConsumerProtocolSubscription {
    fn from(value: ConsumerProtocolSubscription) -> Self {
        let absent = NullableU16String::default;
        let (topics, user_data, owned, generation, rack) = match value {
            ConsumerProtocolSubscription::V0(v0) => (v0.topics, v0.user_data, None, None, absent()),
            ConsumerProtocolSubscription::V1(v1) => {
                let owned = Some(v1.owned_partitions);
                (v1.topics, v1.user_data, owned, None, absent())
            }
            ConsumerProtocolSubscription::V2(v2) => {
                let owned = Some(v2.owned_partitions);
                let generation = Some(v2.generation_id);
                (v2.topics, v2.user_data, owned, generation, absent())
            }
            ConsumerProtocolSubscription::V3(v3) => {
                let owned = Some(v3.owned_partitions);
                let generation = Some(v3.generation_id);
                (v3.topics, v3.user_data, owned, generation, v3.rack_id)
            }
        };
        super::ConsumerProtocolSubscription {
            generation_id: generation,
            owned_partitions: owned.map(Into::into),
            rack_id: Option::from(rack),
            topics: Vec::from(topics),
            user_data: Option::from(user_data),
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV0 {
    topics: Sequence<U16String>,
    user_data: Octets,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV1 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV2 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
    generation_id: i32,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV3 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
    generation_id: i32,
    rack_id: NullableU16String,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct TopicPartition {
    topic: U16String,
    partitions: Sequence<i32>,
}

impl From<super::TopicPartition> for TopicPartition {
    fn from(value: super::TopicPartition) -> Self {
        let super::TopicPartition { topic, partitions } = value;
        let partitions = Sequence::from(partitions);
        let topic = U16String::from(topic);
        TopicPartition { topic, partitions }
    }
}

impl From<TopicPartition> for super::TopicPartition {
    fn from(value: TopicPartition) -> Self {
        let TopicPartition { topic, partitions } = value;
        let partitions = Vec::from(partitions);
        let topic = String::from(topic);
        super::TopicPartition { topic, partitions }
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct MemberAssignment {
    version: i16,
    assignment: ConsumerProtocolAssignment,
}

impl From<super::MemberAssignment> for MemberAssignment {
    fn from(value: super::MemberAssignment) -> Self {
        let super::MemberAssignment {
            version,
            assignment,
        } = value;
        let assignment = ConsumerProtocolAssignment::from(assignment);
        MemberAssignment {
            assignment,
            version,
        }
    }
}

impl From<MemberAssignment> for super::MemberAssignment {
    fn from(value: MemberAssignment) -> Self {
        let MemberAssignment {
            version,
            assignment,
        } = value;
        let assignment = super::ConsumerProtocolAssignment::from(assignment);
        super::MemberAssignment {
            assignment,
            version,
        }
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
struct ConsumerProtocolAssignment {
    assigned_partitions: Sequence<TopicPartition>,
    user_data: Octets,
}

impl From<super::ConsumerProtocolAssignment> for ConsumerProtocolAssignment {
    fn from(value: super::ConsumerProtocolAssignment) -> Self {
        let super::ConsumerProtocolAssignment {
            assigned_partitions,
            user_data,
        } = value;
        ConsumerProtocolAssignment {
            assigned_partitions: Sequence::from(assigned_partitions),
            user_data: Octets::from(user_data),
        }
    }
}

impl From<ConsumerProtocolAssignment> for super::ConsumerProtocolAssignment {
    fn from(value: ConsumerProtocolAssignment) -> Self {
        let ConsumerProtocolAssignment {
            assigned_partitions,
            user_data,
        } = value;
        super::ConsumerProtocolAssignment {
            assigned_partitions: Vec::from(assigned_partitions),
            user_data: Option::from(user_data),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Sequence<T>(Vec<T>);

impl<T, U> From<Vec<T>> for Sequence<U>
where
    T: Into<U>,
{
    fn from(value: Vec<T>) -> Self {
        let converted = value.into_iter().map(T::into).collect();
        Sequence(converted)
    }
}

impl<T, U> From<Sequence<T>> for Vec<U>
where
    T: Into<U>,
{
    fn from(value: Sequence<T>) -> Self {
        let Sequence(items) = value;
        items.into_iter().map(T::into).collect()
    }
}

impl<T> Serialize for Sequence<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Sequence(items) = self;
        let mut seq = serializer.serialize_seq(Some(items.len()))?;
        let length: u32 = prefix(items.len())?;
        seq.serialize_element(&length)?;
        for item in items {
            seq.serialize_element(item)?;
        }
        seq.end()
    }
}

impl<'de, T> Deserialize<'de> for Sequence<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Sequence<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let count: i32 = required(seq.next_element()?, "length")?;
                tracing::debug!("length: {count}");
                if count > 1024 {
                    return Err(refused(format!("consumer maximum array length: {count}")));
                }
                let capacity = usize::try_from(count)
                    .map_err(|e| refused(format!("length: {count}, caused: {e:?}")))?;
                let mut items = Vec::with_capacity(capacity);
                for _ in 0..count {
                    items.push(required(seq.next_element::<T>()?, "item")?);
                }
                Ok(Sequence(items))
            }
        }

        let visitor = V(PhantomData);
        let decoded = Deserializer::deserialize_seq(deserializer, visitor);
        decoded.inspect_err(|error| tracing::debug!(err = ?error))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct U16String(String);

impl Serialize for U16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let U16String(text) = self;
        let mut seq = serializer.serialize_seq(Some(text.len()))?;
        written::<u16, _>(&mut seq, text.as_bytes())?;
        seq.end()
    }
}

impl From<String> for U16String {
    fn from(value: String) -> Self {
        U16String(value)
    }
}

impl From<U16String> for String {
    fn from(value: U16String) -> Self {
        let U16String(text) = value;
        text
    }
}

impl<'de> Deserialize<'de> for U16String {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = U16String;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let size: i16 = required(seq.next_element()?, "length")?;
                tracing::debug!(length = size);
                if size > 4096 {
                    return Err(refused(format!("maximum string size: {size}")));
                }
                let utf8 = octets(&mut seq, i32::from(size))?;
                let text = String::from_utf8(utf8).map_err(|_| refused("from_utf8"))?;
                Ok(U16String(text))
            }
        }

        Deserializer::deserialize_seq(deserializer, V)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct NullableU16String(Option<String>);

impl Serialize for NullableU16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let NullableU16String(text) = self;
        let mut seq = serializer.serialize_seq(text.as_ref().map(String::len))?;
        match text {
            Some(present) => written::<u16, _>(&mut seq, present.as_bytes())?,
            None => seq.serialize_element(&-1i16)?,
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for NullableU16String {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = NullableU16String;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let size: i16 = required(seq.next_element()?, "length")?;
                tracing::debug!(length = size);
                if size == -1 {
                    return Ok(NullableU16String(None));
                }
                if size > 4096 {
                    return Err(refused(format!("maximum string size: {size}")));
                }
                let utf8 = octets(&mut seq, i32::from(size))?;
                let text = String::from_utf8(utf8).map_err(|_| refused("from_utf8"))?;
                Ok(NullableU16String(Some(text)))
            }
        }

        Deserializer::deserialize_seq(deserializer, V)
    }
}

impl From<Option<String>> for NullableU16String {
    fn from(value: Option<String>) -> Self {
        NullableU16String(value)
    }
}

impl From<NullableU16String> for Option<String> {
    fn from(value: NullableU16String) -> Self {
        let NullableU16String(text) = value;
        text
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Octets(Option<Bytes>);

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Octets(data) = self;
        let mut seq = serializer.serialize_seq(data.as_ref().map(Bytes::len))?;
        match data {
            Some(present) => written::<u32, _>(&mut seq, present)?,
            None => seq.serialize_element(&-1i32)?,
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Octets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Octets;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("Bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let size: i32 = required(seq.next_element()?, "length")?;
                tracing::debug!(length = size);
                if size == -1 {
                    return Ok(Octets(None));
                }
                let data = octets(&mut seq, size)?;
                Ok(Octets(Some(Bytes::from(data))))
            }
        }

        Deserializer::deserialize_seq(deserializer, V)
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Octets(value)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        let Octets(data) = value;
        data
    }
}

#[cfg(test)]
mod tests {
    //! The consumer protocol's embedded encoding, version by version (#556).
    //!
    //! A member's subscription travels as opaque bytes inside a `JoinGroup`
    //! request — the broker's own message schema says only `bytes` — so this
    //! layout is not version-negotiated by the frame. The version is the first
    //! two bytes of the payload, and it is the *client* that chose it. Which is
    //! why every version has to be read: a `librdkafka` consumer sends v1 or
    //! v3, a Java consumer v3, and something older sends v0, all to the same
    //! broker.

    use super::*;
    use crate::consumer::{
        ConsumerProtocolSubscription as Subscription, MemberMetadata as Metadata,
        TopicPartition as Partitions,
    };

    fn subscription() -> Subscription {
        Subscription {
            topics: vec!["a".into(), "b".into()],
            user_data: Some(Bytes::from_static(b"u")),
            owned_partitions: Some(vec![Partitions {
                topic: "a".into(),
                partitions: vec![0, 1],
            }]),
            generation_id: Some(9),
            rack_id: Some("r".into()),
        }
    }

    /// Encodes a member's metadata and reads it back, which is what the broker
    /// does with the bytes a member sent.
    fn round_trip(version: i16) -> Result<Metadata, Error> {
        let metadata = Metadata {
            version,
            subscription: subscription(),
        };

        Metadata::try_from(Bytes::try_from(&metadata)?)
    }

    /// The bytes a v3 subscription travels as, field by field: every length
    /// big-endian ahead of what it counts, the rack last.
    #[test]
    fn a_v3_subscription_is_these_bytes_on_the_wire() -> Result<(), Error> {
        let metadata = Metadata {
            version: 3,
            subscription: subscription(),
        };
        let version: &[u8] = &[0, 3];
        let topics: &[u8] = &[0, 0, 0, 2, 0, 1, b'a', 0, 1, b'b'];
        let user_data: &[u8] = &[0, 0, 0, 1, b'u'];
        let owned: &[u8] = &[0, 0, 0, 1, 0, 1, b'a', 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1];
        let generation: &[u8] = &[0, 0, 0, 9];
        let rack: &[u8] = &[0, 1, b'r'];
        let expected = [version, topics, user_data, owned, generation, rack].concat();
        assert_eq!(&expected[..], &Bytes::try_from(&metadata)?[..]);
        Ok(())
    }

    /// A v0 subscription carries topics and user data, and the fields later
    /// versions added come back absent.
    ///
    /// Absent rather than defaulted is the assertion: `owned_partitions` empty
    /// and `owned_partitions` unrepresentable are different answers, and the
    /// first would tell a cooperative-sticky assignor that this member holds
    /// nothing when in fact it cannot say.
    #[test]
    fn a_v0_subscription_carries_topics_and_user_data_and_nothing_else() -> Result<(), Error> {
        let decoded = round_trip(0)?;

        assert_eq!(0, decoded.version);
        assert_eq!(vec!["a", "b"], decoded.subscription.topics);
        assert_eq!(
            Some(Bytes::from_static(b"u")),
            decoded.subscription.user_data
        );
        assert_eq!(None, decoded.subscription.owned_partitions);
        assert_eq!(None, decoded.subscription.generation_id);
        assert_eq!(None, decoded.subscription.rack_id);

        Ok(())
    }

    /// v1 adds the partitions the member already owns, which is what makes an
    /// incremental rebalance possible.
    #[test]
    fn a_v1_subscription_adds_the_partitions_the_member_owns() -> Result<(), Error> {
        let decoded = round_trip(1)?;

        assert_eq!(1, decoded.version);
        assert_eq!(
            Some(vec![Partitions {
                topic: "a".into(),
                partitions: vec![0, 1],
            }]),
            decoded.subscription.owned_partitions
        );
        assert_eq!(None, decoded.subscription.generation_id);
        assert_eq!(None, decoded.subscription.rack_id);

        Ok(())
    }

    /// v2 adds the generation the member believes it is in.
    #[test]
    fn a_v2_subscription_adds_the_generation() -> Result<(), Error> {
        let decoded = round_trip(2)?;

        assert_eq!(2, decoded.version);
        assert_eq!(Some(9), decoded.subscription.generation_id);
        assert_eq!(None, decoded.subscription.rack_id);

        Ok(())
    }

    /// v3 adds the rack, and is the whole subscription surviving unchanged.
    #[test]
    fn a_v3_subscription_adds_the_rack_and_loses_nothing() -> Result<(), Error> {
        assert_eq!(
            Metadata {
                version: 3,
                subscription: subscription(),
            },
            round_trip(3)?
        );

        Ok(())
    }

    /// A version outside `0..=3` is refused by the encoder.
    ///
    /// Before #556 this was a `todo!()`: a member metadata a caller can build
    /// with one setter aborted the process when it was written. There is no
    /// wire encoding to fall back on — the version *is* the layout — so an
    /// error is the only other answer, and
    /// [`Error::InvalidConsumerProtocolSubscriptionVersion`] had been declared
    /// and never constructed since the type was written.
    #[test]
    fn a_version_this_fork_does_not_encode_is_refused_rather_than_fatal() {
        let metadata = Metadata {
            version: 4,
            subscription: subscription(),
        };

        let error = Bytes::try_from(&metadata).expect_err("a v4 subscription");

        assert!(
            matches!(&error, Error::Message(message) if message
                .contains("InvalidConsumerProtocolSubscriptionVersion(4)")),
            "expected the version to be named, got {error:?}"
        );
    }

    /// A version outside `0..=3` is refused by the decoder as well.
    #[test]
    fn a_version_this_fork_does_not_decode_is_refused() {
        let error = Metadata::try_from(Bytes::from_static(&[0, 4])).expect_err("a v4 subscription");

        assert!(
            matches!(&error, Error::Message(message) if message.contains("unsupported: 4")),
            "expected the version to be named, got {error:?}"
        );
    }

    /// A subscription of more than 1024 topics cannot be read back.
    ///
    /// The bound is this fork's, not the protocol's: the count is four bytes a
    /// client chose, and it is read before any element, so without a ceiling it
    /// is a `Vec::with_capacity` of up to two billion from a frame that then
    /// supplies nothing. What it costs is real though — a consumer subscribing
    /// to more topics than this by name cannot join at all — and 1024 is the
    /// number that says so.
    #[test]
    fn a_subscription_of_more_than_1024_topics_cannot_be_read_back() -> Result<(), Error> {
        let metadata = Metadata {
            version: 0,
            subscription: Subscription {
                topics: (0..1025).map(|i| format!("t{i}")).collect(),
                ..Default::default()
            },
        };

        let encoded = Bytes::try_from(&metadata)?;
        let error = Metadata::try_from(encoded).expect_err("1025 topics");

        assert!(
            matches!(&error, Error::Message(message)
                if message.contains("consumer maximum array length: 1025")),
            "expected the array bound, got {error:?}"
        );

        Ok(())
    }

    /// A topic name longer than 4096 bytes cannot be read back either, by the
    /// same argument: the length precedes the bytes.
    #[test]
    fn a_topic_name_longer_than_4096_bytes_cannot_be_read_back() -> Result<(), Error> {
        let metadata = Metadata {
            version: 0,
            subscription: Subscription {
                topics: vec!["t".repeat(4097)],
                ..Default::default()
            },
        };

        let encoded = Bytes::try_from(&metadata)?;
        let error = Metadata::try_from(encoded).expect_err("a 4097 byte topic");

        assert!(
            matches!(&error, Error::Message(message)
                if message.contains("maximum string size: 4097")),
            "expected the string bound, got {error:?}"
        );

        Ok(())
    }

    /// A rack longer than 4096 bytes cannot be read back either.
    ///
    /// A separate bound from the topic name's, in a separate codec — the rack
    /// is the one nullable string in the subscription — and so a separate
    /// assertion.
    #[test]
    fn a_rack_longer_than_4096_bytes_cannot_be_read_back() -> Result<(), Error> {
        let metadata = Metadata {
            version: 3,
            subscription: Subscription {
                topics: vec!["a".into()],
                rack_id: Some("r".repeat(4097)),
                ..Default::default()
            },
        };

        let encoded = Bytes::try_from(&metadata)?;
        let error = Metadata::try_from(encoded).expect_err("a 4097 byte rack");

        assert!(
            matches!(&error, Error::Message(message)
                if message.contains("maximum string size: 4097")),
            "expected the string bound, got {error:?}"
        );

        Ok(())
    }

    /// An absent rack and an absent user data survive as absent, which is the
    /// null encoding rather than an empty one.
    #[test]
    fn an_absent_rack_is_null_and_not_empty() -> Result<(), Error> {
        let metadata = Metadata {
            version: 3,
            subscription: Subscription {
                topics: vec!["a".into()],
                user_data: None,
                owned_partitions: Some(vec![]),
                generation_id: Some(-1),
                rack_id: None,
            },
        };

        assert_eq!(metadata, Metadata::try_from(Bytes::try_from(&metadata)?)?);

        Ok(())
    }
}
