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
    fmt::{self, Formatter},
    marker::PhantomData,
};

use bytes::Bytes;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::{self, SerializeSeq as _, SerializeStruct},
};
use tracing::debug;

use crate::Error;

#[derive(Clone, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub(crate) struct MemberMetadata(ConsumerProtocolSubscription);

impl From<MemberMetadata> for super::MemberMetadata {
    fn from(value: MemberMetadata) -> Self {
        let version = *value.0.as_ref();

        Self {
            version,
            subscription: value.0.into(),
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
        Ok(Self(match value.version {
            0 => ConsumerProtocolSubscription::V0(ConsumerProtocolSubscriptionV0 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
            }),

            1 => ConsumerProtocolSubscription::V1(ConsumerProtocolSubscriptionV1 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
            }),

            2 => ConsumerProtocolSubscription::V2(ConsumerProtocolSubscriptionV2 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
                generation_id: value.subscription.generation_id.unwrap_or(-1),
            }),

            3 => ConsumerProtocolSubscription::V3(ConsumerProtocolSubscriptionV3 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
                generation_id: value.subscription.generation_id.unwrap_or(-1),
                rack_id: value.subscription.rack_id.into(),
            }),

            version => {
                return Err(Error::InvalidConsumerProtocolSubscriptionVersion(version));
            }
        }))
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
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()
                    .and_then(|version| version.ok_or_else(|| de::Error::custom("length")))
                    .and_then(|version| {
                        match version {
                            0 => seq
                                .next_element::<ConsumerProtocolSubscriptionV0>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v0")))
                                .map(ConsumerProtocolSubscription::V0),

                            1 => seq
                                .next_element::<ConsumerProtocolSubscriptionV1>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v1")))
                                .map(ConsumerProtocolSubscription::V1),

                            2 => seq
                                .next_element::<ConsumerProtocolSubscriptionV2>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v2")))
                                .map(ConsumerProtocolSubscription::V2),

                            3 => seq
                                .next_element::<ConsumerProtocolSubscriptionV3>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v3")))
                                .map(ConsumerProtocolSubscription::V3),

                            version => Err(de::Error::custom(format!("unsupported: {version}"))),
                        }
                        .map(MemberMetadata)
                    })
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl Serialize for MemberMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("MemberMetadata", 2)?;

        s.serialize_field(
            "version",
            match self.0 {
                ConsumerProtocolSubscription::V0(_) => &0i16,
                ConsumerProtocolSubscription::V1(_) => &1i16,
                ConsumerProtocolSubscription::V2(_) => &2i16,
                ConsumerProtocolSubscription::V3(_) => &3i16,
            },
        )?;

        match self.0 {
            ConsumerProtocolSubscription::V0(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V1(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V2(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V3(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }
        }

        s.end()
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
            ConsumerProtocolSubscription::V0(_) => &super::ConsumerProtocolSubscription::V0,
            ConsumerProtocolSubscription::V1(_) => &super::ConsumerProtocolSubscription::V1,
            ConsumerProtocolSubscription::V2(_) => &super::ConsumerProtocolSubscription::V2,
            ConsumerProtocolSubscription::V3(_) => &super::ConsumerProtocolSubscription::V3,
        }
    }
}

impl From<ConsumerProtocolSubscription> for super::ConsumerProtocolSubscription {
    fn from(value: ConsumerProtocolSubscription) -> Self {
        match value {
            ConsumerProtocolSubscription::V0(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: None,
                generation_id: None,
                rack_id: None,
            },

            ConsumerProtocolSubscription::V1(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: None,
                rack_id: None,
            },

            ConsumerProtocolSubscription::V2(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: Some(subscription.generation_id),
                rack_id: None,
            },

            ConsumerProtocolSubscription::V3(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: Some(subscription.generation_id),
                rack_id: subscription.rack_id.into(),
            },
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
        Self {
            topic: value.topic.into(),
            partitions: value.partitions.into(),
        }
    }
}

impl From<TopicPartition> for super::TopicPartition {
    fn from(value: TopicPartition) -> Self {
        Self {
            topic: value.topic.into(),
            partitions: value.partitions.into(),
        }
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct MemberAssignment {
    version: i16,
    assignment: ConsumerProtocolAssignment,
}

impl From<super::MemberAssignment> for MemberAssignment {
    fn from(value: super::MemberAssignment) -> Self {
        Self {
            version: value.version,
            assignment: value.assignment.into(),
        }
    }
}

impl From<MemberAssignment> for super::MemberAssignment {
    fn from(value: MemberAssignment) -> Self {
        Self {
            version: value.version,
            assignment: value.assignment.into(),
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
        Self {
            assigned_partitions: value.assigned_partitions.into(),
            user_data: value.user_data.into(),
        }
    }
}

impl From<ConsumerProtocolAssignment> for super::ConsumerProtocolAssignment {
    fn from(value: ConsumerProtocolAssignment) -> Self {
        Self {
            assigned_partitions: value.assigned_partitions.into(),
            user_data: value.user_data.into(),
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
        Self(value.into_iter().map(Into::into).collect())
    }
}

impl<T, U> From<Sequence<T>> for Vec<U>
where
    T: Into<U>,
{
    fn from(value: Sequence<T>) -> Self {
        value.0.into_iter().map(Into::into).collect()
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
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;

        u32::try_from(self.0.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in self.0.iter() {
            s.serialize_element(i)?;
        }
        s.end()
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
                formatter.write_str(stringify!(Sequence))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i32>()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("length"))
                    .inspect(|length| debug!("length: {length}"))
                    .and_then(|length| {
                        if length > 1024 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "consumer maximum array length: {}",
                                length
                            )))
                        } else {
                            (0..length).try_fold(
                                Vec::with_capacity(length.try_into().map_err(|e| {
                                    <A::Error as de::Error>::custom(format!(
                                        "length: {length}, caused: {e:?}"
                                    ))
                                })?),
                                |mut acc, _| {
                                    seq.next_element::<T>()?
                                        .ok_or_else(|| <A::Error as de::Error>::custom("item"))
                                        .map(|t| {
                                            acc.push(t);
                                            acc
                                        })
                                },
                            )
                        }
                    })
                    .map(Sequence)
            }
        }

        deserializer
            .deserialize_seq(V(PhantomData))
            .inspect_err(|err| debug!(?err))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct U16String(String);

impl Serialize for U16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;

        u16::try_from(self.0.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in self.0.as_bytes() {
            s.serialize_element(&i)?;
        }

        s.end()
    }
}

impl From<String> for U16String {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<U16String> for String {
    fn from(value: U16String) -> Self {
        value.0
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
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        if length > 4096 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "maximum string size: {}",
                                length
                            )))
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                        }
                    })
                    .and_then(|bytes| {
                        String::from_utf8(bytes)
                            .map_err(|_| <A::Error as de::Error>::custom("from_utf8"))
                    })
                    .map(U16String)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct NullableU16String(Option<String>);

impl Serialize for NullableU16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(self.0.as_ref().map(|bytes| bytes.len()))?;

        if let Some(bytes) = self.0.as_deref().map(|s| s.as_bytes()) {
            u16::try_from(bytes.len())
                .map_err(|e| ser::Error::custom(e.to_string()))
                .and_then(|length| s.serialize_element(&length))?;

            for i in bytes {
                s.serialize_element(&i)?;
            }
        } else {
            s.serialize_element(&-1i16)?;
        }

        s.end()
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
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        let q = i16::MAX;

                        if length == -1 {
                            Ok(None)
                        } else if length > 4096 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "maximum string size: {}",
                                length
                            )))
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .and_then(|bytes| {
                                    String::from_utf8(bytes)
                                        .map_err(|_| <A::Error as de::Error>::custom("from_utf8"))
                                })
                                .map(Some)
                        }
                    })
                    .map(NullableU16String)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Option<String>> for NullableU16String {
    fn from(value: Option<String>) -> Self {
        Self(value)
    }
}

impl From<NullableU16String> for Option<String> {
    fn from(value: NullableU16String) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Octets(Option<Bytes>);

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(self.0.as_ref().map(|bytes| bytes.len()))?;

        if let Some(ref bytes) = self.0 {
            u32::try_from(bytes.len())
                .map_err(|e| ser::Error::custom(e.to_string()))
                .and_then(|length| s.serialize_element(&length))?;

            for i in bytes {
                s.serialize_element(&i)?;
            }
        } else {
            s.serialize_element(&-1i32)?;
        }

        s.end()
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
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i32>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        if length == -1 {
                            Ok(None)
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map(Bytes::from)
                                .map(Some)
                        }
                    })
                    .map(Octets)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Octets(value)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        value.0
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
