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

use std::any::{type_name, type_name_of_val};
use std::fmt::Formatter;
use std::ops::Deref;

use bytes::{Buf as _, Bytes};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::debug;

use crate::{ByteSize, Decode, Encode};
use crate::{Error, Result};

const CONTINUATION: u8 = 0x80;
const MASK: u8 = 0x7f;

/// `value`'s 7-bit groups, least significant first, with the continuation
/// bit set on every group but the last.
fn groups(value: u64) -> impl Iterator<Item = u8> {
    let mut rest = Some(value);
    std::iter::from_fn(move || {
        let left = rest?;
        let more = left >= u64::from(CONTINUATION);
        rest = more.then_some(left >> 7);
        let low = left as u8;
        Some(if more { low | CONTINUATION } else { low })
    })
}

/// Whether another group of the same integer follows `byte`.
fn continues(byte: u8) -> bool {
    byte & CONTINUATION == CONTINUATION
}

/// The groups `next` reads, gathered into a `u32`: `overflow` once a group
/// would land past bit 31, or the sum would wrap.
fn checked_u32<E>(
    mut next: impl FnMut() -> Result<u8, E>,
    overflow: impl Fn() -> E,
) -> Result<u32, E> {
    let mut shift = 0u8;
    let mut gathered = 0u32;
    loop {
        let byte = next()?;
        let placed = u32::from(byte & MASK).checked_shl(u32::from(shift));
        let sum = placed.and_then(|bits| gathered.checked_add(bits));
        gathered = sum.ok_or_else(&overflow)?;
        if !continues(byte) {
            return Ok(gathered);
        }
        shift = shift.checked_add(7).ok_or_else(&overflow)?;
    }
}

/// [`checked_u32`], gathered into a `u64`.
fn checked_u64<E>(
    mut next: impl FnMut() -> Result<u8, E>,
    overflow: impl Fn() -> E,
) -> Result<u64, E> {
    let mut shift = 0u8;
    let mut gathered = 0u64;
    loop {
        let byte = next()?;
        let placed = u64::from(byte & MASK).checked_shl(u32::from(shift));
        let sum = placed.and_then(|bits| gathered.checked_add(bits));
        gathered = sum.ok_or_else(&overflow)?;
        if !continues(byte) {
            return Ok(gathered);
        }
        shift = shift.checked_add(7).ok_or_else(&overflow)?;
    }
}

/// [`checked_u32`] with plain arithmetic, the way the signed serde visitors
/// have always gathered.
fn unchecked_u32<E>(mut next: impl FnMut() -> Result<u8, E>) -> Result<u32, E> {
    let mut shift = 0u8;
    let mut gathered = 0u32;
    loop {
        let byte = next()?;
        gathered += u32::from(byte & MASK) << shift;
        if !continues(byte) {
            return Ok(gathered);
        }
        shift += 7;
    }
}

/// [`unchecked_u32`], gathered into a `u64`.
fn unchecked_u64<E>(mut next: impl FnMut() -> Result<u8, E>) -> Result<u64, E> {
    let mut shift = 0u8;
    let mut gathered = 0u64;
    loop {
        let byte = next()?;
        gathered += u64::from(byte & MASK) << shift;
        if !continues(byte) {
            return Ok(gathered);
        }
        shift += 7;
    }
}

/// The next byte of `seq`, an error saying `missing` when it has run out.
fn element<'de, A>(seq: &mut A, missing: &str) -> Result<u8, A::Error>
where
    A: SeqAccess<'de>,
{
    let next = seq.next_element::<u8>()?;
    next.ok_or_else(|| <A::Error as serde::de::Error>::custom(missing))
}

/// `value`'s groups, written by `serializer` as a sequence of bytes.
fn emitted<S>(value: u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(None)?;
    for group in groups(value) {
        seq.serialize_element(&group)?;
    }
    seq.end()
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VarInt(pub i32);

impl From<VarInt> for i32 {
    fn from(value: VarInt) -> Self {
        let VarInt(inner) = value;
        inner
    }
}

impl Deref for VarInt {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        let VarInt(inner) = self;
        inner
    }
}

impl Decode for VarInt {
    #[tracing::instrument(skip_all, ret)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let zigzag = checked_u32(|| Ok(encoded.try_get_u8()?), || Error::Overflow)?;
        Ok(VarInt(VarInt::de_zigzag(zigzag)))
    }
}

impl Encode for VarInt {
    #[tracing::instrument]
    fn encode(&self) -> Result<Bytes> {
        let zigzag = VarInt::en_zigzag(**self);
        let encoded: Bytes = groups(u64::from(zigzag)).collect();
        debug!(encoded = ?encoded[..]);
        Ok(encoded)
    }
}

impl VarInt {
    fn en_zigzag(decoded: i32) -> u32 {
        let doubled = decoded << 1;
        let sign = decoded >> 31;
        (doubled ^ sign) as u32
    }

    fn de_zigzag(encoded: u32) -> i32 {
        let halved = (encoded >> 1) as i32;
        let sign = -((encoded & 1) as i32);
        halved ^ sign
    }

    pub fn serialize<S>(i: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        debug!(i = ?i);
        emitted(u64::from(VarInt::en_zigzag(*i)), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VarIntVisitor;

        impl<'de> Visitor<'de> for VarIntVisitor {
            type Value = i32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("VarInt")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let zigzag = unchecked_u32(|| element(&mut seq, "u8"))?;
                let decoded = VarInt::de_zigzag(zigzag);
                debug!("i: {decoded}");
                Ok(decoded)
            }
        }

        Deserializer::deserialize_seq(deserializer, VarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        let size = bs.size_in_bytes()?;
        let prefix = VarInt(i32::try_from(size)?).size_in_bytes()?;
        Ok(prefix + size)
    }
}

impl ByteSize for VarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let zigzag = VarInt::en_zigzag(**self);
        Ok(groups(u64::from(zigzag)).count())
    }
}

impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        VarInt(value)
    }
}

impl TryFrom<usize> for VarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(VarInt(i32::try_from(value)?))
    }
}

impl Serialize for VarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let VarInt(value) = self;
        VarInt::serialize(value, serializer)
    }
}

impl<'de> Deserialize<'de> for VarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        VarInt::deserialize(deserializer).map(VarInt)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LongVarInt(pub i64);

impl From<LongVarInt> for i64 {
    fn from(value: LongVarInt) -> Self {
        let LongVarInt(inner) = value;
        inner
    }
}

impl Deref for LongVarInt {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        let LongVarInt(inner) = self;
        inner
    }
}

impl Encode for LongVarInt {
    fn encode(&self) -> Result<Bytes> {
        let zigzag = LongVarInt::en_zigzag(**self);
        Ok(groups(zigzag).collect())
    }
}

impl Decode for LongVarInt {
    #[tracing::instrument(skip_all, ret)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let zigzag = checked_u64(|| Ok(encoded.try_get_u8()?), || Error::Overflow)?;
        Ok(LongVarInt(LongVarInt::de_zigzag(zigzag)))
    }
}

impl LongVarInt {
    fn en_zigzag(decoded: i64) -> u64 {
        let doubled = decoded << 1;
        let sign = decoded >> 63;
        (doubled ^ sign) as u64
    }

    fn de_zigzag(encoded: u64) -> i64 {
        let halved = (encoded >> 1) as i64;
        let sign = -((encoded & 1) as i64);
        halved ^ sign
    }

    pub fn serialize<S>(i: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        emitted(LongVarInt::en_zigzag(*i), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LongVarIntVisitor;

        impl<'de> Visitor<'de> for LongVarIntVisitor {
            type Value = i64;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("LongVarInt")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let zigzag = unchecked_u64(|| element(&mut seq, "u8"))?;
                Ok(LongVarInt::de_zigzag(zigzag))
            }
        }

        Deserializer::deserialize_seq(deserializer, LongVarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        let size = bs.size_in_bytes()?;
        let prefix = LongVarInt(i64::try_from(size)?).size_in_bytes()?;
        Ok(prefix + size)
    }
}

impl ByteSize for LongVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let zigzag = LongVarInt::en_zigzag(**self);
        Ok(groups(zigzag).count())
    }
}

impl From<i64> for LongVarInt {
    fn from(value: i64) -> Self {
        LongVarInt(value)
    }
}

impl TryFrom<usize> for LongVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(LongVarInt(i64::try_from(value)?))
    }
}

impl Serialize for LongVarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let LongVarInt(value) = self;
        LongVarInt::serialize(value, serializer)
    }
}

impl<'de> Deserialize<'de> for LongVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        LongVarInt::deserialize(deserializer).map(LongVarInt)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnsignedVarInt(pub u32);

impl Deref for UnsignedVarInt {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        let UnsignedVarInt(inner) = self;
        inner
    }
}

impl Encode for UnsignedVarInt {
    fn encode(&self) -> Result<Bytes> {
        Ok(groups(u64::from(**self)).collect())
    }
}

impl UnsignedVarInt {
    #[tracing::instrument(skip(serializer), fields(serializer = type_name::<S>()))]
    pub fn serialize<S>(i: &u32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut left = *i;
        let mut seq = serializer.serialize_seq(None)?;
        for group in groups(u64::from(*i)) {
            debug!(v = left);
            seq.serialize_element(&group)?;
            left >>= 7;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = u32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("UnsignedVarInt")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let access = type_name_of_val(&seq);
                debug!("seq: {access}");
                let read = || {
                    let got = element(&mut seq, "byte")?;
                    debug!("byte: {got}");
                    Ok(got)
                };
                let overflow = || <A::Error as serde::de::Error>::custom("overflow");
                let gathered = checked_u32(read, overflow)?;
                debug!("accumulator: {gathered}");
                Ok(gathered)
            }
        }

        let source = type_name_of_val(&deserializer);
        debug!("deserializer: {source}");
        Deserializer::deserialize_seq(deserializer, V)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        let size = bs.size_in_bytes()?;
        let prefix = UnsignedVarInt(u32::try_from(size)?).size_in_bytes()?;
        Ok(prefix + size)
    }
}

impl ByteSize for UnsignedVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(groups(u64::from(**self)).count())
    }
}

impl From<u32> for UnsignedVarInt {
    fn from(value: u32) -> Self {
        UnsignedVarInt(value)
    }
}

impl From<UnsignedVarInt> for u32 {
    fn from(value: UnsignedVarInt) -> Self {
        let UnsignedVarInt(inner) = value;
        inner
    }
}

impl From<UnsignedVarInt> for usize {
    fn from(value: UnsignedVarInt) -> Self {
        let UnsignedVarInt(inner) = value;
        inner as usize
    }
}

impl TryFrom<usize> for UnsignedVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(UnsignedVarInt(u32::try_from(value)?))
    }
}

impl Serialize for UnsignedVarInt {
    #[tracing::instrument(skip_all, fields(serializer = type_name::<S>()))]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let UnsignedVarInt(value) = self;
        UnsignedVarInt::serialize(value, serializer)
    }
}

impl<'de> Deserialize<'de> for UnsignedVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        UnsignedVarInt::deserialize(deserializer).map(UnsignedVarInt)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::ser::Encoder;

    /// `value` written through the serde [`Encoder`].
    fn serialized(value: &impl Serialize) -> Result<Bytes> {
        let mut encoder = Encoder::new(BytesMut::new());
        value.serialize(&mut encoder)?;
        Ok(Bytes::from(encoder))
    }

    /// Kafka's zigzag: small magnitudes of either sign take one byte, and a
    /// group past the seventh bit sets the continuation bit.
    #[test]
    fn a_var_int_encodes_zigzagged_and_decodes_back() -> Result<()> {
        let wire: [(i32, &[u8]); 5] = [
            (0, &[0]),
            (-1, &[1]),
            (1, &[2]),
            (300, &[0xd8, 0x04]),
            (i32::MIN, &[0xff, 0xff, 0xff, 0xff, 0x0f]),
        ];
        for (value, bytes) in wire {
            let mut encoded = VarInt(value).encode()?;
            assert_eq!(bytes, &encoded[..], "{value}");
            assert_eq!(bytes.len(), VarInt(value).size_in_bytes()?, "{value}");
            assert_eq!(bytes, &serialized(&VarInt(value))?[..], "{value}");
            assert_eq!(value, i32::from(VarInt::decode(&mut encoded)?), "{value}");
        }
        Ok(())
    }

    #[test]
    fn a_long_var_int_round_trips_its_extremes() -> Result<()> {
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            let mut encoded = LongVarInt(value).encode()?;
            assert_eq!(encoded.len(), LongVarInt(value).size_in_bytes()?);
            assert_eq!(encoded, serialized(&LongVarInt(value))?);
            assert_eq!(value, i64::from(LongVarInt::decode(&mut encoded)?));
        }
        assert_eq!(10, LongVarInt(i64::MIN).size_in_bytes()?);
        Ok(())
    }

    #[test]
    fn an_unsigned_var_int_is_not_zigzagged() -> Result<()> {
        assert_eq!(&[1u8], &serialized(&UnsignedVarInt(1))?[..]);
        let widest = UnsignedVarInt(u32::MAX);
        assert_eq!(&[0xff, 0xff, 0xff, 0xff, 0x0f], &widest.encode()?[..]);
        assert_eq!(widest.encode()?, serialized(&widest)?);
        assert_eq!(5, widest.size_in_bytes()?);
        Ok(())
    }

    /// A group shifted past the 32nd bit is an overflow, not a wrap.
    #[test]
    fn a_var_int_too_long_to_fit_is_an_overflow() {
        let mut encoded = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert!(matches!(VarInt::decode(&mut encoded), Err(Error::Overflow)));
    }

    #[test]
    fn the_inclusive_size_counts_the_length_prefix() -> Result<()> {
        assert_eq!(3, VarInt::size_inclusive(&VarInt(300))?);
        assert_eq!(
            6,
            UnsignedVarInt::size_inclusive(&UnsignedVarInt(u32::MAX))?
        );
        Ok(())
    }
}
