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

//! The protocol's `uuid` type: a topic id, a client instance id.

/// Bytes in a UUID, on the wire and in memory.
const LEN: usize = 16;

/// Sixteen raw bytes, in wire order.
///
/// A newtype struct, which the codec writes as its inner value with nothing
/// around it: the sixteen bytes go on the wire as they are, with no length
/// prefix. The all-zero value, which is also [`Default`], is Kafka's "no id".
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct Uuid(pub [u8; LEN]);

impl From<[u8; LEN]> for Uuid {
    fn from(bytes: [u8; LEN]) -> Self {
        Uuid(bytes)
    }
}

impl crate::ByteSize for Uuid {
    /// Always 16: the encoding is fixed width.
    fn size_in_bytes(&self) -> crate::Result<usize> {
        Ok(LEN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ByteSize, Result, de::Decoder, ser::Encoder};
    use bytes::BytesMut;
    use std::io::Cursor;

    const ID: [u8; LEN] = [
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ];

    #[test]
    fn a_uuid_is_sixteen_bytes_on_the_wire_with_no_prefix() -> Result<()> {
        let mut encoder = Encoder::new(BytesMut::new());
        serde::Serialize::serialize(&Uuid::from(ID), &mut encoder)?;
        let wire = BytesMut::from(encoder);
        assert_eq!(ID.as_slice(), &wire[..]);
        assert_eq!(wire.len(), Uuid::from(ID).size_in_bytes()?);

        let mut reader = Cursor::new(&wire[..]);
        assert_eq!(
            Uuid(ID),
            <Uuid as serde::Deserialize>::deserialize(&mut Decoder::new(&mut reader))?
        );

        Ok(())
    }

    #[test]
    fn the_default_uuid_is_all_zero() {
        assert_eq!([0; 16], Uuid::default().0);
    }
}
