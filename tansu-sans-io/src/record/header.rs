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

use bytes::BufMut as _;
use bytes::{Bytes, BytesMut};
use serde::Deserialize;
use serde::Serialize;

use crate::record::codec::Octets;
use crate::{ByteSize, Decode, Encode, Result};

/// A record header: an optional key and an optional value, each encoded as
/// varint-length-prefixed octets.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Header {
    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub key: Option<Bytes>,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub value: Option<Bytes>,
}

impl ByteSize for Header {
    fn size_in_bytes(&self) -> Result<usize> {
        let key_size = Octets(self.key.clone()).size_in_bytes()?;
        let value_size = Octets(self.value.clone()).size_in_bytes()?;
        Ok(key_size + value_size)
    }
}

impl Encode for Header {
    #[tracing::instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let capacity = self.size_in_bytes()?;
        let mut encoded = BytesMut::with_capacity(capacity);
        for half in [&self.key, &self.value] {
            encoded.put(Octets(half.clone()).encode()?);
        }
        Ok(encoded.freeze())
    }
}

impl Decode for Header {
    #[tracing::instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let key_octets = Octets::decode(encoded)?;
        let value_octets = Octets::decode(encoded)?;
        Ok(Self {
            key: key_octets.0,
            value: value_octets.0,
        })
    }
}

impl Header {
    pub fn builder() -> Builder {
        Default::default()
    }
}

impl From<Builder> for Header {
    fn from(value: Builder) -> Self {
        let Builder { key, value } = value;
        let (key, value) = (Option::from(key), Option::from(value));
        Self { key, value }
    }
}

/// Builds a [`Header`] one half at a time.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Builder {
    key: Octets,
    value: Octets,
}

impl Builder {
    #[must_use]
    pub fn key(mut self, key: Bytes) -> Self {
        self.key = Octets(Some(key));
        self
    }

    #[must_use]
    pub fn value(mut self, value: Bytes) -> Self {
        self.value = Octets(Some(value));
        self
    }

    #[must_use]
    pub fn build(self) -> Header {
        Header::from(self)
    }
}

impl From<Header> for Builder {
    fn from(value: Header) -> Self {
        let Header { key, value } = value;
        let (key, value) = (Octets(key), Octets(value));
        Self { key, value }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(self.key.size_in_bytes()? + self.value.size_in_bytes()?)
    }
}
