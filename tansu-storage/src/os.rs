// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use object_store::{ObjectMeta, PutResult, UpdateVersion};

use crate::Version;

impl From<Version> for UpdateVersion {
    fn from(value: Version) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

/// A [`Version`] from a *listing* is not interchangeable with one from a put or
/// a head, on Azure (#421).
///
/// `object_store`'s Azure client reads `x-ms-version-id` into the version on a
/// put and on a get, but hard-codes `version: None` when converting a listed
/// `Blob` — "for consistency with S3 and GCP which don't include this"
/// (`azure/client.rs:1394`). With blob versioning enabled on the account, the
/// same object therefore yields `Some(..)` through one path and `None` through
/// the other.
///
/// Harmless today, twice over: `PutMode::Update` on Azure conditions on the
/// etag alone and ignores the version entirely (`azure/client.rs:762`), and
/// nothing here compares two [`Version`]s. It is written down because both of
/// those would have to stay true — a conditional write that started keying on
/// the version, or a cache that compared a listed version against a fetched
/// one, would work on S3 and be wrong on a versioned Azure account.
///
/// Blob versioning is off in a correct deployment either way (`docs/adls.md`).
impl From<ObjectMeta> for Version {
    fn from(value: ObjectMeta) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<UpdateVersion> for Version {
    fn from(value: UpdateVersion) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<PutResult> for Version {
    fn from(value: PutResult) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}
