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
/// a head (#421, #512).
///
/// A listed `ObjectMeta` carries the etag and **no version**, on every backend:
/// `object_store` deserialises both the S3 and the GCS listing into the same
/// S3-shaped `ListContents`, which has no version element at all and converts to
/// `version: None` (`client/s3.rs:85`); Azure's listing has one and it is
/// hard-coded away anyway, "for consistency with S3 and GCP which don't include
/// this" (`azure/client.rs:1394`). A put and a get, by contrast, read the
/// version out of a response header on all three.
///
/// What that costs depends entirely on the backend, and the spread is the whole
/// reason this is written down:
///
/// - **S3** conditions `PutMode::Update` on the etag. A listed version works.
/// - **Azure** conditions on the etag too and ignores `version` outright
///   (`azure/client.rs:762`), so a listed version works there as well — and
///   would keep working on a versioned account, where the same object differs by
///   path.
/// - **GCS** conditions on the *generation*, which is exactly the field a
///   listing drops: `PutMode::Update(v)` reads `v.version` and returns
///   `Error::MissingVersion` when it is absent (`gcp/client.rs:407`). A listed
///   version there is not a stale version, it is not a version — and the
///   `Generic` it produces is not `Precondition`, so no CAS loop in this crate
///   retries it and the coordinator returns it to the client.
///
/// The invariant, therefore: **every `Version` handed to a conditional update
/// must have come from a GET or a PUT of that object, never from a listing.** It
/// holds today — all four `PutMode::Update` sites in `dynostore` take theirs
/// from a preceding `get` — and `dynostore::tests::gcs_generation` is what keeps
/// it holding, because nothing else can: `InMemory` leaves `version` empty and
/// conditions on the etag, so the whole suite runs on `memory://` with the field
/// GCS requires unset and passes.
///
/// Blob versioning is off in a correct deployment either way (`docs/adls.md`),
/// and GCS object versioning likewise (`docs/gcs.md`).
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
