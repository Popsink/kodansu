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

//! The boot-time conditional-put probe (#597), against an `InMemory` that
//! conforms and against wrappers that drop one precondition each — the
//! behaviour of an "S3-compatible" store that ignores the header rather than
//! refusing it.

use std::fmt::{self, Debug, Display};

use async_trait::async_trait;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use url::Url;

use crate::{
    Error, Result,
    dynostore::{DynoStore, probe::skip_conditional_put_check, tests::init_tracing},
};

/// What the wrapper does to a conditional PUT.
#[derive(Clone, Copy, Debug)]
enum Drop {
    /// `PutMode::Create` becomes an overwrite: `If-None-Match: *` ignored.
    IfNoneMatch,
    /// `PutMode::Update` becomes an overwrite: `If-Match` ignored.
    IfMatch,
    /// `PutMode::Update` is answered `NotImplemented`: the header is not
    /// spoken at all.
    UpdateNotImplemented,
}

#[derive(Clone)]
struct Lax {
    inner: InMemory,
    drop: Drop,
}

impl Lax {
    fn new(inner: &InMemory, drop: Drop) -> Self {
        Self {
            inner: inner.clone(),
            drop,
        }
    }
}

impl Debug for Lax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lax").field("drop", &self.drop).finish()
    }
}

impl Display for Lax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

#[async_trait]
impl ObjectStore for Lax {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> std::result::Result<PutResult, object_store::Error> {
        match (self.drop, &opts.mode) {
            (Drop::IfNoneMatch, PutMode::Create) | (Drop::IfMatch, PutMode::Update(_)) => {
                opts.mode = PutMode::Overwrite;
            }

            (Drop::UpdateNotImplemented, PutMode::Update(_)) => {
                return Err(object_store::Error::NotImplemented {
                    operation: String::from("conditional update"),
                    implementer: String::from("Lax"),
                });
            }

            _ => {}
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> std::result::Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> std::result::Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, std::result::Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, std::result::Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, std::result::Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, std::result::Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> std::result::Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> std::result::Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn s3() -> Result<Url> {
    Url::parse("s3://tansu/").map_err(Into::into)
}

/// Every probe key left behind on `bucket`.
async fn leftovers(bucket: &InMemory) -> Result<Vec<Path>> {
    bucket
        .list(Some(&Path::from("__probe")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .map_err(Into::into)
}

/// The refusal's scheme and reason, or a failure naming what came back instead.
async fn refused(store: impl ObjectStore, storage: &Url) -> Result<(String, String)> {
    match DynoStore::new("tansu", 111, store)
        .conditional_put_probed(storage)
        .await
    {
        Err(Error::ConditionalPutRefused { scheme, reason }) => Ok((scheme, reason)),
        Err(otherwise) => Err(otherwise),
        Ok(_) => Err(Error::Message(String::from("the probe passed a lax store"))),
    }
}

#[tokio::test]
async fn a_conforming_store_passes_and_keeps_no_probe_key() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    _ = DynoStore::new("tansu", 111, bucket.clone())
        .conditional_put_probed(&s3()?)
        .await?;

    assert_eq!(Vec::<Path>::new(), leftovers(&bucket).await?);

    Ok(())
}

/// The GCS-shaped invented version (a generation, not just an etag) is refused
/// by a conforming store too, so `gs://` is not a refusal by construction.
#[tokio::test]
async fn a_conforming_store_passes_under_the_gs_version_shape() -> Result<()> {
    let _guard = init_tracing()?;

    _ = DynoStore::new("tansu", 111, InMemory::new())
        .conditional_put_probed(&Url::parse("gs://tansu/")?)
        .await?;

    Ok(())
}

#[tokio::test]
async fn ignoring_if_none_match_is_refused() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let (scheme, reason) = refused(Lax::new(&bucket, Drop::IfNoneMatch), &s3()?).await?;

    assert_eq!("s3", scheme);
    assert!(reason.contains("If-None-Match"), "{reason}");
    assert!(reason.contains("s3://"), "{reason}");
    assert!(reason.contains("skip_conditional_put_check"), "{reason}");
    assert_eq!(Vec::<Path>::new(), leftovers(&bucket).await?);

    Ok(())
}

#[tokio::test]
async fn ignoring_if_match_is_refused() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let (scheme, reason) = refused(Lax::new(&bucket, Drop::IfMatch), &s3()?).await?;

    assert_eq!("s3", scheme);
    assert!(reason.contains("`If-Match`"), "{reason}");
    assert!(!reason.contains("If-None-Match"), "{reason}");
    assert_eq!(Vec::<Path>::new(), leftovers(&bucket).await?);

    Ok(())
}

#[tokio::test]
async fn ignoring_the_generation_is_refused_naming_the_gcs_header() -> Result<()> {
    let _guard = init_tracing()?;

    let (scheme, reason) = refused(
        Lax::new(&InMemory::new(), Drop::IfMatch),
        &Url::parse("gs://tansu/")?,
    )
    .await?;

    assert_eq!("gs", scheme);
    assert!(reason.contains("x-goog-if-generation-match"), "{reason}");

    Ok(())
}

#[tokio::test]
async fn not_speaking_the_header_is_refused_with_the_error() -> Result<()> {
    let _guard = init_tracing()?;

    let (_, reason) = refused(
        Lax::new(&InMemory::new(), Drop::UpdateNotImplemented),
        &s3()?,
    )
    .await?;

    assert!(reason.contains("never issued"), "{reason}");
    assert!(reason.contains("not yet implemented"), "{reason}");

    Ok(())
}

#[tokio::test]
async fn the_escape_hatch_skips_the_probe() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    _ = DynoStore::new("tansu", 111, Lax::new(&bucket, Drop::IfNoneMatch))
        .conditional_put_probed(&Url::parse("s3://tansu/?skip_conditional_put_check=true")?)
        .await?;

    assert_eq!(Vec::<Path>::new(), leftovers(&bucket).await?);

    Ok(())
}

#[test]
fn only_true_skips_the_probe() -> Result<()> {
    for (url, expected) in [
        ("s3://tansu/", false),
        ("s3://tansu/?skip_conditional_put_check=true", true),
        ("s3://tansu/?skip_conditional_put_check=false", false),
        ("s3://tansu/?skip_conditional_put_check=yes", false),
        ("s3://tansu/?skip_conditional_put_check=", false),
    ] {
        assert_eq!(
            expected,
            skip_conditional_put_check(&Url::parse(url)?),
            "for {url}"
        );
    }

    Ok(())
}
