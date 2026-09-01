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

//! The configurable coalescing prefix shape, and the seal that stops it moving
//! (#464).
//!
//! Two halves. The derivation — `prefix_depth` / `prefix_separator` decide which
//! prefix a topic's segments are written under, and the defaults reproduce the
//! shape every deployment before this had. And the seal — the shape is recorded
//! once per cluster and a store configured with a different one never gets
//! built.
//!
//! The seal is the substantive half. A shape change on a live cluster is not a
//! data-loss bug: produce and fetch read the per-topic routing pin (#236), so
//! records stay reachable. It is a *silent* bug, which is worse to operate.
//! Retention and compaction re-derive the prefix rather than pay a GET per topic
//! per tick (#407), so they would sweep prefixes holding nothing and report
//! themselves clean while the real prefixes grew without bound. Nothing logs an
//! error, because from the sweeps' point of view nothing is wrong.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, PrefixShape, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A store over `bucket` deriving prefixes with `depth` / `separator`. `None`
/// leaves that half at its default, exactly as an absent query-string key does.
fn store(bucket: &InMemory, depth: Option<usize>, separator: Option<&str>) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_depth: depth,
        prefix_separator: separator.map(str::to_owned),
        ..Default::default()
    })
}

fn batch(value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create(storage: &DynoStore, topic: &str) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await?;

    Ok(())
}

/// The sealed shape recorded on `bucket`, or `None` if nothing sealed it.
async fn sealed(bucket: &InMemory) -> Result<Option<PrefixShape>> {
    match bucket
        .get(&Path::from(format!("clusters/{CLUSTER}/prefix-shape.json")))
        .await
    {
        Ok(get_result) => get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_slice::<PrefixShape>(&encoded).map_err(Into::into))
            .map(Some),

        Err(object_store::Error::NotFound { .. }) => Ok(None),

        Err(otherwise) => Err(otherwise.into()),
    }
}

/// The derivation, over the shapes the issue names. `prefix_of` is `&self` and
/// pure, so this is the whole of it — where the segments land is
/// `segments_land_under_the_configured_prefix` below.
#[test]
fn the_shape_decides_the_prefix() {
    let bucket = InMemory::new();
    let prefix = |depth, separator, topic: &str| {
        store(&bucket, depth, separator).prefix_of(&Topition::new(topic, 0))
    };

    // The default: today's derivation, unchanged.
    assert_eq!("a.b.c", prefix(None, None, "a.b.c.d"));
    assert_eq!(
        "org.env.conn",
        prefix(None, None, "org.env.conn.public.orders")
    );

    assert_eq!("a.b", prefix(Some(2), None, "a.b.c.d"));

    // Depth at or past the component count, and depth 0, are the same answer for
    // opposite reasons: the topic has nothing left to trim, and the topic is
    // never trimmed.
    assert_eq!("a.b.c.d", prefix(Some(4), None, "a.b.c.d"));
    assert_eq!("a.b.c.d", prefix(Some(0), None, "a.b.c.d"));
    assert_eq!("a.b.c.d", prefix(Some(9), None, "a.b.c.d"));

    // Fewer components than the depth: its own prefix, as before.
    assert_eq!("a.b", prefix(None, None, "a.b"));
    assert_eq!("solo", prefix(None, None, "solo"));

    assert_eq!("a_b_c", prefix(None, Some("_"), "a_b_c_d"));
    assert_eq!("a_b", prefix(Some(2), Some("_"), "a_b_c_d"));

    // A separator the name does not use leaves the topic whole — one prefix per
    // topic, which is what depth 0 asks for explicitly.
    assert_eq!("a.b.c.d", prefix(None, Some("_"), "a.b.c.d"));

    // Multi-character separators are not a special case.
    assert_eq!("a__b__c", prefix(None, Some("__"), "a__b__c__d"));
}

/// The derivation is not a string function in isolation: it decides the object
/// key a produce ends up under.
#[tokio::test]
async fn segments_land_under_the_configured_prefix() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "a.b.c.d";
    let topition = Topition::new(topic, 0);

    for (depth, expected) in [(None, "a.b.c"), (Some(2), "a.b"), (Some(0), "a.b.c.d")] {
        let bucket = InMemory::new();
        let storage = store(&bucket, depth, None).sealed_prefix_shape().await?;

        create(&storage, topic).await?;
        _ = storage.produce(None, &topition, batch(b"one")?).await?;

        let listing = Path::from(format!("clusters/{CLUSTER}/prefixes/{expected}/segments/"));
        let segments = bucket
            .list(Some(&listing))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await
            .expect("list segments")
            .len();

        assert_eq!(1, segments, "depth {depth:?} must write under {expected}");

        // And the records are readable back through the same routing.
        assert_eq!(1, storage.high_watermark(&topition).await?);
    }

    Ok(())
}

/// An unsealed bucket adopts the configured shape and records it.
#[tokio::test]
async fn an_unsealed_bucket_adopts_the_configured_shape() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    assert_eq!(None, sealed(&bucket).await?);

    _ = store(&bucket, Some(2), Some("_"))
        .sealed_prefix_shape()
        .await?;

    assert_eq!(
        Some(PrefixShape {
            depth: 2,
            separator: "_".into()
        }),
        sealed(&bucket).await?
    );

    Ok(())
}

/// A bucket that has always run on the defaults — every bucket in production
/// before #464 — seals the shape its routing pins already describe, so nothing
/// about its layout moves.
#[tokio::test]
async fn a_default_configuration_seals_todays_shape() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    _ = store(&bucket, None, None).sealed_prefix_shape().await?;

    assert_eq!(
        Some(PrefixShape {
            depth: 3,
            separator: ".".into()
        }),
        sealed(&bucket).await?
    );

    // And a restart on the same configuration is a no-op, not a rewrite.
    _ = store(&bucket, None, None).sealed_prefix_shape().await?;
    _ = store(&bucket, Some(3), Some("."))
        .sealed_prefix_shape()
        .await?;

    Ok(())
}

/// The point of the issue: a shape that disagrees with the seal fails the build,
/// names both shapes, and leaves the seal alone.
#[tokio::test]
async fn a_disagreeing_shape_fails_the_build() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    _ = store(&bucket, None, None).sealed_prefix_shape().await?;

    for (depth, separator) in [
        (Some(2), None),
        (Some(4), None),
        (Some(0), None),
        (None, Some("_")),
        (Some(2), Some("_")),
    ] {
        match store(&bucket, depth, separator).sealed_prefix_shape().await {
            Err(Error::PrefixShapeSealed { sealed, configured }) => {
                assert_eq!("depth 3 separator \".\"", sealed);
                assert_ne!(sealed, configured);
            }

            otherwise => panic!("{depth:?}/{separator:?} must not build: {otherwise:?}"),
        }
    }

    // The seal is untouched by every one of those refusals.
    assert_eq!(
        Some(PrefixShape {
            depth: 3,
            separator: ".".into()
        }),
        sealed(&bucket).await?
    );

    Ok(())
}

/// Replicas starting together race for the create. The winner's shape is the
/// cluster's: a loser that agrees adopts it, and a loser that does not fails
/// rather than running on a shape half the fleet is not using.
#[tokio::test]
async fn racing_replicas_converge_on_the_winner() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    // Two replicas configured alike, both seeing an unsealed bucket: exactly one
    // create wins and both build.
    let (first, second) = tokio::join!(
        store(&bucket, Some(2), None).sealed_prefix_shape(),
        store(&bucket, Some(2), None).sealed_prefix_shape(),
    );
    _ = first?;
    _ = second?;

    let winner = sealed(&bucket).await?.expect("sealed");
    assert_eq!(
        PrefixShape {
            depth: 2,
            separator: ".".into()
        },
        winner
    );

    // A third replica, misconfigured, arrives after them.
    assert!(matches!(
        store(&bucket, Some(3), None).sealed_prefix_shape().await,
        Err(Error::PrefixShapeSealed { .. })
    ));

    Ok(())
}

/// The seal covers the derivation the *maintenance* paths use, which is the
/// reason it exists: `routed_prefix` (retention grouping, the compaction
/// universe) and the pinned prefix produce and fetch resolve must agree for
/// every topic.
#[tokio::test]
async fn the_derived_and_pinned_prefixes_agree_under_the_seal() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topic = "a.b.c.d";
    let topition = Topition::new(topic, 0);

    let storage = store(&bucket, Some(2), None).sealed_prefix_shape().await?;
    create(&storage, topic).await?;
    _ = storage.produce(None, &topition, batch(b"one")?).await?;

    // A cold replica on the same (sealed) configuration derives what the first
    // one pinned.
    let cold = store(&bucket, Some(2), None).sealed_prefix_shape().await?;
    assert_eq!(
        cold.routed_prefix(&topition, false),
        cold.routed_prefix_of(&topition).await?
    );
    assert_eq!("a.b", cold.routed_prefix_of(&topition).await?);

    Ok(())
}

/// The other half of the race: a replica that read an unsealed bucket and then
/// *lost* the create. It must adopt the winner's shape, not its own — and fail
/// if it cannot.
///
/// The window has to be forced. `InMemory` never yields, so two futures over one
/// bucket run to completion in turn and the create conflict cannot happen by
/// scheduling; [`SealAfterMiss`] holds the window open by sealing the bucket the
/// moment a replica observes it unsealed.
#[tokio::test]
async fn a_replica_that_loses_the_create_adopts_the_winner() -> Result<()> {
    let _guard = init_tracing()?;

    let peer = PrefixShape {
        depth: 2,
        separator: ".".into(),
    };

    // The loser agrees with the winner: it adopts and builds.
    {
        let bucket = InMemory::new();
        let loser = DynoStore::new(CLUSTER, NODE, SealAfterMiss::new(&bucket, &peer))
            .coalesce_tuning(CoalesceTuning {
                prefix_depth: Some(2),
                ..Default::default()
            });

        _ = loser.sealed_prefix_shape().await?;
        assert_eq!(Some(peer.clone()), sealed(&bucket).await?);
    }

    // The loser does not: it fails rather than running on a shape half the fleet
    // is not using, and the winner's seal stands.
    {
        let bucket = InMemory::new();
        let loser = DynoStore::new(CLUSTER, NODE, SealAfterMiss::new(&bucket, &peer))
            .coalesce_tuning(CoalesceTuning {
                prefix_depth: Some(4),
                ..Default::default()
            });

        match loser.sealed_prefix_shape().await {
            Err(Error::PrefixShapeSealed { sealed, configured }) => {
                assert_eq!("depth 2 separator \".\"", sealed);
                assert_eq!("depth 4 separator \".\"", configured);
            }

            otherwise => panic!("a losing replica must not build: {otherwise:?}"),
        }

        assert_eq!(Some(peer.clone()), sealed(&bucket).await?);
    }

    Ok(())
}

/// An `InMemory` that seals the bucket with a peer's shape the first time a GET
/// of `prefix-shape.json` misses, standing in for the peer that sealed it
/// between this replica's read and its create.
#[derive(Clone)]
struct SealAfterMiss {
    inner: InMemory,
    peer: Bytes,
    sealed: Arc<AtomicBool>,
}

impl SealAfterMiss {
    fn new(bucket: &InMemory, peer: &PrefixShape) -> Self {
        Self {
            inner: bucket.clone(),
            peer: serde_json::to_vec(peer)
                .map(Bytes::from)
                .expect("peer shape"),
            sealed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Debug for SealAfterMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealAfterMiss").finish()
    }
}

impl Display for SealAfterMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealAfterMiss").finish()
    }
}

#[async_trait]
impl ObjectStore for SealAfterMiss {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> std::result::Result<PutResult, object_store::Error> {
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
        let outcome = self.inner.get_opts(location, options).await;

        if location.as_ref().ends_with("prefix-shape.json")
            && matches!(outcome, Err(object_store::Error::NotFound { .. }))
            && !self.sealed.swap(true, Ordering::Relaxed)
        {
            _ = self
                .inner
                .put_opts(
                    location,
                    PutPayload::from(self.peer.clone()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
                .expect("peer seal");
        }

        outcome
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
