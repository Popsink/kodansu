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

//! Conditional-put conformance for whichever object store the suite is pointed at
//! (#357).
//!
//! Everything that makes the broker stateless is a conditional put: offset
//! assignment is a create-only segment-sequence CAS, group mutation is the etag
//! CAS in `update_group`, maintenance work-splitting is a per-prefix lease.
//! `InMemory` *emulates* those semantics behind a mutex; S3 implements them with
//! `If-None-Match` and GCS with a generation precondition. The two stores anyone
//! runs in production were therefore the two nothing tested, and a divergence
//! between them surfaces as a consumer-group wedge rather than as a red test.
//!
//! Kept in its own target rather than folded into the general suite: these
//! assertions are about the *store*, not about a Kafka API, and running them
//! alone against a candidate store is the point —
//! `just test-conditional-put s3://tansu/`.
//!
//! Two levels, because the interesting failure lives between them:
//!
//! - the raw `ObjectStore`, asserting the error *class* a losing writer gets;
//! - `Storage::update_group`, asserting the engine maps that class onto
//!   `UpdateError::Outdated` carrying the current value, which is the contract
//!   the group coordinator's retry loop is written against.
//!
//! Not covered: GCS generation preconditions. There is no GCS emulator in
//! `compose.yaml` and `GoogleCloudStorageBuilder::from_env` needs real
//! credentials, so a `gs://` run has nothing to run against here. Pointing
//! `TANSU_TEST_STORAGE_URL` at a real `gs://` bucket runs every test in this file
//! unchanged — that is the whole reason the store is a parameter — but no
//! assertion here has ever been observed against GCS, and none of them fakes one.

#![cfg(feature = "dynostore")]

use std::{collections::BTreeMap, sync::Arc, time::SystemTime};

use bytes::Bytes;
use futures::{StreamExt as _, stream::FuturesUnordered};
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
    aws::{AmazonS3Builder, S3ConditionalPut},
    gcp::GoogleCloudStorageBuilder,
    memory::InMemory,
    path::Path,
};
use tansu_storage::{GroupDetail, GroupState, Storage, StorageContainer, UpdateError, Version};
use url::Url;
use uuid::Uuid;

use crate::common::{Error, cluster_id, init_tracing, storage_url};

mod common;

/// How many writers race for one key. Enough that a store serialising puts behind
/// a lock and a store resolving the race with `If-None-Match` are both asked the
/// same question more than once.
const WRITERS: usize = 16;

/// The raw store behind the configured URL, with the same conditional-put
/// configuration `StorageContainer::builder` gives the engine.
///
/// Notably `S3ConditionalPut::ETagMatch`: without it `object_store` refuses
/// `PutMode::Create` outright rather than translating it to `If-None-Match: *`,
/// so a conformance run that built the store any other way would be reporting on
/// a configuration the broker never uses.
fn object_store() -> Result<Arc<dyn ObjectStore>, Error> {
    let storage = storage_url()?;
    let bucket = storage.host_str().unwrap_or("tansu");

    match storage.scheme() {
        "memory" => Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>),

        "s3" => AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map(|object_store| Arc::new(object_store) as Arc<dyn ObjectStore>)
            .map_err(Into::into),

        "gs" => GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map(|object_store| Arc::new(object_store) as Arc<dyn ObjectStore>)
            .map_err(Into::into),

        _ => Err(Error::Message(format!(
            "conditional put is not a property of {storage}"
        ))),
    }
}

/// A key no other test — and no other run against the same bucket — writes to.
fn key(test: &str) -> Path {
    Path::from(format!("conformance/{}/{test}.json", Uuid::now_v7()))
}

fn payload(body: &'static str) -> PutPayload {
    PutPayload::from(Bytes::from_static(body.as_bytes()))
}

fn create() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

fn update(version: UpdateVersion) -> PutOptions {
    PutOptions {
        mode: PutMode::Update(version),
        ..Default::default()
    }
}

/// A losing *create* is `AlreadyExists`, on both stores.
///
/// Measured, not assumed. `InMemory` raises it directly from the occupied map
/// entry; S3 sends `If-None-Match: *`, gets `412 PreconditionFailed` back and
/// `object_store`'s S3 create path translates that to `AlreadyExists`. The
/// translation lives in `object_store`, not in the server, so it is the same for
/// minio and for real S3.
///
/// Asserted exactly rather than as "either conflict variant": the two variants do
/// both occur, but for two *different* operations, and each store is consistent
/// about which. `dynostore`'s `put` accepts both because that one function serves
/// both operations — that is not licence for this test to be vague.
fn is_already_exists(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::AlreadyExists { .. })
}

/// A losing *CAS* is `Precondition`, on both stores — for a stale etag, for an
/// etag the store never issued, and for a CAS against an absent key.
fn is_precondition(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Precondition { .. })
}

/// create-only: N writers, one key, exactly one winner.
///
/// This is offset assignment. A segment object is named for the offset range it
/// covers, so two brokers that both believe they own offset N write the same key,
/// and "exactly one wins" is what stops the second one's records from vanishing.
/// A store that let both puts succeed would lose a batch with no error anywhere;
/// one that failed both would stall produce.
#[tokio::test]
async fn create_only_admits_exactly_one_writer() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = object_store()?;
    let location = key("create_only");

    let outcomes = (0..WRITERS)
        .map(|_| {
            let object_store = object_store.clone();
            let location = location.clone();

            async move {
                object_store
                    .put_opts(&location, payload("mine"), create())
                    .await
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let (won, lost): (Vec<_>, Vec<_>) = outcomes.iter().partition(|outcome| outcome.is_ok());

    assert_eq!(1, won.len(), "exactly one create must win: {outcomes:?}");
    assert_eq!(WRITERS - 1, lost.len());

    for outcome in lost {
        let error = outcome.as_ref().expect_err("partitioned as an error");

        assert!(
            is_already_exists(error),
            "a losing create must present as AlreadyExists, got {error:?}"
        );
    }

    Ok(())
}

/// etag CAS: N writers race on one version. One wins, and every loser is refused
/// rather than allowed to clobber.
///
/// The stale version is not fabricated — it is the one the store itself handed
/// out, which is the only way to tell a store that compares etags from one that
/// ignores the precondition and always writes.
#[tokio::test]
async fn a_stale_etag_cas_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = object_store()?;
    let location = key("stale_cas");

    let created = object_store
        .put_opts(&location, payload("first"), create())
        .await?;

    let version = UpdateVersion {
        e_tag: created.e_tag.clone(),
        version: created.version.clone(),
    };

    assert!(
        version.e_tag.is_some(),
        "a store that issues no etag cannot CAS at all: {created:?}"
    );

    let outcomes = (0..WRITERS)
        .map(|_| {
            let object_store = object_store.clone();
            let location = location.clone();
            let version = version.clone();

            async move {
                object_store
                    .put_opts(&location, payload("second"), update(version))
                    .await
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let (won, lost): (Vec<_>, Vec<_>) = outcomes.iter().partition(|outcome| outcome.is_ok());

    assert_eq!(
        1,
        won.len(),
        "one CAS on a shared version must win: {outcomes:?}"
    );

    for outcome in lost {
        let error = outcome.as_ref().expect_err("partitioned as an error");

        assert!(
            is_precondition(error),
            "a stale CAS must present as Precondition, got {error:?}"
        );
    }

    // The version is spent, not merely contended: replaying it once the race has
    // settled is still refused.
    let replayed = object_store
        .put_opts(&location, payload("third"), update(version))
        .await;

    assert!(
        replayed.as_ref().is_err_and(is_precondition),
        "a spent version must stay spent, got {replayed:?}"
    );

    // An etag the store never issued is refused the same way, and a CAS against a
    // key that does not exist is a precondition failure rather than a create —
    // the fallback that would silently turn a lost lease into a fresh one.
    for (what, location) in [
        ("an invented etag", location.clone()),
        ("an absent key", key("stale_cas_absent")),
    ] {
        let refused = object_store
            .put_opts(
                &location,
                payload("nope"),
                update(UpdateVersion {
                    e_tag: Some(String::from("\"0bad0bad0bad0bad0bad0bad0bad0bad\"")),
                    version: None,
                }),
            )
            .await;

        assert!(
            refused.as_ref().is_err_and(is_precondition),
            "{what} must be refused with Precondition, got {refused:?}"
        );
    }

    Ok(())
}

/// etag stability: an object nobody wrote keeps the etag it was created with.
///
/// #111's GET-first gate depends on exactly this. A heartbeat that changed
/// nothing persistent skips the group-state PUT, so the etag the coordinator
/// holds has to still be current on the *next* commit — the one that does write.
/// A store that rotated etags on read, or on an unrelated mutation of the
/// bucket, would turn every skipped write into a spurious `Outdated` and every
/// steady-state heartbeat into a rebalance.
#[tokio::test]
async fn an_unchanged_object_keeps_its_etag() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = object_store()?;
    let location = key("etag_stability");

    let created = object_store
        .put_opts(&location, payload("stable"), create())
        .await?;

    let e_tag = created.e_tag.clone().expect("a created object has an etag");

    for read in 0..4 {
        let head = object_store.head(&location).await?;
        assert_eq!(
            Some(&e_tag),
            head.e_tag.as_ref(),
            "etag moved on head {read}"
        );

        let got = object_store.get(&location).await?;
        assert_eq!(
            Some(&e_tag),
            got.meta.e_tag.as_ref(),
            "etag moved on get {read}"
        );
    }

    // Unrelated traffic in the same prefix must not disturb it either: an etag
    // derived from a store-wide counter passes the reads above and fails here.
    _ = object_store
        .put_opts(&key("etag_stability_neighbour"), payload("noise"), create())
        .await?;

    assert_eq!(
        Some(e_tag.clone()),
        object_store.head(&location).await?.e_tag
    );

    // And the proof that the etag is not merely unchanged but still *current*: a
    // CAS against it succeeds.
    let updated = object_store
        .put_opts(
            &location,
            payload("moved"),
            update(UpdateVersion {
                e_tag: Some(e_tag.clone()),
                version: created.version.clone(),
            }),
        )
        .await?;

    assert_ne!(
        Some(e_tag),
        updated.e_tag,
        "a write that changed the object must change the etag"
    );

    Ok(())
}

/// A rewrite that changes nothing still hands back a version the next CAS can
/// use.
///
/// This is where `InMemory` and S3 genuinely diverge, and measurably so (#357):
///
/// | store | rewrite the same bytes | two keys, same bytes |
/// |---|---|---|
/// | `InMemory` | `"2"` becomes `"3"` | `"2"` vs `"4"` |
/// | S3 (minio) | `ee0cbdba…` stays `ee0cbdba…` | equal |
///
/// `InMemory`'s etag is a per-object write counter; S3's is the MD5 of the body.
/// So on S3 an etag is *content*-addressed: a no-op rewrite does not move it, and
/// "the etag changed" is not evidence that a write happened. The consequence to
/// know about is ABA — an object driven X to Y and back to X presents its
/// original etag again on S3, and a CAS still holding that etag succeeds despite
/// two intervening writes, where `InMemory` would refuse it.
///
/// Nothing in the engine depends on the difference today: `GroupDetail` carries
/// `inception` and `generation_id`, so byte-identical group state is not a state
/// the coordinator returns to. What is asserted here is the invariant that *is*
/// depended on and that both stores honour — the version a put hands back is
/// usable for the next CAS however the store derives it — so the per-generation
/// CAS'd objects the group-state decomposition adds inherit a test rather than an
/// assumption.
#[tokio::test]
async fn a_no_op_rewrite_still_yields_a_usable_version() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = object_store()?;
    let location = key("no_op_rewrite");

    let created = object_store
        .put_opts(&location, payload("identical"), create())
        .await?;

    let rewritten = object_store
        .put_opts(
            &location,
            payload("identical"),
            update(UpdateVersion {
                e_tag: created.e_tag.clone(),
                version: created.version.clone(),
            }),
        )
        .await?;

    let next = object_store
        .put_opts(
            &location,
            payload("different"),
            update(UpdateVersion {
                e_tag: rewritten.e_tag.clone(),
                version: rewritten.version.clone(),
            }),
        )
        .await?;

    assert_ne!(
        rewritten.e_tag, next.e_tag,
        "a write that changed the content must move the etag"
    );

    Ok(())
}

/// conditional read: `If-None-Match` on an unchanged object is `NotModified`.
///
/// This is the cheap (tier-2) read the coordinator uses to notice a change
/// another replica made without paying for the body. A store that answered with
/// the payload instead would only be correct-but-expensive; one that answered
/// `NotModified` for a *changed* object would silently hide a cross-replica
/// rebalance, so both directions are asserted.
#[tokio::test]
async fn if_none_match_is_not_modified_until_the_object_changes() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = object_store()?;
    let location = key("conditional_read");

    let created = object_store
        .put_opts(&location, payload("body"), create())
        .await?;

    let e_tag = created.e_tag.clone().expect("a created object has an etag");

    let unchanged = object_store
        .get_opts(
            &location,
            GetOptions::new().with_if_none_match(Some(e_tag.clone())),
        )
        .await;

    assert!(
        matches!(unchanged, Err(object_store::Error::NotModified { .. })),
        "an unchanged object must answer NotModified, got {unchanged:?}"
    );

    let updated = object_store
        .put_opts(
            &location,
            payload("changed"),
            update(UpdateVersion {
                e_tag: Some(e_tag.clone()),
                version: created.version.clone(),
            }),
        )
        .await?;

    let changed = object_store
        .get_opts(&location, GetOptions::new().with_if_none_match(Some(e_tag)))
        .await?;

    assert_eq!(updated.e_tag, changed.meta.e_tag);
    assert_eq!(Bytes::from_static(b"changed"), changed.bytes().await?);

    Ok(())
}

/// A group state that differs from another only in `generation_id`.
///
/// `inception` is pinned rather than `SystemTime::now()`: two details that
/// differed by a timestamp would make the value assertions below pass whatever
/// the store did.
fn group_detail(generation_id: i32) -> GroupDetail {
    GroupDetail {
        session_timeout_ms: 45_000,
        rebalance_timeout_ms: None,
        members: BTreeMap::new(),
        generation_id,
        skip_assignment: Some(false),
        inception: SystemTime::UNIX_EPOCH,
        state: GroupState::Forming {
            protocol_type: None,
            protocol_name: None,
            leader: None,
        },
    }
}

/// The version a write that is fixture rather than assertion must have produced.
///
/// [`UpdateError`] is generic in the value it carries, so it cannot be a variant
/// of the suite-wide [`Error`]. The tests below match on it where it is the
/// subject and flatten it here, where a failure means the fixture never got
/// built.
fn must_write(result: Result<Version, UpdateError<GroupDetail>>) -> Result<Version, Error> {
    result.map_err(|error| Error::Message(format!("{error:?}")))
}

async fn engine() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await
        .map_err(Into::into)
}

/// The engine's own contract: a stale `update_group` is [`UpdateError::Outdated`]
/// and it carries the value that won, not just a failure.
///
/// The group coordinator does not retry blindly — it re-derives its next action
/// from `current`. An `Outdated` without the winning value, or a bare
/// `UpdateError::Error` wrapping the store's conflict, would leave it retrying
/// the same stale generation until the budget is gone (#157).
#[tokio::test]
async fn a_stale_update_group_is_outdated_carrying_the_current_value() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = engine().await?;
    let group = "conformance";

    let first = must_write(storage.update_group(group, group_detail(1), None).await)?;

    let second = must_write(
        storage
            .update_group(group, group_detail(2), Some(first.clone()))
            .await,
    )?;

    match storage
        .update_group(group, group_detail(3), Some(first))
        .await
    {
        Err(UpdateError::Outdated { current, version }) => {
            assert_eq!(group_detail(2), *current);
            assert_eq!(second, version);
        }

        otherwise => panic!("a stale CAS must be Outdated, got {otherwise:?}"),
    }

    Ok(())
}

/// The engine's create-only path: `update_group` with no version is a create, so
/// N replicas forming the same group concurrently produce one winner, and every
/// loser is told the state that won.
#[tokio::test]
async fn concurrent_group_creation_admits_exactly_one_writer() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = engine().await?;
    let group = "conformance-create";

    let outcomes = (0..WRITERS)
        .map(|generation| {
            let storage = storage.clone();
            let detail = group_detail(i32::try_from(generation).expect("WRITERS fits in i32"));

            async move { storage.update_group(group, detail, None).await }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let won = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(1, won, "exactly one create must win: {outcomes:?}");

    let (persisted, _) = storage
        .read_group(group)
        .await?
        .expect("the winner's state is readable");

    for outcome in &outcomes {
        match outcome {
            Ok(_) => {}

            // The loser is told what is there now, and it is what a subsequent
            // read sees: an `Outdated` carrying an already-stale value would
            // send the coordinator round the loop again.
            Err(UpdateError::Outdated { current, .. }) => assert_eq!(persisted, **current),

            otherwise => panic!("a losing create must be Outdated, got {otherwise:?}"),
        }
    }

    Ok(())
}

/// A `Version` the store never issued must not write.
///
/// The downgrade that turns an unsatisfiable precondition into an unconditional
/// put is the one bug in this area no other test would catch: every assertion
/// above still passes with it in place, and the broker would then lose whichever
/// concurrent mutation it overwrote.
#[tokio::test]
async fn a_version_the_store_never_issued_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = engine().await?;
    let group = "conformance-invented";

    _ = must_write(storage.update_group(group, group_detail(1), None).await)?;

    let invented = Version::from(&Uuid::now_v7());

    match storage
        .update_group(group, group_detail(2), Some(invented))
        .await
    {
        Err(UpdateError::Outdated { current, .. }) => assert_eq!(group_detail(1), *current),

        otherwise => panic!("an invented version must not write, got {otherwise:?}"),
    }

    Ok(())
}
