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

//! The engine's conditional writes over a store that conditions the way GCS
//! does — the generation, not the etag.
//!
//! [`GenerationConditioned`] explains why this needs its own store: `InMemory`,
//! S3 and Azure all leave `UpdateVersion::version` unused, so on every backend
//! but GCS a conditional update works whether or not the field is populated.
//! GCS reads the generation out of that field and answers
//! `Generic { MissingVersion }` when it is empty — an error class no CAS loop
//! here retries, because it is not `Precondition`.
//!
//! These tests are the standing statement of the invariant:
//!
//! > every `Version` handed to a conditional update came from a GET or a PUT of
//! > that object, never from a listing.
//!
//! Each asserts both halves. `missing_version() == 0` is the property;
//! `conditioned() > 0` is the control, because a run that made no conditional
//! update at all would report the same zero.

use bytes::Bytes;
use futures::stream::StreamExt as _;
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
    memory::InMemory, path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, MemberRef, OffsetCommitRequest, Result, Storage as _, Topition, UpdateError,
    dynostore::{CoalesceTuning, DynoStore},
    gcs::generation::{GenerationConditioned, Refusals},
};

use std::time::Duration;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.table";

/// One GCS-shaped bucket, and two replicas over it.
///
/// Two stores rather than one for the same reason the Azure tests use two: a
/// writer serves reads out of the prefix index it populated as it wrote, so a
/// single-process test exercises far less of the read path than a fleet does.
fn replicas() -> (DynoStore, DynoStore, Refusals) {
    let bucket = GenerationConditioned::new(InMemory::new());
    let refusals = bucket.refusals();

    let store = |bucket: GenerationConditioned<InMemory>| {
        DynoStore::new(CLUSTER, NODE, bucket).coalesce_tuning(CoalesceTuning {
            coalesce_batches: Some(1),
            ..Default::default()
        })
    };

    (store(bucket.clone()), store(bucket), refusals)
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// The decorator is faithful to `object_store`'s GCS client: this is the
/// behaviour it reproduces, asserted directly, so the engine tests below rest on
/// something checked rather than on a comment.
#[tokio::test]
async fn a_conditional_update_without_a_generation_is_refused() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = GenerationConditioned::new(InMemory::new());
    let refusals = bucket.refusals();
    let location = Path::from("a.json");

    let created = bucket
        .put_opts(
            &location,
            PutPayload::from(Bytes::from_static(b"one")),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;

    assert!(
        created.version.is_some(),
        "a GCS put answers with the generation it minted",
    );
    assert!(
        created.e_tag.is_some(),
        "and with an etag, which is what makes the etag-only path compile",
    );

    // The etag is present and correct, and on S3 or Azure this update would
    // land. On GCS it is not a lost race — it never reaches the bucket.
    let etag_only = bucket
        .put_opts(
            &location,
            PutPayload::from(Bytes::from_static(b"two")),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag.clone(),
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await;

    assert!(
        matches!(
            etag_only,
            Err(object_store::Error::Generic { store: "GCS", .. })
        ),
        "an update with no generation must fail the way GCS fails it, and \
         `Generic` is deliberately not `Precondition`: nothing retries it",
    );
    assert_eq!(1, refusals.missing_version());

    // The generation from the put does land.
    let updated = bucket
        .put_opts(
            &location,
            PutPayload::from(Bytes::from_static(b"three")),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag.clone(),
                    version: created.version.clone(),
                }),
                ..Default::default()
            },
        )
        .await?;

    // And replaying it does not.
    let stale = bucket
        .put_opts(
            &location,
            PutPayload::from(Bytes::from_static(b"four")),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag,
                    version: created.version,
                }),
                ..Default::default()
            },
        )
        .await;

    assert!(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "a superseded generation loses the race and stays retryable",
    );

    // A listing carries no generation at all, which is what makes "never from a
    // listing" a rule rather than a preference.
    let listed = bucket.list_with_delimiter(None).await?;
    assert_eq!(1, listed.objects.len());
    assert_eq!(
        None, listed.objects[0].version,
        "a GCS listing has no generation element; `object_store` writes None",
    );
    assert!(
        listed.objects[0].e_tag.is_some(),
        "the etag it does carry is the trap: it looks like a usable version",
    );

    // A GET does carry one, and it is the one the update wanted.
    let got = bucket.get(&location).await?;
    assert_eq!(updated.version, got.meta.version);

    Ok(())
}

/// Topic creation, produce and a cross-replica fetch issue **no** conditional
/// update at all — and that is the property, not an accident.
///
/// The segment layout is create-only: a produce mints a new key and never writes
/// one twice, so `PutMode::Update` — and with it the generation the update would
/// have to carry — does not appear on the data plane. This is what makes
/// `docs/storage-tuning.md`'s "GCS: safe by construction" true of produce and
/// fetch, and it is why the per-object write cap (#427) reaches the group plane
/// and nothing else.
///
/// Asserted as zero rather than "no refusals": a zero-refusal assertion over a
/// plane that issues no conditional update is vacuous, which is exactly what
/// this test was before it was measured.
#[tokio::test]
async fn the_data_plane_issues_no_conditional_update() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (writer, reader, refusals) = replicas();

    create_topic(&writer, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..8 {
        _ = writer.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    let fetched = reader
        .fetch(
            &tp,
            0,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?;

    assert_eq!(
        8u32,
        fetched.iter().map(|batch| batch.record_count).sum::<u32>(),
    );

    assert_eq!(
        0,
        refusals.conditioned(),
        "the data plane took a conditional update; if that is deliberate the \
         generation it carries now has to be accounted for on GCS, and the \
         per-object cap can reach produce",
    );

    assert_eq!(0, refusals.missing_version());

    Ok(())
}

/// Group formation with real contention, plus the offset commit that follows —
/// the two CAS-heaviest paths in the engine, and the ones a GCS wedge shows up
/// in first (#13, #427).
#[tokio::test(flavor = "multi_thread")]
async fn the_group_plane_carries_a_generation_into_every_conditional_write() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    const MEMBERS: usize = 8;
    const GROUP: &str = "g-1";

    let (writer, _reader, refusals) = replicas();

    create_topic(&writer, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);
    _ = writer.produce(None, &tp, batch("v0")?).await?;

    refusals.reset();

    // Sequential, but each iteration re-reads and re-applies, so every one after
    // the first is a `PutMode::Update` against a generation the store minted.
    for member in 0..MEMBERS {
        let member_id = format!("m-{member}");

        loop {
            let (mut doc, version) = writer
                .read_group_generation(GROUP)
                .await?
                .map(|(doc, version)| (doc, Some(version)))
                .unwrap_or_default();

            doc.seq += 1;
            doc.generation_id += 1;
            _ = doc.members.insert(member_id.clone(), MemberRef::default());

            match writer.update_group_generation(GROUP, doc, version).await {
                Ok(_) => break,
                Err(UpdateError::Outdated { .. }) => continue,
                Err(err) => return Err(Error::Message(format!("{err:?}"))),
            }
        }
    }

    assert_eq!(
        MEMBERS,
        writer
            .read_group_generation(GROUP)
            .await?
            .map_or(0, |(doc, _)| doc.members.len()),
    );

    let committed = writer
        .offset_commit(
            GROUP,
            None,
            &[(
                tp.clone(),
                OffsetCommitRequest {
                    offset: 1,
                    leader_epoch: None,
                    timestamp: None,
                    metadata: None,
                },
            )],
        )
        .await?;

    assert_eq!(vec![(tp, ErrorCode::None)], committed);

    assert!(
        refusals.conditioned() >= MEMBERS - 1,
        "only {} conditional updates were evaluated; the run did not exercise \
         the path this test is about",
        refusals.conditioned(),
    );

    assert_eq!(
        0,
        refusals.missing_version(),
        "a conditional update reached the store with no generation",
    );

    Ok(())
}

/// What the rule forbids, done deliberately, at the `Storage` level.
///
/// A `Version` built from a listed `ObjectMeta` carries the etag and no
/// generation. Handed to `update_group_generation` it is not a stale version —
/// it is not a version at all, and the error it produces is
/// `Generic { MissingVersion }`, which the coordinator's retry loop does not
/// recognise: `join` matches `UpdateError::Outdated | Vanished` to re-read and
/// anything else is returned to the client.
///
/// On S3, on Azure and on `memory://` the same call succeeds — the etag is all
/// three of them look at — which is why this cannot be caught anywhere else.
#[tokio::test]
async fn a_version_from_a_listing_cannot_condition_a_write_on_gcs() -> Result<(), Error> {
    use crate::Version;

    let _guard = super::init_tracing()?;

    const GROUP: &str = "g-listed";

    let bucket = GenerationConditioned::new(InMemory::new());
    let refusals = bucket.refusals();
    let storage = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let (mut doc, version): (crate::GenerationDoc, Option<Version>) = Default::default();
    doc.seq += 1;
    _ = storage
        .update_group_generation(GROUP, doc, version)
        .await
        .map_err(|err| Error::Message(format!("{err:?}")))?;

    let (doc, read_version) = storage
        .read_group_generation(GROUP)
        .await?
        .expect("the group was just written");

    let listed = bucket
        .list(None)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .find(|meta| meta.location.as_ref().ends_with("generation.json"))
        .expect("generation.json is in the bucket");

    assert_eq!(
        None, listed.version,
        "the listing is where the generation goes missing",
    );

    let outcome = storage
        .update_group_generation(GROUP, doc.clone(), Some(Version::from(listed)))
        .await;

    assert!(
        !matches!(outcome, Err(UpdateError::Outdated { .. })),
        "a listed version does not lose the race, it never runs — treating it \
         as `Outdated` would be the coordinator retrying forever",
    );
    assert!(outcome.is_err(), "and it certainly must not win");
    assert_eq!(1, refusals.missing_version());

    // The version from the read does work, which is the control.
    _ = storage
        .update_group_generation(GROUP, doc, Some(read_version))
        .await
        .map_err(|err| Error::Message(format!("{err:?}")))?;

    Ok(())
}
