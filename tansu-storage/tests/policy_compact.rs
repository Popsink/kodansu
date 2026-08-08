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

//! `cleanup.policy=compact` for the dynostore (object store) backend.
//!
//! Every scenario runs twice: against the legacy `records/` layout (the
//! in-place `policy_compact` rewrite) and in segment mode
//! (`prefix_coalesce` + `prefix_leaseless` + `compacted_segments`, #175),
//! where the per-key pass rewrites whole create-only segments. The pair pins
//! that flipping the routing flag changes WHERE compaction happens, never its
//! observable outcome — the invariant the #175 rollout depends on.

use std::{sync::Arc, time::Duration};

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};
use bytes::Bytes;
use tansu_sans_io::{
    IsolationLevel, ListOffset,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};
use tansu_storage::{Storage, StorageContainer, Topition};
use url::Url;

mod common;

type Sc = Arc<Box<dyn Storage>>;

async fn storage_at(query: &str) -> Result<Sc, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
        .build()
        .await
        .map_err(Into::into)
}

async fn default_storage() -> Result<Sc, Error> {
    storage_at("").await
}

/// Segment mode (#175): compacted topics are prefix-coalesced into segments
/// under their own dedicated prefix and per-key compacted there.
async fn segment_storage() -> Result<Sc, Error> {
    storage_at("prefix_coalesce=true&prefix_leaseless=true&compacted_segments=true").await
}

async fn create_topic(
    storage: &Sc,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

fn cleanup_compact() -> Vec<CreatableTopicConfig> {
    vec![
        CreatableTopicConfig::default()
            .name("cleanup.policy".into())
            .value(Some("compact".into())),
    ]
}

fn keyed_batch(key: &'static [u8], value: &'static [u8]) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// (key, value, absolute offset) of every record currently fetchable.
async fn fetched_records(
    storage: &Sc,
    topition: &Topition,
) -> Result<Vec<(Option<Bytes>, Option<Bytes>, i64)>, Error> {
    let batches = storage
        .fetch(
            topition,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;

    let mut records = vec![];

    for deflated in &batches {
        let inflated = inflated::Batch::try_from(deflated)?;

        for record in &inflated.records {
            records.push((
                record.key.clone(),
                record.value.clone(),
                inflated.base_offset + i64::from(record.offset_delta),
            ));
        }
    }

    Ok(records)
}

/// Without this, a repeatedly-updated key retains every stale version forever
/// and a `connect.*`-class topic replays obsolete state on restart.
async fn keeps_latest_per_key(storage: Sc) -> Result<(), Error> {
    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // three single-record batches, all the same key (offsets 0, 1, 2)
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage
            .produce(None, &topition, keyed_batch(b"alpha", value)?)
            .await?;
    }

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    storage.maintain(std::time::SystemTime::now()).await?;

    // only the latest record for the key survives, at its original offset
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![(
            Some(Bytes::from_static(b"alpha")),
            Some(Bytes::from_static(b"three")),
            2,
        )],
        records
    );

    Ok(())
}

#[tokio::test]
async fn keeps_latest_per_key_legacy() -> Result<(), Error> {
    let _guard = init_tracing()?;
    keeps_latest_per_key(default_storage().await?).await
}

#[tokio::test]
async fn keeps_latest_per_key_segments() -> Result<(), Error> {
    let _guard = init_tracing()?;
    keeps_latest_per_key(segment_storage().await?).await
}

/// Without this, an over-eager compactor could treat "same topic" as "same
/// key" and delete live data that merely shares a partition.
async fn distinct_keys_are_retained(storage: Sc) -> Result<(), Error> {
    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    _ = storage
        .produce(None, &topition, keyed_batch(b"a", b"1")?)
        .await?;
    _ = storage
        .produce(None, &topition, keyed_batch(b"b", b"2")?)
        .await?;

    storage.maintain(std::time::SystemTime::now()).await?;

    // different keys: nothing is superseded
    assert_eq!(2, fetched_records(&storage, &topition).await?.len());

    Ok(())
}

#[tokio::test]
async fn distinct_keys_are_retained_legacy() -> Result<(), Error> {
    let _guard = init_tracing()?;
    distinct_keys_are_retained(default_storage().await?).await
}

#[tokio::test]
async fn distinct_keys_are_retained_segments() -> Result<(), Error> {
    let _guard = init_tracing()?;
    distinct_keys_are_retained(segment_storage().await?).await
}

/// Without this, duplicates inside a single batch survive forever — and in
/// segment mode (#175) it also pins that the per-key pass has NO hot-tail
/// exemption: everything here lives in the one newest segment, which a
/// `keep_hot`-style guard would never touch.
async fn compacts_within_a_multi_record_batch(storage: Sc) -> Result<(), Error> {
    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // a single batch holding k1@0, k2@1, k1@2 — k1@0 is superseded by k1@2
    let batch = inflated::Batch::builder()
        .last_offset_delta(2)
        .record(
            Record::builder()
                .offset_delta(0)
                .key(Some(Bytes::from_static(b"k1")))
                .value(Some(Bytes::from_static(b"v1"))),
        )
        .record(
            Record::builder()
                .offset_delta(1)
                .key(Some(Bytes::from_static(b"k2")))
                .value(Some(Bytes::from_static(b"v2"))),
        )
        .record(
            Record::builder()
                .offset_delta(2)
                .key(Some(Bytes::from_static(b"k1")))
                .value(Some(Bytes::from_static(b"v3"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage.produce(None, &topition, batch).await?;

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    storage.maintain(std::time::SystemTime::now()).await?;

    // the batch is rewritten in place: k2@1 and k1@3 survive, offsets preserved
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![
            (
                Some(Bytes::from_static(b"k2")),
                Some(Bytes::from_static(b"v2")),
                1,
            ),
            (
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v3")),
                2,
            ),
        ],
        records
    );

    Ok(())
}

#[tokio::test]
async fn compacts_within_a_multi_record_batch_legacy() -> Result<(), Error> {
    let _guard = init_tracing()?;
    compacts_within_a_multi_record_batch(default_storage().await?).await
}

#[tokio::test]
async fn compacts_within_a_multi_record_batch_segments() -> Result<(), Error> {
    let _guard = init_tracing()?;
    compacts_within_a_multi_record_batch(segment_storage().await?).await
}

/// Without this, key-based removal could leak onto `delete`-policy topics —
/// in segment mode (#175) that would be the per-key pass rewriting a shared
/// CDC prefix it must never touch.
async fn without_compact_policy_nothing_is_removed(storage: Sc) -> Result<(), Error> {
    create_topic(&storage, "plain", vec![]).await?;

    let topition = Topition::new("plain", 0);

    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage
            .produce(None, &topition, keyed_batch(b"alpha", value)?)
            .await?;
    }

    storage.maintain(std::time::SystemTime::now()).await?;

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    Ok(())
}

#[tokio::test]
async fn without_compact_policy_nothing_is_removed_legacy() -> Result<(), Error> {
    let _guard = init_tracing()?;
    without_compact_policy_nothing_is_removed(default_storage().await?).await
}

#[tokio::test]
async fn without_compact_policy_nothing_is_removed_segments() -> Result<(), Error> {
    let _guard = init_tracing()?;
    without_compact_policy_nothing_is_removed(segment_storage().await?).await
}

/// Compaction must not corrupt `ListOffsets(EARLIEST)`.
///
/// Selection used to order candidate objects by their `last_modified`. Compaction
/// rewrites surviving batches in place with `PutMode::Overwrite`, which bumps
/// `last_modified`; the rewritten *oldest* batch then looked *newest* by mtime, so
/// EARLIEST returned a later batch's base offset and a consumer skipped the records
/// still held at the true log start (see #26).
///
/// Here a multi-record batch at offset 0 survives compaction (rewritten in place),
/// while the newer single-record batch at offset 2 is not rewritten — inverting the
/// mtime order. EARLIEST must still report 0, the smallest surviving base offset.
/// In segment mode (#175) the same shape pins Earliest=0/Latest=3 because the
/// rewritten segment's header residue keeps base offset 0 in the footer.
async fn earliest_after_compaction_is_the_log_start_not_the_newest_mtime(
    storage: Sc,
) -> Result<(), Error> {
    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // batch at offsets 0..=1: k1@0, k2@1
    let head = inflated::Batch::builder()
        .last_offset_delta(1)
        .record(
            Record::builder()
                .offset_delta(0)
                .key(Some(Bytes::from_static(b"k1")))
                .value(Some(Bytes::from_static(b"v1"))),
        )
        .record(
            Record::builder()
                .offset_delta(1)
                .key(Some(Bytes::from_static(b"k2")))
                .value(Some(Bytes::from_static(b"v2"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage.produce(None, &topition, head).await?;

    // newer single-record batch at offset 2 supersedes k2@1
    _ = storage
        .produce(None, &topition, keyed_batch(b"k2", b"v3")?)
        .await?;

    storage.maintain(std::time::SystemTime::now()).await?;

    // k2@1 is dropped; the head batch is rewritten in place (base offset 0 kept),
    // so k1@0 is still fetchable while k2 now lives at offset 2.
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![
            (
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v1")),
                0,
            ),
            (
                Some(Bytes::from_static(b"k2")),
                Some(Bytes::from_static(b"v3")),
                2,
            ),
        ],
        records
    );

    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (topition.clone(), ListOffset::Earliest),
                (topition.clone(), ListOffset::Latest),
            ],
        )
        .await?;

    // EARLIEST is the log start (0), not the newest-mtime batch's offset (2).
    assert_eq!(Some(0), responses[0].1.offset);
    // LATEST (high watermark) is unchanged by compaction.
    assert_eq!(Some(3), responses[1].1.offset);

    Ok(())
}

#[tokio::test]
async fn earliest_after_compaction_is_the_log_start_not_the_newest_mtime_legacy()
-> Result<(), Error> {
    let _guard = init_tracing()?;
    earliest_after_compaction_is_the_log_start_not_the_newest_mtime(default_storage().await?).await
}

#[tokio::test]
async fn earliest_after_compaction_is_the_log_start_not_the_newest_mtime_segments()
-> Result<(), Error> {
    let _guard = init_tracing()?;
    earliest_after_compaction_is_the_log_start_not_the_newest_mtime(segment_storage().await?).await
}

/// Multi-key, multi-window per-key compaction over segments (#175): survivors
/// keep their ORIGINAL absolute offsets and a superseded record leaves a gap,
/// not a shift. Without this, a consumer's committed offset would point at a
/// different record after compaction — silent misdelivery.
#[tokio::test]
async fn offsets_survive_segment_compaction() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = segment_storage().await?;

    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // k1@0, k2@1, k1@2 across three flush windows — k1@0 is superseded.
    _ = storage
        .produce(None, &topition, keyed_batch(b"k1", b"v1")?)
        .await?;
    _ = storage
        .produce(None, &topition, keyed_batch(b"k2", b"v1")?)
        .await?;
    _ = storage
        .produce(None, &topition, keyed_batch(b"k1", b"v2")?)
        .await?;

    storage.maintain(std::time::SystemTime::now()).await?;

    // Fetch from 0: the survivors at their original offsets, a gap at 0.
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![
            (
                Some(Bytes::from_static(b"k2")),
                Some(Bytes::from_static(b"v1")),
                1,
            ),
            (
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v2")),
                2,
            ),
        ],
        records
    );

    Ok(())
}

/// The offset span survives a per-key rewrite (#175): emptied batches are kept
/// as headers so the footer's `record_count` stays the sub-stream's span.
/// Without this the recovered tail would regress and the next produce would
/// REUSE an offset — the corruption class the whole emptied-header design
/// exists to prevent.
#[tokio::test]
async fn compaction_preserves_offset_span() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = segment_storage().await?;

    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // Three versions of one key: after compaction the first two batches are
    // fully emptied — only their headers remain to carry the span.
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage
            .produce(None, &topition, keyed_batch(b"alpha", value)?)
            .await?;
    }

    storage.maintain(std::time::SystemTime::now()).await?;

    // LATEST is still the pre-compaction tail...
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(Some(3), responses[0].1.offset);

    // ...and the next produce continues from it rather than reusing an offset.
    let next = storage
        .produce(None, &topition, keyed_batch(b"alpha", b"four")?)
        .await?;
    assert_eq!(3, next);

    Ok(())
}
