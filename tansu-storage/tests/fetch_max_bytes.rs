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

//! The upper bound on one `Fetch` response (#539).
//!
//! It is a *clamp*, not a default: a client configured with
//! `fetch.max.bytes=16m` is answered with 5 MiB and told nothing. That bound
//! was a local `const` for thirteen months and was never chosen against a
//! measurement — #535 and #537 measured what a response *over* it costs, not
//! what the bound should be.
//!
//! What makes it worth reaching is the shape of the workload underneath: every
//! topic in this fleet is single-partition, so one consumer thread is the whole
//! of the parallelism and the only term it can amortise is the per-round-trip
//! cost. Records per response is set by this bound. Whether widening it is
//! worth the broker memory — the bound is charged per fetch in flight — is a
//! question for a fleet A/B, and that is what these tests make possible: the
//! default does not move.

use std::sync::Arc;

use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};
use tansu_storage::{FetchService, Storage, StorageContainer, Topition};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};

mod common;

const TOPIC: &str = "fetch-max-bytes";
const PARTITION: i32 = 0;

/// One record's payload. Under the 1 MiB `message.max.bytes` default, and large
/// enough that a handful of them clear the 5 MiB clamp without the test moving
/// a thousand batches through the store.
const RECORD_BYTES: usize = 768 * 1024;

/// Twelve of those is ~9 MiB: comfortably over the default clamp, under a
/// raised one. Sized so an unclamped response is nowhere near the permitted
/// one-batch overshoot — a log only just over 5 MiB would let a clamp that
/// stopped working pass.
const RECORDS: usize = 12;

/// What a production consumer actually asks for. Every assertion below sends
/// this, because the whole point of the clamp is what happens to a client
/// asking for more than the broker will give.
const CLIENT_ASK: i32 = 16 * 1024 * 1024;

/// A store holding `RECORDS` batches, built with `query` on its storage URL.
async fn seeded(query: &str) -> Result<Arc<dyn Storage>, Error> {
    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(TOPIC, PARTITION);

    for _ in 0..RECORDS {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from(vec![7u8; RECORD_BYTES]))))
            .build()
            .and_then(deflated::Batch::try_from)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    Ok(storage)
}

/// Response bytes for a fetch of the whole partition asking for `max_bytes`.
async fn delivered(storage: &Arc<dyn Storage>, max_bytes: i32) -> Result<usize, Error> {
    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(FetchService)
    };

    let response = service
        .serve(
            Context::default(),
            FetchRequest::default()
                .max_wait_ms(1_000)
                .min_bytes(1)
                .max_bytes(Some(max_bytes))
                .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(TOPIC.into()))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(PARTITION)
                                .fetch_offset(0)
                                .partition_max_bytes(CLIENT_ASK)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(partitions[0]
        .records
        .as_ref()
        .map(|frame| {
            frame
                .batches
                .iter()
                .map(|batch| batch.batch_length.max(0) as usize)
                .sum()
        })
        .unwrap_or_default())
}

/// `max_bytes` is not an absolute maximum in Kafka: a log answers with at least
/// one whole batch whatever the budget, or a request smaller than the next
/// batch would never make progress. So every bound here is `+ RECORD_BYTES`.
fn assert_bounded(delivered: usize, bound: usize, what: &str) {
    let permitted = bound + RECORD_BYTES + 1024;

    assert!(
        delivered <= permitted,
        "{what}: delivered {delivered} bytes against a {bound} byte bound \
         (+{RECORD_BYTES} of permitted one-batch overshoot)",
    );

    assert!(delivered > 0, "{what}: bounded the response to nothing");
}

/// The production case, and the reason the issue exists: the client asks for
/// 16 MiB, the log holds ~6 MiB, and 5 MiB comes back.
#[tokio::test]
async fn the_default_clamp_bounds_a_client_asking_for_more() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let delivered = delivered(&seeded("").await?, CLIENT_ASK).await?;

    assert_bounded(delivered, 5 * 1024 * 1024, "default clamp");

    assert!(
        delivered < RECORDS * RECORD_BYTES,
        "the whole {} byte log came back, so nothing clamped it",
        RECORDS * RECORD_BYTES,
    );

    Ok(())
}

/// The A/B lever. Same log, same client, one storage URL key: the client's ask
/// now reaches the whole partition instead of being truncated at 5 MiB.
///
/// The bar is the *most* a 5 MiB clamp could ever deliver, overshoot included —
/// not 5 MiB. Written the obvious way this test passed against the unpatched
/// call site, because one 768 KiB batch of permitted overshoot clears 5 MiB on
/// its own.
#[tokio::test]
async fn the_clamp_is_raised_from_the_storage_url() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let delivered = delivered(&seeded("fetch_max_bytes=16m").await?, CLIENT_ASK).await?;

    let clamped_ceiling = 5 * 1024 * 1024 + RECORD_BYTES;

    assert!(
        delivered > clamped_ceiling,
        "raised to 16 MiB and delivered {delivered} bytes, which a 5 MiB clamp \
         could have delivered on its own (ceiling {clamped_ceiling})",
    );

    Ok(())
}

/// And lowered, which is what makes the boundary testable without moving
/// megabytes — and what a deployment short of memory would reach for.
#[tokio::test]
async fn the_clamp_is_lowered_from_the_storage_url() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let delivered = delivered(&seeded("fetch_max_bytes=64k").await?, CLIENT_ASK).await?;

    assert_bounded(delivered, 64 * 1024, "lowered clamp");

    Ok(())
}

/// The clamp is a ceiling on the request, never a floor under it. Getting the
/// `min` backwards would answer a client that asked for 64 KiB with 16 MiB —
/// which is #537's defect again, from the other direction.
#[tokio::test]
async fn a_request_below_the_clamp_still_binds() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let delivered = delivered(&seeded("fetch_max_bytes=16m").await?, 64 * 1024).await?;

    assert_bounded(delivered, 64 * 1024, "request under a raised clamp");

    Ok(())
}

/// An unparseable value keeps the default rather than becoming something else.
/// A response bound that silently changed is worse than one that was ignored:
/// the operator believes a number that is not in force — and here the number
/// governs broker memory.
#[tokio::test]
async fn an_unparseable_clamp_keeps_the_default() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let delivered = delivered(&seeded("fetch_max_bytes=enormous").await?, CLIENT_ASK).await?;

    assert_bounded(delivered, 5 * 1024 * 1024, "unparseable clamp");

    assert!(
        delivered < RECORDS * RECORD_BYTES,
        "an unparseable value lifted the clamp instead of keeping it",
    );

    Ok(())
}
