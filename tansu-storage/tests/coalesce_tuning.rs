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

//! The URL-provided coalescing thresholds must reach the segment flush (#181).
//!
//! `coalesce_tuning_parses_all_keys` pins that the keys are *parsed* into a
//! `CoalesceTuning`, and several unit tests set `coalesce_batches` directly as a
//! lever to force flush boundaries — but nothing pinned the wiring in between:
//! URL -> `coalesce_tuning()` -> `Builder` -> `DynoStore` ->
//! `flush_prefix_coalesced_leaseless`. A knob that parses and then does nothing
//! is the hole #227 closed for the producer-checkpoint keys, and it is invisible
//! from either end.

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tansu_storage::{Storage, StorageContainer, Topition};
use tokio::time::timeout;
use url::Url;

mod common;

/// Far beyond anything this test waits for: if the linger reached the store, no
/// flush can be attributed to it here.
const LINGER: &str = "600s";

/// The batch-count trigger under test.
const BATCHES: usize = 2;

fn batch() -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"tuned"))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn storage_from(query: &str) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
        .build()
        .await
        .map_err(Into::into)
}

/// A `coalesce_batches` given in the storage URL must be the threshold the
/// segment flush actually uses.
///
/// Two phases, because either one alone can pass for the wrong reason:
///
/// 1. `BATCHES` concurrent produces complete promptly. With the default
///    threshold (64) they would not — the buffer would still be filling.
/// 2. A single produce does **not** complete. This is what makes phase 1 mean
///    something: it shows the long linger reached the store too, so phase 1's
///    flush cannot be attributed to the timer. Without it, a `coalesce_linger`
///    that silently kept its 50ms default would flush phase 1 on time and the
///    test would pass with the count trigger doing nothing at all.
///
/// Nothing else can flush here: the byte trigger defaults to 1 MiB and the
/// record cap to 100_000, both far above two one-record batches.
#[tokio::test]
async fn url_batch_count_threshold_reaches_the_segment_flush() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from(&format!(
        "coalesce_linger={LINGER}&coalesce_batches={BATCHES}"
    ))
    .await?;

    let topic = "org.env.conn.tuned";
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic, 0);

    // Phase 1: exactly `BATCHES` in flight, so the count trigger is reached.
    let mut prepared = Vec::with_capacity(BATCHES);
    for _ in 0..BATCHES {
        prepared.push(batch()?);
    }

    let reached = timeout(
        Duration::from_secs(10),
        futures::future::join_all(
            prepared
                .into_iter()
                .map(|b| async { storage.produce(None, &topition, b).await }),
        ),
    )
    .await
    .map_err(|_| {
        Error::Message(format!(
            "{BATCHES} batches did not flush: the URL's coalesce_batches never reached the flush"
        ))
    })?;

    for offset in reached {
        _ = offset?;
    }

    // Phase 2: one short of the threshold stays parked, proving the linger is
    // genuinely long and phase 1 was the count trigger.
    let parked = timeout(
        Duration::from_secs(2),
        storage.produce(None, &topition, batch()?),
    )
    .await;

    assert!(
        parked.is_err(),
        "a lone batch flushed: the URL's coalesce_linger never reached the flush, \
         so phase 1 proves nothing about the count trigger",
    );

    Ok(())
}
