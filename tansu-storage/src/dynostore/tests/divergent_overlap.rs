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

//! A merge never retires one side of an overlap whose sides disagree (#614).
//!
//! On the use1 cluster a consumer was served one set of records at a range of
//! `EUDAT.STKT` at 12:46, a merge at 13:21 kept another, and the partition has
//! served the second ever since: same offsets, different records, nothing left
//! in the bucket to show the first ever existed. The merge picks the overlap's
//! winner from the fenced view and retires the whole run, which heals a
//! duplicated write and destroys anything else.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{
        CoalesceTuning, CompactRun, DynoStore,
        compaction::{BatchSpan, first_divergence},
        tests::init_tracing,
    },
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";

/// A batch of `records` records whose values all start with `row`.
fn batch(row: &str, records: usize) -> Result<deflated::Batch> {
    (0..records)
        .fold(inflated::Batch::builder(), |builder, i| {
            builder.record(
                Record::builder()
                    .offset_delta(i as i32)
                    .value(Some(Bytes::from(format!("{row}-{i}")))),
            )
        })
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn new_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
}

async fn put_segment(
    store: &DynoStore,
    bucket: &InMemory,
    seq: u64,
    base: i64,
    batches: Vec<deflated::Batch>,
) -> Result<()> {
    let (payload, _footer) =
        store.encode_segment_v3(&[(Topition::new(TOPIC, 0), base, batches)], 1, seq)?;
    _ = bucket
        .put(
            &Path::from(format!(
                "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
            )),
            payload,
        )
        .await?;

    Ok(())
}

async fn segments(bucket: &InMemory) -> usize {
    bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments")
        .len()
}

/// Every record the partition serves from offset 0, as `(offset, value)`.
async fn served(store: &DynoStore) -> Result<Vec<(i64, String)>> {
    let mut served = Vec::new();

    for deflated in store
        .fetch(
            &Topition::new(TOPIC, 0),
            0,
            0,
            1_000_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?
    {
        for record in inflated::Batch::try_from(&deflated)?.records {
            let value = record
                .value()
                .map(|value| String::from_utf8_lossy(&value).into_owned())
                .unwrap_or_default();
            served.push((deflated.base_offset + i64::from(record.offset_delta), value));
        }
    }

    Ok(served)
}

/// Two segments put different rows at `[50, 100)`, on the same batch
/// boundaries — the shape the merge used to heal by deleting one side. Both
/// survive the maintenance pass, and the partition serves exactly what it
/// served before it.
#[tokio::test]
async fn a_merge_keeps_both_sides_of_a_divergent_overlap() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    _ = store
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

    put_segment(
        &store,
        &bucket,
        0,
        0,
        vec![batch("a", 50)?, batch("x", 50)?],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        1,
        50,
        vec![batch("y", 50)?, batch("b", 100)?],
    )
    .await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    let before = served(&store).await?;

    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Retry,
    ));
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Drained,
    ));

    assert_eq!(2, segments(&bucket).await, "a refused run retires nothing");
    assert_eq!(before, served(&store).await?);

    Ok(())
}

fn span(from: i64, to: i64, seq: u64, crc: u32) -> BatchSpan {
    BatchSpan { from, to, seq, crc }
}

#[test]
fn a_copy_is_not_a_divergence() {
    assert_eq!(
        None,
        first_divergence(vec![
            span(0, 50, 0, 1),
            span(50, 100, 0, 2),
            span(0, 50, 7, 1),
            span(50, 100, 7, 2),
            span(100, 150, 7, 3),
        ])
    );
}

#[test]
fn the_same_offsets_with_other_records_diverge() {
    assert_eq!(
        Some((span(50, 100, 0, 2), span(50, 100, 1, 9))),
        first_divergence(vec![
            span(0, 50, 0, 1),
            span(50, 100, 0, 2),
            span(50, 100, 1, 9),
        ])
    );
}

#[test]
fn a_batch_straddling_another_diverges() {
    assert_eq!(
        Some((span(0, 50, 0, 1), span(25, 75, 1, 1))),
        first_divergence(vec![span(0, 50, 0, 1), span(25, 75, 1, 1)])
    );
}
