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

//! A compaction's merged segment does not move the index cursor past segments
//! the index never saw (#616).
//!
//! The maintainer takes the merged segment's sequence from a name-only LIST or
//! its own hint, not from its index, so it can win a sequence above segments a
//! broker created since the index was last refreshed. Inserting that merged
//! segment moved [`PrefixIndex::resolved_max`] past them, and both the
//! incremental listing and the tail probe start above the cursor: those
//! segments stayed out of the index until the whole-prefix reconcile, and run
//! selection meanwhile saw seq-adjacent segments as contiguous across them.

use std::collections::BTreeSet;

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{
        CoalesceTuning, CompactRun, DynoStore, PrefixIndex, Substream, tests::init_tracing,
    },
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";

/// Offsets per segment.
const RECORDS: usize = 50;

fn batch() -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..RECORDS {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(RECORDS as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn new_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
}

/// Segment `seq` holding `[seq * RECORDS, (seq + 1) * RECORDS)`, as a peer's
/// flush would leave it.
async fn put_segment(writer: &DynoStore, bucket: &InMemory, seq: u64) -> Result<()> {
    let base = seq as i64 * RECORDS as i64;
    let (payload, _footer) =
        writer.encode_segment_v3(&[(Topition::new(TOPIC, 0), base, vec![batch()?])], 1, seq)?;
    let path = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ));
    _ = bucket.put(&path, payload).await?;

    Ok(())
}

fn indexed(store: &DynoStore) -> Result<BTreeSet<u64>> {
    Ok(store
        .prefixes
        .index()?
        .get(PREFIX)
        .map(|entry| entry.segments.keys().copied().collect())
        .unwrap_or_default())
}

#[tokio::test]
async fn a_merged_segment_does_not_skip_a_peers_segments() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let broker = new_store(&bucket);
    let maintainer = new_store(&bucket);

    _ = maintainer
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

    for seq in 0..3 {
        put_segment(&broker, &bucket, seq).await?;
    }
    maintainer.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(BTreeSet::from([0, 1, 2]), indexed(&maintainer)?);

    // Within the maintainer's freshness TTL, so the refresh compaction opens
    // with is a no-op and these are what its index lags behind (#616).
    for seq in 3..6 {
        put_segment(&broker, &bucket, seq).await?;
    }

    assert!(matches!(
        maintainer.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(2)
    ));

    let merged = maintainer
        .prefixes
        .index()?
        .get(PREFIX)
        .and_then(PrefixIndex::resolved_max)
        .expect("the merged segment is indexed");
    assert_eq!(6, merged, "the merge wins the first free name");

    assert_eq!(
        BTreeSet::from([2, 3, 4, 5, 6]),
        indexed(&maintainer)?,
        "every live segment is indexed, without waiting for the reconcile"
    );

    let served: Vec<(u64, i64)> = maintainer
        .valid_substream_segments(PREFIX, &Substream::Name(TOPIC.into()), 0)?
        .into_iter()
        .map(|segment| (segment.seq, segment.served_from))
        .collect();
    assert_eq!(
        vec![(6, 0), (2, 100), (3, 150), (4, 200), (5, 250)],
        served,
        "the fenced view tiles [0, 300) with no hole where the peer's segments are"
    );

    Ok(())
}
