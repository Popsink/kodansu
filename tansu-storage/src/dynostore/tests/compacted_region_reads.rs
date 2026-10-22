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

//! A `Fetch` on a compacted prefix reads about what it serves (#610).
//!
//! The fetch plan reads every region it admits with one ranged GET over the
//! region's whole extent, and a merge used to write each sub-stream as one
//! region of up to `prefix_compact_target_bytes`. A consumer working through
//! that region paid the whole GET on every `Fetch`: 12.6 GiB read from the
//! store to serve 286 MiB, measured against minio. Compaction now caps a region
//! at `prefix_compact_region_bytes`, and these tests measure the store bytes a
//! read costs — the records it returns were already right before the fix.
//!
//! Measured as the bytes of the ranged GETs issued, not as wall clock or as
//! the `SEGMENT_DATA_BYTES` counter: the first would measure the runner, and
//! the second is the footer's claim, which is exactly the number under test.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{
        CoalesceTuning, DynoStore, SegmentFooter, Substream, SubstreamEntry, SubstreamWrite,
        compaction::capped_regions, tests::init_tracing,
    },
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const TOPIC: &str = "org.env.conn.backlog";

/// Batches produced, one flush window each, so the merge folds them all.
const BATCHES: usize = 64;

/// Records per batch, and the value bytes per record: ~16 KiB a batch, ~1 MiB
/// in all.
const RECORDS: usize = 16;
const VALUE_BYTES: usize = 1024;

/// The region cap these tests compact with: four batches a region.
const CAP: usize = 64 * 1024;

/// The client's partition budget: two batches a fetch.
const MAX_BYTES: u32 = 32 * 1024;

/// Sums the bytes of every ranged GET, which is every region read a fetch
/// makes. The footer warm is a `Suffix` GET and is not counted.
#[derive(Clone)]
struct ReadBytes<O> {
    inner: O,
    bytes: Arc<AtomicU64>,
}

impl<O> Debug for ReadBytes<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBytes").finish()
    }
}

impl<O> Display for ReadBytes<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBytes").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for ReadBytes<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if let Some(GetRange::Bounded(range)) = &options.range {
            _ = self.bytes.fetch_add(range.end - range.start, SeqCst);
        }

        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn bulk_batch(batch: usize) -> Result<deflated::Batch> {
    let value = Bytes::from(vec![b'v'; VALUE_BYTES]);

    (0..RECORDS)
        .fold(inflated::Batch::builder(), |builder, n| {
            builder.record(
                Record::builder()
                    .key(Some(Bytes::from(format!("k{batch}-{n}"))))
                    .value(Some(value.clone())),
            )
        })
        .last_offset_delta(RECORDS as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A store whose whole backlog has been merged into one segment, compacted
/// with `region_bytes` as the cap, and the gauge counting its reads.
async fn compacted_backlog(region_bytes: usize) -> Result<(DynoStore, Arc<AtomicU64>)> {
    let bytes = Arc::new(AtomicU64::new(0));
    let gauge = ReadBytes {
        inner: InMemory::new(),
        bytes: bytes.clone(),
    };

    let store = DynoStore::new(CLUSTER, NODE, gauge).coalesce_tuning(CoalesceTuning {
        coalesce_linger: Some(Duration::from_millis(1)),
        coalesce_batches: Some(1),
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_region_bytes: Some(region_bytes),
        ..Default::default()
    });

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

    let tp = Topition::new(TOPIC, 0);

    for batch in 0..BATCHES {
        _ = store.produce(None, &tp, bulk_batch(batch)?).await?;
    }

    store.maintain(SystemTime::now()).await?;

    let (prefix, substream) = store.routed_substream_of(&tp).await?;
    let segments = store
        .valid_substream_segments(&prefix, &substream, 0)?
        .into_iter()
        .map(|fenced| fenced.seq)
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        1,
        segments.len(),
        "the backlog must be merged into one segment for the read to be a compacted one"
    );

    bytes.store(0, SeqCst);
    Ok((store, bytes))
}

async fn fetch(store: &DynoStore, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            &Topition::new(TOPIC, 0),
            offset,
            1,
            MAX_BYTES,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(300),
        )
        .await
}

/// A fetch from the middle of a compacted sub-stream reads the region holding
/// its offset, not the sub-stream (#610).
///
/// The same fetch is made against the same backlog compacted without a cap,
/// which is what every merge wrote before: that one reads the whole merged
/// region, and is what makes the bound below a measurement rather than a
/// tautology.
#[tokio::test]
async fn a_mid_region_fetch_reads_one_capped_region() -> Result<()> {
    let _guard = init_tracing()?;

    let (uncapped, uncapped_bytes) = compacted_backlog(0).await?;
    let (capped, capped_bytes) = compacted_backlog(CAP).await?;

    let from = uncapped.high_watermark(&Topition::new(TOPIC, 0)).await? / 2;
    assert!(from > 0, "the backlog must be long enough to have a middle");

    // Spans rather than batches: the two backlogs were produced at different
    // times, so their batch timestamps differ.
    let spans = |batches: &[deflated::Batch]| {
        batches
            .iter()
            .map(|batch| (batch.base_offset, batch.last_offset_delta))
            .collect::<Vec<_>>()
    };

    let served = fetch(&capped, from).await?;
    assert_eq!(
        spans(&fetch(&uncapped, from).await?),
        spans(&served),
        "the cap changes how a fetch reads, never what it returns"
    );

    assert!(!served.is_empty(), "a mid-log fetch returns something");

    // The region holding `from`, which may begin well below it, then regions
    // until the budget is spent, the last of them crossing it. No batch here is
    // larger than `CAP`, so no region is either (#610).
    let bound = 2 * CAP as u64 + u64::from(MAX_BYTES);
    let read = capped_bytes.load(SeqCst);
    assert!(
        read <= bound,
        "a {MAX_BYTES} B fetch at offset {from} read {read} B from the store, \
         over the {bound} B of the regions it needs"
    );

    let whole = uncapped_bytes.load(SeqCst);
    assert!(
        whole > bound,
        "uncapped, the same fetch read {whole} B: the backlog is too small for \
         the cap to be what bounds the read"
    );

    Ok(())
}

/// A consumer catching up through a compacted backlog reads it back whole and
/// in order, for store bytes within ~2x of the bytes it is served (#610).
///
/// The issue's acceptance, in miniature: before the cap, every one of the ~32
/// fetches below re-read the whole ~1 MiB region.
#[tokio::test]
async fn a_catch_up_reads_about_what_it_is_served() -> Result<()> {
    let _guard = init_tracing()?;

    let (store, read) = compacted_backlog(CAP).await?;
    let high_watermark = store.high_watermark(&Topition::new(TOPIC, 0)).await?;

    let mut offset = 0;
    let mut served = 0u64;

    while offset < high_watermark {
        let batches = fetch(&store, offset).await?;
        assert!(!batches.is_empty(), "no progress at offset {offset}");

        for batch in batches {
            assert_eq!(
                offset, batch.base_offset,
                "a catch-up must not skip offsets"
            );
            offset += i64::from(batch.last_offset_delta) + 1;
            served += batch.wire_size() as u64;
        }
    }

    assert_eq!((BATCHES * RECORDS) as i64, offset);

    let read = read.load(SeqCst);
    assert!(
        read <= 2 * served + CAP as u64,
        "{read} B read from the store to serve {served} B"
    );

    Ok(())
}

/// Splitting keeps every batch, in order, and the pieces tile the original's
/// offsets — the property the merged region's readers depend on, since they
/// assign offsets by running from each entry's `base_offset`.
#[test]
fn capped_regions_tile_the_original() -> Result<()> {
    let batches = (0..10).map(bulk_batch).collect::<Result<Vec<_>>>()?;
    let size = batches[0].wire_size();
    let cap = 3 * size;

    let capped = capped_regions(
        vec![SubstreamWrite {
            topition: Topition::new(TOPIC, 0),
            substream: Substream::Name(TOPIC.into()),
            base_offset: 100,
            batches: batches.clone(),
        }],
        cap,
    );

    assert_eq!(
        vec![3, 3, 3, 1],
        capped.iter().map(|w| w.batches.len()).collect::<Vec<_>>()
    );

    let mut next = 100;
    for region in &capped {
        assert_eq!(next, region.base_offset);
        next += region
            .batches
            .iter()
            .map(|batch| i64::from(batch.last_offset_delta) + 1)
            .sum::<i64>();
    }
    assert_eq!(100 + 10 * RECORDS as i64, next);

    assert_eq!(
        batches,
        capped
            .into_iter()
            .flat_map(|w| w.batches)
            .collect::<Vec<_>>()
    );

    let oversized = capped_regions(
        vec![SubstreamWrite {
            topition: Topition::new(TOPIC, 0),
            substream: Substream::Name(TOPIC.into()),
            base_offset: 0,
            batches: batches[..2].to_vec(),
        }],
        1,
    );
    assert_eq!(
        vec![1, 1],
        oversized
            .iter()
            .map(|w| w.batches.len())
            .collect::<Vec<_>>(),
        "a batch larger than the cap is a region of its own"
    );

    Ok(())
}

/// The trailer resolvers find the entry for the region they were handed, not
/// the sub-stream's first (#610): with several regions per sub-stream, the first
/// disagrees with every correct entry above it.
#[test]
fn a_footer_resolves_the_region_not_the_substream() {
    let entry = |base_offset, byte_start| SubstreamEntry {
        topic: Arc::from(TOPIC),
        topic_id: None,
        partition: 0,
        base_offset,
        record_count: 16,
        byte_start,
        byte_len: 64,
        max_timestamp: 0,
        producers: Box::default(),
    };

    let footer = SegmentFooter {
        writer_epoch: 0,
        nonce: 0,
        entries: vec![entry(0, 0), entry(16, 64), entry(32, 128)],
    };

    assert_eq!(Some(&footer.entries[1]), footer.region_of(&entry(16, 64)));

    assert_eq!(
        Some(&footer.entries[0]),
        footer.region_of(&entry(48, 192)),
        "an entry naming no region here resolves as before, to the first"
    );
}
