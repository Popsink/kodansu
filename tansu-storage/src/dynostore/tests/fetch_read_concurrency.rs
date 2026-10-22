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

//! The region read fan-out of one `Fetch` is a storage-URL key (#587).
//!
//! A fetch over a backlog of small flush regions makes one ranged GET per
//! region, `buffered` at a width that was the `const` 32 (#426). Each wave
//! waits for its slowest GET, so the fetch costs about `regions ÷ width` round
//! trips, and with the width compiled in it could not be A/B'd on one replica.
//!
//! Observed as the peak number of region GETs in flight, not as wall clock: a
//! timing assertion on a shared runner measures the runner, and the number of
//! waves is exactly what the peak decides. Only `GetRange::Bounded` reads are
//! gauged — the footer warm above the data read is a `Suffix` GET with its own
//! fixed fan-out, and counting it would let a width of 8 read as 32.

use std::{
    fmt::{self, Debug, Display},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
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
use tokio::time::sleep;

use crate::{
    Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// More regions than the widest configuration below, so every width is reached
/// and none is capped by the plan instead.
const SEGMENTS: usize = 96;

/// Holds each region GET long enough that a whole wave is in flight together.
#[derive(Clone)]
struct RegionGauge<O> {
    inner: O,
    hold: Duration,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}

impl<O> RegionGauge<O> {
    fn reset(&self) {
        self.in_flight.store(0, SeqCst);
        self.peak.store(0, SeqCst);
        self.reads.store(0, SeqCst);
    }
}

impl<O> Debug for RegionGauge<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionGauge").finish()
    }
}

impl<O> Display for RegionGauge<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionGauge").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for RegionGauge<O>
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
        if !matches!(options.range, Some(GetRange::Bounded(_))) {
            return self.inner.get_opts(location, options).await;
        }

        _ = self.reads.fetch_add(1, SeqCst);
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        _ = self.peak.fetch_max(now, SeqCst);
        sleep(self.hold).await;
        let outcome = self.inner.get_opts(location, options).await;
        _ = self.in_flight.fetch_sub(1, SeqCst);
        outcome
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

fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(
            Record::builder().value(Some(Bytes::copy_from_slice(format!("r-{i}").as_bytes()))),
        );
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Peak region GETs in flight, and region GETs issued, for one fetch over
/// [`SEGMENTS`] regions with `fetch_read_concurrency` set to `configured`.
async fn peak_in_flight(configured: Option<usize>) -> Result<(usize, usize)> {
    let gauge = RegionGauge {
        inner: InMemory::new(),
        hold: Duration::from_millis(20),
        in_flight: Arc::new(AtomicUsize::new(0)),
        peak: Arc::new(AtomicUsize::new(0)),
        reads: Arc::new(AtomicUsize::new(0)),
    };

    let storage = DynoStore::new(CLUSTER, NODE, gauge.clone()).coalesce_tuning(CoalesceTuning {
        fetch_read_concurrency: configured.and_then(NonZeroUsize::new),
        ..Default::default()
    });

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("t".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("t", 0);

    for _ in 0..SEGMENTS {
        _ = storage.produce(None, &topition, batch(4)?).await?;
    }

    gauge.reset();

    let fetched = storage
        .fetch(
            &topition,
            0,
            1,
            64 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(300),
        )
        .await?;

    assert_eq!(SEGMENTS, fetched.len(), "every region's batch is returned");

    Ok((gauge.peak.load(SeqCst), gauge.reads.load(SeqCst)))
}

/// The configured width is the width, below the old 32 and above it.
#[tokio::test(flavor = "multi_thread")]
async fn the_configured_width_is_the_peak_in_flight() -> Result<()> {
    let _guard = init_tracing()?;

    for configured in [8, 64] {
        let (peak, reads) = peak_in_flight(Some(configured)).await?;

        assert_eq!(
            SEGMENTS, reads,
            "the width changes how many GETs wait at once, never how many are made",
        );

        assert_eq!(
            configured, peak,
            "fetch_read_concurrency={configured} peaked at {peak} region GETs in flight",
        );
    }

    Ok(())
}

/// Absent, or zero through the builder, is the width that always shipped.
#[tokio::test(flavor = "multi_thread")]
async fn unset_keeps_the_shipped_width() -> Result<()> {
    let _guard = init_tracing()?;

    for configured in [None, Some(0)] {
        let (peak, _) = peak_in_flight(configured).await?;

        assert_eq!(
            DynoStore::FETCH_READ_CONCURRENCY.get(),
            peak,
            "{configured:?} peaked at {peak} region GETs in flight",
        );
    }

    Ok(())
}
