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

//! **A measurement, not a guard.** How many object-store requests does a
//! produce cost, and how many of them are the `watermark.json` read that #316
//! made unconditional?
//!
//! #316 folds the persisted floor into `leaseless_base` on every flush instead
//! of only on a cold sub-stream. The justification given for having no memo was
//! that `persisted_high` goes through the cached `OptiCon<Watermark>` handle, so
//! it is a *conditional* GET answering 304 rather than a full download — cheap
//! next to the LIST and create-CAS PUT a flush already does.
//!
//! That reasoning compares **latency**. It is the wrong axis for this
//! deployment: S3 bills per request, a 304 is a billed request, and request
//! fan-out across ~14.7k topics is the cost driver (#56). So the number that
//! matters is *requests per flush*, and this file measures it rather than
//! arguing it.
//!
//! It reports through `panic!` on purpose: nextest hides stdout for passing
//! tests, so a measurement that prints is a measurement nobody reads.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

#[derive(Clone, Debug, Default)]
struct Tally {
    watermark_gets: Arc<AtomicU64>,
    watermark_conditional: Arc<AtomicU64>,
    watermark_puts: Arc<AtomicU64>,
    other_gets: Arc<AtomicU64>,
    puts: Arc<AtomicU64>,
    lists: Arc<AtomicU64>,
}

impl Tally {
    fn get(counter: &Arc<AtomicU64>) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            Self::get(&self.watermark_gets),
            Self::get(&self.watermark_conditional),
            Self::get(&self.watermark_puts),
            Self::get(&self.other_gets),
            Self::get(&self.puts),
            Self::get(&self.lists),
        )
    }
}

/// Counts requests by kind, separating the `watermark.json` reads from
/// everything else, and separating a *conditional* watermark read (one carrying
/// `if_none_match`, i.e. a revalidation) from a full one.
struct Counted {
    inner: InMemory,
    tally: Tally,
}

impl Counted {
    fn new(tally: Tally) -> Self {
        Self {
            inner: InMemory::new(),
            tally,
        }
    }

    fn is_watermark(location: &Path) -> bool {
        location.as_ref().ends_with("watermark.json")
    }
}

impl Debug for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counted").finish()
    }
}

impl Display for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counted").finish()
    }
}

#[async_trait]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if Self::is_watermark(location) {
            _ = self.tally.watermark_puts.fetch_add(1, Ordering::Relaxed);
        }
        _ = self.tally.puts.fetch_add(1, Ordering::Relaxed);

        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        _ = self.tally.puts.fetch_add(1, Ordering::Relaxed);

        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if Self::is_watermark(location) {
            _ = self.tally.watermark_gets.fetch_add(1, Ordering::Relaxed);

            if options.if_none_match.is_some() {
                _ = self
                    .tally
                    .watermark_conditional
                    .fetch_add(1, Ordering::Relaxed);
            }
        } else {
            _ = self.tally.other_gets.fetch_add(1, Ordering::Relaxed);
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
        _ = self.tally.lists.fetch_add(1, Ordering::Relaxed);

        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        _ = self.tally.lists.fetch_add(1, Ordering::Relaxed);

        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Count the object-store requests a steady-state produce costs, broken down by
/// kind, and fail with the numbers so CI shows them.
///
/// Run this on both sides of #316 to get the A/B: the difference in
/// `watermark_gets` per flush is exactly what the unconditional fold costs.
#[tokio::test]
async fn what_does_a_produce_cost_in_object_store_requests() -> Result<()> {
    let _guard = init_tracing()?;

    const FLUSHES: u64 = 10;

    let tally = Tally::default();
    let storage = DynoStore::new(CLUSTER, NODE, Counted::new(tally.clone()));

    let topic = "org.env.conn.tab_cost";
    let tp = Topition::new(topic, 0);

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

    // Warm the partition first: the first produce is cold by definition (its
    // index is empty, its hint absent), and a cold flush read the floor even
    // before #316. What is being measured is the *steady state*.
    _ = storage.produce(None, &tp, batch(1)?).await?;

    let before = tally.snapshot();

    for _ in 0..FLUSHES {
        _ = storage.produce(None, &tp, batch(1)?).await?;
    }

    let after = tally.snapshot();

    let watermark_gets = after.0 - before.0;
    let watermark_conditional = after.1 - before.1;
    let watermark_puts = after.2 - before.2;
    let other_gets = after.3 - before.3;
    let puts = after.4 - before.4;
    let lists = after.5 - before.5;

    panic!(
        "MEASUREMENT over {FLUSHES} steady-state flushes (per flush in brackets):\n  \
         watermark.json GETs      {watermark_gets} [{:.1}]\n  \
         ...of which conditional  {watermark_conditional} [{:.1}]\n  \
         watermark.json PUTs      {watermark_puts} [{:.1}]\n  \
         other GETs               {other_gets} [{:.1}]\n  \
         PUTs (all)               {puts} [{:.1}]\n  \
         LISTs                    {lists} [{:.1}]\n\
         Note: InMemory may not implement `if_none_match`/NotModified, so the \
         conditional count says whether tansu *asks* for revalidation, not \
         whether the store answers 304. The billed unit is the request either way.",
        watermark_gets as f64 / FLUSHES as f64,
        watermark_conditional as f64 / FLUSHES as f64,
        watermark_puts as f64 / FLUSHES as f64,
        other_gets as f64 / FLUSHES as f64,
        puts as f64 / FLUSHES as f64,
        lists as f64 / FLUSHES as f64,
    );
}
