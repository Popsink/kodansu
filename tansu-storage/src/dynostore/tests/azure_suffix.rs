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

//! The read path against a store that cannot serve a suffix range (#419).
//!
//! `azure::suffix`'s own tests cover the translation in isolation. These cover
//! what actually matters: that a `DynoStore` over such a store produces and
//! reads records, and that it does so without falling back to the expensive
//! tier.
//!
//! **Every test here reads from a second replica, and that is the whole point.**
//! The writer's own flush populates its in-process prefix index with the footers
//! it just wrote, so the writer never reads a footer back and its fetches
//! succeed against an Azure-shaped store *whether or not the translation is
//! there*. Measured: the same four records read `Ok(4)` in the writing process
//! and `NotSupported` from any other one. So #419's "every fetch fails" holds for
//! every replica that did not write the segment — which is every replica in a
//! fleet, and none in a single-process smoke test.
//!
//! Each test also has its control, because a read path can be broken two ways
//! here: it can fail, or it can succeed by LISTing every time. Only the first
//! has a failing assertion of its own.

use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, Topition,
    azure::suffix::{
        SuffixRange,
        reject::{Requests, SuffixRejecting},
    },
    dynostore::{CoalesceTuning, DynoStore},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const TOPIC: &str = "org.env.conn.table";

/// One batch per segment, so every produce leaves a footer for a reader to find.
/// At the default of 256 there would be one segment and the tail probe would
/// never be asked anything.
fn new_store<O>(bucket: O) -> DynoStore
where
    O: object_store::ObjectStore,
{
    DynoStore::new(CLUSTER, NODE, bucket).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    })
}

/// A writer and a reader over one Azure-shaped bucket, with the suffix-range
/// translation installed or not.
///
/// Two `DynoStore`s over one bucket is how the other tests here model a fleet:
/// each holds its own prefix index, so the reader has to learn the partition
/// from the objects rather than from the writer's memory.
fn replicas(translate: bool) -> (DynoStore, DynoStore, Requests) {
    let rejecting = SuffixRejecting::new(InMemory::new());
    let requests = rejecting.requests();

    let (writer, reader) = if translate {
        (
            new_store(SuffixRange::new(rejecting.clone())),
            new_store(SuffixRange::new(rejecting)),
        )
    } else {
        (new_store(rejecting.clone()), new_store(rejecting))
    };

    (writer, reader, requests)
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

async fn fetch_from(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await
}

async fn produce(
    store: &DynoStore,
    tp: &Topition,
    values: impl IntoIterator<Item = usize>,
) -> Result<()> {
    for i in values {
        _ = store.produce(None, tp, batch(&format!("v{i}"))?).await?;
    }

    Ok(())
}

/// A replica that did not write the segments reads every record through the
/// translation.
#[tokio::test]
async fn a_reader_replica_reads_every_record_through_the_translation() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (writer, reader, requests) = replicas(true);

    create_topic(&writer, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    produce(&writer, &tp, 0..4).await?;

    let fetched = fetch_from(&reader, &tp, 0).await?;

    assert_eq!(4, fetched.len(), "the whole partition must be readable");
    assert_eq!(
        0,
        requests.refused(),
        "a suffix range reached the store, so something bypassed the decorator",
    );
    assert!(
        requests.heads() > 0,
        "no size probe was issued, so the translation never ran and this \
         asserts nothing about it",
    );

    Ok(())
}

/// The control, and the reason it has to be a second replica.
///
/// #418 shipped the Azure arm with reads knowingly broken. This is what "broken"
/// meant — and what it did *not* mean: the writing process reads its own
/// partition back perfectly well, because its index already holds the footers it
/// wrote. Anything that produced and consumed in one process would have found
/// this backend healthy.
#[tokio::test]
async fn without_the_translation_only_the_writer_can_read() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (writer, reader, requests) = replicas(false);

    create_topic(&writer, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    // Produce still works. Every footer read on the write path is best-effort —
    // `fold_segment_footer` returns `false`, the tail probe goes `Inconclusive`,
    // and both fall back to a listing rather than fail.
    produce(&writer, &tp, 0..4).await?;

    assert_eq!(
        4,
        fetch_from(&writer, &tp, 0).await?.len(),
        "the writer reads its own partition from its own index, which is why \
         this defect is invisible to a single-process test",
    );

    let fetched = fetch_from(&reader, &tp, 0).await;

    // `Error::ObjectStore` wraps in an `Arc`, every `Error` here being `Clone`.
    assert!(
        matches!(
            &fetched,
            Err(Error::ObjectStore(error))
                if matches!(**error, object_store::Error::NotSupported { .. }),
        ),
        "a replica that did not write the segments cannot read them: {fetched:?}",
    );
    assert!(
        requests.refused() > 0,
        "the read path must have tried a suffix range for this to be the \
         control it claims to be",
    );

    Ok(())
}

/// A proven tail costs no listing — asserted against the case where it is not
/// proven.
///
/// This is the acceptance criterion #419 words as "`probe_prefix_tail` returns
/// `Absent`, not `Inconclusive`". It is asserted through the only observable
/// consequence, because `TailProbe` has no `Absent` variant: absence proven is
/// `Resolved`, and what `Resolved` buys is that the caller does not LIST.
///
/// The listing count rather than the enum is also the more honest test.
/// `Inconclusive` is not a failure — the read still returns every record, by
/// listing — so a decorator that lost the 404 would pass every other test in
/// this file while making each fetch pay the tier-1 request the whole
/// coalesced-segment design exists to avoid (#411, #412).
#[tokio::test]
async fn a_proven_tail_costs_no_listing_and_an_unproven_one_does() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    /// Segment listings a steady-state produce-then-read costs.
    async fn steady_state_listings(translate: bool) -> Result<usize, Error> {
        let (writer, reader, requests) = replicas(translate);

        create_topic(&writer, TOPIC).await?;
        let tp = Topition::new(TOPIC, 0);

        produce(&writer, &tp, 0..4).await?;

        // Warm both indexes, so what follows is steady state and not a cold
        // start. The reader's first read is allowed to list; the question is
        // what the *next* one costs.
        _ = fetch_from(&reader, &tp, 0).await;
        _ = fetch_from(&writer, &tp, 0).await;

        requests.reset();

        produce(&writer, &tp, 4..5).await?;
        _ = fetch_from(&writer, &tp, 0).await;

        Ok(requests.segment_lists())
    }

    assert_eq!(
        0,
        steady_state_listings(true).await?,
        "the tail was proven by a 404, so nothing should have listed \
         `segments/`",
    );

    assert!(
        steady_state_listings(false).await? > 0,
        "without the translation the probe cannot take its 404, so this test \
         is measuring a difference that exists",
    );

    Ok(())
}
