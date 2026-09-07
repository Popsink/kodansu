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

//! The offline audit against whichever object store the suite is pointed at
//! (#531).
//!
//! `tansu-storage/src/audit/tests.rs` covers what the audit *concludes* — the
//! gap arithmetic, the compaction rule, the structural faults — over `InMemory`
//! and a local directory. What it cannot cover is whether the audit can read a
//! footer out of a real store at all, and that is a per-backend question:
//! **every read the audit makes is a suffix GET**, and Azure serves none.
//! `object_store` refuses `Range: bytes=-N` client-side, before sending it
//! (#419), so the broker wraps its Azure store in `SuffixRange` — and until #531
//! the audit, which has its own builder, did not.
//!
//! The failure that has to be caught here is quiet. A `NotSupported` from
//! [`Audit::tail`] is not an error the run returns: it lands in
//! `segments_unreadable` with a fault beside it, so an audit against ADLS Gen2
//! would have printed a bucket in which *no* segment can be read — indexed as
//! damage rather than as an unsupported backend. Hence `segments_unreadable`
//! being asserted at zero and not merely `run()` returning `Ok`.
//!
//! Parameterised on `TANSU_TEST_STORAGE_URL` exactly as the conditional-put
//! conformance target is: unset it and this runs over `memory://` with no
//! service, and `just test-audit-azurite` points it at Azurite, which is where
//! the suffix-range translation is actually exercised. Azurite is not ADLS Gen2
//! — no hierarchical namespace, no throttling, `docs/testing.md` is precise
//! about the gap — but the client-side refusal of a suffix range is
//! `object_store`'s and not the account's, so it reproduces this defect exactly.

#![cfg(feature = "dynostore")]

use bytes::Bytes;
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tansu_storage::{Audit, Backend, Storage as _, StorageContainer, Topition};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url};

mod common;

const TOPIC: &str = "tab_a";
const SEGMENTS: usize = 3;
const RECORDS_PER_SEGMENT: usize = 4;

fn batch(records: usize) -> Result<deflated::Batch, Error> {
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

/// A log the broker wrote, walked by the audit through the same URL.
///
/// The two halves of the fix are both load-bearing and each fails differently:
/// without the scheme arm `try_from_url` returns `UnsupportedStorageUrl` and
/// there is no report at all; with the arm and without the wrap there is a
/// report in which every segment is unreadable.
#[tokio::test]
async fn a_produced_log_is_walked_clean() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster = cluster_id();
    let url = storage_url()?;

    let storage = StorageContainer::builder()
        .cluster_id(cluster.clone())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(url.clone())
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await?;

    let tp = Topition::new(TOPIC, 0);

    for segment in 0..SEGMENTS {
        assert_eq!(
            (segment * RECORDS_PER_SEGMENT) as i64,
            storage
                .produce(None, &tp, batch(RECORDS_PER_SEGMENT)?)
                .await?
        );
    }

    let report = Audit::try_from_url(&url, cluster.as_str())?.run().await?;

    // `memory://` is the one URL that does not name a *shared* store: each
    // resolution of it is a fresh `InMemory`, so the audit walks its own empty
    // one rather than the broker's and can only ever report nothing. Asserted
    // rather than skipped, because "the audit found no segments" is precisely
    // what a broken read path looks like, and this is the one URL where it is
    // the correct answer. The walk that matters runs when the suite is pointed
    // at a store two processes can share — `just test-audit-azurite`.
    if Backend::try_from_url(&url)? == Backend::Memory {
        assert_eq!(0, report.segments);
        return Ok(());
    }

    // Not `> 0`: the walk has to find every segment the produce wrote, since a
    // store that answered one read and refused the rest would otherwise pass.
    assert_eq!(SEGMENTS, report.segments);
    assert_eq!(0, report.segments_unreadable);
    assert!(
        report.faults.is_empty(),
        "a log nothing has damaged has no faults, got {:?}",
        report.faults
    );

    assert_eq!(0, report.lost_records());
    assert_eq!(
        (SEGMENTS * RECORDS_PER_SEGMENT) as i64,
        report.spanned_records()
    );

    Ok(())
}
