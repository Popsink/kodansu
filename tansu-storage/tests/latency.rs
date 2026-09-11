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

//! The latency-injecting `Storage` wrapper.
//!
//! It is test-only code that other tests' conclusions rest on, which is the
//! reason to assert it rather than let the tests using it stand as its
//! coverage: `group_scale` and `cg_latency` assert bounds on *its* counters, so
//! a counter that counted the wrong call, or a delay that was not applied,
//! would make those tests agree with a broker that is not the one running in
//! production. Nothing they assert would go red.
//!
//! What is pinned here is therefore the wrapper's own three claims — every call
//! pays the configured delay, the five counted methods are the five that are
//! counted, and the two synchronous methods are *forwarded* rather than
//! answered — and not the behaviour of whatever it wraps (#556's fifth
//! milestone).

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use tansu_sans_io::{
    IsolationLevel,
    join_group_response::JoinGroupResponseMember,
    record::{Record, deflated, inflated},
};
use tansu_storage::{
    AutoTopicCreate, DEFAULT_FETCH_MAX_BYTES, GenerationDoc, LatencyIntroducingStorage, MemberDoc,
    Storage, StorageContainer, Topition,
};
use tokio::time::Instant;
use url::Url;

use crate::common::init_tracing;

mod common;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

/// The `null://` sink, because what is under test is the wrapper and not what
/// it wraps: the sink needs no service, no feature and no clock.
async fn sink(storage: &str) -> Result<Arc<dyn Storage>> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse(storage)?)
        .silent(true)
        .build()
        .await
        .map_err(Into::into)
}

fn member(member_id: &str) -> MemberDoc {
    MemberDoc {
        last_contact_ms: 1_000,
        session_timeout_ms: 45_000,
        join_response: JoinGroupResponseMember::default().member_id(member_id.into()),
        ..Default::default()
    }
}

/// Every call pays the configured delay, including `produce` — which is the one
/// method with a body of its own, because `#[instrument]` cannot be generated
/// into attribute position and it forwards to a hand-written frame (#551).
///
/// Asserted on the paused clock rather than the wall clock: what the delay is
/// *for* is that a test can put a hundred simulated members through a group and
/// have the object-store latency be the reason the coordinator's own timings
/// come out where they do, and a real 100ms per call would make that suite take
/// hours. The paused clock makes the delay exact and free at once.
#[tokio::test(start_paused = true)]
async fn every_call_pays_the_configured_delay() -> Result {
    let _guard = init_tracing()?;

    let storage = LatencyIntroducingStorage::new(sink("null://sink/").await?)
        .with_seed(6)
        .with_latency(100..101);

    let started = Instant::now();

    storage.ping().await?;
    assert_eq!(Duration::from_millis(100), started.elapsed());

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some("abc".into())))
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage
        .produce(None, &Topition::new("abc", 0), batch)
        .await?;
    assert_eq!(Duration::from_millis(200), started.elapsed());

    Ok(())
}

/// The five counted methods are the five the counters name.
///
/// Each counter is the counterpart of a claim the decomposed group layout makes
/// (#359), and `group_scale` falsifies those claims by reading these handles. A
/// counter incremented on the wrong call would move a bound that test asserts
/// without moving anything a broker does.
#[tokio::test]
async fn the_group_cost_signals_count_the_calls_they_name() -> Result {
    let _guard = init_tracing()?;

    let storage = LatencyIntroducingStorage::new(sink("null://sink/").await?).with_latency(0..1);

    let puts = storage.member_puts_handle();
    let reads = storage.member_reads_handle();
    let lists = storage.member_lists_handle();
    let generations = storage.generation_updates_handle();
    let conflicts = storage.generation_cas_conflicts_handle();

    _ = storage
        .write_group_member("g1", "m1", member("m1"), None)
        .await
        .expect("create");

    _ = storage.read_group_member("g1", "m1").await?;

    // Both listings, because both are a LIST of the group's member documents:
    // the one that reads every document and the cheap one batch admission
    // elects from (#427). A bound on "no LIST on the request path" that saw
    // only the first would have been satisfied by moving to the second.
    _ = storage.list_group_members("g1").await?;
    _ = storage.list_group_member_stamps("g1").await?;

    _ = storage
        .update_group_generation("g1", GenerationDoc::default(), None)
        .await
        .expect("create");

    // Uncounted: the delay is paid, but nothing about a read of the generation
    // or a delete of a member is a cost signal the decomposition claims a bound
    // on.
    _ = storage.read_group_generation("g1").await?;
    storage.delete_group_member("g1", "m1").await?;

    assert_eq!(1, puts.load(Ordering::Relaxed));
    assert_eq!(1, reads.load(Ordering::Relaxed));
    assert_eq!(2, lists.load(Ordering::Relaxed));
    assert_eq!(1, generations.load(Ordering::Relaxed));

    // The claim the whole decomposition rests on: an uncontended generation
    // write loses no race.
    assert_eq!(0, conflicts.load(Ordering::Relaxed));

    Ok(())
}

/// The two synchronous methods are forwarded, not answered.
///
/// This is #273's defect in the one place it costs most: both were `Storage`
/// defaults until #551, so a wrapper that inherited them answered
/// `AutoTopicCreate::default()` and `DEFAULT_FETCH_MAX_BYTES` for a backend
/// configured otherwise — silently turning auto-creation back on and discarding
/// #547's per-deployment fetch clamp. A store configured away from both
/// defaults is what makes the assertion falsifiable.
#[cfg(feature = "dynostore")]
#[tokio::test]
async fn the_synchronous_methods_answer_from_the_store_beneath() -> Result {
    let _guard = init_tracing()?;

    let configured =
        sink("memory://tansu/?auto_create_topics=false&num_partitions=3&fetch_max_bytes=16M")
            .await?;

    assert_ne!(
        AutoTopicCreate::default(),
        configured.auto_create_topic_config()
    );
    assert_ne!(DEFAULT_FETCH_MAX_BYTES, configured.fetch_max_bytes());

    let storage = LatencyIntroducingStorage::new(configured.clone());

    assert_eq!(
        configured.auto_create_topic_config(),
        storage.auto_create_topic_config()
    );
    assert_eq!(configured.fetch_max_bytes(), storage.fetch_max_bytes());

    Ok(())
}

/// `offset_stage_at` is the third method #551 took the default off, and the
/// only one of the three that is `async` — so it is generated by the other
/// repetition group and pays the delay.
#[tokio::test]
async fn the_isolation_aware_offset_stage_is_forwarded() -> Result {
    let _guard = init_tracing()?;

    let inner = sink("null://sink/").await?;
    let storage = LatencyIntroducingStorage::new(inner.clone()).with_latency(0..1);

    let topition = Topition::new("abc", 0);

    assert_eq!(
        inner
            .offset_stage_at(&topition, IsolationLevel::ReadCommitted)
            .await?,
        storage
            .offset_stage_at(&topition, IsolationLevel::ReadCommitted)
            .await?
    );

    Ok(())
}
