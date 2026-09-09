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

//! The `gs` arm's retry budget is chosen per request, not per process (#519).
//!
//! GCS has two rate limits with opposite failure shapes — a bucket that ramps
//! and answers a retryable `429` while it redistributes, and a single object
//! name capped at one write per second — and the budget the arm used to carry
//! was right for the second one and wrong for the first.
//! [`crate::gcs::retry`] has the reasoning and the table; this file is what
//! holds the mapping in place.
//!
//! **What it cannot observe is a retry.** `RetryConfig` is consumed inside
//! `object_store`'s HTTP client, below the `ObjectStore` trait, so no decorator
//! and no test over `InMemory` can see a backoff happen. What is observable, and
//! what actually broke in #519, is *which client each request is handed to*: the
//! budget follows from that, and the two clients differ in nothing else. So
//! [`Tallied`] tags the two halves of the split and records every call, and the
//! tests assert a property over the whole log rather than a call at a time —
//! a routing rule that is right for the paths a test remembered to name and
//! wrong for the rest is exactly the failure this is guarding.
//!
//! What is *not* here is the relationship between the two budgets — that the
//! short one stays the short one, which is the mistake #519 warns against. It is
//! a `const` assertion beside the constants in [`crate::gcs::retry`], so it
//! fails to build rather than failing here.
//!
//! [`the_shipped_chain_routes_a_real_workload`] is the one that matters: it runs
//! topic creation, produce, fetch, group formation and an offset commit through
//! `DynoStore` over the decorator stack the `gs` arm actually builds, and asserts
//! the split over everything that stack emitted — including the keys nobody
//! thought to list.

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, MemberRef, OffsetCommitRequest, Result, Storage as _, Topition, UpdateError,
    dynostore::{CoalesceTuning, DynoStore, is_immutable, tests::init_tracing},
    gcs::{limit::PutRateLimiter, retry::RetrySplit},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.table";

/// Which of the two clients served a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Budget {
    /// [`BUCKET_RAMP_RETRY`] — 32 retries over 300 s.
    Ramp,

    /// [`OBJECT_CAP_RETRY`] — 5 retries over 15 s.
    Cap,
}

/// One request as the store below the split saw it.
#[derive(Clone, Debug)]
struct Served {
    budget: Budget,
    method: &'static str,
    location: Option<Path>,
}

/// A tagged view of one bucket.
///
/// Two of these share the `InMemory` behind them and differ only in `budget`,
/// which is the whole point: on `gs://` the two clients are two connection pools
/// onto the *same* bucket, so a test in which they had separate contents would
/// route correctly and still not resemble the deployment. Every call appends to
/// the shared log before delegating.
#[derive(Clone)]
struct Tallied {
    budget: Budget,
    inner: Arc<InMemory>,
    served: Arc<Mutex<Vec<Served>>>,
}

impl std::fmt::Debug for Tallied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tallied")
            .field("budget", &self.budget)
            .finish()
    }
}

impl std::fmt::Display for Tallied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tallied")
            .field("budget", &self.budget)
            .finish()
    }
}

impl Tallied {
    fn record(&self, method: &'static str, location: Option<&Path>) {
        if let Ok(mut served) = self.served.lock() {
            served.push(Served {
                budget: self.budget,
                method,
                location: location.cloned(),
            });
        }
    }
}

#[async_trait]
impl ObjectStore for Tallied {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.record("put_opts", Some(location));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.record("put_multipart_opts", Some(location));
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.record("get_opts", Some(location));
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let this = self.clone();

        locations
            .then(move |location| {
                let this = this.clone();

                async move {
                    let location = location?;
                    this.record("delete_stream", Some(&location));
                    this.inner.delete(&location).await.map(|()| location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.record("list", prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.record("list_with_offset", prefix);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.record("list_with_delimiter", prefix);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.record("copy_opts", Some(to));
        self.inner.copy_opts(from, to, opts).await
    }
}

/// The log of everything both halves of one split saw.
#[derive(Clone)]
struct Ledger(Arc<Mutex<Vec<Served>>>);

impl Ledger {
    fn drain(&self) -> Vec<Served> {
        self.0
            .lock()
            .map(|mut served| served.drain(..).collect())
            .unwrap_or_default()
    }

    /// Assert the routing rule over every request recorded so far.
    ///
    /// Returns `(writes_on_ramp, writes_on_cap)` so the caller can also show the
    /// run reached both — a rule asserted over an empty population passes.
    fn assert_split(&self) -> (usize, usize) {
        let mut data_writes = 0;
        let mut metadata_writes = 0;

        for served in self.drain() {
            let Served {
                budget,
                method,
                location,
            } = served;

            match method {
                // A write to a create-only object cannot meet the per-object
                // cap; a write to anything else is the same name written again.
                "put_opts" | "put_multipart_opts" | "copy_opts" => {
                    let location = location.expect("a write addresses a key");

                    let expected = if is_immutable(&location) {
                        data_writes += 1;
                        Budget::Ramp
                    } else {
                        metadata_writes += 1;
                        Budget::Cap
                    };

                    assert_eq!(
                        expected, budget,
                        "{method} of {location} went to the {budget:?} budget",
                    );
                }

                // Reads, listings and deletes can only ever have met the
                // bucket: there is no per-object read cap, a listing addresses a
                // prefix, and a key is deleted once.
                _ => assert_eq!(
                    Budget::Ramp,
                    budget,
                    "{method} of {location:?} went to the {budget:?} budget",
                ),
            }
        }

        (data_writes, metadata_writes)
    }
}

/// One bucket, two tagged clients over it, split the way the `gs` arm splits
/// them.
fn split() -> (RetrySplit<Tallied>, Ledger) {
    let inner = Arc::new(InMemory::new());
    let served = Arc::new(Mutex::new(Vec::new()));

    let client = |budget| Tallied {
        budget,
        inner: inner.clone(),
        served: served.clone(),
    };

    (
        RetrySplit::new(client(Budget::Ramp), client(Budget::Cap)),
        Ledger(served),
    )
}

/// The `gs` arm's decorator stack, minus the pacing.
///
/// `PutRateLimiter` is here because the order matters — it wraps the split, so
/// its `delete_stream` fan-out (#518) resolves each location through the split
/// rather than the other way round — but its rate is relaxed from the shipped
/// one put per second per key. At 1/s a run that rewrites `meta.json` a dozen
/// times spends a dozen seconds proving something about #427 and nothing about
/// this file; the cap itself is measured in `gcs::limit::tests` and in
/// `tansu-broker/tests/group_formation_cap.rs`.
fn gs_arm(split: RetrySplit<Tallied>) -> PutRateLimiter<RetrySplit<Tallied>> {
    PutRateLimiter::new(split, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(10_000))
        .with_jitter(Some(Duration::from_millis(0)))
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Each method, at a key of each class, straight at the split.
///
/// The end-to-end test below covers the same rule over a real workload; this one
/// is what says which method was wired wrongly when that one fails, and it
/// reaches the two methods a `memory://` workload does not
/// (`put_multipart_opts`, `copy_opts`).
#[tokio::test]
async fn every_method_routes_by_what_it_can_meet() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (split, ledger) = split();

    let segment = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/p/segments/00000000000000000001.seg"
    ));
    let legacy = Path::from(format!(
        "clusters/{CLUSTER}/topics/{TOPIC}/partitions/0000000000/records/0.batch"
    ));

    // A write of every mutable object class the layout has. Each one is a name
    // this engine writes more than once, which is the per-object cap.
    let mutable = [
        format!("clusters/{CLUSTER}/meta.json"),
        format!("clusters/{CLUSTER}/topics/{TOPIC}/partitions/0000000000/watermark.json"),
        format!("clusters/{CLUSTER}/prefixes/p/seq-floor.json"),
        format!("clusters/{CLUSTER}/prefixes/p/era.json"),
        format!("clusters/{CLUSTER}/prefixes/p/lease.json"),
        format!("clusters/{CLUSTER}/topic-metadata/{TOPIC}.json"),
        format!("clusters/{CLUSTER}/producers/1.json"),
        format!("clusters/{CLUSTER}/groups/consumers/g/generation.json"),
        format!("clusters/{CLUSTER}/groups/consumers/g/members/m-0.json"),
        format!("clusters/{CLUSTER}/groups/consumers/g/offsets.json"),
        format!("clusters/{CLUSTER}/acls.bin"),
    ];

    for location in [&segment, &legacy]
        .into_iter()
        .cloned()
        .chain(mutable.iter().map(|location| Path::from(location.as_str())))
    {
        _ = split
            .put_opts(
                &location,
                PutPayload::from(Bytes::from_static(b"{}")),
                PutOptions::default(),
            )
            .await?;

        _ = split.get(&location).await?;
        _ = split.head(&location).await?;
    }

    // A multipart write of a segment is still a data-plane write.
    let mut upload = split.put_multipart(&segment).await?;
    upload
        .put_part(PutPayload::from(Bytes::from_static(b"x")))
        .await?;
    _ = upload.complete().await?;

    // A copy is classed by what it writes, not by what it reads.
    split
        .copy(
            &segment,
            &Path::from(format!("clusters/{CLUSTER}/meta.json")),
        )
        .await?;

    _ = split.list(None).collect::<Vec<_>>().await;
    _ = split
        .list_with_offset(None, &Path::from(format!("clusters/{CLUSTER}/a")))
        .collect::<Vec<_>>()
        .await;
    _ = split.list_with_delimiter(None).await?;

    _ = split
        .delete_stream(futures::stream::iter([Ok(legacy), Ok(segment)]).boxed())
        .collect::<Vec<_>>()
        .await;

    let (data, metadata) = ledger.assert_split();

    assert_eq!(3, data, "two segment writes and a multipart one");
    assert_eq!(
        mutable.len() + 1,
        metadata,
        "every mutable class, plus the copy that lands on one",
    );

    Ok(())
}

/// The shipped chain, over a workload that touches every plane.
///
/// `DynoStore` wraps whatever it is given in `Cache(Metron(_))`, so this is
/// `DynoStore(Cache(Metron(PutRateLimiter(RetrySplit(_, _)))))` — the `gs` arm
/// exactly. The assertion is the same property, over the keys the engine chose
/// rather than the keys this file thought of: a produce, a cross-replica fetch,
/// eight contended group admissions and an offset commit.
///
/// This is also what catches the two clients handed to `RetrySplit::new` in the
/// wrong order, which nothing in the type system can.
#[tokio::test(flavor = "multi_thread")]
async fn the_shipped_chain_routes_a_real_workload() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const MEMBERS: usize = 8;
    const GROUP: &str = "g-1";

    let (split, ledger) = split();

    let storage = DynoStore::new(CLUSTER, NODE, gs_arm(split)).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

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

    let tp = Topition::new(TOPIC, 0);

    for i in 0..8 {
        _ = storage.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    let fetched = storage
        .fetch(
            &tp,
            0,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?;

    assert_eq!(
        8u32,
        fetched.iter().map(|batch| batch.record_count).sum::<u32>(),
    );

    for member in 0..MEMBERS {
        let member_id = format!("m-{member}");

        loop {
            let (mut doc, version) = storage
                .read_group_generation(GROUP)
                .await?
                .map(|(doc, version)| (doc, Some(version)))
                .unwrap_or_default();

            doc.seq += 1;
            doc.generation_id += 1;
            _ = doc.members.insert(member_id.clone(), MemberRef::default());

            match storage.update_group_generation(GROUP, doc, version).await {
                Ok(_) => break,
                Err(UpdateError::Outdated { .. }) => continue,
                Err(err) => return Err(Error::Message(format!("{err:?}"))),
            }
        }
    }

    let committed = storage
        .offset_commit(
            GROUP,
            None,
            &[(
                tp.clone(),
                OffsetCommitRequest {
                    offset: 1,
                    leader_epoch: None,
                    timestamp: None,
                    metadata: None,
                },
            )],
        )
        .await?;

    assert_eq!(vec![(tp, ErrorCode::None)], committed);

    let (data, metadata) = ledger.assert_split();

    // Both halves are non-vacuous, and stated as a floor rather than a count:
    // what the engine writes per produce is #464's and #442's business and moves
    // with them, but a run that wrote no segment — or no metadata — would be
    // asserting the rule over nothing.
    assert!(data > 0, "the run wrote no create-only data object");
    assert!(
        metadata >= MEMBERS,
        "only {metadata} mutable writes; the group plane did not run",
    );

    Ok(())
}
