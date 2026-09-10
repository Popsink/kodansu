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

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use rand::{prelude::*, rngs::SmallRng};
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::time::sleep;
use tracing::{debug, instrument};
use url::Url;
use uuid::Uuid;

use crate::delegate::storage_methods;
use crate::{
    AclBinding, AclFilter, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, GenerationDoc, ListOffsetResponse, MemberDoc,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    QuotaAlteration, QuotaEntity, QuotaFilterComponent, QuotaLimits, Quotas, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Clone, Debug)]
pub struct LatencyIntroducingStorage<S> {
    storage: S,
    rng: Arc<Mutex<SmallRng>>,
    latency: Range<u64>,
    /// The decomposed layout's four cost signals (#359), each the counterpart
    /// of a claim the design makes and a test has to be able to falsify:
    ///
    /// - `member_puts`: writes of a member's own document. Bounded by one per
    ///   member per session/2 — liveness churn, moved off the contended object.
    /// - `generation_updates`: writes of `generation.json`, won or lost. The
    ///   claim is that a steady-state group does not write it at all.
    /// - `generation_cas_conflicts`: how many of those lost the etag CAS. The
    ///   whole point of the decomposition is that this converges to zero
    ///   without a per-group owner.
    /// - `member_lists`: listings of a group's member documents, of either kind
    ///   — the one that reads every document and the cheap one batch admission
    ///   elects from (#427). The claim is that no *steady-state* request path
    ///   issues either: only a member the generation does not name yet lists,
    ///   which is a join and not a heartbeat.
    /// - `member_reads`: reads of a member's own document. Bounded by the same
    ///   one per member per session/2 as the writes (#406) — for a long time it
    ///   was not, because the read came *before* the guard that bounds them.
    member_puts: Arc<AtomicU64>,
    generation_updates: Arc<AtomicU64>,
    generation_cas_conflicts: Arc<AtomicU64>,
    member_lists: Arc<AtomicU64>,
    member_reads: Arc<AtomicU64>,

    /// Generation updates still to be refused as a lost CAS before the real one
    /// is attempted (#486).
    ///
    /// The one thing here that is not a counter, and it is why: a join that
    /// loses the generation CAS retries, and what a retry does with the member's
    /// *identity* is the whole of #486. Racing two live coordinators reproduces
    /// it only sometimes; this reproduces it every time, without any test having
    /// to reason about which of two futures the runtime polls first.
    ///
    /// Zero by default, so every other user of this store is unaffected.
    outdate_generation_updates: Arc<AtomicU64>,
}

impl<S> LatencyIntroducingStorage<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(0))),
            latency: 50..150,
            member_puts: Arc::new(AtomicU64::new(0)),
            generation_updates: Arc::new(AtomicU64::new(0)),
            generation_cas_conflicts: Arc::new(AtomicU64::new(0)),
            member_lists: Arc::new(AtomicU64::new(0)),
            member_reads: Arc::new(AtomicU64::new(0)),
            outdate_generation_updates: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A shared handle to this store's count of member-document writes (#359).
    pub fn member_puts_handle(&self) -> Arc<AtomicU64> {
        self.member_puts.clone()
    }

    /// A shared handle to this store's count of `generation.json` writes, won
    /// or lost (#359).
    pub fn generation_updates_handle(&self) -> Arc<AtomicU64> {
        self.generation_updates.clone()
    }

    /// A shared handle to this store's count of `generation.json` writes that
    /// lost the etag CAS (#359).
    pub fn generation_cas_conflicts_handle(&self) -> Arc<AtomicU64> {
        self.generation_cas_conflicts.clone()
    }

    /// A shared handle to this store's count of member-document listings
    /// (#359) — the LIST no request path is supposed to issue.
    pub fn member_lists_handle(&self) -> Arc<AtomicU64> {
        self.member_lists.clone()
    }

    /// A shared handle to this store's count of member-document reads (#406) —
    /// the request the renewal guard could not bound until it was consulted
    /// before the read rather than after it.
    pub fn member_reads_handle(&self) -> Arc<AtomicU64> {
        self.member_reads.clone()
    }

    /// Refuse the next `updates` generation writes as a lost CAS, whatever the
    /// store beneath would have answered (#486).
    ///
    /// The refusal carries the generation as it stands, which is what a real
    /// `Outdated` carries and what the caller's retry re-reads — so a caller
    /// that handles a genuine race handles this, and one that does not is
    /// exercised here rather than in production.
    pub fn outdating_generation_updates(self, updates: u64) -> Self {
        self.outdate_generation_updates
            .store(updates, Ordering::Relaxed);
        self
    }

    pub fn with_seed(self, seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            ..self
        }
    }

    pub fn with_latency(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }

    #[instrument(skip_all)]
    async fn introduce_latency(&self) -> Result<()> {
        let latency = self
            .rng
            .lock()
            .map(|mut rng| rng.random_range(self.latency.clone()))
            .map(Duration::from_millis)
            .inspect(|latency| debug!(?latency))?;

        sleep(latency).await;

        Ok(())
    }
}

/// The one method that carries a span.
///
/// `#[instrument]` cannot be generated: a `macro_rules!` does not expand into
/// attribute position, so the generated `produce` below forwards here and the
/// span sits on this frame instead (#551).
impl<G> LatencyIntroducingStorage<G>
where
    G: Storage + Clone,
{
    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn traced_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.introduce_latency().await?;

        self.storage.produce(transaction_id, topition, batch).await
    }
}

/// One body per method, and the reason this is a second macro: it is invoked
/// in expression position, which `#[async_trait]` can see through, where an
/// item-position call inside the `impl` would still be unexpanded when
/// `async_trait` rewrites it (#551).
///
/// `$s` is the `self` token threaded from the signature list, so the receiver
/// generated below and the `self` used here share a hygiene context.
macro_rules! latency_body {
    ($s:ident, produce, ($($arg:ident),*)) => {{
        $s.traced_produce($($arg),*).await
    }};

    ($s:ident, write_group_member, ($($arg:ident),*)) => {{
        $s.introduce_latency().await?;

        _ = $s.member_puts.fetch_add(1, Ordering::Relaxed);

        $s.storage.write_group_member($($arg),*).await
    }};

    ($s:ident, read_group_member, ($($arg:ident),*)) => {{
        $s.introduce_latency().await?;

        _ = $s.member_reads.fetch_add(1, Ordering::Relaxed);

        $s.storage.read_group_member($($arg),*).await
    }};

    ($s:ident, list_group_members, ($($arg:ident),*)) => {{
        $s.introduce_latency().await?;

        _ = $s.member_lists.fetch_add(1, Ordering::Relaxed);

        $s.storage.list_group_members($($arg),*).await
    }};

    ($s:ident, list_group_member_stamps, ($($arg:ident),*)) => {{
        $s.introduce_latency().await?;

        // Counted as a listing of the group's member documents, because that
        // is what it is: the "no LIST on the request path" assertion in
        // `group_scale` has to see the cheap listing batch admission added
        // (#427) as well as the expensive one it was written against.
        _ = $s.member_lists.fetch_add(1, Ordering::Relaxed);

        $s.storage.list_group_member_stamps($($arg),*).await
    }};

    ($s:ident, update_group_generation, ($group_id:ident, $generation:ident, $version:ident)) => {{
        $s.introduce_latency().await?;

        _ = $s.generation_updates.fetch_add(1, Ordering::Relaxed);

        // Injected loss (#486), before the write rather than after it: a CAS the
        // store accepted and this reported as lost would leave the two
        // disagreeing about what the group holds.
        let held = $s.outdate_generation_updates.load(Ordering::Relaxed);

        if let Some(left) = held.checked_sub(1) {
            $s.outdate_generation_updates.store(left, Ordering::Relaxed);

            _ = $s.generation_cas_conflicts.fetch_add(1, Ordering::Relaxed);

            let (current, version) = $s
                .storage
                .read_group_generation($group_id)
                .await?
                .unwrap_or_default();

            return Err(UpdateError::Outdated {
                current: Box::new(current),
                version,
            });
        }

        let result = $s
            .storage
            .update_group_generation($group_id, $generation, $version)
            .await;

        // `Vanished` is a lost CAS too — the winner's document was deleted
        // before it could be read back (#431) — so it belongs in the conflict
        // count rather than being invisible to it.
        if matches!(
            result,
            Err(UpdateError::Outdated { .. } | UpdateError::Vanished)
        ) {
            _ = $s.generation_cas_conflicts.fetch_add(1, Ordering::Relaxed);
        }

        result
    }};

    ($s:ident, $name:ident, ($($arg:ident),*)) => {{
        $s.introduce_latency().await?;

        $s.storage.$name($($arg),*).await
    }};
}

/// `Storage` for the latency-injecting wrapper, generated from
/// [`storage_methods`](crate::delegate::storage_methods).
///
/// Forty-five of the fifty are `introduce_latency` then forward; five count
/// something on the way past. All fifty were written out by hand until #551.
macro_rules! latency_delegation {
    (
        $(fn $name:ident(&$s:ident $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty;)*
        $(sync fn $sname:ident(&$ss:ident $(, $sarg:ident : $sty:ty)* $(,)?) -> $sret:ty;)*
    ) => {
        #[async_trait]
        impl<G> Storage for LatencyIntroducingStorage<G>
        where
            G: Storage + Clone,
        {
            $(
                async fn $name(&$s $(, $arg: $ty)*) -> $ret {
                    latency_body!($s, $name, ($($arg),*))
                }
            )*
            $(
                fn $sname(&$ss $(, $sarg: $sty)*) -> $sret {
                    $ss.storage.$sname($($sarg),*)
                }
            )*
        }
    };
}

storage_methods!(latency_delegation);
