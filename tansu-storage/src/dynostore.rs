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

//! Dynamic Object Storage engine (S3, memory, ...)

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt::{Debug, Display},
    str::FromStr,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{BoxStream, TryStreamExt},
};
use metadata::Cache;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyOptions, DynObjectStore, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use opticon::OptiCon;
use rand::{prelude::*, rng};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

mod metadata;
mod opticon;

#[cfg(test)]
mod tests;

use crate::{
    AutoTopicCreate, BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, UpdateError, Version,
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
    lake: Option<House>,
    watermarks: Arc<Mutex<BTreeMap<Topition, OptiCon<Watermark>>>>,
    meta: OptiCon<Meta>,

    /// Per-topic optimistic-concurrency handle on
    /// `topic-metadata/{name}.json`, the authoritative record of a topic's id
    /// and config. Decomposing topic metadata out of the cluster-global
    /// `meta.json` removes the create-time CAS contention on that monolith and,
    /// crucially, makes a freshly created topic immediately visible to every
    /// replica: a replica that has never read the topic holds no cached etag,
    /// so the conditional GET cannot be short-circuited to a stale
    /// `NotModified` (the cross-replica create-then-produce race, #28).
    topic_metas: Arc<Mutex<BTreeMap<Topic, OptiCon<TopicMetadata>>>>,

    /// Broker auto-topic-creation policy (Kafka `auto.create.topics.enable` /
    /// `num.partitions` / `default.replication.factor`), consulted by the
    /// Metadata handler.
    auto_create: AutoTopicCreate,

    /// Per-partition cache of the next offset to assign (== the high watermark).
    ///
    /// This is a *hint*, not the authority: the authority is the set of
    /// immutable, create-only batch objects whose names encode their base
    /// offset. The hint lets the common produce path skip a tail listing, and
    /// is reconciled against the listing on a `Create` conflict or a cold read
    /// (see [`DynoStore::refresh_high`]). Keeping offset assignment off a single
    /// mutable `watermark` object is what takes the produce hot path off the
    /// GCS per-object update-rate cap (#13).
    next_offsets: Arc<Mutex<BTreeMap<Topition, i64>>>,

    /// Per-producer optimistic-concurrency handle on `producers/{id}.json`,
    /// holding that producer's idempotent sequence state. Sharding the sequence
    /// CAS per producer (instead of CASing the single cluster-global `meta`
    /// object on every idempotent batch) removes the cross-producer contention
    /// that serialised every `acks=all`/Debezium producer on GCS (#13). The
    /// linearizable CAS is kept, so the exact `OutOfOrderSequenceNumber` /
    /// `DuplicateSequenceNumber` / `ProducerFenced` semantics are preserved.
    producers: Arc<Mutex<BTreeMap<ProducerId, OptiCon<ProducerDetail>>>>,

    object_store: Arc<DynObjectStore>,
}

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Meta {
    producers: BTreeMap<ProducerId, ProducerDetail>,
    transactions: BTreeMap<String, Txn>,
}

impl OptiCon<Meta> {
    fn new(cluster: &str) -> Self {
        Self::path(format!("clusters/{cluster}/meta.json"))
    }
}

impl Meta {
    fn produced(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<BTreeMap<Topition, TxnProduceOffset>> {
        let Some(txn) = self.transactions.get(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if txn.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(txn_detail) = txn.epochs.get(&producer_epoch) else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        let mut produced = BTreeMap::new();

        for (topic, partitions) in txn_detail.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, produced.insert(tp, *offset_range));
            }
        }

        Ok(produced)
    }

    fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Vec<TxnId>> {
        let candidates = self.produced(transaction_id, producer_id, producer_epoch)?;

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        let tp = Topition::new(topic.to_owned(), *partition);

                        if let Some(candidate) = candidates.get(&tp)
                            && offset_range.offset_start < candidate.offset_end
                        {
                            overlapping.push(TxnId {
                                transaction: candidate_id.to_owned(),
                                producer_id: txn.producer,
                                producer_epoch: *epoch,
                                state,
                            });

                            continue 'candidates;
                        }
                    }
                }
            }
        }

        Ok(overlapping)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

impl OptiCon<ProducerDetail> {
    fn new(cluster: &str, producer_id: ProducerId) -> Self {
        Self::path(format!("clusters/{cluster}/producers/{producer_id}.json"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnId {
    transaction: String,
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Txn {
    producer: ProducerId,
    epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

impl From<&TxnDetail> for BTreeMap<Topition, Offset> {
    fn from(value: &TxnDetail) -> Self {
        let mut result = BTreeMap::new();

        for (topic, partitions) in value.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, result.insert(tp, offset_range.offset_start));
            }
        }

        result
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
struct TxnProduceOffset {
    offset_start: Offset,
    offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnCommitOffset {
    committed_offset: Offset,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicMetadata {
    id: Uuid,
    topic: CreatableTopic,
}

impl TopicMetadata {
    /// Apply an incremental config change in place (used under the per-topic
    /// `OptiCon::with_mut` CAS). Mirrors Kafka `IncrementalAlterConfigs`
    /// Set/Delete semantics on `topic.configs`.
    fn alter_configs(&mut self, changes: &[AlterableConfig]) -> Result<()> {
        let mut configuration = self
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .fold(BTreeMap::new(), |mut acc, item| {
                _ = acc.insert(item.name.as_str(), item.value.as_deref());
                acc
            });

        for change in changes {
            match OpType::try_from(change.config_operation)? {
                OpType::Set => {
                    _ = configuration.insert(change.name.as_str(), change.value.as_deref());
                }
                OpType::Delete => {
                    _ = configuration.remove(change.name.as_str());
                }
                OpType::Append => todo!(),
                OpType::Subtract => todo!(),
            }
        }

        _ = self.topic.configs.replace(configuration.into_iter().fold(
            Vec::new(),
            |mut acc, (key, value)| {
                acc.push(
                    CreatableTopicConfig::default()
                        .name(key.to_owned())
                        .value(value.map(|value| value.to_owned())),
                );
                acc
            },
        ));

        Ok(())
    }
}

impl OptiCon<TopicMetadata> {
    fn new(cluster: &str, name: &str) -> Self {
        Self::path(format!("clusters/{cluster}/topic-metadata/{name}.json"))
    }
}

/// Pointer object at `topic-ids/{uuid}.json` mapping a topic's id back to its
/// name, so a metadata lookup by topic-id can resolve to the per-topic
/// `topic-metadata/{name}.json` object. Written create-only alongside the
/// topic in [`DynoStore::create_topic`].
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicIdRef {
    name: Topic,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
}

impl OptiCon<Watermark> {
    fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            cluster, topition.topic, topition.partition,
        ))
    }
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

impl DynoStore {
    pub fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Self {
        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),
            schemas: None,

            lake: None,

            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            next_offsets: Arc::new(Mutex::new(BTreeMap::new())),
            producers: Arc::new(Mutex::new(BTreeMap::new())),
            topic_metas: Arc::new(Mutex::new(BTreeMap::new())),
            auto_create: AutoTopicCreate::default(),
            meta: OptiCon::<Meta>::new(cluster),
            object_store: Arc::new(Cache::new(
                Metron::new(object_store, cluster),
                Duration::from_millis(5_000),
            )),
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub fn auto_create(self, auto_create: AutoTopicCreate) -> Self {
        Self {
            auto_create,
            ..self
        }
    }

    /// Optimistic-concurrency handle on a topic's `topic-metadata/{name}.json`.
    fn topic_meta(&self, name: &str) -> Result<OptiCon<TopicMetadata>> {
        self.topic_metas
            .lock()
            .map(|mut locked| {
                locked
                    .entry(name.to_owned())
                    .or_insert_with(|| OptiCon::<TopicMetadata>::new(self.cluster.as_str(), name))
                    .to_owned()
            })
            .map_err(Into::into)
    }

    fn topic_id_path(&self, id: &Uuid) -> Path {
        Path::from(format!("clusters/{}/topic-ids/{}.json", self.cluster, id))
    }

    /// Resolve a topic-id to its name via the `topic-ids/{uuid}.json` pointer.
    async fn topic_name_by_id(&self, id: &Uuid) -> Result<Option<Topic>> {
        match self.object_store.get(&self.topic_id_path(id)).await {
            Ok(get_result) => get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(|encoded| {
                    serde_json::from_slice::<TopicIdRef>(&encoded).map_err(Into::into)
                })
                .map(|id_ref| Some(id_ref.name)),

            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Every topic's metadata, by listing the `topic-metadata/` prefix and
    /// reading each object. Used by the list-all Metadata path and the cleanup
    /// policies; not on the produce/fetch hot path. The prefix holds only the
    /// per-topic metadata objects (topic *data* lives under `topics/`), so the
    /// listing does not scan record objects.
    async fn all_topics(&self) -> Result<Vec<TopicMetadata>> {
        let prefix = Path::from(format!("clusters/{}/topic-metadata/", self.cluster));

        let mut list_stream = self.object_store.list(Some(&prefix));
        let mut topics = Vec::new();

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err))?
        {
            let encoded = self.object_store.get(&meta.location).await?.bytes().await?;
            topics.push(serde_json::from_slice::<TopicMetadata>(&encoded)?);
        }

        Ok(topics)
    }

    /// One-shot, idempotent backfill from the legacy monolithic `meta.json`
    /// (which embedded a `topics` map) to per-topic `topic-metadata/{name}.json`
    /// objects plus their `topic-ids/{uuid}.json` pointers.
    ///
    /// Safe to run on every boot and from every replica concurrently: each
    /// object is written create-only, so an already migrated (or freshly
    /// created) topic is skipped. A cluster with no legacy `meta.json`, or whose
    /// topics are already decomposed, is a no-op. The legacy `topics` bytes are
    /// left in `meta.json` untouched — they are dead data the current `Meta`
    /// deserialiser ignores.
    async fn migrate_legacy_topic_metadata(&self) -> Result<()> {
        #[derive(Deserialize)]
        struct LegacyMeta {
            #[serde(default)]
            topics: BTreeMap<Topic, TopicMetadata>,
        }

        let path = Path::from(format!("clusters/{}/meta.json", self.cluster));

        let encoded = match self.object_store.get(&path).await {
            Ok(get_result) => get_result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(otherwise) => return Err(otherwise.into()),
        };

        let legacy = serde_json::from_slice::<LegacyMeta>(&encoded)?;
        if legacy.topics.is_empty() {
            return Ok(());
        }

        let mut migrated = 0u64;

        for (name, metadata) in legacy.topics {
            let id = metadata.id;

            // Both writes are create-only and tolerant of an already-present
            // object, so a partially completed prior run converges.
            if self
                .topic_meta(name.as_str())?
                .create(&self.object_store, metadata)
                .await?
            {
                migrated += 1;
            }

            match self
                .object_store
                .put_opts(
                    &self.topic_id_path(&id),
                    serde_json::to_vec(&TopicIdRef { name })
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(otherwise) => return Err(otherwise.into()),
            }
        }

        if migrated > 0 {
            info!(
                cluster = %self.cluster,
                migrated,
                "backfilled legacy topic metadata into per-topic objects"
            );
        }

        Ok(())
    }

    async fn topic_metadata(&self, topic: &TopicId) -> Result<Option<TopicMetadata>> {
        debug!(?topic);

        match topic {
            TopicId::Name(name) => self.topic_meta(name)?.get_opt(&self.object_store).await,
            TopicId::Id(id) => match self.topic_name_by_id(id).await? {
                Some(name) => self.topic_meta(&name)?.get_opt(&self.object_store).await,
                None => Ok(None),
            },
        }
    }

    fn records_prefix(&self, topition: &Topition) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition,
        ))
    }

    fn batch_location(&self, topition: &Topition, offset: i64) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, topition.topic, topition.partition, offset,
        ))
    }

    fn watermark(&self, topition: &Topition) -> Result<OptiCon<Watermark>> {
        self.watermarks
            .lock()
            .map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })
            .map_err(Into::into)
    }

    /// Optimistic-concurrency handle on the per-producer `producers/{id}.json`
    /// object holding `producer_id`'s idempotent sequence state.
    fn producer(&self, producer_id: ProducerId) -> Result<OptiCon<ProducerDetail>> {
        self.producers
            .lock()
            .map(|mut locked| {
                locked
                    .entry(producer_id)
                    .or_insert_with(|| {
                        OptiCon::<ProducerDetail>::new(self.cluster.as_str(), producer_id)
                    })
                    .to_owned()
            })
            .map_err(Into::into)
    }

    /// Seed (or epoch-bump) the per-producer sequence object so the produce hot
    /// path can validate against it. Called from `init_producer` on the cold
    /// registration path; an absent epoch entry is what distinguishes a
    /// registered producer from `UnknownProducerId`.
    async fn seed_producer(&self, response: &ProducerIdResponse) -> Result<()> {
        if response.error != ErrorCode::None || response.id < 0 {
            return Ok(());
        }

        let epoch = response.epoch;

        self.producer(response.id)?
            .with_mut(&self.object_store, |pd| {
                _ = pd.sequences.entry(epoch).or_default();
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// The cached next offset (== high watermark) hint for `topition`, if known
    /// to this process. `None` means the partition has not been read or written
    /// here yet and the tail must be listed.
    fn cached_high(&self, topition: &Topition) -> Result<Option<i64>> {
        self.next_offsets
            .lock()
            .map(|locked| locked.get(topition).copied())
            .map_err(Into::into)
    }

    /// Advance the cached next-offset hint for `topition`. Monotonic: a slower
    /// task can never lower a value a faster one already published, so the hint
    /// only ever moves forward (offsets are never reused).
    fn set_high(&self, topition: &Topition, high: i64) -> Result<()> {
        self.next_offsets
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(topition.to_owned()).or_default();
                *entry = (*entry).max(high);
            })
            .map_err(Into::into)
    }

    /// Read the base offset and record count of the partition's tail batch,
    /// scanning only objects at or after `from` (a cached floor). Returns the
    /// next offset past the tail (`base + last_offset_delta + 1`), or `from`
    /// (defaulting to `0`) when no batch exists at/after the floor.
    ///
    /// The authority for the high watermark is the immutable batch objects, not
    /// any mutable `watermark` object — so this is correct across replicas: a
    /// batch another replica created is visible here as soon as it is listed
    /// (GCS object listing is strongly consistent).
    async fn tail_next_offset(&self, topition: &Topition, from: Option<i64>) -> Result<i64> {
        let prefix = self.records_prefix(topition);

        let mut max: Option<(i64, Path)> = None;

        let mut list_stream = match from {
            Some(floor) => {
                // Batch names are zero-padded offsets, so this strict prefix of
                // `{floor:0>20}.batch` yields base offsets >= floor (same trick
                // as `fetch`), bounding the scan to the contended tail region.
                let start_after = Path::from(format!(
                    "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
                    self.cluster, topition.topic, topition.partition, floor,
                ));

                self.object_store
                    .list_with_offset(Some(&prefix), &start_after)
            }
            None => self.object_store.list(Some(&prefix)),
        };

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            if max.as_ref().is_none_or(|(m, _)| offset > *m) {
                max = Some((offset, meta.location));
            }
        }

        let Some((base, location)) = max else {
            return Ok(from.unwrap_or(0));
        };

        let encoded = self
            .object_store
            .get(&location)
            .await
            .inspect_err(|err| error!(?err, ?topition, base))?
            .bytes()
            .await
            .inspect_err(|err| error!(?err, ?topition, base))?;

        let batch = self.decode(encoded)?;

        Ok(base + batch.last_offset_delta as i64 + 1)
    }

    /// Reconcile the cached high-watermark hint for `topition` against the
    /// partition's batch objects and return the authoritative next offset.
    async fn refresh_high(&self, topition: &Topition) -> Result<i64> {
        let floor = self.cached_high(topition)?;
        let high = self.tail_next_offset(topition, floor).await?;
        self.set_high(topition, high)?;
        Ok(high)
    }

    /// The log end offset (high watermark) for `topition`.
    ///
    /// For ordinary topics the authority is the immutable batch objects
    /// ([`Self::refresh_high`]). Lake-sink topics (`tansu.lake.sink`) write *no*
    /// batch objects — their records go straight to the lake and the offset is
    /// carried in the mutable `watermark` object — so we take the max of the two
    /// sources. `watermark.high` is otherwise no longer advanced on the produce
    /// hot path (#13), so for ordinary topics the listing always wins.
    async fn high_watermark(&self, topition: &Topition) -> Result<i64> {
        let from_objects = self.refresh_high(topition).await?;

        let from_watermark = self
            .watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.high.unwrap_or(0))
            })
            .await?;

        Ok(from_objects.max(from_watermark))
    }

    /// Assign the next offset to `deflated` and persist it as an immutable,
    /// create-only batch object whose name encodes the base offset.
    ///
    /// The `PutMode::Create` *is* the offset-assignment authority: two writers
    /// (tasks on one replica, or different replicas) racing the same offset both
    /// attempt the create, exactly one wins and the loser gets `AlreadyExists`
    /// (GCS guards this with `x-goog-if-generation-match: 0`), resyncs from the
    /// partition tail and retries at the next free offset. This keeps the
    /// produce hot path on *creating distinct new objects* (high create rate is
    /// fine on GCS) instead of *updating one hot `watermark` object* (capped at
    /// ~1/s), which is the #13 collapse. Contiguity holds because the resync
    /// reads the real tail, never a speculative reservation — no gaps, no
    /// overlaps.
    async fn assign_and_create(
        &self,
        topition: &Topition,
        deflated: &deflated::Batch,
        payload: PutPayload,
    ) -> Result<i64> {
        /// Bounds the conflict-resync loop so a pathologically hot partition
        /// fails fast instead of spinning; far above any real contention.
        const MAX_ATTEMPTS: usize = 64;

        let record_count = deflated.last_offset_delta as i64 + 1;

        let mut candidate = match self.cached_high(topition)? {
            Some(hint) => hint,
            None => self.refresh_high(topition).await?,
        };

        for attempt in 0..MAX_ATTEMPTS {
            match self
                .object_store
                .put_opts(
                    &self.batch_location(topition, candidate),
                    payload.clone(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(outcome) => {
                    debug!(?outcome, candidate, ?topition);
                    self.set_high(topition, candidate + record_count)?;
                    return Ok(candidate);
                }

                Err(object_store::Error::AlreadyExists { .. }) => {
                    debug!(
                        candidate,
                        attempt,
                        ?topition,
                        "offset taken, resyncing tail"
                    );
                    candidate = self.tail_next_offset(topition, Some(candidate)).await?;
                    self.set_high(topition, candidate)?;
                }

                Err(err) => return Err(err.into()),
            }
        }

        error!(?topition, candidate, "offset assignment exhausted retries");
        Err(Error::Api(ErrorCode::UnknownServerError))
    }

    /// Enforce the `delete` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `delete`, drop the batches whose records are
    /// older than `retention.ms` (defaulting to 7 days, matching the SQL
    /// backends). Returns the number of batches removed.
    #[instrument(skip(self), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        const DEFAULT_RETENTION: Duration = Duration::from_hours(7 * 24);

        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);

        let topics = self
            .all_topics()
            .await?
            .into_iter()
            .filter_map(|metadata| {
                let configs = metadata.topic.configs.as_deref().unwrap_or_default();

                let delete = configs.iter().any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("delete"))
                });

                if !delete {
                    return None;
                }

                let retention_ms = configs
                    .iter()
                    .find(|config| config.name == "retention.ms")
                    .and_then(|config| config.value.as_deref())
                    .and_then(|value| i64::from_str(value).ok())
                    .unwrap_or(DEFAULT_RETENTION.as_millis() as i64);

                Some((
                    metadata.topic.name.clone(),
                    metadata.topic.num_partitions,
                    retention_ms,
                ))
            })
            .collect::<Vec<_>>();

        let mut deleted = 0;

        for (topic, num_partitions, retention_ms) in topics {
            let threshold_ms = now_ms.saturating_sub(retention_ms);

            for partition in 0..num_partitions {
                let topition = Topition::new(topic.clone(), partition);

                deleted += self
                    .expire_partition(&topition, threshold_ms)
                    .await
                    .inspect_err(|err| error!(?err, ?topition))?;
            }
        }

        Ok(deleted)
    }

    /// List the batch files of `topition` as a map of base offset to its object
    /// metadata (sorted ascending by offset).
    async fn list_batch_offsets(&self, topition: &Topition) -> Result<BTreeMap<i64, ObjectMeta>> {
        let prefix = self.records_prefix(topition);

        let mut offsets = BTreeMap::new();
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            _ = offsets.insert(offset, meta);
        }

        Ok(offsets)
    }

    /// List the base offsets of `topition`'s batch files, sorted ascending.
    ///
    /// Unlike [`Self::list_batch_offsets`] this only retains the `i64` offsets
    /// (not the full [`ObjectMeta`] of every object), so the transient
    /// allocation stays small on hot partitions with many batch objects (#8).
    async fn list_batch_offset_keys(&self, topition: &Topition) -> Result<Vec<i64>> {
        let prefix = self.records_prefix(topition);

        let mut offsets = Vec::new();
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            offsets.push(offset);
        }

        offsets.sort_unstable();

        Ok(offsets)
    }

    /// Set the log start offset (`watermark.low`) to `low`.
    async fn advance_low_watermark(&self, topition: &Topition, low: Option<i64>) -> Result<()> {
        self.watermark(topition)?
            .with_mut(&self.object_store, |watermark| {
                watermark.low = low;
                Ok(())
            })
            .await
    }

    /// Delete the given batch object locations.
    ///
    /// S3 can throttle a multi-object `DeleteObjects` by returning **HTTP 200**
    /// with a top-level `<Error><Code>SlowDown</Code></Error>` body instead of
    /// the expected `<DeleteResult>`. The `object_store` S3 client only retries
    /// on the HTTP status, so a 200-with-error body slips past its retry loop
    /// and then fails XML deserialisation (`unknown variant `Code``), surfacing
    /// as a non-retryable error even though *nothing was deleted* (#5).
    ///
    /// We therefore retry the whole bulk delete ourselves, with backoff, on a
    /// detected throttle; once those retries are exhausted we fall back to
    /// per-key deletes, whose `503 SlowDown` is status-coded and so is retried
    /// by the store's own `RetryConfig`, side-stepping the bulk parse bug.
    async fn delete_batches(&self, locations: Vec<Path>) -> Result<()> {
        /// Times to retry the whole bulk delete on a detected S3 throttle before
        /// falling back to per-key deletes.
        const MAX_BULK_THROTTLE_RETRIES: u32 = 5;

        if locations.is_empty() {
            return Ok(());
        }

        let mut attempt = 0u32;

        loop {
            match self.bulk_delete(locations.clone()).await {
                Ok(()) => return Ok(()),

                Err(err) if is_s3_throttle(&err) => {
                    if attempt >= MAX_BULK_THROTTLE_RETRIES {
                        warn!(
                            %err,
                            "bulk DeleteObjects still throttled after retries; falling back to per-key deletes"
                        );
                        return self.delete_each(locations).await;
                    }

                    let backoff = throttle_backoff(attempt);
                    warn!(%err, attempt, ?backoff, "S3 throttled DeleteObjects; backing off then retrying");
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Issue a single bulk `DeleteObjects` for `locations`.
    async fn bulk_delete(&self, locations: Vec<Path>) -> Result<(), object_store::Error> {
        let stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();

        self.object_store
            .delete_stream(stream)
            .try_collect::<Vec<Path>>()
            .await
            .map(|_| ())
    }

    /// Delete each location individually, ignoring already-absent objects. Used
    /// as a throttle fallback: a single-object DELETE returns a real `503`
    /// status that the store's `RetryConfig` retries.
    async fn delete_each(&self, locations: Vec<Path>) -> Result<()> {
        /// Bounded concurrency for the per-key fallback. A purely sequential
        /// pass would issue up to a full `DeleteObjects` batch (1000) of
        /// round-trips back-to-back; a small fan-out makes progress without
        /// re-creating the request burst that tripped the throttle.
        const DELETE_EACH_CONCURRENCY: usize = 16;

        let object_store = &self.object_store;

        futures::stream::iter(locations)
            .map(|location| async move {
                match object_store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(err) => Err(Error::from(err)),
                }
            })
            .buffer_unordered(DELETE_EACH_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
            .map(|_| ())
    }

    /// Delete every batch of `topition` written before `now - retention_ms`, then
    /// advance the log start offset (`watermark.low`) to the oldest surviving
    /// batch. Age is taken from the object's `last_modified` (the append time),
    /// streamed from the object store so no per-batch GET is required.
    ///
    /// The listing is consumed as a stream and expired locations are deleted in
    /// bounded chunks (`EXPIRE_DELETE_CHUNK`) rather than materialising the whole
    /// partition's object metadata in memory first — a hot partition can hold a
    /// very large number of tiny batch objects, and collecting them all into a
    /// map drives the per-tick memory spike seen on every broker pod (#8).
    async fn expire_partition(&self, topition: &Topition, threshold_ms: i64) -> Result<u64> {
        /// Maximum number of expired locations buffered before a delete is
        /// flushed. Matches the S3 `DeleteObjects` per-request key cap.
        const EXPIRE_DELETE_CHUNK: usize = 1_000;

        let prefix = self.records_prefix(topition);
        let mut list_stream = self.object_store.list(Some(&prefix));

        let mut surviving_low: Option<i64> = None;
        let mut chunk: Vec<Path> = Vec::new();
        let mut deleted: u64 = 0;

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            if meta.last_modified.timestamp_millis() < threshold_ms {
                chunk.push(meta.location);

                if chunk.len() >= EXPIRE_DELETE_CHUNK {
                    deleted += chunk.len() as u64;
                    self.delete_batches(std::mem::take(&mut chunk)).await?;
                }
            } else if surviving_low.is_none_or(|low| offset < low) {
                surviving_low = Some(offset);
            }
        }

        if !chunk.is_empty() {
            deleted += chunk.len() as u64;
            self.delete_batches(chunk).await?;
        }

        if deleted == 0 {
            return Ok(0);
        }

        self.advance_low_watermark(topition, surviving_low).await?;

        Ok(deleted)
    }

    /// Delete every batch of `topition` whose base offset is below `before` and
    /// set the log start offset to `before`. `before` of `-1` means the log end
    /// offset (delete everything). Returns the new log start offset.
    async fn delete_records_before(&self, topition: &Topition, before: i64) -> Result<i64> {
        // The log end offset comes from the immutable batch objects (the
        // authority), not the write-behind `watermark` object, which is no
        // longer advanced on the produce hot path (#13).
        let high = self.high_watermark(topition).await?;

        let before = if before < 0 { high } else { before.min(high) };

        let removed = self
            .list_batch_offsets(topition)
            .await?
            .into_iter()
            .filter(|(offset, _)| *offset < before)
            .map(|(_, meta)| meta.location)
            .collect::<Vec<_>>();

        self.delete_batches(removed).await?;
        self.advance_low_watermark(topition, Some(before)).await?;

        Ok(before)
    }

    /// Enforce the `compact` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `compact`, keep only the latest record per key
    /// (by offset), dropping the earlier versions. Surviving offsets are
    /// preserved. Returns the number of records removed.
    #[instrument(skip(self), ret)]
    async fn policy_compact(&self) -> Result<u64> {
        let topics = self
            .all_topics()
            .await?
            .into_iter()
            .filter_map(|metadata| {
                let configs = metadata.topic.configs.as_deref().unwrap_or_default();

                let compact = configs.iter().any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("compact"))
                });

                compact.then(|| (metadata.topic.name.clone(), metadata.topic.num_partitions))
            })
            .collect::<Vec<_>>();

        let mut compacted = 0;

        for (topic, num_partitions) in topics {
            for partition in 0..num_partitions {
                let topition = Topition::new(topic.clone(), partition);

                compacted += self
                    .compact_partition(&topition)
                    .await
                    .inspect_err(|err| error!(?err, ?topition))?;
            }
        }

        Ok(compacted)
    }

    /// Compact a single partition: walking the batches newest first, drop every
    /// record whose key reappears in a more recent batch (and earlier duplicates
    /// within a batch). Emptied batch files are removed, partially compacted ones
    /// are rewritten in place (preserving base offset and record offsets).
    async fn compact_partition(&self, topition: &Topition) -> Result<u64> {
        let offsets = self.list_batch_offset_keys(topition).await?;

        if offsets.is_empty() {
            return Ok(0);
        }

        let mut seen: BTreeSet<Bytes> = BTreeSet::new();
        let mut removed = vec![];
        let mut surviving_low: Option<i64> = None;
        let mut compacted = 0;

        // newest to oldest: a key kept in a newer batch supersedes older ones
        for offset in offsets.into_iter().rev() {
            let location = self.batch_location(topition, offset);

            let deflated = match self.object_store.get(&location).await {
                Ok(get_result) => self.decode(get_result.bytes().await?)?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(err) => return Err(err.into()),
            };

            let inflated::Compaction { batch, records } =
                inflated::Batch::try_from(&deflated)?.compact(&seen)?;

            compacted += records;
            seen.extend(batch.keys());

            if batch.records.is_empty() {
                removed.push(location);
            } else {
                if records > 0 {
                    let payload = self.encode(deflated::Batch::try_from(batch)?)?;

                    _ = self
                        .object_store
                        .put_opts(
                            &location,
                            payload,
                            PutOptions {
                                mode: PutMode::Overwrite,
                                attributes: Attributes::new(),
                                ..Default::default()
                            },
                        )
                        .await
                        .inspect_err(|err| error!(?err, ?topition, offset))?;
                }

                if surviving_low.is_none_or(|low| offset < low) {
                    surviving_low = Some(offset);
                }
            }
        }

        if !removed.is_empty() {
            self.delete_batches(removed).await?;
            self.advance_low_watermark(topition, surviving_low).await?;
        }

        Ok(compacted as u64)
    }

    fn encode(&self, deflated: deflated::Batch) -> Result<PutPayload> {
        Ok(PutPayload::from(Bytes::from(deflated)))
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        debug!(encoded = ?&encoded[..]);
        deflated::Batch::try_from(encoded)
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn get<V>(&self, location: &Path) -> Result<(V, Version)>
    where
        V: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    async fn put<V>(
        &self,
        location: &Path,
        value: V,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<V>>
    where
        V: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            attributes,
            ..Default::default()
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(%location, ?error))?;

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated {
                    current: Box::new(current),
                    version,
                })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.to_string())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }
}

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        // Idempotent, concurrency-safe backfill of any legacy monolithic topic
        // metadata into per-topic objects on first boot of this version.
        self.migrate_legacy_topic_metadata().await
    }

    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        self.auto_create
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _ = resource;

        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let handle = self.topic_meta(resource.resource_name.as_str())?;

                // Only mutate an existing topic: `with_mut` would otherwise
                // create one from `Default` (Kafka alter on an unknown topic is
                // a no-op here, matching the previous behaviour).
                if handle.get_opt(&self.object_store).await?.is_some() {
                    handle
                        .with_mut(&self.object_store, |topic_metadata| {
                            topic_metadata
                                .alter_configs(resource.configs.as_deref().unwrap_or_default())
                        })
                        .await?;
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let id = Uuid::now_v7();
        debug!(%id);

        // Create-only PUT of the per-topic object. A losing creator (another
        // replica racing the same name) gets `false` here and returns
        // `TopicAlreadyExists` without overwriting the winner's object.
        let created = self
            .topic_meta(topic.name.as_str())?
            .create(
                &self.object_store,
                TopicMetadata {
                    id,
                    topic: topic.clone(),
                },
            )
            .await?;

        if !created {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        // id -> name pointer so a lookup by topic-id can resolve to the named
        // object. Written after the topic object so only the winning creator
        // (whose id is the topic's id) ever writes it.
        _ = self
            .object_store
            .put_opts(
                &self.topic_id_path(&id),
                serde_json::to_vec(&TopicIdRef {
                    name: topic.name.clone(),
                })
                .map(Bytes::from)
                .map(PutPayload::from)?,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        for partition in 0..topic.num_partitions {
            let topition = Topition::new(topic.name.as_str(), partition);

            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), &topition))
                    .to_owned()
            })?;

            watermark
                .with_mut(&self.object_store, |watermark| {
                    _ = watermark.high.take();
                    _ = watermark.low.take();

                    Ok(())
                })
                .await?;

            // Drop any stale next-offset hint (e.g. a topic of the same
            // name was previously deleted) so the fresh, empty partition
            // re-derives offset 0 from listing.
            _ = self
                .next_offsets
                .lock()
                .map(|mut locked| locked.remove(&topition))?;
        }

        Ok(id)
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let topition = Topition::new(topic.name.clone(), partition.partition_index);

                    let partition_result = match self
                        .delete_records_before(&topition, partition.offset)
                        .await
                    {
                        Ok(low_watermark) => DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(ErrorCode::None.into()),

                        Err(err) => {
                            error!(?err, ?topition);

                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(ErrorCode::UnknownServerError.into())
                        }
                    };

                    partition_responses.push(partition_result);
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some(metadata) = self.topic_metadata(topic).await? {
            // Remove the per-topic metadata object and its id -> name pointer.
            self.topic_meta(metadata.topic.name.as_str())?
                .remove(&self.object_store)
                .await?;

            match self
                .object_store
                .delete(&self.topic_id_path(&metadata.id))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(otherwise) => return Err(otherwise.into()),
            }

            let prefix = Path::from(format!(
                "clusters/{}/topics/{}/",
                self.cluster, metadata.topic.name,
            ));

            let locations = self
                .object_store
                .list(Some(&prefix))
                .map_ok(|m| m.location)
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));

            let topic_name = metadata.topic.name.clone();
            let prefix_clone = prefix.clone();
            let locations = self
                .object_store
                .list(Some(&prefix))
                .filter_map(move |m| {
                    let prefix = prefix_clone.clone();
                    let topic_name = topic_name.clone();
                    async move {
                        m.map_or(None, |m| {
                            debug!(?m.location);

                            m.location.prefix_match(&prefix).and_then(|mut i| {
                                // skip over the consumer group name
                                _ = i.next();

                                let sub = Path::from_iter(i);
                                debug!(?sub);

                                if sub.prefix_matches(&Path::from(format!(
                                    "offsets/{}/partitions/",
                                    topic_name
                                ))) {
                                    Some(Ok(m.location.clone()))
                                } else {
                                    None
                                }
                            })
                        })
                    }
                })
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;
            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let config = self
            .describe_config(topition.topic(), ConfigResource::Topic, None)
            .await
            .inspect_err(|err| debug!(?err))?;

        if self.lake.is_some()
            && config
                .configs
                .as_ref()
                .map(|configs| {
                    configs
                        .iter()
                        .inspect(|config| debug!(?config))
                        .any(|config| {
                            config.name.as_str() == "tansu.lake.sink"
                                && config
                                    .value
                                    .as_deref()
                                    .and_then(|value| bool::from_str(value).ok())
                                    .unwrap_or(false)
                        })
                })
                .unwrap_or(false)
        {
            // Get watermark to calculate proper offset for lake sink
            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    let offset = watermark.high.unwrap_or_default();
                    watermark.high = watermark.high.map_or_else(
                        || Some(deflated.last_offset_delta as i64 + 1i64),
                        |high| Some(high + deflated.last_offset_delta as i64 + 1i64),
                    );

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect(|batch_attribute| debug!(?batch_attribute))
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated = inflated::Batch::try_from(&deflated)
                        .inspect(|inflated| debug!(?inflated))
                        .inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect(|validation| debug!(?validation))
                        .inspect_err(|err| debug!(?err))?;

                    if let Some(ref lake) = self.lake {
                        lake.store(
                            topition.topic(),
                            topition.partition(),
                            offset,
                            &inflated,
                            config,
                        )
                        .await
                        .inspect(|store| debug!(?store))
                        .inspect_err(|err| debug!(?err))?;
                    }
                }
            }

            Ok(offset)
        } else {
            if deflated.is_idempotent() {
                // Validate (and advance) the idempotent sequence on the producer's
                // own `producers/{id}.json` object rather than the cluster-global
                // `meta` object, so distinct producers no longer contend on one
                // hot object on GCS (#13). The CAS is still linearizable, so the
                // exact OutOfOrder/Duplicate/Fenced semantics are unchanged.
                self.producer(deflated.producer_id)?
                .with_mut(&self.object_store, |pd| {
                    let Some(mut current) = pd.sequences.last_entry() else {
                        // An empty/absent producer object means the id was never
                        // registered (InitProducerId seeds it).
                        debug!(producer_id = deflated.producer_id, ?pd);
                        return Err(Error::Api(ErrorCode::UnknownProducerId));
                    };

                    if current.key() != &deflated.producer_epoch {
                        debug!(current = ?current.key(), producer_epoch = deflated.producer_epoch);
                        return Err(Error::Api(ErrorCode::ProducerFenced));
                    }

                    let sequences = current.get_mut();
                    debug!(?sequences);

                    match sequences
                        .entry(topition.topic.clone())
                        .or_default()
                        .entry(topition.partition)
                        .or_default()
                    {
                        sequence if *sequence < deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                        }

                        sequence if *sequence > deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                        }

                        sequence => {
                            debug!(?sequence, delta = deflated.last_offset_delta + 1);

                            *sequence += deflated.last_offset_delta + 1;
                            Ok(())
                        }
                    }
                })
                .await
                .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated =
                        inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect_err(|err| debug!(?err))?;
                }
            }

            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            // Assign the offset by *creating* the immutable batch object (its
            // name encodes the base offset), rather than by updating a hot
            // per-partition `watermark` object on every batch. The create is the
            // authority; the watermark stays a write-behind cache only. This is
            // the #13 fix: the produce hot path no longer hammers a single
            // object capped at ~1 write/s on GCS.
            let payload = self
                .encode(deflated.clone())
                .inspect_err(|err| debug!(?err))?;

            let offset = self
                .assign_and_create(topition, &deflated, payload)
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            if !attributes.control
                && let Some(ref lake) = self.lake
            {
                let inflated =
                    inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    &inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                debug!(?txn_detail);

                                let offset_end = offset + deflated.last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            Ok(offset)
        }
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let started_at = SystemTime::now();

        let has_deadline_expired = || {
            started_at
                .elapsed()
                .inspect(|elapsed| debug!(?elapsed, ?max_wait))
                .map(|elapsed| max_wait.saturating_sub(elapsed).is_zero())
                .unwrap_or_default()
        };

        // Read-committed needs the last-stable offset, which `offset_stage`
        // derives from the cluster `meta.json` transactions. Read-uncommitted
        // (the common case) only needs the high watermark, derived purely from
        // the immutable batch objects — so go straight to `high_watermark` and
        // keep the hot fetch path off the meta object entirely.
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            self.offset_stage(topition).await?.last_stable
        } else {
            self.high_watermark(topition).await?
        };

        debug!(high_watermark);

        let mut batches = vec![];

        if offset < high_watermark {
            let prefix = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                self.cluster, topition.topic, topition.partition
            ));

            // Seek directly to the requested offset using `start-after` instead of
            // enumerating the whole partition prefix on every fetch. Batch filenames are
            // zero-padded to 20 digits, so lexicographic order == numeric order. The start
            // key is the 20-digit offset *without* the `.batch` suffix: it is a strict prefix
            // of `{offset:0>20}.batch`, so listing returns exactly `base_offset >= offset`
            // (matching the previous `split_off(&offset)` semantics) while also handling
            // `offset == 0` without underflow.
            let start_after = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
                self.cluster, topition.topic, topition.partition, offset,
            ));

            let mut list_stream = self
                .object_store
                .list_with_offset(Some(&prefix), &start_after);

            let mut bytes = max_bytes as u64;

            // The object_store trait does not guarantee ordering, but every dynostore backend
            // yields ascending keys (S3 ListObjectsV2 sorts by key, the memory store iterates a
            // BTreeMap). We rely on that to stop as soon as enough batches to satisfy `max_bytes`
            // are collected, keeping fetch cost proportional to the bytes returned rather than to
            // the partition's history.
            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
                && !has_deadline_expired()
            {
                let Some(file_name) = meta.location.parts().next_back() else {
                    continue;
                };

                let base_offset = i64::from_str(&file_name.as_ref()[0..20])?;
                debug!(base_offset);

                if base_offset >= high_watermark {
                    break;
                }

                let size = meta.size;

                let mut batch = self
                    .object_store
                    .get(&meta.location)
                    .await
                    .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                    .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
                    .bytes()
                    .await
                    .inspect_err(|error| error!(?error, location = %meta.location))
                    .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                    .and_then(|encoded| self.decode(encoded))?;
                batch.base_offset = base_offset;
                batches.push(batch);

                if size > bytes {
                    break;
                } else {
                    bytes = bytes.saturating_sub(size);
                }
            }
        }

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let stable = self
            .meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        debug!(?txn);

                        txn.epochs
                            .values()
                            .filter(|detail| {
                                detail.state.is_some_and(|state| {
                                    state != TxnState::Committed && state != TxnState::Aborted
                                })
                            })
                            .map(BTreeMap::<Topition, Offset>::from)
                            .collect::<Vec<_>>()
                    })
                    .reduce(|mut acc, e| {
                        debug!(?acc, ?e);

                        for (topition, offset_start) in e.iter() {
                            _ = acc
                                .entry(topition.to_owned())
                                .and_modify(|existing_offset_start| {
                                    if *existing_offset_start > *offset_start {
                                        *existing_offset_start = *offset_start
                                    }
                                })
                                .or_insert(*offset_start);
                        }

                        acc
                    })
                    .unwrap_or(BTreeMap::new()))
            })
            .await?;

        debug!(?stable);

        // The high watermark is derived from the immutable batch objects (the
        // authority), not from the mutable `watermark` object — so it is correct
        // across replicas even though offset assignment no longer CASes the
        // watermark on every produce (#13). The `watermark` object is consulted
        // only for the log start offset (`low`), advanced on the cold
        // maintain/expire path (and for lake-sink topics' high, folded into
        // `high_watermark`).
        let high_watermark = self.high_watermark(topition).await?;

        let watermark = self.watermarks.lock().map(|mut locked| {
            locked
                .entry(topition.to_owned())
                .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                .to_owned()
        })?;

        watermark
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                let log_start = watermark.low.unwrap_or(0);
                let last_stable = stable.get(topition).copied().unwrap_or(high_watermark);

                Ok(OffsetStage {
                    last_stable,
                    high_watermark,
                    log_start,
                })
            })
            .await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let stable = if isolation_level == IsolationLevel::ReadCommitted {
            self.meta
                .with(&self.object_store, |meta| {
                    Ok(meta
                        .transactions
                        .values()
                        .flat_map(|txn| {
                            txn.epochs
                                .values()
                                .filter(|detail| {
                                    detail.state.is_some_and(|state| {
                                        state != TxnState::Committed && state != TxnState::Aborted
                                    })
                                })
                                .map(BTreeMap::<Topition, Offset>::from)
                                .collect::<Vec<_>>()
                        })
                        .reduce(|mut acc, e| {
                            debug!(?acc, ?e);
                            for (topition, offset_start) in e.iter() {
                                _ = acc
                                    .entry(topition.to_owned())
                                    .and_modify(|existing_offset_start| {
                                        if *existing_offset_start > *offset_start {
                                            *existing_offset_start = *offset_start
                                        }
                                    })
                                    .or_insert(*offset_start);
                            }

                            acc
                        })
                        .unwrap_or(BTreeMap::new()))
                })
                .await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for (topition, offset_request) in offsets {
            // LATEST (outside an open read-committed transaction) is the log end
            // offset == high watermark, derived from the immutable batch objects
            // (max base offset + that batch's record count). The previous
            // `last_modified` ordering was wrong under inter-replica clock skew
            // and `max_base + 1` ignored multi-record batches; `high_watermark`
            // is correct on both counts. The timestamp is still the tail batch's
            // mtime (a positive value), preserving the contract the SQL backends
            // also honour; `None` (→ -1 on the wire) only for an empty log.
            if *offset_request == ListOffset::Latest && !stable.contains_key(topition) {
                let high = self.high_watermark(topition).await?;

                let timestamp = self
                    .list_batch_offsets(topition)
                    .await?
                    .last_key_value()
                    .map(|(_, meta)| meta.last_modified.into());

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(high),
                        timestamp,
                    },
                ));

                continue;
            }

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records",
                self.cluster, topition.topic, topition.partition,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            let mut candidate: Option<ObjectMeta> = None;

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                if let Some(last) = stable.get(topition)
                    && offset_request == &ListOffset::Latest
                {
                    let Some(found_offset) = candidate
                        .as_ref()
                        .and_then(|found| found.location.parts().next_back())
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    let Some(meta_offset) = meta
                        .location
                        .parts()
                        .next_back()
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    if meta_offset >= *last && found_offset > meta_offset {
                        _ = candidate.replace(meta);
                    }
                } else {
                    // Selection orders on the immutable base offset encoded in
                    // the object key, never on `last_modified`. Compaction
                    // rewrites surviving batches in place with
                    // `PutMode::Overwrite` (`compact_partition`), which bumps
                    // `last_modified`; ordering on it inverted the mtime↔offset
                    // relation and made EARLIEST/TIMESTAMP return an offset past
                    // retained data, so consumers silently skipped it (see #26).
                    // LATEST was already moved off mtime onto `high_watermark`.
                    let Some(meta_offset) = meta
                        .location
                        .parts()
                        .next_back()
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    let found_offset = candidate.as_ref().and_then(|found| {
                        found
                            .location
                            .parts()
                            .next_back()
                            .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    });

                    match offset_request {
                        // EARLIEST: the smallest base offset present == the log
                        // start (matches the SQL backends' `min(offset_id)`).
                        ListOffset::Earliest
                            if found_offset.is_none_or(|found| meta_offset < found) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        ListOffset::Latest
                            if found_offset.is_none_or(|found| meta_offset > found) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        // TIMESTAMP: earliest offset whose batch is not older than
                        // the target. The `last_modified` predicate is the batch's
                        // write time (best effort — a fully correct answer needs
                        // per-record timestamps), but the candidate is chosen by
                        // smallest base offset so the result stays monotonic in
                        // offset rather than in mtime.
                        ListOffset::Timestamp(system_time)
                            if SystemTime::from(meta.last_modified) > *system_time
                                && found_offset.is_none_or(|found| meta_offset < found) =>
                        {
                            _ = candidate.replace(meta);
                        }
                        _ => continue,
                    }
                }
            }

            debug!(?candidate);

            if let Some(ref found) = candidate {
                let Some(offset) = found.location.parts().next_back() else {
                    continue;
                };

                let offset = i64::from_str(&offset.as_ref()[0..20])?;
                debug!(offset);

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(match offset_request {
                            ListOffset::Latest => offset + 1,
                            _ => offset,
                        }),
                        timestamp: Some(found.last_modified.into()),
                    },
                ))
            } else {
                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(0),
                        ..Default::default()
                    },
                ))
            }
        }

        Ok(responses)
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let mut responses = vec![];

        for (topition, offset_commit) in offsets {
            if self
                .topic_metadata(&TopicId::from(topition))
                .await?
                .is_some()
            {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let payload = serde_json::to_vec(&offset_commit)
                    .map(Bytes::from)
                    .map(PutPayload::from)?;

                let options = PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: json_content_type(),
                    ..Default::default()
                };

                let error_code = self
                    .object_store
                    .put_opts(&location, payload, options)
                    .await
                    .inspect_err(|err| error!(?err))
                    .inspect(|outcome| debug!(?outcome))
                    .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

                responses.push((topition.to_owned(), error_code));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
            }
        }

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let mut topitions = vec![];

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.cluster, group_id,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                debug!(?meta);
                let Some(topic): Option<String> = meta
                    .location
                    .parts()
                    .nth(6)
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .map(|partition| i32::from_str(&partition.as_ref()[0..10]))
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        self.offset_fetch(Some(group_id), topitions.as_ref(), Some(false))
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let offset = match self.object_store.get(&location).await {
                    Ok(get_result) => get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetCommitRequest>(&encoded[..])
                                .map_err(Error::from)
                        })
                        .map(|commit| commit.offset)
                        .inspect_err(|error| error!(?error, ?group_id, ?topition))
                        .map_err(|_| Error::Api(ErrorCode::UnknownServerError)),

                    Err(object_store::Error::NotFound { .. }) => Ok(-1),

                    Err(error) => {
                        error!(?error, ?group_id, ?topition);
                        Err(Error::Api(ErrorCode::UnknownServerError))
                    }
                }?;

                _ = responses.insert(topition.to_owned(), offset);
            }
        }

        Ok(responses)
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    let response = match self
                        .topic_metadata(topic)
                        .await
                        .inspect_err(|error| error!(?error))
                    {
                        Ok(Some(topic_metadata)) => {
                            let name = Some(topic_metadata.topic.name.to_owned());
                            let error_code = ErrorCode::None.into();
                            let topic_id = Some(topic_metadata.id.into_bytes());
                            let is_internal = Some(false);
                            let partitions = topic_metadata.topic.num_partitions;
                            let replication_factor = topic_metadata.topic.replication_factor;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let mut brokers = broker_ids.into_iter().cycle();

                            let partitions = Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some(
                                            (0..replication_factor)
                                                .map(|_replica| brokers.next().expect("cycling"))
                                                .collect(),
                                        );
                                        let isr_nodes = replica_nodes.clone();

                                        MetadataResponsePartition::default()
                                            .error_code(error_code)
                                            .partition_index(partition_index)
                                            .leader_id(leader_id)
                                            .leader_epoch(Some(0))
                                            .replica_nodes(replica_nodes)
                                            .isr_nodes(isr_nodes)
                                            .offline_replicas(Some([].into()))
                                    })
                                    .collect(),
                            );

                            MetadataResponseTopic::default()
                                .error_code(error_code)
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(is_internal)
                                .partitions(partitions)
                                .topic_authorized_operations(Some(-2147483648))
                        }

                        Ok(None) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),

                        Err(_) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => Some("".into()),
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),
                    };

                    responses.push(response);
                }

                responses
            }

            _ => {
                let mut responses = vec![];

                for topic_metadata in self.all_topics().await? {
                    debug!(?topic_metadata);

                    let name = Some(topic_metadata.topic.name.clone());
                    let error_code = ErrorCode::None.into();
                    let topic_id = Some(topic_metadata.id.into_bytes());
                    let is_internal = Some(false);
                    let partitions = topic_metadata.topic.num_partitions;
                    let replication_factor = topic_metadata.topic.replication_factor;

                    debug!(
                        ?error_code,
                        ?topic_id,
                        ?name,
                        ?is_internal,
                        ?partitions,
                        ?replication_factor
                    );

                    let mut rng = rng();
                    let mut broker_ids: Vec<_> =
                        brokers.iter().map(|broker| broker.node_id).collect();
                    broker_ids.shuffle(&mut rng);

                    let mut brokers = broker_ids.into_iter().cycle();

                    let partitions = Some(
                        (0..partitions)
                            .map(|partition_index| {
                                let leader_id = brokers.next().expect("cycling");

                                let replica_nodes = Some(
                                    (0..replication_factor)
                                        .map(|_replica| brokers.next().expect("cycling"))
                                        .collect(),
                                );
                                let isr_nodes = replica_nodes.clone();

                                MetadataResponsePartition::default()
                                    .error_code(error_code)
                                    .partition_index(partition_index)
                                    .leader_id(leader_id)
                                    .leader_epoch(Some(0))
                                    .replica_nodes(replica_nodes)
                                    .isr_nodes(isr_nodes)
                                    .offline_replicas(Some([].into()))
                            })
                            .collect(),
                    );

                    responses.push(
                        MetadataResponseTopic::default()
                            .error_code(error_code)
                            .name(name)
                            .topic_id(topic_id)
                            .is_internal(is_internal)
                            .partitions(partitions)
                            .topic_authorized_operations(Some(-2147483648)),
                    );
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigType::String.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })))
                }

                Ok(None) => Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some(vec![]))),

                Err(_) => todo!(),
            },

            _ => Ok(DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(vec![]))),
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (partition_limit, cursor);

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self
                .topic_metadata(topic)
                .await
                .inspect_err(|error| error!(?error))
            {
                Ok(Some(topic_metadata)) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(topic_metadata.topic.name))
                        .topic_id(topic_metadata.id.into_bytes())
                        .is_internal(false)
                        .partitions(Some(
                            (0..topic_metadata.topic.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition_index(partition_index)
                                        .leader_id(self.node)
                                        .leader_epoch(0)
                                        .replica_nodes(Some(vec![
                                            self.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .isr_nodes(Some(vec![
                                            self.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .eligible_leader_replicas(Some(vec![]))
                                        .last_known_elr(Some(vec![]))
                                        .offline_replicas(Some(vec![]))
                                })
                                .collect(),
                        ))
                        .topic_authorized_operations(-2147483648),
                ),

                Ok(None) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),

                Err(_) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownServerError.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),
            }
        }

        Ok(responses)
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let location = Path::from(format!("clusters/{}/groups/consumers/", self.cluster,));
        let list_result = self
            .object_store
            .list_with_delimiter(Some(&location))
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.cluster))?;

        let mut listed_groups = vec![];

        for prefix in list_result.common_prefixes {
            if let Some(group_id) = prefix.parts().next_back() {
                listed_groups.push(
                    ListedGroup::default()
                        .group_id(group_id.as_ref().into())
                        .protocol_type("consumer".into())
                        .group_state(Some("Unknown".into()))
                        .group_type(Some("classic".into())),
                );
            }
        }

        Ok(listed_groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                let had_group_state = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| error!(group_id, ?err))
                    .is_ok();

                debug!(group_id, had_group_state);

                let prefix = Path::from(format!(
                    "clusters/{}/groups/consumers/{}",
                    self.cluster, group_id,
                ));

                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .boxed();

                let deleted_committed_offsets = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted_committed_offsets);

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group_state || !deleted_committed_offsets.is_empty() {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let mut results = vec![];
        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                match self
                    .get::<GroupDetail>(&location)
                    .await
                    .inspect(|o| debug!(?o, group_id))
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok((group_detail, _)) => {
                        results.push(NamedGroupDetail::found(group_id.into(), group_detail));
                    }

                    Err(Error::ObjectStore(error)) => match error.as_ref() {
                        object_store::Error::NotFound { .. } => {
                            results.push(NamedGroupDetail::found(
                                group_id.into(),
                                GroupDetail::default(),
                            ));
                        }

                        _otherwise => {
                            results.push(NamedGroupDetail::found(
                                group_id.into(),
                                GroupDetail::default(),
                            ));
                        }
                    },

                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let location = Path::from(format!(
            "clusters/{}/groups/consumers/{}.json",
            self.cluster, group_id,
        ));

        self.put(
            &location,
            detail,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        #[derive(Clone, Debug)]
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        if let Some(transaction_id) = transaction_id {
            match self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            match meta.transactions.entry(transaction_id.to_string()) {
                                Entry::Vacant(vacant) => {
                                    let id = meta
                                        .producers
                                        .last_key_value()
                                        .map_or(1.into(), |(k, _v)| k + 1);

                                    let mut pd = ProducerDetail::default();
                                    assert_eq!(None, pd.sequences.insert(0, BTreeMap::new()));
                                    assert_eq!(None, meta.producers.insert(id, pd));

                                    let mut epochs = BTreeMap::new();
                                    assert_eq!(
                                        None,
                                        epochs.insert(
                                            0,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        )
                                    );

                                    _ = vacant.insert(Txn {
                                        producer: id,
                                        epochs,
                                    });

                                    Ok(InitProducer::Completed(ProducerIdResponse {
                                        id,
                                        epoch: 0,
                                        error: ErrorCode::None,
                                    }))
                                }

                                Entry::Occupied(mut occupied) => {
                                    if let Some((current_epoch, txn_detail)) =
                                        occupied.get().epochs.last_key_value()
                                    {
                                        if txn_detail.state == Some(TxnState::Begin) {
                                            Ok(InitProducer::NeedToRollback {
                                                producer_id: occupied.get().producer,
                                                producer_epoch: *current_epoch,
                                            })
                                        } else {
                                            let id = occupied.get().producer;
                                            let epoch = current_epoch + 1;

                                            _ = meta.producers.entry(id).and_modify(|pd| {
                                                assert_eq!(
                                                    None,
                                                    pd.sequences.insert(epoch, BTreeMap::new())
                                                );
                                            });

                                            assert_eq!(
                                                None,
                                                occupied.get_mut().epochs.insert(
                                                    epoch,
                                                    TxnDetail {
                                                        transaction_timeout_ms,
                                                        ..Default::default()
                                                    }
                                                )
                                            );

                                            Ok(InitProducer::Completed(ProducerIdResponse {
                                                id,
                                                epoch,
                                                error: ErrorCode::None,
                                            }))
                                        }
                                    } else {
                                        todo!()
                                    }
                                }
                            }
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            }))
                        }
                    }
                })
                .await?
            {
                InitProducer::Completed(completed) => {
                    self.seed_producer(&completed).await?;
                    Ok(completed)
                }
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(?rollback_producer_id, ?rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        return self
                            .init_producer(
                                Some(transaction_id),
                                transaction_timeout_ms,
                                producer_id,
                                producer_epoch,
                            )
                            .await;
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            let response = self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let producer = meta
                                .producers
                                .last_key_value()
                                .map_or(1.into(), |(k, _v)| k + 1);

                            let epoch = 0;
                            let mut pd = ProducerDetail::default();
                            assert_eq!(None, pd.sequences.insert(epoch, BTreeMap::new()));
                            debug!(?producer, ?pd);
                            assert_eq!(None, meta.producers.insert(producer, pd));

                            Ok(ProducerIdResponse {
                                id: producer,
                                epoch,
                                ..Default::default()
                            })
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            })
                        }
                    }
                })
                .await
                .inspect(|response| debug!(?response))?;

            self.seed_producer(&response).await?;

            Ok(response)
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let Some(transaction) = meta.transactions.get_mut(&transaction_id) else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::TransactionalIdNotFound.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if transaction.producer != producer_id {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::UnknownProducerId.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if &producer_epoch != current_epoch.key() {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let txn_detail = current_epoch.get_mut();

                        let mut results = vec![];

                        for topic in topics {
                            let mut results_by_partition = vec![];

                            for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                _ = txn_detail
                                    .produces
                                    .entry(topic.name.clone())
                                    .or_default()
                                    .entry(*partition_index)
                                    .or_default();

                                results_by_partition.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(*partition_index)
                                        .partition_error_code(i16::from(ErrorCode::None)),
                                );
                            }

                            results.push(
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition)),
                            )
                        }

                        txn_detail.started_at = Some(SystemTime::now());
                        txn_detail.state = Some(TxnState::Begin);

                        Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
                    })
                    .await
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                }

                let txn_detail = current_epoch.get_mut();

                let mut responses = vec![];

                for topic in &offsets.topics {
                    let mut partition_responses = vec![];

                    if let Some(partitions) = topic.partitions.as_deref() {
                        for partition in partitions {
                            _ = txn_detail
                                .offsets
                                .entry(offsets.group_id.clone())
                                .or_default()
                                .entry(topic.name.clone())
                                .or_default()
                                .insert(
                                    partition.partition_index,
                                    TxnCommitOffset {
                                        committed_offset: partition.committed_offset,
                                        leader_epoch: partition.committed_leader_epoch,
                                        metadata: partition.committed_metadata.clone(),
                                    },
                                );

                            partition_responses.push(
                                TxnOffsetCommitResponsePartition::default()
                                    .partition_index(partition.partition_index)
                                    .error_code(ErrorCode::None.into()),
                            );
                        }
                    }

                    responses.push(
                        TxnOffsetCommitResponseTopic::default()
                            .name(topic.name.to_string())
                            .partitions(Some(partition_responses)),
                    );
                }

                Ok(responses)
            })
            .await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let txn_detail = current_epoch.get_mut();

                let mut produced = vec![];

                if txn_detail.state == Some(TxnState::Begin) {
                    assert_eq!(
                        Some(TxnState::Begin),
                        txn_detail.state.replace(if committed {
                            TxnState::PrepareCommit
                        } else {
                            TxnState::PrepareAbort
                        })
                    );

                    for (topic, partitions) in &txn_detail.produces {
                        for (partition, offset_range) in partitions {
                            debug!(?topic, partition, ?offset_range);

                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await
            .inspect(|produced| debug!(?produced))
            .inspect_err(|err| error!(?err))?;

        for topition in produced {
            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };

            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(control_batch.into())
                        .value(end_transaction_marker.into()),
                )
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            _ = self
                .produce(Some(transaction_id), &topition, batch)
                .await
                .inspect(|offset| {
                    debug!(
                        offset,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })?;
        }

        let offsets_to_commit = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let mut overlaps =
                    meta.overlapping_transactions(transaction_id, producer_id, producer_epoch)?;
                debug!(?overlaps);

                let mut offsets_to_commit: BTreeMap<
                    Group,
                    BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>,
                > = BTreeMap::new();

                if overlaps.iter().all(|txn_id| txn_id.state.is_prepared()) {
                    let txn_ids = {
                        overlaps.push(TxnId {
                            transaction: transaction_id.into(),
                            producer_id,
                            producer_epoch,
                            state: if committed {
                                TxnState::PrepareCommit
                            } else {
                                TxnState::PrepareAbort
                            },
                        });

                        overlaps
                    };

                    for txn_id in txn_ids {
                        debug!(?txn_id);

                        if let Some(txn) = meta.transactions.get_mut(txn_id.transaction.as_str())
                            && let Some(txn_detail) = txn.epochs.get_mut(&txn_id.producer_epoch)
                        {
                            debug!(?txn_detail);

                            match txn_detail.state {
                                None | Some(TxnState::PrepareCommit) => {
                                    _ = txn_detail.state.replace(TxnState::Committed);
                                }

                                Some(TxnState::PrepareAbort) => {
                                    _ = txn_detail.state.replace(TxnState::Aborted);
                                }

                                otherwise => {
                                    warn!(
                                        transaction = txn_id.transaction,
                                        producer = txn_id.producer_id,
                                        epoch = txn_id.producer_epoch,
                                        ?otherwise,
                                    );

                                    continue;
                                }
                            }

                            if txn_id.state == TxnState::PrepareCommit {
                                for (group, topics) in txn_detail.offsets.iter() {
                                    for (topic, partitions) in topics.iter() {
                                        for (partition, committed_offset) in partitions {
                                            _ = offsets_to_commit
                                                .entry(group.to_owned())
                                                .or_default()
                                                .entry(topic.to_owned())
                                                .or_default()
                                                .insert(*partition, committed_offset.to_owned());
                                        }
                                    }
                                }
                            }

                            txn_detail.produces.clear();
                            txn_detail.offsets.clear();
                            _ = txn_detail.started_at.take();
                        }
                    }
                }

                Ok(offsets_to_commit)
            })
            .await
            .inspect(|outcome| debug!(?outcome))
            .inspect_err(|err| error!(?err))?;

        debug!(?offsets_to_commit);

        for (group, topics) in offsets_to_commit.iter() {
            let mut offsets = vec![];

            for (topic, partitions) in topics.iter() {
                for (partition, txn_co) in partitions {
                    let tp = Topition::new(topic.to_owned(), *partition);
                    let ocr = OffsetCommitRequest {
                        offset: txn_co.committed_offset,
                        leader_epoch: txn_co.leader_epoch,
                        timestamp: None,
                        metadata: txn_co.metadata.clone(),
                    };

                    offsets.push((tp, ocr));
                }
            }

            _ = self.offset_commit(group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let deleted = self.policy_delete(now).await?;
        let compacted = self.policy_compact().await?;
        debug!(deleted, compacted);

        if let Some(ref lake) = self.lake {
            return lake
                .maintain()
                .await
                .inspect(|maintain| debug!(?maintain))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into);
        }

        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        // Verify connectivity by listing objects at the root
        let _ = self.object_store.list(Some(&Path::from("/"))).next().await;
        Ok(())
    }
}

/// Backoff (with jitter) before retrying a throttled bulk delete: 0.5s, 1s, 2s,
/// 4s, 8s … capped at 30s, plus up to 50% jitter to desynchronise replicas that
/// were throttled at the same instant.
fn throttle_backoff(attempt: u32) -> Duration {
    let base_ms = 500u64.saturating_mul(1 << attempt.min(6)).min(30_000);
    let jitter = rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms + jitter)
}

/// True if `error` looks like an S3 request-rate throttle on a multi-object
/// delete — either a surfaced `SlowDown`, or the `object_store` deserialisation
/// failure that a `200`-with-`<Error>` throttle body produces (the response is
/// `<Error><Code>SlowDown</Code></Error>` rather than `<DeleteResult>`, so the
/// parser reports `unknown variant `Code``). See [`DynoStore::delete_batches`].
fn is_s3_throttle(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string();

        if text.contains("SlowDown")
            || text.contains("unknown variant `Code`")
            || text.contains("invalid DeleteObjects response")
        {
            return true;
        }

        current = err.source();
    }

    false
}

fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

#[derive(Debug, Clone)]
struct Metron<O> {
    request_duration: Histogram<u64>,
    request_error: Counter<u64>,

    cluster: String,
    object_store: O,
}

impl<O> Display for Metron<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metron").finish()
    }
}

impl<O> Metron<O>
where
    O: ObjectStore,
{
    fn new(object_store: O, cluster: &str) -> Self {
        Self {
            cluster: cluster.into(),
            request_duration: METER
                .u64_histogram("tansu_object_store_request_duration")
                .with_unit("ms")
                .with_description("The object store request latencies in milliseconds")
                .build(),
            request_error: METER
                .u64_counter("tansu_object_store_request_error")
                .with_description("The object store request errors")
                .build(),

            object_store,
        }
    }
}

#[async_trait]
impl<O> ObjectStore for Metron<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_opts(location, payload, opts.clone())
            .await
            .inspect(|put_result| {
                debug!(%location, etag = ?put_result.e_tag, version = ?put_result.version);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%location, opts = ?opts, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_multipart_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    #[instrument(skip_all, fields(%location), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "get_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                debug!(meta = ?get_result.meta);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        debug!("delete_stream");

        let cluster = self.cluster.clone();
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            let attributes = vec![
                KeyValue::new("method", "delete_stream"),
                KeyValue::new("cluster", cluster.clone()),
            ];

            if let Err(err) = result {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.extend(attributes);
                request_error.add(1, &additional[..]);
            } else {
                request_duration.record(0, attributes.as_ref());
            }
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);

        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "list_with_delimiter"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        if let Some(prefix) = prefix {
            attributes.push(KeyValue::new("prefix", prefix.to_string()));
        }

        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect(|_list_result| {
                debug!(?prefix);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?prefix, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect(|_| {
                debug!(%from, %to);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%from, %to, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}

#[cfg(test)]
mod throttle_tests {
    use super::{is_s3_throttle, throttle_backoff};

    fn generic_s3(source: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: source.into(),
        }
    }

    #[test]
    fn slowdown_503_body_is_throttle() {
        // A genuine 503 whose body is surfaced after retries are exhausted.
        assert!(is_s3_throttle(&generic_s3(
            "Status { status: 503, body: \"<Error><Code>SlowDown</Code></Error>\" }"
        )));
    }

    #[test]
    fn parse_error_from_200_throttle_body_is_throttle() {
        // S3 returned 200 with <Error><Code>SlowDown</Code></Error>; object_store
        // tried to parse it as <DeleteResult> and tripped on the <Code> element.
        assert!(is_s3_throttle(&generic_s3(
            "Got invalid DeleteObjects response: unknown variant `Code`, expected `Deleted` or `Error`"
        )));
    }

    #[test]
    fn unrelated_errors_are_not_throttle() {
        assert!(!is_s3_throttle(&object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        }));
        assert!(!is_s3_throttle(&generic_s3("connection reset by peer")));
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        // attempt 0: base 500ms + up to 50% jitter => [500, 750]ms
        let first = throttle_backoff(0).as_millis();
        assert!((500..=750).contains(&first), "{first}");

        // large attempt: base capped at 30s + up to 50% jitter => [30s, 45s]
        let capped = throttle_backoff(20).as_millis();
        assert!((30_000..=45_000).contains(&capped), "{capped}");
    }
}
