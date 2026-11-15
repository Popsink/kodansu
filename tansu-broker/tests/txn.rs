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

// Every case here builds a `memory://` container, which needs the one storage
// engine this fork ships. The gate was on `mod in_memory` until #552 dissolved
// it; with the module gone it belongs to the file.
#![cfg(feature = "dynostore")]

use std::{collections::BTreeSet, slice, sync::Arc, time::Duration};

use anyhow::Result;
use rand::{RngExt as _, rng};
use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnPartitionResult;
use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnTopicResult;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::record::{Record, inflated};
use tansu_sans_io::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use tansu_sans_io::{BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode};
use tansu_sans_io::{IsolationLevel, ListOffset};
use tansu_storage::{
    ProducerIdResponse, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnOffsetCommitRequest,
};

use crate::common::{alphanumeric_string, init_tracing, register_broker};

mod common;

const PARTITIONS: i32 = 6;

/// How many single-record batches [`Partition::begin`] produces.
const RECORDS: i32 = 6;

const TIMEOUT_MS: i32 = 10_000;

const MAX_WAIT: Duration = Duration::from_millis(500);

/// A transaction id and the producer it was initialised with.
type Txn = (String, ProducerIdResponse);

/// One partition, picked at random, of a fresh topic on a freshly registered
/// broker: every transaction in a test writes here.
struct Partition {
    storage: Arc<dyn Storage>,
    id: uuid::Uuid,
    topition: Topition,
    produced: BTreeSet<i64>,
}

impl Partition {
    async fn new() -> Result<Self> {
        let (cluster, node) = (uuid::Uuid::now_v7(), rng().random_range(0..i32::MAX));
        let url = url::Url::parse("tcp://127.0.0.1/")?;
        let storage = common::storage_container(cluster, node, url).await?;
        register_broker(cluster, node, storage.clone()).await?;

        let name = alphanumeric_string(15);
        let topic = CreatableTopic::default()
            .name(name.clone())
            .num_partitions(PARTITIONS)
            .replication_factor(0)
            .assignments(Some([].into()))
            .configs(Some([].into()));
        let id = storage.create_topic(topic, false).await?;

        Ok(Self {
            storage,
            id,
            topition: Topition::new(name, rng().random_range(0..PARTITIONS)),
            produced: BTreeSet::new(),
        })
    }

    fn name(&self) -> String {
        self.topition.topic().to_owned()
    }

    fn index(&self) -> i32 {
        self.topition.partition()
    }

    async fn init(&self, txn: &str) -> Result<ProducerIdResponse> {
        Ok(self
            .storage
            .init_producer(Some(txn), TIMEOUT_MS, Some(-1), Some(-1))
            .await?)
    }

    /// A transaction that has only been initialised.
    async fn open(&self) -> Result<Txn> {
        let txn = alphanumeric_string(10);
        let owner = self.init(&txn).await?;
        Ok((txn, owner))
    }

    /// A transaction with this partition added to it, holding [`RECORDS`]
    /// uncommitted batches.
    async fn begin(&mut self) -> Result<Txn> {
        let (txn, owner) = self.open().await?;

        let topic = AddPartitionsToTxnTopic::default()
            .name(self.name())
            .partitions(Some(vec![self.index()]));
        let request = TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: txn.clone(),
            producer_id: owner.id,
            producer_epoch: owner.epoch,
            topics: vec![topic],
        };
        let added = self.storage.txn_add_partitions(request).await?;
        let accepted = AddPartitionsToTxnPartitionResult::default()
            .partition_index(self.index())
            .partition_error_code(ErrorCode::None.into());
        let expected = AddPartitionsToTxnTopicResult::default()
            .name(self.name())
            .results_by_partition(Some(vec![accepted]));
        assert_eq!([expected], added.zero_to_three());

        let transactional: i16 = BatchAttribute::default().transaction(true).into();
        for sequence in 0..RECORDS {
            let record = Record::builder()
                .key(Some(alphanumeric_string(15).into()))
                .value(Some(alphanumeric_string(15).into()));
            let transactional_batch = inflated::Batch::builder()
                .record(record)
                .attributes(transactional)
                .producer_id(owner.id)
                .producer_epoch(owner.epoch)
                .base_sequence(sequence)
                .build()
                .and_then(TryInto::try_into)?;

            let offset = self
                .storage
                .produce(Some(&txn), &self.topition, transactional_batch)
                .await?;
            assert!(self.produced.insert(offset), "{offset} handed out twice");
        }

        Ok((txn, owner))
    }

    async fn end(&self, (txn, owner): &Txn, commit: bool) -> Result<()> {
        let ended = self
            .storage
            .txn_end(txn, owner.id, owner.epoch, commit)
            .await?;
        assert_eq!(ErrorCode::None, ended);
        Ok(())
    }

    async fn latest(&self, isolation: IsolationLevel) -> Result<Option<i64>> {
        let asked = [(self.topition.clone(), ListOffset::Latest)];
        let listed = self.storage.list_offsets(isolation, &asked).await?;
        let [(topition, offset)] = listed.as_slice() else {
            panic!("one partition asked for, {} listed", listed.len())
        };
        assert_eq!(&self.topition, topition);
        assert_eq!(ErrorCode::None, offset.error_code);
        Ok(offset.offset)
    }

    async fn uncommitted(&self) -> Result<Option<i64>> {
        self.latest(IsolationLevel::ReadUncommitted).await
    }

    async fn committed(&self) -> Result<Option<i64>> {
        self.latest(IsolationLevel::ReadCommitted).await
    }

    async fn group_offset(&self, group: &str) -> Result<Option<i64>> {
        let asked = slice::from_ref(&self.topition);
        let fetching = self.storage.offset_fetch(Some(group), asked, Some(false));
        let offsets = fetching.await?;
        Ok(offsets.get(&self.topition).map(|stored| stored.offset))
    }

    async fn commit_offset(&self, (txn, owner): &Txn, group: &str, offset: i64) -> Result<()> {
        let partition = TxnOffsetCommitRequestPartition::default()
            .partition_index(self.index())
            .committed_offset(offset);
        let topic = TxnOffsetCommitRequestTopic::default()
            .name(self.name())
            .partitions(Some(vec![partition]));

        let (generation_id, member_id, group_instance_id) = (None, None, None);
        let request = TxnOffsetCommitRequest {
            transaction_id: txn.clone(),
            group_id: group.to_owned(),
            producer_id: owner.id,
            producer_epoch: owner.epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics: vec![topic],
        };
        let topics = self.storage.txn_offset_commit(request).await?;

        let [topic] = topics.as_slice() else {
            panic!("one topic committed, {} answered", topics.len())
        };
        assert_eq!(self.name(), topic.name);
        let [partition] = topic.partitions.as_deref().unwrap_or_default() else {
            panic!("one partition committed, {topic:?} answered")
        };
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        Ok(())
    }

    async fn fetch(&self, offset: i64) -> Result<Vec<inflated::Batch>> {
        let uncommitted = IsolationLevel::ReadUncommitted;
        self.storage
            .fetch(&self.topition, offset, 1, 50 * 1024, uncommitted, MAX_WAIT)
            .await?
            .into_iter()
            .map(|batch| inflated::Batch::try_from(batch).map_err(Into::into))
            .collect()
    }

    async fn delete(self) -> Result<()> {
        let deleted = self.storage.delete_topic(&TopicId::from(self.id)).await?;
        assert_eq!(ErrorCode::None, deleted);
        Ok(())
    }
}

/// Asserts that `batches` is the single end marker of `txn`, as `control`.
fn assert_marker(
    batches: &[inflated::Batch],
    (_, owner): &Txn,
    control: ControlBatch,
) -> Result<()> {
    let [batch] = batches else {
        panic!("one marker expected, {} batches fetched", batches.len())
    };
    assert_eq!(
        (owner.id, owner.epoch),
        (batch.producer_id, batch.producer_epoch)
    );
    let [marker] = batch.records.as_slice() else {
        panic!("one marker record expected, {batch:?} fetched")
    };
    assert_eq!(Some(control.try_into()?), marker.key);
    let end = EndTransactionMarker::default();
    assert_eq!(Some(end.try_into()?), marker.value);
    Ok(())
}

/// Commits an offset inside a transaction that is then ended with `commit`,
/// and answers what the group sees afterwards.
async fn offset_after_txn_end(commit: bool) -> Result<Option<i64>> {
    let partition = Partition::new().await?;
    let group = alphanumeric_string(10);
    let txn = partition.open().await?;

    assert_eq!(Some(-1), partition.group_offset(&group).await?);
    partition.commit_offset(&txn, &group, 32123).await?;
    assert_eq!(
        Some(-1),
        partition.group_offset(&group).await?,
        "an offset is invisible until its transaction ends"
    );

    partition.end(&txn, commit).await?;
    let seen = partition.group_offset(&group).await?;
    partition.delete().await?;
    Ok(seen)
}

#[tokio::test]
async fn offset_committed_in_an_aborted_txn_is_never_seen() -> Result<()> {
    let _guard = init_tracing()?;
    assert_eq!(Some(-1), offset_after_txn_end(false).await?);
    Ok(())
}

#[tokio::test]
async fn offset_committed_in_a_txn_is_seen_once_it_commits() -> Result<()> {
    let _guard = init_tracing()?;
    assert_eq!(Some(32123), offset_after_txn_end(true).await?);
    Ok(())
}

/// Produces in one transaction that is then ended with `commit`: either way
/// its marker is the last offset, at both isolation levels.
async fn produce_then_end(commit: bool) -> Result<()> {
    let mut partition = Partition::new().await?;
    assert_eq!(Some(0), partition.uncommitted().await?);

    let txn = partition.begin().await?;
    let produced = i64::from(RECORDS);
    assert_eq!(Some(produced), partition.uncommitted().await?);
    assert_eq!(Some(0), partition.committed().await?);

    partition.end(&txn, commit).await?;
    assert_eq!(Some(produced + 1), partition.uncommitted().await?);
    assert_eq!(Some(produced + 1), partition.committed().await?);

    partition.delete().await
}

#[tokio::test]
async fn committed_produce_ends_with_its_marker() -> Result<()> {
    let _guard = init_tracing()?;
    produce_then_end(true).await
}

#[tokio::test]
async fn aborted_produce_ends_with_its_marker() -> Result<()> {
    let _guard = init_tracing()?;
    produce_then_end(false).await
}

#[tokio::test]
async fn an_overlapping_open_txn_hides_an_earlier_commit() -> Result<()> {
    let _guard = init_tracing()?;

    let mut partition = Partition::new().await?;
    assert_eq!(Some(0), partition.uncommitted().await?);

    let first = partition.begin().await?;
    let second = partition.begin().await?;
    let produced = i64::from(2 * RECORDS);
    assert_eq!(Some(produced), partition.uncommitted().await?);
    assert_eq!(Some(0), partition.committed().await?);

    partition.end(&first, true).await?;
    assert_eq!(Some(produced + 1), partition.uncommitted().await?);
    assert_eq!(
        Some(0),
        partition.committed().await?,
        "the first commit is hidden while the second, overlapping, is open"
    );

    partition.end(&second, true).await?;
    assert_eq!(Some(produced + 2), partition.uncommitted().await?);
    assert_eq!(Some(produced + 2), partition.committed().await?);

    partition.delete().await
}

#[tokio::test]
async fn init_producer_again_aborts_the_open_txn() -> Result<()> {
    let _guard = init_tracing()?;

    let mut partition = Partition::new().await?;
    let first = partition.begin().await?;
    let second = partition.begin().await?;
    let produced = i64::from(2 * RECORDS);
    assert_eq!(Some(produced), partition.uncommitted().await?);
    assert_eq!(Some(0), partition.committed().await?);

    partition.end(&first, true).await?;
    assert_eq!(Some(produced + 1), partition.uncommitted().await?);
    let fetched = partition.fetch(produced).await?;
    assert_marker(&fetched, &first, ControlBatch::default().commit())?;

    let fenced = partition.init(&second.0).await?;
    assert_eq!(second.1.id, fenced.id);
    assert_eq!(
        second.1.epoch + 1,
        fenced.epoch,
        "initialising an open transaction again bumps its epoch, and aborts it"
    );
    assert_eq!(Some(produced + 2), partition.uncommitted().await?);
    let fetched = partition.fetch(produced + 1).await?;
    assert_marker(&fetched, &second, ControlBatch::default().abort())?;

    partition.delete().await
}
