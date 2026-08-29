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

//! A transaction can be ended over the wire, not only through `Storage` (#441).
//!
//! `Storage::txn_end` has been implemented and tested since transactions
//! landed, and `tansu-broker/tests/txn.rs` exercises commit and abort
//! thoroughly — by calling `sc.txn_end(..)` on the storage container. Every one
//! of those tests passed while api key 26 had **no route**, so `ApiVersions`
//! advertised `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn` and
//! `TxnOffsetCommit` and not `EndTxn`. A transactional producer engaged fully —
//! init, begin, produce — and hit `_UNSUPPORTED_FEATURE` at commit *and* at
//! abort, with its records written and permanently invisible to a
//! read-committed consumer.
//!
//! That gap is a shape, not an oversight: a storage-level test cannot see which
//! APIs the broker answers. So these go over a real socket through the real
//! service stack, and the first assertion is on the advertised set itself.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::Bytes;
use rama::{Context, Layer as _, Service as _};
use tansu_broker::{coordinator::group::administrator::Controller, service::services};
use tansu_client::{
    BytesConnectionService, ConnectionManager, FrameConnectionLayer, FramePoolLayer, Pool,
};
use tansu_sans_io::{
    AddPartitionsToTxnRequest, ApiKey as _, BatchAttribute, Body, EndTxnRequest, ErrorCode, Frame,
    Header, InitProducerIdRequest, IsolationLevel, ListOffset, ListOffsetsRequest, ProduceRequest,
    TxnOffsetCommitRequest,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tansu_service::FrameBytesLayer;
use tansu_storage::{Storage, StorageContainer};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const TOPIC: &str = "txn-api-surface";
const TRANSACTION: &str = "t1";
const PARTITION: i32 = 0;

/// The highest `AddPartitionsToTxn` a *producer* speaks. v4 moved the request
/// to a `transactions` array for the transaction coordinator's own use; a
/// client stays at v3.
const PRODUCER_ADD_PARTITIONS_VERSION: i16 = 3;

/// A frame-level Kafka client over a real socket, as in `group_api_answer.rs`.
struct FrameClient {
    pool: Pool,
}

impl FrameClient {
    async fn connect(port: u16) -> Result<Self> {
        ConnectionManager::builder(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
            .client_id(Some("txn-api-surface".into()))
            .max_size(Some(1))
            .build()
            .await
            .map(|pool| Self { pool })
            .map_err(Into::into)
    }

    /// The negotiated version for `api_key`, or `UnknownApiKey` when the broker
    /// does not advertise it — which is exactly what a client sees, and what
    /// `librdkafka` reports as `_UNSUPPORTED_FEATURE`.
    fn api_version(&self, api_key: i16) -> Result<i16> {
        self.pool.manager().api_version(api_key).map_err(Into::into)
    }

    async fn call(&self, api_key: i16, body: Body) -> Result<Body> {
        self.call_at(api_key, self.api_version(api_key)?, body)
            .await
    }

    /// At a chosen version, because a negotiated version is `min(broker, client)`
    /// and this client has no ceiling of its own. `AddPartitionsToTxn` v4+ is
    /// the coordinator-to-broker shape (a `transactions` array); a producer
    /// speaks v3 and below, so that is what a test about producer-facing
    /// behaviour has to send — the same reason `group_api_answer.rs` clamps
    /// `OffsetFetch` to v7.
    async fn call_at(&self, api_key: i16, api_version: i16, body: Body) -> Result<Body> {
        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id: 0,
                client_id: Some("txn-api-surface".into()),
            },
            body,
        };

        (
            FramePoolLayer::new(self.pool.clone()),
            FrameConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService)
            .serve(Context::default(), frame)
            .await
            .map(|response| response.body)
            .map_err(Into::into)
    }
}

async fn storage() -> Result<Arc<Box<dyn Storage>>> {
    StorageContainer::builder()
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
        .map_err(Into::into)
}

/// The real broker stack on an ephemeral port, with the real group coordinator:
/// nothing here is a stub, because the property under test is which APIs the
/// production route table answers.
async fn serve_broker_stack() -> Result<(u16, Arc<Box<dyn Storage>>)> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let storage = storage().await?;

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

    let served = storage.clone();

    _ = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let Ok(coordinator) = Controller::with_storage(served.clone()) else {
                return;
            };

            let Ok(service) = services(
                "tansu-441",
                coordinator,
                served.clone(),
                None,
                CancellationToken::new(),
                None,
                None,
            ) else {
                return;
            };

            _ = tokio::spawn(async move {
                _ = service.serve(Context::default(), stream).await;
            });
        }
    });

    Ok((port, storage))
}

/// One transactional record, ready for `Produce`.
fn transactional_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"in a transaction"))))
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Init, begin, produce — the state a client is in when it calls
/// `commit_transaction`. Returns the producer coordinates.
async fn produce_in_a_transaction(client: &FrameClient) -> Result<(i64, i16)> {
    let Body::InitProducerIdResponse(init) = client
        .call(
            InitProducerIdRequest::KEY,
            InitProducerIdRequest::default()
                .transactional_id(Some(TRANSACTION.into()))
                .transaction_timeout_ms(15_000)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1))
                .into(),
        )
        .await?
    else {
        panic!("InitProducerId must be answered")
    };

    assert_eq!(i16::from(ErrorCode::None), init.error_code);

    let Body::AddPartitionsToTxnResponse(added) = client
        .call_at(
            AddPartitionsToTxnRequest::KEY,
            PRODUCER_ADD_PARTITIONS_VERSION,
            AddPartitionsToTxnRequest::default()
                .v_3_and_below_transactional_id(Some(TRANSACTION.into()))
                .v_3_and_below_producer_id(Some(init.producer_id))
                .v_3_and_below_producer_epoch(Some(init.producer_epoch))
                .v_3_and_below_topics(Some(
                    [AddPartitionsToTxnTopic::default()
                        .name(TOPIC.into())
                        .partitions(Some([PARTITION].into()))]
                    .into(),
                ))
                .into(),
        )
        .await?
    else {
        panic!("AddPartitionsToTxn must be answered")
    };

    assert!(added.results_by_topic_v_3_and_below.is_some());

    let Body::ProduceResponse(produced) = client
        .call(
            ProduceRequest::KEY,
            ProduceRequest::default()
                .transactional_id(Some(TRANSACTION.into()))
                .acks(-1)
                .timeout_ms(1_000)
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(TOPIC.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(PARTITION)
                                .records(Some(deflated::Frame {
                                    batches: [transactional_batch(
                                        init.producer_id,
                                        init.producer_epoch,
                                        0,
                                    )?]
                                    .into(),
                                }))]
                            .into(),
                        ))]
                    .into(),
                ))
                .into(),
        )
        .await?
    else {
        panic!("Produce must be answered")
    };

    let responses = produced.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    let partitions = responses[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok((init.producer_id, init.producer_epoch))
}

async fn end_txn(client: &FrameClient, producer: (i64, i16), committed: bool) -> Result<ErrorCode> {
    let Body::EndTxnResponse(ended) = client
        .call(
            EndTxnRequest::KEY,
            EndTxnRequest::default()
                .transactional_id(TRANSACTION.into())
                .producer_id(producer.0)
                .producer_epoch(producer.1)
                .committed(committed)
                .into(),
        )
        .await?
    else {
        panic!("EndTxn must be answered")
    };

    ErrorCode::try_from(ended.error_code).map_err(Into::into)
}

/// The latest offset a reader at `isolation_level` may see: for
/// `READ_COMMITTED` that is the last stable offset, which is what tells us the
/// transaction actually resolved rather than merely being answered.
async fn latest_offset(client: &FrameClient, isolation_level: IsolationLevel) -> Result<i64> {
    let Body::ListOffsetsResponse(listed) = client
        .call(
            ListOffsetsRequest::KEY,
            ListOffsetsRequest::default()
                .replica_id(-1)
                .isolation_level(Some(i8::from(isolation_level)))
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(TOPIC.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(PARTITION)
                                .current_leader_epoch(Some(-1))
                                .timestamp(i64::try_from(ListOffset::Latest)?)]
                            .into(),
                        ))]
                    .into(),
                ))
                .into(),
        )
        .await?
    else {
        panic!("ListOffsets must be answered")
    };

    let topics = listed.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok(partitions[0].offset.unwrap_or_default())
}

/// The trap itself: the four entry APIs were advertised and `EndTxn` was not,
/// so a client could reach the point of no return before there was anything to
/// fail on. Whatever the transaction support is, the advertised set has to be
/// self-consistent — either all five, or none.
#[tokio::test]
async fn every_transaction_api_is_advertised_or_none_is() -> Result<()> {
    let (port, _storage) = serve_broker_stack().await?;
    let client = FrameClient::connect(port).await?;

    let entry = [
        ("InitProducerId", InitProducerIdRequest::KEY),
        ("AddPartitionsToTxn", AddPartitionsToTxnRequest::KEY),
        ("TxnOffsetCommit", TxnOffsetCommitRequest::KEY),
    ];

    let advertised_entry = entry
        .iter()
        .filter(|(_, key)| client.api_version(*key).is_ok())
        .count();

    assert_eq!(
        entry.len(),
        advertised_entry,
        "the entry APIs are advertised, so a client will engage a transaction"
    );

    assert!(
        client.api_version(EndTxnRequest::KEY).is_ok(),
        "EndTxn (api key 26) must be advertised: without it a transactional \
         producer engages fully and then can neither commit nor abort"
    );

    Ok(())
}

/// A committed transaction is committed over the wire.
#[tokio::test]
async fn a_transaction_commits_over_the_wire() -> Result<()> {
    let (port, _storage) = serve_broker_stack().await?;
    let client = FrameClient::connect(port).await?;

    let producer = produce_in_a_transaction(&client).await?;

    // Before the commit the record is written but not stable: a read-committed
    // reader is held at the transaction's first offset. This is the state the
    // records were stuck in forever.
    assert_eq!(
        0,
        latest_offset(&client, IsolationLevel::ReadCommitted).await?
    );
    assert_eq!(
        1,
        latest_offset(&client, IsolationLevel::ReadUncommitted).await?
    );

    assert_eq!(
        ErrorCode::None,
        timeout(Duration::from_secs(10), end_txn(&client, producer, true)).await??
    );

    // The commit marker occupies an offset of its own, so the last stable
    // offset lands past both the record and the marker — the transaction is
    // visible to a read-committed consumer, which is the whole point.
    assert_eq!(
        2,
        latest_offset(&client, IsolationLevel::ReadCommitted).await?
    );

    Ok(())
}

/// And an aborted one is abortable — the other half of the dead end, and the
/// one a client reaches when it is trying to *recover*.
#[tokio::test]
async fn a_transaction_aborts_over_the_wire() -> Result<()> {
    let (port, _storage) = serve_broker_stack().await?;
    let client = FrameClient::connect(port).await?;

    let producer = produce_in_a_transaction(&client).await?;

    assert_eq!(
        ErrorCode::None,
        timeout(Duration::from_secs(10), end_txn(&client, producer, false)).await??
    );

    Ok(())
}

/// A rejection is an answer. `Storage::txn_end` reports a fence as
/// `Err(Error::Api(PRODUCER_FENCED))`, unlike its siblings which report a
/// rejection as `Ok(ErrorCode)` — and an `Err` out of a service ends the
/// connection with no response written. `PRODUCER_FENCED` is a code a
/// transactional client is required to handle by aborting and re-initialising;
/// a dropped socket instead turns a recoverable fence into reconnect-and-replay.
#[tokio::test]
async fn a_fenced_end_is_answered_not_dropped() -> Result<()> {
    let (port, _storage) = serve_broker_stack().await?;
    let client = FrameClient::connect(port).await?;

    let (producer_id, producer_epoch) = produce_in_a_transaction(&client).await?;

    let fenced = timeout(
        Duration::from_secs(10),
        end_txn(&client, (producer_id, producer_epoch + 1), true),
    )
    .await??;

    assert_eq!(ErrorCode::ProducerFenced, fenced);

    // The connection survived it, so the client can act on the code it was
    // given rather than reconnecting to find out.
    assert_eq!(
        ErrorCode::None,
        timeout(
            Duration::from_secs(10),
            end_txn(&client, (producer_id, producer_epoch), true)
        )
        .await??
    );

    Ok(())
}
