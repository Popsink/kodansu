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

//! The three transaction [`Service`] wrappers.
//!
//! Every other API in `tansu-storage/src/service/` has a test alongside it;
//! these three had none, at 0% covered. They are thin — they translate a wire
//! request into a storage call and the answer back — but thin is precisely the
//! defect class this crate has already been bitten by: #273 was two wrappers
//! that silently failed to delegate, and a wrapper that drops a field or fills
//! the wrong response arm reads exactly like one that works.
//!
//! So each test here follows one value all the way through: a topic named in
//! the request has to come back named in the response, at the right version arm.

use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ErrorCode, TxnOffsetCommitRequest,
    add_partitions_to_txn_request::{AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction},
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
};
use tansu_storage::{
    StorageContainer, TxnAddOffsetsService, TxnAddPartitionService, TxnOffsetCommitService,
};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url};

mod common;

const NODE_ID: i32 = 111;
const TRANSACTION_ID: &str = "txn-1";

type Shared = std::sync::Arc<Box<dyn tansu_storage::Storage>>;

/// Storage with `txn-1` already open. The transaction has to exist first: the
/// object store answers `UNSUPPORTED_VERSION` for a producer it has never
/// issued, so a test that made one up would be asserting on the rejection path
/// rather than on the delegation it is here to check.
async fn transactional_storage() -> Result<(Shared, i64, i16), Error> {
    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .silent(true)
        .build()
        .await?;

    let producer = storage
        .init_producer(Some(TRANSACTION_ID), 30_000, None, None)
        .await?;

    Ok((storage, producer.id, producer.epoch))
}

#[tokio::test]
async fn add_offsets_answers_without_throttling() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, producer_id, producer_epoch) = transactional_storage().await?;

    let response = MapStateLayer::new(move |_| storage.clone())
        .into_layer(TxnAddOffsetsService)
        .serve(
            Context::default(),
            AddOffsetsToTxnRequest::default()
                .transactional_id(TRANSACTION_ID.into())
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .group_id("g1".into()),
        )
        .await?;

    assert_eq!(0, response.throttle_time_ms);
    // The storage's error code, reported as the storage's — not a hardcoded
    // `None` that would report a rejected transaction as accepted.
    assert_eq!(i16::from(ErrorCode::None), response.error_code);

    Ok(())
}

fn v4_plus_request(producer_id: i64, producer_epoch: i16) -> AddPartitionsToTxnRequest {
    AddPartitionsToTxnRequest::default()
        .transactions(Some(vec![
            AddPartitionsToTxnTransaction::default()
                .transactional_id(TRANSACTION_ID.into())
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .verify_only(false)
                .topics(Some(vec![
                    AddPartitionsToTxnTopic::default()
                        .name("abc".into())
                        .partitions(Some(vec![0])),
                ])),
        ]))
        .v_3_and_below_topics(None)
}

/// v0-3 results belong in `results_by_topic_v_3_and_below`, and the other field
/// has to come back present-and-empty rather than absent: a client reads both.
#[tokio::test]
async fn add_partitions_v0_to_3_fills_the_below_v4_results() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, producer_id, producer_epoch) = transactional_storage().await?;

    let response = MapStateLayer::new(move |_| storage.clone())
        .into_layer(TxnAddPartitionService)
        .serve(
            Context::default(),
            AddPartitionsToTxnRequest::default()
                .v_3_and_below_transactional_id(Some(TRANSACTION_ID.into()))
                .v_3_and_below_producer_id(Some(producer_id))
                .v_3_and_below_producer_epoch(Some(producer_epoch))
                .v_3_and_below_topics(Some(vec![
                    AddPartitionsToTxnTopic::default()
                        .name("abc".into())
                        .partitions(Some(vec![0])),
                ]))
                .transactions(None),
        )
        .await?;

    assert_eq!(0, response.throttle_time_ms);
    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);
    assert_eq!(
        Some(vec!["abc".to_owned()]),
        response
            .results_by_topic_v_3_and_below
            .as_ref()
            .map(|results| results.iter().map(|result| result.name.clone()).collect())
    );
    assert_eq!(Some(0), response.results_by_transaction.map(|by| by.len()));

    Ok(())
}

/// v4+ is deliberately not implemented on the object store (#276): a client
/// picks its own API version regardless of what the broker advertises, and the
/// unhandled case used to panic the request task. It is an error code now, and
/// this pins it — a silent `Ok` with an empty result would look to the client
/// like partitions were added to a transaction that has none.
#[tokio::test]
async fn add_partitions_v4_plus_is_unsupported_by_the_object_store() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, producer_id, producer_epoch) = transactional_storage().await?;

    assert!(matches!(
        MapStateLayer::new(move |_| storage.clone())
            .into_layer(TxnAddPartitionService)
            .serve(
                Context::default(),
                v4_plus_request(producer_id, producer_epoch),
            )
            .await,
        Err(tansu_storage::Error::Api(ErrorCode::UnsupportedVersion))
    ));

    Ok(())
}

/// The wrapper's other arm. `null://` is the one engine that answers a v4+ add
/// with `VersionFourPlus`, so it is the only way to reach the branch that fills
/// `results_by_transaction` — which is otherwise dead code no test can enter.
#[tokio::test]
async fn add_partitions_v4_plus_fills_the_by_transaction_results() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://sink/")?)
        .silent(true)
        .build()
        .await?;

    let producer = storage
        .init_producer(Some(TRANSACTION_ID), 30_000, None, None)
        .await?;

    let response = MapStateLayer::new(move |_| storage.clone())
        .into_layer(TxnAddPartitionService)
        .serve(
            Context::default(),
            v4_plus_request(producer.id, producer.epoch),
        )
        .await?;

    assert_eq!(0, response.throttle_time_ms);
    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);
    assert_eq!(Some(1), response.results_by_transaction.map(|by| by.len()));
    assert_eq!(
        Some(0),
        response.results_by_topic_v_3_and_below.map(|by| by.len())
    );

    Ok(())
}

/// The wrapper rebuilds the request field by field into the storage type, which
/// is where a field goes missing. The topic named going in has to be the topic
/// named coming out.
#[tokio::test]
async fn offset_commit_carries_its_topics_through() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, producer_id, producer_epoch) = transactional_storage().await?;

    let response = MapStateLayer::new(move |_| storage.clone())
        .into_layer(TxnOffsetCommitService)
        .serve(
            Context::default(),
            TxnOffsetCommitRequest::default()
                .transactional_id(TRANSACTION_ID.into())
                .group_id("g1".into())
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .generation_id(Some(-1))
                .member_id(Some("".into()))
                .group_instance_id(None)
                .topics(Some(vec![
                    TxnOffsetCommitRequestTopic::default()
                        .name("abc".into())
                        .partitions(Some([].into())),
                ])),
        )
        .await?;

    assert_eq!(0, response.throttle_time_ms);
    assert_eq!(
        Some(vec!["abc".to_owned()]),
        response
            .topics
            .as_ref()
            .map(|topics| topics.iter().map(|topic| topic.name.clone()).collect())
    );

    Ok(())
}
