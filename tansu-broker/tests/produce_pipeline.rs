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

//! One connection's produces share a coalescing window (#588).
//!
//! The connection loop used to read a request, wait for its answer — the linger
//! plus the segment PUT — and only then read the next. A producer's later
//! requests sat in the socket, so a window never held more than one request
//! from a connection, `max.in.flight` 1 and 5 performed the same, and for a
//! prefix with one writer the linger bought nothing but latency.
//!
//! Raw sockets, five requests written before any answer is read: what a client
//! with `max.in.flight.requests.per.connection=5` does.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use futures::TryStreamExt as _;
use object_store::{ObjectStore, memory::InMemory};
use rama::{Context, Service as _};
use tansu_broker::{coordinator::group::administrator::Controller, service::services};
use tansu_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tansu_service::TcpContext;
use tansu_storage::{DynoStore, Storage};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const TOPIC: &str = "org.env.conn.pipeline";
const PARTITION: i32 = 0;
const PRODUCE_VERSION: i16 = 9;

/// Far above one window (~50ms) plus a PUT against memory, and far below five
/// of anything the test could be waiting on by mistake.
const PATIENCE: Duration = Duration::from_secs(10);

/// A broker over an in-memory bucket the test can list.
async fn serve_broker_stack() -> Result<(u16, InMemory)> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();

    let bucket = InMemory::new();
    let storage: Arc<dyn Storage> = Arc::new(DynoStore::new("tansu", 111, bucket.clone()));

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

    _ = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let Ok(coordinator) = Controller::with_storage(storage.clone()) else {
                return;
            };

            let Ok(service) = services(
                TcpContext::default().cluster_id(Some("tansu-588".into())),
                coordinator,
                storage.clone(),
                None,
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

    Ok((port, bucket))
}

async fn segments(bucket: &InMemory) -> Result<usize> {
    Ok(bucket
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .filter(|meta| meta.location.as_ref().ends_with(".seg"))
        .count())
}

fn request(api_key: i16, api_version: i16, correlation_id: i32) -> Header {
    Header::Request {
        api_key,
        api_version,
        correlation_id,
        client_id: Some("produce-pipeline".into()),
    }
}

/// One record, with a sequence when `sequence` is given.
fn produce(sequence: Option<i32>) -> Result<Body> {
    let builder = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"pipelined"))));

    let builder = match sequence {
        Some(sequence) => builder
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(sequence),
        None => builder,
    };

    let batch = builder.build().and_then(deflated::Batch::try_from)?;

    Ok(ProduceRequest::default()
        .acks(-1)
        .timeout_ms(30_000)
        .topic_data(Some(
            [TopicProduceData::default()
                .name(TOPIC.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(PARTITION)
                        .records(Some(deflated::Frame {
                            batches: [batch].into(),
                        }))]
                    .into(),
                ))]
            .into(),
        ))
        .into())
}

fn list_offsets() -> Body {
    ListOffsetsRequest::default()
        .replica_id(-1)
        .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
        .topics(Some(
            [ListOffsetsTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [ListOffsetsPartition::default()
                        .partition_index(PARTITION)
                        .current_leader_epoch(Some(-1))
                        .timestamp(i64::try_from(ListOffset::Latest).unwrap_or(-1))]
                    .into(),
                ))]
            .into(),
        ))
        .into()
}

async fn send(sock: &mut TcpStream, header: Header, body: Body) -> Result<()> {
    sock.write_all(&Frame::request(header, body)?)
        .await
        .map_err(Into::into)
}

async fn response(sock: &mut TcpStream) -> Result<Bytes> {
    let mut size = [0u8; 4];
    _ = timeout(PATIENCE, sock.read_exact(&mut size)).await??;

    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    _ = sock.read_exact(&mut body).await?;

    let mut frame = BytesMut::new();
    frame.put_slice(&size);
    frame.put_slice(&body);

    Ok(frame.freeze())
}

/// The correlation id, error and base offset of a produce answer.
fn produced(frame: Bytes) -> Result<(i32, ErrorCode, i64)> {
    let correlation_id = i32::from_be_bytes(frame[4..8].try_into()?);

    let Frame {
        body: Body::ProduceResponse(produced),
        ..
    } = Frame::response_from_bytes(frame, ProduceRequest::KEY, PRODUCE_VERSION)?
    else {
        panic!("a Produce response")
    };

    let partition = produced.responses.unwrap_or_default()[0]
        .partition_responses
        .clone()
        .unwrap_or_default()[0]
        .clone();

    Ok((
        correlation_id,
        ErrorCode::try_from(partition.error_code)?,
        partition.base_offset,
    ))
}

/// Five produces written back to back, then five answers read.
async fn five_in_flight(
    sock: &mut TcpStream,
    sequence: impl Fn(i32) -> Option<i32>,
) -> Result<Vec<(i32, ErrorCode, i64)>> {
    for n in 0..5 {
        send(
            sock,
            request(ProduceRequest::KEY, PRODUCE_VERSION, 100 + n),
            produce(sequence(n))?,
        )
        .await?;
    }

    let mut answers = Vec::new();
    for _ in 0..5 {
        answers.push(produced(response(sock).await?)?);
    }

    Ok(answers)
}

/// **The acceptance shape of #588.**
///
/// Five idempotent produces in flight on one connection to one partition: all
/// five in one segment PUT, answered in request order, offsets in send order.
/// Before #588 this was five windows and five PUTs, one per request.
#[tokio::test]
async fn five_produces_in_flight_share_one_window() -> Result<()> {
    let (port, bucket) = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    let answers = five_in_flight(&mut sock, Some).await?;

    assert_eq!(
        vec![
            (100, ErrorCode::None, 0),
            (101, ErrorCode::None, 1),
            (102, ErrorCode::None, 2),
            (103, ErrorCode::None, 3),
            (104, ErrorCode::None, 4),
        ],
        answers,
    );

    assert_eq!(1, segments(&bucket).await?, "one window, one PUT");

    Ok(())
}

/// **Produces without a sequence are pipelined too**: a snapshot producer
/// with idempotence off shares a window exactly as an idempotent one does.
///
/// A window that fails ahead of one that succeeds still leaves a gap that
/// nothing refuses — see
/// `a_batch_without_a_sequence_behind_a_failed_window_is_written_past_it` in
/// the engine's tests — but that reordering is one the client opted into by
/// sending the second request before the first was answered.
#[tokio::test]
async fn produces_without_a_sequence_share_one_window() -> Result<()> {
    let (port, bucket) = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    let answers = five_in_flight(&mut sock, |_| None).await?;

    assert_eq!(
        vec![
            (100, ErrorCode::None, 0),
            (101, ErrorCode::None, 1),
            (102, ErrorCode::None, 2),
            (103, ErrorCode::None, 3),
            (104, ErrorCode::None, 4),
        ],
        answers,
    );

    assert_eq!(1, segments(&bucket).await?, "one window, one PUT");

    Ok(())
}

/// **Anything but a produce is a barrier** (#588): a `ListOffsets` sent behind
/// two pipelined produces is answered after them, and sees both.
#[tokio::test]
async fn a_request_behind_pipelined_produces_sees_them() -> Result<()> {
    let (port, _bucket) = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    for n in 0..2 {
        send(
            &mut sock,
            request(ProduceRequest::KEY, PRODUCE_VERSION, 1 + n),
            produce(Some(n))?,
        )
        .await?;
    }

    send(
        &mut sock,
        request(ListOffsetsRequest::KEY, 9, 3),
        list_offsets(),
    )
    .await?;

    assert_eq!(
        (1, ErrorCode::None, 0),
        produced(response(&mut sock).await?)?
    );
    assert_eq!(
        (2, ErrorCode::None, 1),
        produced(response(&mut sock).await?)?
    );

    let listed = response(&mut sock).await?;
    assert_eq!(3, i32::from_be_bytes(listed[4..8].try_into()?));

    let Frame {
        body: Body::ListOffsetsResponse(listed),
        ..
    } = Frame::response_from_bytes(listed, ListOffsetsRequest::KEY, 9)?
    else {
        panic!("a ListOffsets response")
    };

    let partitions = listed.topics.unwrap_or_default()[0]
        .partitions
        .clone()
        .unwrap_or_default();

    assert_eq!(Some(2), partitions[0].offset, "both produces are visible");

    Ok(())
}
