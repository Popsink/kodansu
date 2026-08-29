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

//! `Produce` with `acks=0` is served and not answered (#440).
//!
//! A client that sets `acks=0` registers no handler for the response, does not
//! wait for one, and fires its delivery report the moment the request is
//! written to the socket. Apache Kafka sends nothing back. This broker sent a
//! full `ProduceResponse` — a frame carrying a correlation id the client has no
//! in-flight request for, which is a protocol error a client answers by
//! **dropping the connection**. Everything still queued on it dies, and since
//! the delivery reports already said "sent", nothing reports a loss.
//!
//! Raw sockets rather than the pooled client, because the property is about
//! bytes that must *not* be on the wire, and a typed client would be the thing
//! being confused by them.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use rama::{Context, Service as _};
use tansu_broker::{coordinator::group::administrator::Controller, service::services};
use tansu_sans_io::{
    ApiKey as _, ErrorCode, Frame, Header, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tansu_storage::{Storage, StorageContainer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const TOPIC: &str = "produce-acks";
const PARTITION: i32 = 0;

/// Long enough that "nothing arrived" means the broker chose not to answer,
/// not that it had not got round to it: a produce is answered in ~50ms (the
/// coalescing linger), so a second is twenty times over.
const SILENCE: Duration = Duration::from_secs(1);

async fn serve_broker_stack() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();

    let storage: Arc<Box<dyn Storage>> = StorageContainer::builder()
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await?;

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
                "tansu-440",
                coordinator,
                storage.clone(),
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

    Ok(port)
}

async fn send(sock: &mut TcpStream, header: Header, body: tansu_sans_io::Body) -> Result<()> {
    let bytes = Frame::request(header, body)?;
    sock.write_all(&bytes).await?;
    sock.flush().await.map_err(Into::into)
}

/// One length-delimited response frame, or `None` if none arrives within
/// `within`.
async fn response(sock: &mut TcpStream, within: Duration) -> Result<Option<Bytes>> {
    let mut size = [0u8; 4];

    match timeout(within, sock.read_exact(&mut size)).await {
        Err(_elapsed) => return Ok(None),
        Ok(read) => _ = read?,
    }

    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    _ = sock.read_exact(&mut body).await?;

    let mut frame = BytesMut::new();
    frame.put_slice(&size);
    frame.put_slice(&body);

    Ok(Some(frame.freeze()))
}

/// A response frame's correlation id, which is what a client matches against
/// its in-flight requests — and what it drops the connection over when it
/// matches nothing.
fn correlation_id(frame: &Bytes) -> Result<i32> {
    Ok(i32::from_be_bytes(frame[4..8].try_into()?))
}

fn request(api_key: i16, api_version: i16, correlation_id: i32) -> Header {
    Header::Request {
        api_key,
        api_version,
        correlation_id,
        client_id: Some("produce-acks".into()),
    }
}

fn produce(acks: i16) -> Result<tansu_sans_io::Body> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"fire and forget"))))
        .build()
        .and_then(deflated::Batch::try_from)?;

    Ok(ProduceRequest::default()
        .acks(acks)
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

fn list_offsets() -> tansu_sans_io::Body {
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

/// The defect, and the reason the loss is silent.
///
/// Both halves matter. Nothing may come back for the `acks=0` produce; and the
/// *next* request must still be answered, in its own right — a broker that
/// muted the connection instead would hang the client rather than desynchronise
/// it, which is a different bug that this same signal could cause if it leaked.
#[tokio::test]
async fn acks_zero_is_served_and_not_answered() -> Result<()> {
    let port = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    send(&mut sock, request(ProduceRequest::KEY, 9, 111), produce(0)?).await?;

    assert!(
        response(&mut sock, SILENCE).await?.is_none(),
        "acks=0 gets no response: a frame here carries a correlation id the \
         client has no request for, and a client meeting one drops the connection"
    );

    // The stream is still in step: this answer is this request's.
    send(
        &mut sock,
        request(ListOffsetsRequest::KEY, 9, 222),
        list_offsets(),
    )
    .await?;

    let listed = response(&mut sock, SILENCE)
        .await?
        .expect("ListOffsets is answered");

    assert_eq!(222, correlation_id(&listed)?);

    Ok(())
}

/// `acks=0` withholds the *answer*, not the durability. The broker still writes
/// the record before moving on, so a reader sees it — the loss this issue is
/// about happens on the connection, never in the log.
#[tokio::test]
async fn acks_zero_still_persists_the_record() -> Result<()> {
    let port = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    send(&mut sock, request(ProduceRequest::KEY, 9, 1), produce(0)?).await?;
    assert!(response(&mut sock, SILENCE).await?.is_none());

    send(
        &mut sock,
        request(ListOffsetsRequest::KEY, 9, 2),
        list_offsets(),
    )
    .await?;

    let listed = response(&mut sock, SILENCE)
        .await?
        .expect("ListOffsets is answered");

    let Frame {
        body: tansu_sans_io::Body::ListOffsetsResponse(listed),
        ..
    } = Frame::response_from_bytes(listed, ListOffsetsRequest::KEY, 9)?
    else {
        panic!("a ListOffsets response")
    };

    let topics = listed.topics.unwrap_or_default();
    let partitions = topics[0].partitions.clone().unwrap_or_default();

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(1), partitions[0].offset, "the record was written");

    Ok(())
}

/// Every other `acks` value is answered exactly as before. The signal is set
/// per request and taken per request, so one `acks=0` produce must not mute
/// anything that follows it — including on the same connection.
#[tokio::test]
async fn acks_one_and_all_are_answered() -> Result<()> {
    let port = serve_broker_stack().await?;
    let mut sock = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;

    // Interleaved with an acks=0 produce, so a leaked signal shows up here.
    for (correlation, acks) in [(1, 1i16), (2, 0), (3, -1), (4, 1)] {
        send(
            &mut sock,
            request(ProduceRequest::KEY, 9, correlation),
            produce(acks)?,
        )
        .await?;

        let answered = response(&mut sock, SILENCE).await?;

        if acks == 0 {
            assert!(answered.is_none(), "acks=0 is not answered");
        } else {
            let answered = answered.expect("acks={acks} is answered");
            assert_eq!(correlation, correlation_id(&answered)?);
        }
    }

    Ok(())
}
