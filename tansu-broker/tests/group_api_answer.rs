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

//! An `Error::Api` reaches the caller as a response frame, not as a closed
//! socket (#300).
//!
//! `Error::Api(NOT_COORDINATOR)` is what #243 added so a failed forward hop
//! told the client to retry against the real owner rather than being processed
//! locally and splitting the group. Nothing converted it into a response: it
//! propagated through the whole service stack into the per-connection task,
//! which abandons the connection **without writing anything**. To the caller
//! that is a read that ends before a response frame arrives — `early eof`.
//!
//! Forwarding is gone (#360) and `NOT_COORDINATOR` with it, but the property is
//! not about forwarding: *any* `Error::Api` a service returns must reach the
//! caller as a response frame. `NOT_COORDINATOR` stays as the specimen because
//! it is the one the incident was reported on.
//!
//! This exercises the real stack over a real socket, because that is the only
//! place the difference between "answered" and "connection dropped" exists. A
//! unit test on the service alone cannot see it: the service returns `Err`
//! either way, and it is the connection loop above that turns the `Err` into a
//! dropped socket. The client used to be the production forward client — the
//! only thing in the tree that spoke frames to a broker — and is now a local
//! one, so the assertion did not follow the deleted code out.

use std::{io, net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use rama::{Context, Layer as _, Service as _};
use tansu_broker::{
    Error,
    coordinator::group::{Coordinator, OffsetCommit},
    service::services,
};
use tansu_client::{
    BytesConnectionService, ConnectionManager, FrameConnectionLayer, FramePoolLayer, Pool,
};
use tansu_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest,
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
};
use tansu_service::FrameBytesLayer;
use tansu_storage::{Storage, StorageContainer};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;
use url::Url;

/// A frame-level Kafka client over a real socket.
///
/// Frame level rather than `Client::call` for the same reason the forward hop
/// was: the typed client stamps the *pool's* client id on every request, and
/// the frame path takes it from the frame, which is what lets one connection
/// speak for several notional callers.
struct FrameClient {
    pool: Pool,
}

impl FrameClient {
    async fn connect(port: u16) -> Result<Self> {
        ConnectionManager::builder(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
            .client_id(Some("group-api-answer".into()))
            .max_size(Some(1))
            .build()
            .await
            .map(|pool| Self { pool })
            .map_err(Into::into)
    }

    async fn call(&self, api_key: i16, client_id: Option<&str>, body: Body) -> Result<Body> {
        let negotiated = self.pool.manager().api_version(api_key)?;

        // `OffsetFetch` in its pre-v8 shape (`group_id` + `topics`) only
        // encodes at v7 and below: v8 restructured the request around a
        // `groups` array, so sending a legacy-shaped call at v8+ would
        // silently drop the group.
        let api_version = match &body {
            Body::OffsetFetchRequest(request) if request.group_id.is_some() => negotiated.min(7),
            _ => negotiated,
        };

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                // swapped for the pooled connection's own correlation id by
                // `FrameConnectionService`.
                correlation_id: 0,
                client_id: client_id.map(ToOwned::to_owned),
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
use uuid::Uuid;

/// A coordinator that answers every API the way a failed forward hop does.
#[derive(Clone, Debug)]
struct AlwaysNotCoordinator;

#[async_trait]
impl Coordinator for AlwaysNotCoordinator {
    async fn join(
        &self,
        _client_id: Option<&str>,
        _group_id: &str,
        _session_timeout_ms: i32,
        _rebalance_timeout_ms: Option<i32>,
        _member_id: &str,
        _group_instance_id: Option<&str>,
        _protocol_type: &str,
        _protocols: Option<&[JoinGroupRequestProtocol]>,
        _reason: Option<&str>,
    ) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
    }

    async fn sync(
        &self,
        _group_id: &str,
        _generation_id: i32,
        _member_id: &str,
        _group_instance_id: Option<&str>,
        _protocol_type: Option<&str>,
        _protocol_name: Option<&str>,
        _assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
    }

    async fn heartbeat(
        &self,
        _group_id: &str,
        _generation_id: i32,
        _member_id: &str,
        _group_instance_id: Option<&str>,
    ) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
    }

    async fn leave(
        &self,
        _group_id: &str,
        _member_id: Option<&str>,
        _members: Option<&[MemberIdentity]>,
    ) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
    }

    async fn offset_commit(&self, _detail: OffsetCommit<'_>) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
    }

    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: Option<&[OffsetFetchRequestTopic]>,
        _groups: Option<&[OffsetFetchRequestGroup]>,
        _require_stable: Option<bool>,
    ) -> Result<Body, Error> {
        Err(Error::Api(ErrorCode::NotCoordinator))
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

/// Serve the real broker stack on an ephemeral port, one accept loop per
/// connection, dropping a connection whose `serve` returns `Err` — which is
/// exactly what `Broker::spawn_connection` does, and the behaviour under test.
async fn serve_broker_stack() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let storage = storage().await?;

    _ = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let Ok(service) = services(
                "tansu-300",
                AlwaysNotCoordinator,
                storage.clone(),
                None,
                CancellationToken::new(),
            ) else {
                return;
            };

            _ = tokio::spawn(async move {
                // The `Err` arm is the whole point: no response is written, the
                // task ends, and the peer's read hits EOF mid-request.
                _ = service.serve(Context::default(), stream).await;
            });
        }
    });

    Ok(port)
}

/// Every group API answers its `Error::Api` in a decodable frame.
///
/// Before #300 each of these returned a transport error instead — the caller's
/// read ended with no response frame — surfacing as `Io(UnexpectedEof)`, which
/// the original report saw as `early eof`.
#[tokio::test]
async fn a_group_api_error_is_answered_not_dropped() -> Result<()> {
    let port = serve_broker_stack().await?;
    let client = FrameClient::connect(port).await?;

    let requests: Vec<(&str, i16, Body)> = vec![
        (
            "join",
            JoinGroupRequest::KEY,
            JoinGroupRequest::default()
                .group_id("g-300".into())
                .session_timeout_ms(45_000)
                .rebalance_timeout_ms(Some(60_000))
                .member_id("m-300".into())
                .protocol_type("consumer".into())
                .protocols(Some(vec![
                    JoinGroupRequestProtocol::default()
                        .name("range".into())
                        .metadata(bytes::Bytes::from_static(b"meta")),
                ]))
                .into(),
        ),
        (
            "sync",
            SyncGroupRequest::KEY,
            SyncGroupRequest::default()
                .group_id("g-300".into())
                .generation_id(1)
                .member_id("m-300".into())
                .assignments(Some([].into()))
                .into(),
        ),
        (
            "heartbeat",
            HeartbeatRequest::KEY,
            HeartbeatRequest::default()
                .group_id("g-300".into())
                .generation_id(1)
                .member_id("m-300".into())
                .into(),
        ),
        (
            "leave",
            LeaveGroupRequest::KEY,
            LeaveGroupRequest::default()
                .group_id("g-300".into())
                .members(Some(vec![
                    MemberIdentity::default().member_id("m-300".into()),
                ]))
                .into(),
        ),
        (
            "offset_commit",
            OffsetCommitRequest::KEY,
            OffsetCommitRequest::default()
                .group_id("g-300".into())
                .generation_id_or_member_epoch(Some(1))
                .member_id(Some("m-300".into()))
                .retention_time_ms(Some(-1))
                .topics(Some(vec![
                    OffsetCommitRequestTopic::default()
                        .name("t-300".into())
                        .partitions(Some(vec![
                            OffsetCommitRequestPartition::default()
                                .partition_index(0)
                                .committed_offset(1)
                                .committed_leader_epoch(Some(-1))
                                .commit_timestamp(Some(-1))
                                .committed_metadata(Some("".into())),
                        ])),
                ]))
                .into(),
        ),
        (
            // KIP-709 shape: from v8 the request carries `groups`, not
            // `group_id` + `topics`, and `FrameForwarder` negotiates the
            // highest common version.
            "offset_fetch",
            OffsetFetchRequest::KEY,
            OffsetFetchRequest::default()
                .groups(Some(vec![
                    OffsetFetchRequestGroup::default()
                        .group_id("g-300".into())
                        // KIP-848 fields, added at v9 — the version negotiated
                        // here, so a request without them does not decode.
                        .member_id(Some("".into()))
                        .member_epoch(Some(-1))
                        .topics(Some(vec![
                            OffsetFetchRequestTopics::default()
                                .name("t-300".into())
                                .partition_indexes(Some(vec![0])),
                        ])),
                ]))
                .require_stable(Some(false))
                .into(),
        ),
    ];

    let not_coordinator = i16::from(ErrorCode::NotCoordinator);

    for (api, api_key, body) in requests {
        let response = timeout(
            Duration::from_secs(10),
            client.call(api_key, Some("c-300"), body),
        )
        .await?
        .unwrap_or_else(|error| panic!("{api}: failed instead of answering: {error:?}"));

        match response {
            Body::JoinGroupResponse(join) => assert_eq!(not_coordinator, join.error_code, "{api}"),
            Body::SyncGroupResponse(sync) => assert_eq!(not_coordinator, sync.error_code, "{api}"),
            Body::HeartbeatResponse(hb) => assert_eq!(not_coordinator, hb.error_code, "{api}"),
            Body::LeaveGroupResponse(leave) => {
                assert_eq!(not_coordinator, leave.error_code, "{api}")
            }
            // From v8 the code is per group; the top-level one is not written.
            Body::OffsetFetchResponse(fetch) => assert_eq!(
                vec![("g-300".to_owned(), not_coordinator)],
                fetch
                    .groups
                    .unwrap_or_default()
                    .into_iter()
                    .map(|group| (group.group_id, group.error_code))
                    .collect::<Vec<_>>(),
                "{api}"
            ),

            // No top-level code on this one: the answer has to name every
            // partition the request did, or the client reads the omission as a
            // silent success.
            Body::OffsetCommitResponse(commit) => {
                let codes = commit
                    .topics
                    .unwrap_or_default()
                    .into_iter()
                    .flat_map(|topic| topic.partitions.unwrap_or_default())
                    .map(|partition| partition.error_code)
                    .collect::<Vec<_>>();

                assert_eq!(vec![not_coordinator], codes, "{api}");
            }

            otherwise => panic!("{api}: unexpected response {otherwise:?}"),
        }
    }

    Ok(())
}

/// The control: an error the broker does *not* mean as an answer still ends the
/// connection, because inventing a code for it would report a definite outcome
/// the broker cannot stand behind.
///
/// This is the half of #300's mechanism that stays. The value of naming it is
/// that a future `early eof` can be attributed: with `Error::Api` no longer in
/// the population, the connection-ending arm means the broker genuinely does not
/// know what happened, and `broker.rs` logs it as such.
#[tokio::test]
async fn a_non_api_error_still_ends_the_connection() -> Result<()> {
    #[derive(Clone, Debug)]
    struct AlwaysBroken;

    #[async_trait]
    impl Coordinator for AlwaysBroken {
        async fn join(
            &self,
            _client_id: Option<&str>,
            _group_id: &str,
            _session_timeout_ms: i32,
            _rebalance_timeout_ms: Option<i32>,
            _member_id: &str,
            _group_instance_id: Option<&str>,
            _protocol_type: &str,
            _protocols: Option<&[JoinGroupRequestProtocol]>,
            _reason: Option<&str>,
        ) -> Result<Body, Error> {
            Err(Error::Message(String::from("missing e-tag")))
        }

        async fn sync(
            &self,
            _group_id: &str,
            _generation_id: i32,
            _member_id: &str,
            _group_instance_id: Option<&str>,
            _protocol_type: Option<&str>,
            _protocol_name: Option<&str>,
            _assignments: Option<&[SyncGroupRequestAssignment]>,
        ) -> Result<Body, Error> {
            unreachable!()
        }

        async fn heartbeat(
            &self,
            _group_id: &str,
            _generation_id: i32,
            _member_id: &str,
            _group_instance_id: Option<&str>,
        ) -> Result<Body, Error> {
            unreachable!()
        }

        async fn leave(
            &self,
            _group_id: &str,
            _member_id: Option<&str>,
            _members: Option<&[MemberIdentity]>,
        ) -> Result<Body, Error> {
            unreachable!()
        }

        async fn offset_commit(&self, _detail: OffsetCommit<'_>) -> Result<Body, Error> {
            unreachable!()
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: Option<&[OffsetFetchRequestTopic]>,
            _groups: Option<&[OffsetFetchRequestGroup]>,
            _require_stable: Option<bool>,
        ) -> Result<Body, Error> {
            unreachable!()
        }
    }

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let storage = storage().await?;

    _ = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let Ok(service) = services(
                "tansu-300",
                AlwaysBroken,
                storage.clone(),
                None,
                CancellationToken::new(),
            ) else {
                return;
            };

            _ = tokio::spawn(async move {
                _ = service.serve(Context::default(), stream).await;
            });
        }
    });

    let client = FrameClient::connect(port).await?;

    let request: Body = JoinGroupRequest::default()
        .group_id("g-300".into())
        .session_timeout_ms(45_000)
        .rebalance_timeout_ms(Some(60_000))
        .member_id("m-300".into())
        .protocol_type("consumer".into())
        .protocols(Some(vec![
            JoinGroupRequestProtocol::default()
                .name("range".into())
                .metadata(bytes::Bytes::from_static(b"meta")),
        ]))
        .into();

    // `TcpListener::bind` completes before the accept task is spawned, so the
    // connection is queued in the backlog rather than refused: no race to wait
    // out, and the ApiVersions negotiation on the same connection succeeds
    // before the JoinGroup that must not be answered.
    let outcome = timeout(
        Duration::from_secs(10),
        client.call(JoinGroupRequest::KEY, Some("c-300"), request),
    )
    .await?;

    let error = match outcome {
        Ok(body) => panic!("a non-answer error must not be answered: {body:?}"),
        Err(error) => error,
    };

    // The shape #300 describes: the read ends before a response frame arrives.
    let message = format!("{error:?}");

    assert!(
        message.contains(&format!("{:?}", io::ErrorKind::UnexpectedEof))
            || message.contains("early eof")
            || message.contains(&format!("{:?}", io::ErrorKind::ConnectionReset)),
        "expected an early-eof-shaped transport error, got: {message}"
    );

    Ok(())
}
