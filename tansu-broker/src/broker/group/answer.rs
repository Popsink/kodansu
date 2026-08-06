// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Turning an [`Error::Api`] into the response frame it was always meant to be
//! (#300).
//!
//! `Error::Api(code)` is not a failure — it is an answer the broker chose, and
//! `NOT_COORDINATOR` is the one #243 introduced so that a failed forward hop
//! tells the client to retry against the real owner instead of being processed
//! locally and splitting the group.
//!
//! It was never delivered. Nothing between the coordinator and the socket
//! converted it: the `Err` propagated out of the group service, out of
//! `FrameRouteService`, out of `BytesFrameService`, out of
//! `TcpBytesService::{process, req, serve}` and into the per-connection task in
//! [`crate::broker`], which **ends the connection without writing a response**.
//! From the caller that is a read that ends before a response frame arrives —
//! `early eof`, which is the symptom #300 is about. The `Classify` comment in
//! `crate::Error` says these are produced "by design" on every rollout, and
//! every one of them cost a connection.
//!
//! Each API answers in its own shape, which is why this is six functions and
//! not one: `OffsetCommit` in particular has no top-level error code — the code
//! goes on every partition the request named — so it needs the request to build
//! its answer.
//!
//! Scope: only [`Error::Api`]. Anything else still ends the connection, because
//! anything else means the broker does not know what it just did, and inventing
//! an error code for it would report a definite answer the broker cannot stand
//! behind.

use bytes::Bytes;
use tansu_sans_io::{
    Body, ErrorCode,
    heartbeat_response::HeartbeatResponse,
    join_group_response::JoinGroupResponse,
    leave_group_response::LeaveGroupResponse,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::OffsetFetchRequestGroup,
    offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseGroup},
    sync_group_response::SyncGroupResponse,
};

/// `JoinGroup` answered with `error_code`.
///
/// `generation_id` is `-1` and the member list empty, as Kafka does for a join
/// it could not process: the client must not read a generation out of an
/// error. The member id is echoed back so a client that already owns one keeps
/// it across the retry.
pub(super) fn join_group(error_code: ErrorCode, member_id: &str) -> Body {
    JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .generation_id(-1)
        .protocol_type(None)
        .protocol_name(Some("".into()))
        .leader("".into())
        .skip_assignment(Some(false))
        .member_id(member_id.into())
        .members(Some([].into()))
        .into()
}

/// `SyncGroup` answered with `error_code`, and an empty assignment — the client
/// must not act on an assignment that comes with an error.
pub(super) fn sync_group(error_code: ErrorCode) -> Body {
    SyncGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .protocol_type(None)
        .protocol_name(None)
        .assignment(Bytes::from_static(b""))
        .into()
}

/// `Heartbeat` answered with `error_code`.
pub(super) fn heartbeat(error_code: ErrorCode) -> Body {
    HeartbeatResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .into()
}

/// `LeaveGroup` answered with `error_code`.
pub(super) fn leave_group(error_code: ErrorCode) -> Body {
    LeaveGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .members(Some([].into()))
        .into()
}

/// `OffsetFetch` answered with `error_code`, in whichever of its two shapes the
/// request used.
///
/// KIP-709 moved the API from one group per request to many at v8: `group_id` +
/// `topics` + a top-level `error_code` up to v7, a `groups` list with a code per
/// group from v8. The encoder writes only the fields valid for the negotiated
/// version, so answering in the wrong shape produces a response with no error
/// code in it at all — a success, as far as the client is concerned. Mirroring
/// the request is what the coordinator's own `offset_fetch` does.
pub(super) fn offset_fetch(
    error_code: ErrorCode,
    groups: Option<&[OffsetFetchRequestGroup]>,
) -> Body {
    OffsetFetchResponse::default()
        .throttle_time_ms(Some(0))
        .topics(None)
        .error_code(Some(error_code.into()))
        .groups(groups.map(|groups| {
            groups
                .iter()
                .map(|group| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some([].into()))
                        .error_code(error_code.into())
                })
                .collect()
        }))
        .into()
}

/// `OffsetCommit` answered with `error_code` on **every partition the request
/// named**.
///
/// `OffsetCommitResponse` has no top-level error code, so the answer has to be
/// shaped like the request — and a response that omitted a partition the client
/// asked about would be read as a silent success for it, which is worse than
/// the dropped connection this replaces.
pub(super) fn offset_commit(
    error_code: ErrorCode,
    topics: Option<&[OffsetCommitRequestTopic]>,
) -> Body {
    OffsetCommitResponse::default()
        .throttle_time_ms(Some(0))
        .topics(Some(
            topics
                .unwrap_or_default()
                .iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .partition_index(partition.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect(),
        ))
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// KIP-709: from v8 the error code lives on each group, and the top-level
    /// one is not written at all. An answer in the v0-v7 shape would encode to a
    /// response carrying no error code — a success (#300).
    #[test]
    fn offset_fetch_answers_per_group_when_the_request_did() -> crate::Result<()> {
        let groups = vec![
            OffsetFetchRequestGroup::default().group_id("g-1".into()),
            OffsetFetchRequestGroup::default().group_id("g-2".into()),
        ];

        let Body::OffsetFetchResponse(response) =
            offset_fetch(ErrorCode::NotCoordinator, Some(&groups))
        else {
            panic!("offset_fetch did not answer with an OffsetFetchResponse")
        };

        let not_coordinator = i16::from(ErrorCode::NotCoordinator);

        assert_eq!(
            vec![
                ("g-1".to_owned(), not_coordinator),
                ("g-2".to_owned(), not_coordinator),
            ],
            response
                .groups
                .unwrap_or_default()
                .into_iter()
                .map(|group| (group.group_id, group.error_code))
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// The point of the module: an error code the client can act on, in a frame
    /// it can decode, rather than a socket that closes.
    #[test]
    fn every_api_carries_the_error_code_it_was_given() -> crate::Result<()> {
        let code = ErrorCode::NotCoordinator;

        let Body::JoinGroupResponse(join) = join_group(code, "m-1") else {
            panic!("join_group did not answer with a JoinGroupResponse")
        };
        assert_eq!(code, ErrorCode::try_from(join.error_code)?);
        assert_eq!(-1, join.generation_id);
        assert_eq!("m-1", join.member_id);

        let Body::SyncGroupResponse(sync) = sync_group(code) else {
            panic!("sync_group did not answer with a SyncGroupResponse")
        };
        assert_eq!(code, ErrorCode::try_from(sync.error_code)?);
        assert!(sync.assignment.is_empty());

        let Body::HeartbeatResponse(heartbeat) = heartbeat(code) else {
            panic!("heartbeat did not answer with a HeartbeatResponse")
        };
        assert_eq!(code, ErrorCode::try_from(heartbeat.error_code)?);

        let Body::LeaveGroupResponse(leave) = leave_group(code) else {
            panic!("leave_group did not answer with a LeaveGroupResponse")
        };
        assert_eq!(code, ErrorCode::try_from(leave.error_code)?);

        let Body::OffsetFetchResponse(fetch) = offset_fetch(code, None) else {
            panic!("offset_fetch did not answer with an OffsetFetchResponse")
        };
        assert_eq!(
            Some(code),
            fetch.error_code.map(ErrorCode::try_from).transpose()?
        );
        assert_eq!(None, fetch.groups);

        Ok(())
    }

    /// `OffsetCommit` carries no top-level code, so the answer has to name every
    /// partition the request did. One the client asked about and the response
    /// omitted would read as a silent success.
    #[test]
    fn offset_commit_answers_on_every_partition_the_request_named() -> crate::Result<()> {
        use tansu_sans_io::offset_commit_request::OffsetCommitRequestPartition;

        let topics = vec![
            OffsetCommitRequestTopic::default()
                .name("a".into())
                .partitions(Some(vec![
                    OffsetCommitRequestPartition::default().partition_index(0),
                    OffsetCommitRequestPartition::default().partition_index(3),
                ])),
            OffsetCommitRequestTopic::default()
                .name("b".into())
                .partitions(Some(vec![
                    OffsetCommitRequestPartition::default().partition_index(7),
                ])),
        ];

        let Body::OffsetCommitResponse(response) =
            offset_commit(ErrorCode::NotCoordinator, Some(&topics))
        else {
            panic!("offset_commit did not answer with an OffsetCommitResponse")
        };

        let answered = response
            .topics
            .unwrap_or_default()
            .into_iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |partition| {
                        (
                            topic.name.clone(),
                            partition.partition_index,
                            partition.error_code,
                        )
                    })
            })
            .collect::<Vec<_>>();

        let not_coordinator = i16::from(ErrorCode::NotCoordinator);

        assert_eq!(
            vec![
                ("a".to_owned(), 0, not_coordinator),
                ("a".to_owned(), 3, not_coordinator),
                ("b".to_owned(), 7, not_coordinator),
            ],
            answered
        );

        Ok(())
    }
}
