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

//! A field that is valid at the negotiated version and not nullable must be
//! written (#351).
//!
//! The wire format is positional. There is no encoding for "absent" outside the
//! nullable sentinels, so leaving such a field `None` did not produce a shorter
//! message — it produced a **different** one, and the peer read the following
//! bytes as that field.
//!
//! What makes it worth a guard rather than a fix at each call site is that both
//! ends stayed quiet. The encoder returned `Ok`. Whether the decoder noticed
//! depended only on how the bytes happened to line up: run past the end and it
//! raised `UnexpectedEof`, land inside the payload and it returned `Ok` with
//! fabricated values.

use tansu_sans_io::{
    ApiKey as _, Body, Error, Frame, Header, NULL_TOPIC_ID, OffsetCommitRequest,
    OffsetFetchRequest, Result, RootMessageMeta,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
};

fn request_header(api_key: i16, api_version: i16) -> Header {
    Header::Request {
        api_key,
        api_version,
        correlation_id: 1,
        client_id: Some("c".into()),
    }
}

fn valid_versions(api_key: i16) -> std::ops::RangeInclusive<i16> {
    let meta = RootMessageMeta::messages().requests()[&api_key];
    meta.version.valid.start..=meta.version.valid.end
}

/// Encode, decode, re-encode: the bytes and the value must both be stable.
///
/// The fixpoint is what pins layout agreement without needing to know which
/// fields survive at this version. An omitted positional field shifts the
/// decode, so the value it recovers re-encodes to different bytes — even in the
/// case where the first decode happened to succeed.
fn round_trips(api_key: i16, api_version: i16, body: Body) -> Result<()> {
    let first = Frame::request(request_header(api_key, api_version), body)?;

    let decoded = Frame::request_from_bytes(first.clone())?;
    let second = Frame::request(decoded.header.clone(), decoded.body.clone())?;

    assert_eq!(
        first, second,
        "v{api_version}: re-encoding what was decoded produced different bytes"
    );
    assert_eq!(
        decoded,
        Frame::request_from_bytes(second)?,
        "v{api_version}: the second decode disagreed with the first"
    );

    Ok(())
}

/// The reported case, refused at the encoder instead of corrupting silently.
///
/// `MemberEpoch` is `int32`, `versions: 9+`, and not nullable. Left `None` it
/// used to encode without complaint, and this exact frame then decoded — `Ok`,
/// no error either side — into:
///
/// ```text
/// group_id: "g", member_epoch: Some(33715202), topics: None
/// ```
///
/// The four bytes were taken from the topics array, so the epoch is fabricated
/// and the topic list is gone.
#[test]
fn an_omitted_non_nullable_field_is_refused_rather_than_encoded() {
    let body: Body = OffsetFetchRequest::default()
        .groups(Some(vec![
            OffsetFetchRequestGroup::default()
                .group_id("g".into())
                .topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .name("t".into())
                        .partition_indexes(Some(vec![0])),
                ])),
        ]))
        .require_stable(Some(false))
        .into();

    match Frame::request(request_header(OffsetFetchRequest::KEY, 9), body) {
        Err(Error::OmittedNonNullableField { field, api_version }) => {
            assert_eq!(
                "Frame.OffsetFetchRequest.OffsetFetchRequestGroup.member_epoch",
                field
            );
            assert_eq!(Some(9), api_version);
        }
        otherwise => panic!("expected a refusal, got {otherwise:?}"),
    }
}

/// The same omission one version down is not an omission at all.
///
/// `MemberEpoch` starts at v9, so at v8 there are no bytes to write and `None`
/// is the only correct encoding. This is the branch the guard must not take —
/// without it the guard would reject every message that does not set every
/// field of every version.
#[test]
fn a_field_not_valid_at_the_version_is_still_omitted() -> Result<()> {
    let body: Body = OffsetFetchRequest::default()
        .groups(Some(vec![
            OffsetFetchRequestGroup::default()
                .group_id("g".into())
                .topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .name("t".into())
                        .partition_indexes(Some(vec![0])),
                ])),
        ]))
        .require_stable(Some(false))
        .into();

    round_trips(OffsetFetchRequest::KEY, 8, body)
}

/// A nullable field left `None` keeps its sentinel, at every version.
///
/// `MemberId` is `versions: 9+` **and** `nullableVersions: 9+`, so unlike its
/// neighbour it has an encoding for absent. Setting only the epoch has to be
/// enough.
#[test]
fn a_nullable_field_left_none_still_encodes() -> Result<()> {
    let body: Body = OffsetFetchRequest::default()
        .groups(Some(vec![
            OffsetFetchRequestGroup::default()
                .group_id("g".into())
                .member_epoch(Some(-1))
                .topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .name("t".into())
                        .partition_indexes(Some(vec![0])),
                ])),
        ]))
        .require_stable(Some(false))
        .into();

    round_trips(OffsetFetchRequest::KEY, 9, body)
}

/// Every version of `OffsetFetchRequest`, both shapes.
///
/// `GroupId`/`Topics` are 0-7 and `Groups` is 8+, so one value populated for
/// both covers the whole range.
#[test]
fn offset_fetch_round_trips_at_every_version() -> Result<()> {
    for api_version in valid_versions(OffsetFetchRequest::KEY) {
        let body: Body = OffsetFetchRequest::default()
            .group_id(Some("g".into()))
            .topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .name("t".into())
                    .partition_indexes(Some(vec![0])),
            ]))
            .groups(Some(vec![
                OffsetFetchRequestGroup::default()
                    .group_id("g".into())
                    .member_id(Some("m".into()))
                    .member_epoch(Some(-1))
                    .topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .name("t".into())
                            .partition_indexes(Some(vec![0])),
                    ])),
            ]))
            .require_stable(Some(false))
            .into();

        round_trips(OffsetFetchRequest::KEY, api_version, body)?;
    }

    Ok(())
}

/// Every version of `OffsetCommitRequest`.
///
/// This is the API that showed the version dependence most sharply. With
/// `CommitTimestamp` (v1 only), `RetentionTimeMs` (v2-4) and
/// `CommittedLeaderEpoch` (v6+) all left `None`, v0 and v5 round-tripped and
/// the other eight versions did not — each tripping on whichever
/// non-nullable field its own version had added.
#[test]
fn offset_commit_round_trips_at_every_version() -> Result<()> {
    for api_version in valid_versions(OffsetCommitRequest::KEY) {
        let body: Body = OffsetCommitRequest::default()
            .group_id("g".into())
            .generation_id_or_member_epoch(Some(1))
            .member_id(Some("m".into()))
            .group_instance_id(Some("i".into()))
            .retention_time_ms(Some(-1))
            .topics(Some(vec![
                OffsetCommitRequestTopic::default()
                    .name("t".into())
                    .partitions(Some(vec![
                        OffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(1)
                            .committed_leader_epoch(Some(-1))
                            .commit_timestamp(Some(-1))
                            .committed_metadata(Some("".into())),
                    ])),
            ]))
            .into();

        round_trips(OffsetCommitRequest::KEY, api_version, body)?;
    }

    Ok(())
}

/// The `topic` → `topic_id` changeover, from both sides.
///
/// `FetchTopic.Topic` is 0-12 and `TopicId` 13+, neither nullable. A value that
/// names only one of them is legal on one side of the boundary and truncated on
/// the other, which is how `FetchResponse` came to omit 16 bytes of uuid.
#[test]
fn fetch_round_trips_across_the_topic_id_boundary() -> Result<()> {
    use tansu_sans_io::{
        FetchRequest,
        fetch_request::{FetchPartition, FetchTopic},
    };

    for api_version in valid_versions(FetchRequest::KEY) {
        let body: Body = FetchRequest::default()
            // 0-14, replaced by the tagged `ReplicaState` from v15.
            .replica_id(Some(-1))
            .max_wait_ms(500)
            .min_bytes(1)
            .max_bytes(Some(1024))
            .isolation_level(Some(0))
            .session_id(Some(0))
            .session_epoch(Some(-1))
            .topics(Some(vec![
                FetchTopic::default()
                    .topic(Some("t".into()))
                    .topic_id(Some(NULL_TOPIC_ID))
                    .partitions(Some(vec![
                        FetchPartition::default()
                            .partition(0)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(1024),
                    ])),
            ]))
            .forgotten_topics_data(Some(vec![]))
            .rack_id(Some("".into()))
            .into();

        round_trips(FetchRequest::KEY, api_version, body)?;
    }

    Ok(())
}
