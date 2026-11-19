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

//! A response encoded at a version older than some of its fields comes back
//! without them, and with every field that version does carry intact.

use tansu_sans_io::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use tansu_sans_io::offset_fetch_response::OffsetFetchResponse;
use tansu_sans_io::offset_fetch_response::{OffsetFetchResponseGroup, OffsetFetchResponseTopic};
use tansu_sans_io::offset_fetch_response::{
    OffsetFetchResponsePartition as Partition, OffsetFetchResponsePartitions as GroupPartition,
    OffsetFetchResponseTopics as GroupTopic,
};
use tansu_sans_io::{Body, ErrorCode, Frame, Header};

pub mod common;

const LEADER: &str = "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4";

/// Encodes `body` as response `correlation_id` at `api_key`/`api_version`,
/// decodes it again and returns the body that came back.
fn round_trip(correlation_id: i32, body: Body, api: (i16, i16)) -> tansu_sans_io::Result<Body> {
    let (api_key, api_version) = api;
    let response_header = Header::Response { correlation_id };
    let encoded = Frame::response(response_header, body, api_key, api_version)?;
    tracing::debug!(?encoded);
    let decoded = Frame::response_from_bytes(&encoded[..], api_key, api_version)?;
    assert!(
        matches!(decoded.header, Header::Response { .. }),
        "{decoded:?}"
    );
    Ok(decoded.body)
}

/// JoinGroup v5 predates `protocol_type` (v7) and `skip_assignment` (v9).
#[test]
fn join_group_v5_loses_protocol_type_and_skip_assignment() -> tansu_sans_io::Result<()> {
    let _guard = common::init_tracing()?;

    let metadata = b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0";
    let member = JoinGroupResponseMember::default().member_id(LEADER.into());
    let member = member.group_instance_id(Some("group_instance_id".into()));
    let member = member.metadata(bytes::Bytes::from_static(metadata));

    let kept = JoinGroupResponse::default().throttle_time_ms(Some(0));
    let kept = kept.generation_id(1).error_code(i16::from(ErrorCode::None));
    let kept = kept.protocol_name(Some("range".into()));
    let kept = kept.leader(LEADER.into()).member_id(LEADER.into());
    let kept = kept.members(Some(vec![member]));
    let sent = kept.clone().protocol_type(Some("consumer".into()));
    let sent = sent.skip_assignment(Some(true));

    pretty_assertions::assert_eq!(Body::from(kept), round_trip(4, sent.into(), (11, 5))?);
    Ok(())
}

/// OffsetFetch v7 predates the batched `groups` (v8).
#[test]
fn offset_fetch_v7_loses_its_groups() -> tansu_sans_io::Result<()> {
    let _guard = common::init_tracing()?;
    let none = i16::from(ErrorCode::None);

    let indexed = |index| Partition::default().partition_index(index);
    let blank = |index| indexed(index).committed_offset(-1).error_code(none);
    let unset = |index| blank(index).committed_leader_epoch(Some(-1));
    let partitions = [1, 0, 6, 5, 4, 3, 2].map(|i| unset(i).metadata(Some(String::new())));
    let topic = OffsetFetchResponseTopic::default().name("benchmark".into());
    let topic = topic.partitions(Some(partitions.into()));

    let grouped = GroupPartition::default().partition_index(32123);
    let grouped = grouped.committed_offset(4343).metadata(Some(String::new()));
    let grouped = grouped.committed_leader_epoch(87_678);
    let group_topic = GroupTopic::default().name("topic-name-a".into());
    let group_topic = group_topic.partitions(Some(vec![grouped.error_code(none)]));
    let group = OffsetFetchResponseGroup::default().group_id("example_consumer_group_id".into());
    let group = group.topics(Some(vec![group_topic])).error_code(none);

    let kept = OffsetFetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(none));
    let kept = kept.topics(Some(vec![topic]));
    let sent = kept.clone().groups(Some(vec![group]));

    pretty_assertions::assert_eq!(Body::from(kept), round_trip(8, sent.into(), (9, 7))?);
    Ok(())
}
