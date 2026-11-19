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

//! The consumer protocol's embedded payloads, decoded and re-encoded byte for
//! byte from what real clients send, and [`GroupConsumer`] walking the group
//! protocol: join, the member id it is told to bring, leading or following,
//! sync and heartbeat.

use tansu_sans_io::SyncGroupResponse;
use tansu_sans_io::consumer::{Assignor, CONSUMER, ConsumerProtocolAssignment};
use tansu_sans_io::consumer::{ConsumerProtocolSubscription, GroupConsumer, MemberAssignment};
use tansu_sans_io::consumer::{MemberMetadata, TopicPartition};
use tansu_sans_io::join_group_response::JoinGroupResponseMember;
use tansu_sans_io::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use tansu_sans_io::{Body, ErrorCode, HeartbeatResponse, JoinGroupResponse, MetadataResponse};

pub mod common;

type Octets = bytes::Bytes;
type Outcome = tansu_sans_io::Result<()>;

/// A topic name with a slash, a space and quotes in it, from a real client.
const TELEMETRY: &str = "telemetry/\"PL20 UXA\"";

const RANGE_BENCHMARK: &str = "000300000001000962656e63686d61726b0000000000000000ffffffff0000";
const RANGE_TEST: &str = "000300000001000474657374ffffffff00000000ffffffffffff";
const RANGE_TELEMETRY: &str =
    "000300000001001474656c656d657472792f22504c32302055584122ffffffff00000000ffffffffffff";
const STICKY_TEST: &str = "00030000000100047465737400000004ffffffff00000000ffffffffffff";
const STICKY_TELEMETRY: &str =
    "000300000001001474656c656d657472792f22504c3230205558412200000004ffffffff00000000ffffffffffff";
const ASSIGNED_TEST: &str = "00030000000100047465737400000003000000000000000100000002ffffffff";
const ASSIGNED_TELEMETRY: &str = "000300000001001474656c656d657472792f22504c3230205558412200000003000000000000000100000002ffffffff";
/// A cooperative-sticky user data claiming partition 2 of `test-topic`, all
/// but its last byte: the generation, which a case appends.
const OWNED_USER_DATA: &str = "00000001000a746573742d746f7069630000000100000002000000";
const ASSIGNED_NOTHING: &str = "000300000000ffffffff";
const OWNING_GEN_17: &str = "000300000001000a746573742d746f7069630000001c00000001000a746573742d746f70696300000001000000020000001100000001000a746573742d746f706963000000010000000200000011ffff";
const OWNING_GEN_14: &str = "000300000001000a746573742d746f7069630000001c00000001000a746573742d746f70696300000001000000020000000e00000001000a746573742d746f70696300000001000000020000000effff";

fn unhex(text: &str) -> Octets {
    let pairs = text
        .as_bytes()
        .chunks(2)
        .map(|pair| std::str::from_utf8(pair).unwrap());
    pairs
        .map(|pair| u8::from_str_radix(pair, 16).unwrap())
        .collect::<Vec<u8>>()
        .into()
}

/// Asserts that `hex` is a v3 subscription to `topic` alone, owning nothing,
/// at generation -1, with `user_data` and `rack`, and that it re-encodes to
/// the same bytes.
fn assert_subscribed(
    hex: &str,
    topic: &str,
    user_data: Option<&[u8]>,
    rack: Option<&str>,
) -> Outcome {
    let wire = unhex(hex);
    let decoded = MemberMetadata::try_from(wire.clone())?;
    let subscribed = &decoded.subscription;
    let topics = vec![String::from(topic)];
    assert_eq!((3, &topics), (decoded.version, &subscribed.topics));
    assert_eq!(user_data.map(Octets::copy_from_slice), subscribed.user_data);
    assert_eq!(Some(0), subscribed.owned_partitions.as_ref().map(Vec::len));
    assert_eq!(
        (Some(-1), rack),
        (subscribed.generation_id, subscribed.rack_id.as_deref())
    );
    assert_eq!(wire, Octets::try_from(&decoded)?);
    Ok(())
}

/// Asserts that `hex` is a v3 assignment of partitions 0 to 2 of `topic`,
/// without user data, and that it re-encodes to the same bytes.
fn assert_assigned(hex: &str, topic: &str) -> Outcome {
    let wire = unhex(hex);
    let decoded = MemberAssignment::try_from(wire.clone())?;
    assert_eq!(
        (3, None),
        (decoded.version, decoded.assignment.user_data.clone())
    );
    let first_three = TopicPartition::default().topic(topic).partitions(0..3);
    assert_eq!(vec![first_three], decoded.assignment.assigned_partitions);
    assert_eq!(wire, Octets::try_from(&decoded)?);
    Ok(())
}

#[test]
fn range_subscriptions_round_trip() -> Outcome {
    let _guard = common::init_tracing()?;
    assert_subscribed(RANGE_BENCHMARK, "benchmark", Some(b""), Some(""))?;
    assert_subscribed(RANGE_TEST, "test", None, None)?;
    assert_subscribed(RANGE_TELEMETRY, TELEMETRY, None, None)
}

#[test]
fn cooperative_sticky_subscriptions_round_trip() -> Outcome {
    let _guard = common::init_tracing()?;
    let sticky_user_data = Some(&b"\xff\xff\xff\xff"[..]);
    assert_subscribed(STICKY_TEST, "test", sticky_user_data, None)?;
    assert_subscribed(STICKY_TELEMETRY, TELEMETRY, sticky_user_data, None)
}

#[test]
fn assignments_round_trip() -> Outcome {
    let _guard = common::init_tracing()?;
    assert_assigned(ASSIGNED_TEST, "test")?;
    assert_assigned(ASSIGNED_TELEMETRY, TELEMETRY)
}

#[test]
fn an_empty_assignment_is_the_default_at_v3() -> Outcome {
    let nothing = MemberAssignment::try_from(unhex(ASSIGNED_NOTHING))?;
    let default = MemberAssignment::default().version(ConsumerProtocolSubscription::V3);
    assert_eq!(default, nothing);
    Ok(())
}

#[test]
fn assignments_build_with_user_data_and_partitions() {
    let abc = Octets::from_static(b"abc");
    let assigned = ConsumerProtocolAssignment::default().user_data(Some(abc.clone()));
    assert_eq!(Some(abc), assigned.user_data);
    let pqr = TopicPartition::default().topic("pqr").partitions(0..3);
    assert_eq!(("pqr", vec![0, 1, 2]), (pqr.topic.as_str(), pqr.partitions));
}

/// A subscription that owns partition 2 of `test-topic` from `generation`,
/// with the same in its user data, as a cooperative-sticky client rejoins.
#[test]
fn subscriptions_owning_a_partition_decode() -> Outcome {
    for (hex, generation) in [(OWNING_GEN_17, 17_u8), (OWNING_GEN_14, 14)] {
        let (mut user_data, rack_id) = (unhex(OWNED_USER_DATA).to_vec(), None);
        user_data.push(generation);
        let owned = TopicPartition::default()
            .topic("test-topic")
            .partitions([2].into_iter());
        let subscription = ConsumerProtocolSubscription {
            topics: vec!["test-topic".into()],
            user_data: Some(user_data.into()),
            owned_partitions: Some(vec![owned]),
            generation_id: Some(i32::from(generation)),
            rack_id,
        };
        let version = 3;
        let wanted = MemberMetadata {
            version,
            subscription,
        };
        assert_eq!(wanted, MemberMetadata::try_from(unhex(hex))?);
    }
    Ok(())
}

/// Topics of the given partition counts, as a `Metadata` response lists them.
fn cluster(topics: &[(&str, i32)]) -> MetadataResponse {
    let partition = |index| MetadataResponsePartition::default().partition_index(index);
    let topic = |&(name, count): &(&str, i32)| {
        let listed = MetadataResponseTopic::default().name(Some(name.to_owned()));
        listed.partitions(Some((0..count).map(partition).collect()))
    };
    MetadataResponse::default().topics(Some(topics.iter().map(topic).collect()))
}

/// A v3 subscription to `topics`, as a member's join metadata.
fn subscribed_to(topics: &[&str]) -> tansu_sans_io::Result<Octets> {
    let names = topics.iter().map(|name| name.to_string());
    let subscription = ConsumerProtocolSubscription::default().topics(names);
    let metadata = MemberMetadata::default().version(ConsumerProtocolSubscription::V3);
    Octets::try_from(&metadata.subscription(subscription))
}

/// A consumer in group `g1` subscribed to `t` of `partitions` partitions,
/// logging each assignment it is handed.
fn consumer_of(partitions: i32) -> GroupConsumer {
    let consumer = GroupConsumer::builder("g1").topics(["t"]);
    let consumer = consumer.metadata(cluster(&[("t", partitions)]));
    let logged: std::sync::Arc<tansu_sans_io::consumer::DynConsumerAssignment> =
        std::sync::Arc::new(|group, assignment| tracing::debug!(group, %assignment));
    consumer.on_assignment(Some(logged)).build()
}

/// A successful join at `generation`, as `member` of a group led by `leader`
/// under `protocol`.
fn joined(generation: i32, member: &str, leader: &str, protocol: &str) -> JoinGroupResponse {
    let answer = JoinGroupResponse::default().error_code(ErrorCode::None.into());
    let answer = answer.generation_id(generation).member_id(member.into());
    let answer = answer
        .leader(leader.into())
        .protocol_name(Some(protocol.into()));
    answer.protocol_type(Some(CONSUMER.into()))
}

/// Members `ids`, each joining with `metadata`.
fn members(ids: &[&str], metadata: &Octets) -> Vec<JoinGroupResponseMember> {
    let member = |id: &&str| JoinGroupResponseMember::default().member_id((*id).into());
    ids.iter()
        .map(|id| member(id).metadata(metadata.clone()))
        .collect()
}

/// What the leader's sync hands `member`.
fn handed_to(
    assignments: &[tansu_sans_io::sync_group_request::SyncGroupRequestAssignment],
    member: &str,
) -> tansu_sans_io::Result<Vec<TopicPartition>> {
    let mut handed_out = assignments.iter();
    let handed = handed_out
        .find(|handed| member == handed.member_id)
        .expect("assigned");
    let decoded = MemberAssignment::try_from(handed.assignment.clone())?;
    Ok(decoded.assignment.assigned_partitions)
}

/// `action` as the request it should be, failing the test otherwise.
fn answered_with<T>(action: Body) -> T
where
    T: TryFrom<Body, Error: std::fmt::Debug>,
{
    let name = std::any::type_name::<T>();
    T::try_from(action).unwrap_or_else(|error| panic!("expected {name}: {error:?}"))
}

fn of_t(partitions: Vec<i32>) -> Vec<TopicPartition> {
    vec![
        TopicPartition::default()
            .topic("t")
            .partitions(partitions.into_iter()),
    ]
}

#[test]
fn a_new_consumer_is_an_outsider() -> Outcome {
    let _guard = common::init_tracing()?;
    let consumer = GroupConsumer::builder("g1").topics(["t"]);
    let consumer = consumer.metadata(cluster(&[("t", 3)])).build();
    assert_eq!("g1", consumer.group_id);
    assert!(consumer.is_outsider(), "a new consumer has not joined");
    assert_eq!(vec![String::from("t")], consumer.topics.to_vec());
    Ok(())
}

#[test]
fn an_outsider_joins_offering_every_assignor() -> Outcome {
    let _guard = common::init_tracing()?;
    let request: tansu_sans_io::JoinGroupRequest = answered_with(consumer_of(3).next_action(None)?);
    assert_eq!(
        ("g1", "", CONSUMER),
        (
            request.group_id.as_str(),
            request.member_id.as_str(),
            request.protocol_type.as_str()
        )
    );

    let offered = request.protocols.as_deref().unwrap_or_default();
    let names = offered.iter().map(|protocol| protocol.name.as_str());
    let mut names: Vec<_> = names.collect();
    names.sort_unstable();
    let mut every = [
        Assignor::RANGE,
        Assignor::ROUND_ROBIN,
        Assignor::UNIFORM,
        Assignor::COOPERATIVE_STICKY,
    ];
    every.sort_unstable();
    assert_eq!(
        every.to_vec(),
        names,
        "all four assignors must be advertised"
    );
    Ok(())
}

#[test]
fn an_outsider_told_its_member_id_rejoins_with_it() -> Outcome {
    let _guard = common::init_tracing()?;
    let refused = JoinGroupResponse::default().error_code(ErrorCode::MemberIdRequired.into());
    let refused = refused.member_id("consumer-abc-123".into());
    let request: tansu_sans_io::JoinGroupRequest =
        answered_with(consumer_of(1).next_action(Some(refused.into()))?);
    assert_eq!("consumer-abc-123", request.member_id);
    Ok(())
}

#[test]
fn an_outsider_led_by_another_syncs_with_no_assignments() -> Outcome {
    let _guard = common::init_tracing()?;
    let answer = joined(3, "me", "leader", Assignor::RANGE);
    let request: tansu_sans_io::SyncGroupRequest =
        answered_with(consumer_of(2).next_action(Some(answer.into()))?);
    assert_eq!(
        ("g1", "me", 3),
        (
            request.group_id.as_str(),
            request.member_id.as_str(),
            request.generation_id
        )
    );
    assert_eq!(Some(vec![]), request.assignments);
    Ok(())
}

#[test]
fn an_outsider_that_leads_assigns_by_range() -> Outcome {
    let _guard = common::init_tracing()?;
    let metadata = subscribed_to(&["t"])?;
    let answer =
        joined(2, "C0", "C0", Assignor::RANGE).members(Some(members(&["C0", "C1"], &metadata)));
    let request: tansu_sans_io::SyncGroupRequest =
        answered_with(consumer_of(4).next_action(Some(answer.into()))?);
    assert_eq!(
        ("C0", 2),
        (request.member_id.as_str(), request.generation_id)
    );
    assert_eq!(Some(Assignor::RANGE.to_string()), request.protocol_name);

    let assignments = request.assignments.expect("a leader assigns");
    assert_eq!(2, assignments.len(), "one per member");
    assert_eq!(of_t(vec![0, 1]), handed_to(&assignments, "C0")?);
    assert_eq!(of_t(vec![2, 3]), handed_to(&assignments, "C1")?);
    Ok(())
}

/// Two members and four partitions of `t`: range would hand C0 `[0, 1]`,
/// round robin hands it `[0, 2]`. Joining under `roundrobin` shows the
/// leader assigns with the protocol the join chose, not its own preference.
#[test]
fn a_leader_assigns_with_the_protocol_the_join_chose() -> Outcome {
    let _guard = common::init_tracing()?;
    let metadata = subscribed_to(&["t"])?;
    let answer = joined(1, "C0", "C0", Assignor::ROUND_ROBIN)
        .members(Some(members(&["C0", "C1"], &metadata)));
    let request: tansu_sans_io::SyncGroupRequest =
        answered_with(consumer_of(4).next_action(Some(answer.into()))?);
    let assignments = request.assignments.expect("a leader assigns");
    assert_eq!(of_t(vec![0, 2]), handed_to(&assignments, "C0")?);
    Ok(())
}

#[test]
fn a_synced_leader_heartbeats() -> Outcome {
    let _guard = common::init_tracing()?;
    let mut consumer = consumer_of(1);
    let metadata = subscribed_to(&["t"])?;
    let answer = joined(5, "me", "me", Assignor::RANGE).members(Some(members(&["me"], &metadata)));
    let _sync = consumer.next_action(Some(answer.into()))?;

    let assigned = ConsumerProtocolAssignment::default().assigned_partitions(of_t(vec![0]));
    let handed = MemberAssignment::default().version(ConsumerProtocolSubscription::V3);
    let handed = Octets::try_from(&handed.assignment(assigned))?;
    let synced = SyncGroupResponse::default()
        .error_code(ErrorCode::None.into())
        .assignment(handed);
    let request: tansu_sans_io::HeartbeatRequest =
        answered_with(consumer.next_action(Some(synced.into()))?);
    assert_eq!(
        ("g1", "me", 5),
        (
            request.group_id.as_str(),
            request.member_id.as_str(),
            request.generation_id
        )
    );
    Ok(())
}

#[test]
fn a_follower_heartbeats_again_after_a_good_heartbeat() -> Outcome {
    let _guard = common::init_tracing()?;
    let mut consumer = consumer_of(2);
    let _sync = consumer.next_action(Some(joined(3, "me", "leader", Assignor::RANGE).into()))?;

    let beat = HeartbeatResponse::default().error_code(ErrorCode::None.into());
    let request: tansu_sans_io::HeartbeatRequest =
        answered_with(consumer.next_action(Some(beat.into()))?);
    assert_eq!(
        ("me", 3),
        (request.member_id.as_str(), request.generation_id)
    );
    Ok(())
}

#[test]
fn a_follower_told_to_rebalance_rejoins() -> Outcome {
    let _guard = common::init_tracing()?;
    let mut consumer = consumer_of(2);
    let sync = consumer.next_action(Some(joined(4, "me", "leader", Assignor::RANGE).into()))?;
    tracing::debug!(req = ?sync);

    let beat = HeartbeatResponse::default().error_code(ErrorCode::RebalanceInProgress.into());
    let rejoin = consumer.next_action(Some(beat.into()))?;
    tracing::debug!(req = ?rejoin);
    let request: tansu_sans_io::JoinGroupRequest = answered_with(rejoin);
    assert_eq!("me", request.member_id);
    Ok(())
}
