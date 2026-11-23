// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{Buf as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    Body, Decoder, Encoder, Error, ErrorCode, HeartbeatRequest, JoinGroupRequest,
    JoinGroupResponse, MetadataResponse, SyncGroupRequest,
    join_group_request::JoinGroupRequestProtocol, join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

mod assignor;
mod codec;

pub use assignor::{CooperativeStickyAssignor, RangeAssignor, RoundRobinAssignor, UniformAssignor};

pub const CONSUMER: &str = "consumer";

pub trait ConsumerAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error>;
}

pub enum Assignor {
    Uniform,
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl Assignor {
    pub const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    pub const RANGE: &str = "range";
    pub const ROUND_ROBIN: &str = "roundrobin";
    pub const UNIFORM: &str = "uniform";
}

impl AsRef<str> for Assignor {
    fn as_ref(&self) -> &str {
        match self {
            Self::CooperativeSticky => Self::COOPERATIVE_STICKY,
            Self::Range => Self::RANGE,
            Self::RoundRobin => Self::ROUND_ROBIN,
            Self::Uniform => Self::UNIFORM,
        }
    }
}

impl FromStr for Assignor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let known = [
            Self::CooperativeSticky,
            Self::Range,
            Self::RoundRobin,
            Self::Uniform,
        ];
        known
            .into_iter()
            .find(|assignor| assignor.as_ref() == s)
            .ok_or_else(|| Error::UnknownAssignor(s.to_owned()))
    }
}

impl ConsumerAssignor for Assignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let assignor: &dyn ConsumerAssignor = match self {
            Self::CooperativeSticky => &CooperativeStickyAssignor,
            Self::Range => &RangeAssignor,
            Self::RoundRobin => &RoundRobinAssignor,
            Self::Uniform => &UniformAssignor,
        };
        assignor.assign(members, metadata)
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
#[serde(try_from = "codec::MemberMetadata")]
pub struct MemberMetadata {
    pub version: i16,
    pub subscription: ConsumerProtocolSubscription,
}

/// Written through [`codec::MemberMetadata`], which is where the version
/// becomes a layout.
///
/// This is by hand rather than `#[serde(into = ...)]` because that attribute
/// takes an infallible conversion and a version outside the range this fork
/// encodes has to be an error (#556).
impl Serialize for MemberMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        codec::MemberMetadata::try_from(self.clone())
            .map_err(serde::ser::Error::custom)
            .and_then(|encoded| encoded.serialize(serializer))
    }
}

impl fmt::Display for MemberMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v: {}, sub: {}", self.version, self.subscription)
    }
}

impl MemberMetadata {
    pub fn version(mut self, version: i16) -> Self {
        self.version = version;
        self
    }

    pub fn subscription(mut self, subscription: ConsumerProtocolSubscription) -> Self {
        self.subscription = subscription;
        self
    }
}

impl TryFrom<Bytes> for MemberMetadata {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        decoded(value)
    }
}

impl TryFrom<&MemberMetadata> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberMetadata) -> Result<Self, Self::Error> {
        encoded(value)
    }
}

/// `T` read from the protocol's own encoding of it in `bytes`.
fn decoded<T>(bytes: Bytes) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    T::deserialize(&mut Decoder::new(&mut bytes.reader()))
}

/// `value` in the protocol's own encoding.
fn encoded(value: &impl Serialize) -> Result<Bytes, Error> {
    let mut encoder = Encoder::new(BytesMut::new());
    value.serialize(&mut encoder)?;
    Ok(Bytes::from(encoder))
}

/// `items`, displayed and separated by commas.
fn listed<T: fmt::Display>(items: &[T]) -> String {
    let shown: Vec<String> = items.iter().map(T::to_string).collect();
    shown.join(", ")
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolSubscription {
    pub topics: Vec<String>,
    pub user_data: Option<Bytes>,
    pub owned_partitions: Option<Vec<TopicPartition>>,
    pub generation_id: Option<i32>,
    pub rack_id: Option<String>,
}

impl fmt::Display for ConsumerProtocolSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let owned = self.owned_partitions.as_deref().map(listed);
        write!(f, "owned: {}", owned.unwrap_or_default())
    }
}

impl ConsumerProtocolSubscription {
    pub const V0: i16 = 0;
    pub const V1: i16 = 1;
    pub const V2: i16 = 2;
    pub const V3: i16 = 3;

    pub fn topics(mut self, topics: impl IntoIterator<Item = String>) -> Self {
        self.topics = Vec::from_iter(topics);
        self
    }

    pub fn user_data(mut self, user_data: Option<Bytes>) -> Self {
        self.user_data = user_data;
        self
    }

    pub fn owned_partitions(
        mut self,
        owned_partitions: impl Iterator<Item = TopicPartition>,
    ) -> Self {
        self.owned_partitions = Some(Vec::from_iter(owned_partitions));
        self
    }

    pub fn generation_id(mut self, generation_id: Option<i32>) -> Self {
        self.generation_id = generation_id;
        self
    }

    pub fn rack_id(mut self, rack_id: Option<String>) -> Self {
        self.rack_id = rack_id;
        self
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl<T, P> From<(T, P)> for TopicPartition
where
    T: Into<String>,
    P: IntoIterator<Item = i32>,
{
    fn from(value: (T, P)) -> TopicPartition {
        let (topic, partitions) = value;
        Self {
            topic: topic.into(),
            partitions: Vec::from_iter(partitions),
        }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: [{}]", self.topic, listed(&self.partitions))
    }
}

impl TopicPartition {
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }

    pub fn partitions(mut self, partitions: impl Iterator<Item = i32>) -> Self {
        self.partitions = Vec::from_iter(partitions);
        self
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberAssignment")]
#[serde(into = "codec::MemberAssignment")]
pub struct MemberAssignment {
    pub version: i16,
    pub assignment: ConsumerProtocolAssignment,
}

impl fmt::Display for MemberAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.assignment, f)
    }
}

impl MemberAssignment {
    pub fn version(mut self, version: i16) -> Self {
        self.version = version;
        self
    }

    pub fn assignment(mut self, assignment: ConsumerProtocolAssignment) -> Self {
        self.assignment = assignment;
        self
    }
}

impl TryFrom<Bytes> for MemberAssignment {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        decoded(value)
    }
}

impl TryFrom<&MemberAssignment> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberAssignment) -> Result<Self, Self::Error> {
        encoded(value)
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolAssignment {
    pub assigned_partitions: Vec<TopicPartition>,
    pub user_data: Option<Bytes>,
}

impl fmt::Display for ConsumerProtocolAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&listed(&self.assigned_partitions))
    }
}

impl ConsumerProtocolAssignment {
    pub fn assigned_partitions(
        self,
        assigned_partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Self {
        let assigned = assigned_partitions.into_iter().map(Into::into);
        Self {
            assigned_partitions: assigned.collect(),
            ..self
        }
    }

    pub fn user_data(self, user_data: impl IntoIterator<Item = Bytes>) -> Self {
        let user_data = user_data.into_iter().next();
        Self { user_data, ..self }
    }
}

pub type DynConsumerAssignment = dyn Fn(String, MemberAssignment) + Send + Sync + 'static;

pub struct Builder<M, T> {
    group_id: String,
    topics: T,
    metadata: M,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
    rebalance_timeout: Option<Duration>,
    session_timeout: Option<Duration>,
}

impl Builder<PhantomData<MetadataResponse>, PhantomData<Vec<String>>> {
    fn new(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            group_id,
            topics: PhantomData,
            metadata: PhantomData,
            on_assignment: None,
            rebalance_timeout: None,
            session_timeout: None,
        }
    }
}

impl<M, T> Builder<M, T> {
    pub fn topics(
        self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Builder<M, Vec<String>> {
        let Self {
            group_id,
            metadata,
            on_assignment,
            rebalance_timeout,
            session_timeout,
            ..
        } = self;

        Builder {
            group_id,
            topics: topics.into_iter().map(Into::into).collect(),
            metadata,
            on_assignment,
            rebalance_timeout,
            session_timeout,
        }
    }

    pub fn metadata(self, metadata: MetadataResponse) -> Builder<MetadataResponse, T> {
        let Self {
            group_id,
            topics,
            on_assignment,
            rebalance_timeout,
            session_timeout,
            ..
        } = self;

        Builder {
            group_id,
            topics,
            metadata,
            on_assignment,
            rebalance_timeout,
            session_timeout,
        }
    }

    pub fn rebalance_timeout(mut self, rebalance_timeout: Option<Duration>) -> Self {
        self.rebalance_timeout = rebalance_timeout;
        self
    }

    pub fn session_timeout(mut self, session_timeout: Option<Duration>) -> Self {
        self.session_timeout = session_timeout;
        self
    }

    pub fn on_assignment(self, on_assignment: Option<Arc<DynConsumerAssignment>>) -> Builder<M, T> {
        Self {
            on_assignment,
            ..self
        }
    }
}

impl Builder<MetadataResponse, Vec<String>> {
    const REBALANCE_TIMEOUT_MS_DEFAULT: u64 = 60_000;
    const SESSION_TIMEOUT_MS_DEFAULT: u64 = 10_000;

    pub fn build(self) -> GroupConsumer {
        let rebalance = Duration::from_millis(Self::REBALANCE_TIMEOUT_MS_DEFAULT);
        let session = Duration::from_millis(Self::SESSION_TIMEOUT_MS_DEFAULT);
        let outsider = MemberState::Outsider {
            generation_id: None,
            member_id: None,
            member_assignment: None,
        };

        GroupConsumer {
            group_id: self.group_id,
            topics: self.topics,
            rebalance_timeout: self.rebalance_timeout.unwrap_or(rebalance),
            session_timeout: self.session_timeout.unwrap_or(session),
            member_state: outsider,
            metadata: self.metadata,
            on_assignment: self.on_assignment,
            errors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum MemberState {
    Outsider {
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
    },
    Follower {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
    Leader {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
}

impl MemberState {
    fn is_outsider(&self) -> bool {
        matches!(self, Self::Outsider { .. })
    }

    fn is_follower(&self) -> bool {
        matches!(self, Self::Follower { .. })
    }

    fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    fn generation_id(&self) -> Option<i32> {
        match self {
            Self::Outsider { generation_id, .. } => *generation_id,
            Self::Follower { generation_id, .. } | Self::Leader { generation_id, .. } => {
                Some(*generation_id)
            }
        }
    }

    fn member_id(&self) -> Option<&str> {
        match self {
            Self::Outsider { member_id, .. } => member_id.as_deref(),
            Self::Follower { member_id, .. } | Self::Leader { member_id, .. } => Some(member_id),
        }
    }

    fn member_assignment(&self) -> Option<&MemberAssignment> {
        let (Self::Outsider {
            member_assignment, ..
        }
        | Self::Follower {
            member_assignment, ..
        }
        | Self::Leader {
            member_assignment, ..
        }) = self;
        member_assignment.as_ref()
    }
}

pub struct GroupConsumer {
    pub group_id: String,
    pub topics: Vec<String>,
    pub rebalance_timeout: Duration,
    pub session_timeout: Duration,
    member_state: MemberState,
    metadata: MetadataResponse,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
    errors: Arc<Mutex<BTreeMap<ErrorCode, u64>>>,
}

impl GroupConsumer {
    pub fn builder(
        group_id: impl Into<String>,
    ) -> Builder<PhantomData<MetadataResponse>, PhantomData<Vec<String>>> {
        Builder::new(group_id)
    }

    pub fn is_outsider(&self) -> bool {
        self.member_state.is_outsider()
    }

    pub fn is_follower(&self) -> bool {
        self.member_state.is_follower()
    }

    pub fn is_leader(&self) -> bool {
        self.member_state.is_leader()
    }

    pub fn generation_id(&self) -> Option<i32> {
        self.member_state.generation_id()
    }

    pub fn member_id(&self) -> Option<&str> {
        self.member_state.member_id()
    }

    pub fn member_assignment(&self) -> Option<&MemberAssignment> {
        self.member_state.member_assignment()
    }

    pub fn rebalance_timeout(&self) -> Duration {
        self.rebalance_timeout
    }

    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    pub fn errors(&self) -> Result<BTreeMap<ErrorCode, u64>, Error> {
        let errors = self.errors.lock()?;
        Ok(errors.clone())
    }
}

/// Whether a response's `error_code` is `expected`.
fn is(error_code: i16, expected: ErrorCode) -> bool {
    error_code == i16::from(expected)
}

impl GroupConsumer {
    const ASSIGNORS: &[Assignor] = &[
        Assignor::Range,
        Assignor::RoundRobin,
        Assignor::Uniform,
        Assignor::CooperativeSticky,
    ];

    fn join_protocol(
        &self,
        assignor: &Assignor,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<JoinGroupRequestProtocol, Error> {
        let version = member_assignment.map_or(ConsumerProtocolSubscription::V3, |assigned| {
            assigned.version
        });
        let owned = member_assignment
            .map(|assigned| assigned.assignment.assigned_partitions.clone())
            .unwrap_or_default();

        let subscription = ConsumerProtocolSubscription::default()
            .topics(self.topics.clone())
            .owned_partitions(owned.into_iter())
            .generation_id(Some(generation_id.unwrap_or(-1)));
        let metadata = MemberMetadata::default()
            .version(version)
            .subscription(subscription);

        Ok(JoinGroupRequestProtocol::default()
            .name(String::from(assignor.as_ref()))
            .metadata(Bytes::try_from(&metadata)?))
    }

    fn join_protocols(
        &self,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<Vec<JoinGroupRequestProtocol>, Error> {
        let protocol =
            |assignor: &Assignor| self.join_protocol(assignor, generation_id, member_assignment);
        Self::ASSIGNORS.iter().map(protocol).collect()
    }

    fn join_request(
        &self,
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
        reason: &str,
    ) -> Result<JoinGroupRequest, Error> {
        let protocols = self.join_protocols(generation_id, member_assignment.as_ref())?;
        let rebalance_timeout_ms = self.rebalance_timeout.as_millis() as i32;

        Ok(JoinGroupRequest::default()
            .group_id(self.group_id.clone())
            .protocol_type(String::from(CONSUMER))
            .rebalance_timeout_ms(Some(rebalance_timeout_ms))
            .session_timeout_ms(self.session_timeout.as_millis() as i32)
            .member_id(member_id.unwrap_or_default())
            .reason(Some(String::from(reason)))
            .protocols(Some(protocols)))
    }

    /// One more response with `error_code`.
    fn count(&self, error_code: ErrorCode) -> Result<(), Error> {
        let mut errors = self.errors.lock()?;
        *errors.entry(error_code).or_default() += 1;
        Ok(())
    }

    /// Back outside the group with what it still knows, asking to join.
    fn rejoin(
        &mut self,
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
        reason: &str,
    ) -> Result<Body, Error> {
        self.member_state = MemberState::Outsider {
            generation_id,
            member_id: member_id.clone(),
            member_assignment: member_assignment.clone(),
        };
        let request = self.join_request(generation_id, member_id, member_assignment, reason)?;
        Ok(request.into())
    }

    fn heartbeat(&self, generation_id: i32, member_id: String) -> Body {
        HeartbeatRequest::default()
            .group_id(self.group_id.clone())
            .generation_id(generation_id)
            .member_id(member_id)
            .into()
    }

    /// The leader's sync: each member's assignment, by the protocol the
    /// coordinator chose, or [`RangeAssignor`] when it is not one of ours.
    fn lead(&mut self, join: JoinGroupResponse) -> Result<Body, Error> {
        let JoinGroupResponse {
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            member_id,
            members,
            ..
        } = join;

        self.member_state = MemberState::Leader {
            generation_id,
            member_id,
            member_assignment: None,
        };
        debug!(?self.member_state);

        let chosen = protocol_name.as_deref().map(str::parse::<Assignor>);
        let assignor: Box<dyn ConsumerAssignor> = match chosen {
            Some(Ok(assignor)) => Box::new(assignor),
            _ => Box::new(RangeAssignor),
        };
        let assignments =
            assignor.assign(members.as_deref().unwrap_or_default(), &self.metadata)?;
        for assignment in &assignments {
            let decoded = MemberAssignment::try_from(assignment.assignment.clone()).ok();
            debug!(member_id = assignment.member_id, assignment = ?decoded);
        }

        Ok(SyncGroupRequest::default()
            .generation_id(generation_id)
            .group_id(self.group_id.clone())
            .member_id(leader)
            .assignments(Some(assignments))
            .protocol_name(protocol_name)
            .protocol_type(protocol_type)
            .into())
    }

    /// A follower's sync, which carries no assignments.
    fn follow(&mut self, join: JoinGroupResponse) -> Body {
        let JoinGroupResponse {
            generation_id,
            protocol_type,
            protocol_name,
            member_id,
            ..
        } = join;

        self.member_state = MemberState::Follower {
            generation_id,
            member_id: member_id.clone(),
            member_assignment: None,
        };
        debug!(?self.member_state);

        SyncGroupRequest::default()
            .generation_id(generation_id)
            .group_id(self.group_id.clone())
            .member_id(member_id)
            .assignments(Some(Vec::new()))
            .protocol_name(protocol_name)
            .protocol_type(protocol_type)
            .into()
    }

    /// The assignment the coordinator synced, handed to `on_assignment`.
    fn assigned(&mut self, assignment: Bytes) -> Result<Body, Error> {
        let (Some(generation_id), Some(member_id)) =
            (self.generation_id(), self.member_id().map(str::to_owned))
        else {
            unreachable!("only a leader or a follower is synced")
        };
        assert!(!assignment.is_empty(), "member: {member_id}");

        let decoded = MemberAssignment::try_from(assignment)?;
        debug!(%decoded);
        if let MemberState::Leader {
            member_assignment, ..
        }
        | MemberState::Follower {
            member_assignment, ..
        } = &mut self.member_state
        {
            *member_assignment = Some(decoded.clone());
        }

        if let Some(on_assignment) = self.on_assignment.as_ref() {
            on_assignment(self.group_id.clone(), decoded)
        }
        Ok(self.heartbeat(generation_id, member_id))
    }

    #[instrument(skip(self, input), fields(group_id = self.group_id, member_state = ?self.member_state), ret)]
    pub fn next_action(&mut self, input: Option<Body>) -> Result<Body, Error> {
        use MemberState::{Follower, Leader, Outsider};

        match (self.member_state.clone(), input) {
            (
                Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                None,
            ) => self.rejoin(generation_id, member_id, member_assignment, "join"),

            (
                Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::JoinGroupResponse(join)),
            ) if is(join.error_code, ErrorCode::NotCoordinator) => {
                self.count(ErrorCode::NotCoordinator)?;
                self.rejoin(generation_id, member_id, member_assignment, "join")
            }

            (
                Outsider {
                    member_assignment, ..
                },
                Some(Body::JoinGroupResponse(join)),
            ) if is(join.error_code, ErrorCode::MemberIdRequired) => {
                self.count(ErrorCode::MemberIdRequired)?;
                let (generation_id, member_id) = (Some(join.generation_id), Some(join.member_id));
                let reason = "join member id required";
                self.rejoin(generation_id, member_id, member_assignment, reason)
            }

            (Outsider { .. }, Some(Body::JoinGroupResponse(join)))
                if is(join.error_code, ErrorCode::None) && join.leader == join.member_id =>
            {
                self.count(ErrorCode::None)?;
                self.lead(join)
            }

            (Outsider { .. }, Some(Body::JoinGroupResponse(join)))
                if is(join.error_code, ErrorCode::None) =>
            {
                self.count(ErrorCode::None)?;
                Ok(self.follow(join))
            }

            (Leader { .. } | Follower { .. }, Some(Body::SyncGroupResponse(sync)))
                if is(sync.error_code, ErrorCode::None) =>
            {
                self.count(ErrorCode::None)?;
                self.assigned(sync.assignment)
            }

            (
                Leader {
                    member_id,
                    member_assignment,
                    ..
                }
                | Follower {
                    member_id,
                    member_assignment,
                    ..
                },
                Some(Body::SyncGroupResponse(sync)),
            ) if is(sync.error_code, ErrorCode::RebalanceInProgress) => {
                self.count(ErrorCode::RebalanceInProgress)?;
                assert!(sync.assignment.is_empty());
                let reason = "join after sync with rebalance";
                self.rejoin(None, Some(member_id), member_assignment, reason)
            }

            (Leader { .. } | Follower { .. }, Some(Body::HeartbeatResponse(heartbeat)))
                if is(heartbeat.error_code, ErrorCode::UnknownMemberId) =>
            {
                self.count(ErrorCode::UnknownMemberId)?;
                self.rejoin(None, None, None, "join after unknown member id")
            }

            (
                Leader {
                    generation_id,
                    member_id,
                    member_assignment,
                }
                | Follower {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::HeartbeatResponse(heartbeat)),
            ) if is(heartbeat.error_code, ErrorCode::RebalanceInProgress) => {
                self.count(ErrorCode::RebalanceInProgress)?;
                let (generation_id, member_id) = (Some(generation_id), Some(member_id));
                let reason = "join after rebalance";
                self.rejoin(generation_id, member_id, member_assignment, reason)
            }

            (
                Leader {
                    generation_id,
                    member_id,
                    ..
                }
                | Follower {
                    generation_id,
                    member_id,
                    ..
                },
                Some(Body::HeartbeatResponse(heartbeat)),
            ) if is(heartbeat.error_code, ErrorCode::None) => {
                self.count(ErrorCode::None)?;
                Ok(self.heartbeat(generation_id, member_id))
            }

            (state, input) => todo!("state: {state:?}, input: {input:?}"),
        }
    }
}
