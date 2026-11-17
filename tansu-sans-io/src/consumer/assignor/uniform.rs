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

use crate::Error;
use crate::MetadataResponse;
use crate::consumer::ConsumerAssignor;
use crate::join_group_response::JoinGroupResponseMember;
use crate::sync_group_request::SyncGroupRequestAssignment;

use super::RoundRobinAssignor;

/// Kafka's `uniform` assignor, answered here with the round-robin assignment.
#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct UniformAssignor;

impl ConsumerAssignor for UniformAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        RoundRobinAssignor.assign(members, metadata)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::consumer::TopicPartition;
    use crate::consumer::{ConsumerProtocolSubscription, MemberAssignment, MemberMetadata};
    use crate::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};

    type Assigned = Vec<(String, Vec<TopicPartition>)>;

    /// A topic named `name` with partitions `0..count`.
    fn topic(name: &str, count: i32) -> MetadataResponseTopic {
        let partition = |index| MetadataResponsePartition::default().partition_index(index);
        let named = MetadataResponseTopic::default().name(Some(name.to_owned()));
        named.partitions(Some((0..count).map(partition).collect()))
    }

    /// A group member called `id`, subscribed to `topics`.
    fn member(id: &str, topics: &[&str]) -> Result<JoinGroupResponseMember, Error> {
        let names = topics.iter().map(|name| (*name).to_owned());
        let subscription = ConsumerProtocolSubscription::default().topics(names);
        let metadata = MemberMetadata::default()
            .version(3)
            .subscription(subscription);
        let encoded = Bytes::try_from(&metadata)?;
        let joined = JoinGroupResponseMember::default().member_id(id.to_owned());
        Ok(joined.metadata(encoded))
    }

    /// `topic`'s `indexes`, as an assignment lists them.
    fn owned(topic: &str, indexes: &[i32]) -> TopicPartition {
        let topic = topic.to_owned();
        let partitions = indexes.to_vec();
        TopicPartition { topic, partitions }
    }

    /// Each member's id and decoded partitions, in the order assigned.
    fn assigned(
        joined: &[JoinGroupResponseMember],
        cluster: Vec<MetadataResponseTopic>,
    ) -> Result<Assigned, Error> {
        let metadata = MetadataResponse::default().topics(Some(cluster));
        let mut decoded = Vec::new();
        for given in UniformAssignor.assign(joined, &metadata)? {
            let assignment = MemberAssignment::try_from(given.assignment)?;
            decoded.push((given.member_id, assignment.assignment.assigned_partitions));
        }
        Ok(decoded)
    }

    /// Three members, one three-partition topic: a partition each, in order —
    /// the round-robin assignor's own single-topic case.
    #[test]
    fn three_members_take_a_partition_each_of_one_topic() -> Result<(), Error> {
        let joined = [
            member("C0", &["t0"])?,
            member("C1", &["t0"])?,
            member("C2", &["t0"])?,
        ];
        let expected: Assigned = (0..3)
            .map(|i| (format!("C{i}"), vec![owned("t0", &[i])]))
            .collect();
        assert_eq!(expected, assigned(&joined, vec![topic("t0", 3)])?);
        Ok(())
    }

    /// A member subscribed to one topic of two is only ever given that one.
    #[test]
    fn a_member_is_given_only_what_it_subscribed_to() -> Result<(), Error> {
        let joined = [member("C0", &["t0", "t1"])?, member("C1", &["t1"])?];
        let both = vec![owned("t0", &[0, 1]), owned("t1", &[1])];
        let expected: Assigned = vec![
            (String::from("C0"), both),
            (String::from("C1"), vec![owned("t1", &[0])]),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 2), topic("t1", 2)])?
        );
        Ok(())
    }
}
