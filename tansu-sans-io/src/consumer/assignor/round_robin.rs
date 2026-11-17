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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::Error;
use crate::MetadataResponse;
use crate::consumer::{ConsumerAssignor, ConsumerProtocolAssignment};
use crate::consumer::{MemberAssignment, MemberMetadata, TopicPartition};
use crate::join_group_response::JoinGroupResponseMember;
use crate::sync_group_request::SyncGroupRequestAssignment;

use crate::consumer::assignor::Topition;

/// Kafka's `roundrobin` assignor: every partition of every topic, in topic and
/// then index order, dealt in turn to the members in member id order, a member
/// not subscribed to a partition's topic passing its turn to the next.
#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct RoundRobinAssignor;

impl ConsumerAssignor for RoundRobinAssignor {
    #[tracing::instrument(skip(members, metadata))]
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let mut by_id: Vec<&JoinGroupResponseMember> = members.iter().collect();
        by_id.sort_by_key(|member| member.member_id.as_str());
        let seats = by_id.len();

        let mut subscriptions: Vec<BTreeSet<String>> = Vec::with_capacity(seats);
        for member in &by_id {
            let decoded = MemberMetadata::try_from(member.metadata.clone())?;
            subscriptions.push(decoded.subscription.topics.into_iter().collect());
        }

        let mut owned: Vec<BTreeMap<String, Vec<i32>>> = vec![BTreeMap::new(); seats];
        let mut turn = 0;
        for Topition { topic, partition } in BTreeSet::<Topition>::from(metadata) {
            let wants = |at: &usize| subscriptions[at % seats].contains(&topic);
            let Some(taker) = (turn..turn + seats).find(wants) else {
                turn += seats;
                continue;
            };
            turn = taker + 1;
            owned[taker % seats]
                .entry(topic)
                .or_default()
                .push(partition);
        }

        let mut answers = Vec::with_capacity(seats);
        for (member, topics) in by_id.iter().zip(owned) {
            let runs = topics.into_iter();
            let runs = runs.map(|(topic, partitions)| TopicPartition { topic, partitions });
            let protocol = ConsumerProtocolAssignment::default().assigned_partitions(runs);
            let assignment = MemberAssignment::default().version(3).assignment(protocol);
            let encoded = bytes::Bytes::try_from(&assignment)?;
            let answer = SyncGroupRequestAssignment::default().member_id(member.member_id.clone());
            answers.push(answer.assignment(encoded));
        }
        Ok(answers)
    }
}

#[cfg(test)]
mod tests {
    use super::RoundRobinAssignor;
    use crate::consumer::{ConsumerAssignor as _, ConsumerProtocolSubscription};
    use crate::consumer::{MemberAssignment, MemberMetadata, TopicPartition};
    use crate::join_group_response::JoinGroupResponseMember;
    use crate::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
    use crate::{Error, MetadataResponse};

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
        let encoded = bytes::Bytes::try_from(&metadata)?;
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
        for given in RoundRobinAssignor.assign(joined, &metadata)? {
            let assignment = MemberAssignment::try_from(given.assignment)?;
            decoded.push((given.member_id, assignment.assignment.assigned_partitions));
        }
        Ok(decoded)
    }

    /// Three members, one three-partition topic: one partition each.
    #[test]
    fn three_members_take_one_partition_each() -> Result<(), Error> {
        let joined = [
            member("C0", &["t0"])?,
            member("C1", &["t0"])?,
            member("C2", &["t0"])?,
        ];
        let one_each = |i: i32| (format!("C{i}"), vec![owned("t0", &[i])]);
        let expected: Assigned = (0..3).map(one_each).collect();
        assert_eq!(expected, assigned(&joined, vec![topic("t0", 3)])?);
        Ok(())
    }

    /// Three members, two three-partition topics: one partition of each per
    /// member.
    #[test]
    fn three_members_take_one_partition_of_each_topic() -> Result<(), Error> {
        let both = ["t0", "t1"];
        let joined = [
            member("C0", &both)?,
            member("C1", &both)?,
            member("C2", &both)?,
        ];
        let one_each = |i: i32| vec![owned("t0", &[i]), owned("t1", &[i])];
        let expected: Assigned = (0..3).map(|i| (format!("C{i}"), one_each(i))).collect();
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 3), topic("t1", 3)])?
        );
        Ok(())
    }

    /// Two members, one three-partition topic: the deal comes back round to
    /// the first member for the third partition.
    #[test]
    fn the_deal_comes_back_round_to_the_first_member() -> Result<(), Error> {
        let joined = [member("C0", &["t0"])?, member("C1", &["t0"])?];
        let expected: Assigned = vec![
            (String::from("C0"), vec![owned("t0", &[0, 2])]),
            (String::from("C1"), vec![owned("t0", &[1])]),
        ];
        assert_eq!(expected, assigned(&joined, vec![topic("t0", 3)])?);
        Ok(())
    }

    /// Two members, two three-partition topics: the deal runs on from one
    /// topic into the next, so t1 starts with the second member.
    #[test]
    fn the_deal_runs_on_from_one_topic_into_the_next() -> Result<(), Error> {
        let both = ["t0", "t1"];
        let joined = [member("C0", &both)?, member("C1", &both)?];
        let expected: Assigned = vec![
            (
                String::from("C0"),
                vec![owned("t0", &[0, 2]), owned("t1", &[1])],
            ),
            (
                String::from("C1"),
                vec![owned("t0", &[1]), owned("t1", &[0, 2])],
            ),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 3), topic("t1", 3)])?
        );
        Ok(())
    }

    /// The second member asked for t1 only, so both of t0's partitions pass
    /// it by and go to the first; its turn still counts, so t1 starts with it.
    #[test]
    fn a_member_passes_its_turn_on_a_topic_it_did_not_ask_for() -> Result<(), Error> {
        let joined = [member("C0", &["t0", "t1"])?, member("C1", &["t1"])?];
        let expected: Assigned = vec![
            (
                String::from("C0"),
                vec![owned("t0", &[0, 1]), owned("t1", &[1])],
            ),
            (String::from("C1"), vec![owned("t1", &[0])]),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 2), topic("t1", 2)])?
        );
        Ok(())
    }
}
