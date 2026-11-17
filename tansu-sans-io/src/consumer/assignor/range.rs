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

use std::collections::BTreeSet;

use bytes::Bytes;
use tracing::debug;

use crate::Error;
use crate::MetadataResponse;
use crate::consumer::{ConsumerAssignor, ConsumerProtocolAssignment};
use crate::consumer::{MemberAssignment, MemberMetadata, TopicPartition};
use crate::join_group_response::JoinGroupResponseMember;
use crate::sync_group_request::SyncGroupRequestAssignment;

/// Kafka's `range` assignor: each topic's partitions, in index order, split
/// into contiguous runs among the members subscribed to it, in member id
/// order, the first members taking one extra each when they do not divide.
#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct RangeAssignor;

impl ConsumerAssignor for RangeAssignor {
    #[tracing::instrument(skip(members, metadata))]
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let mut by_id: Vec<&JoinGroupResponseMember> = members.iter().collect();
        by_id.sort_by_key(|member| member.member_id.as_str());

        let mut subscriptions: Vec<BTreeSet<String>> = Vec::with_capacity(by_id.len());
        for member in &by_id {
            let decoded = MemberMetadata::try_from(member.metadata.clone())?;
            debug!(mm = ?decoded);
            subscriptions.push(decoded.subscription.topics.into_iter().collect());
        }

        let mut owned: Vec<Vec<TopicPartition>> = vec![Vec::new(); by_id.len()];
        for listed in metadata.topics.as_deref().unwrap_or_default() {
            let Some(name) = listed.name.as_deref() else {
                continue;
            };
            let subscribed = |at: &usize| subscriptions[*at].contains(name);
            let takers: Vec<usize> = (0..by_id.len()).filter(subscribed).collect();
            if takers.is_empty() {
                continue;
            }

            let known = listed.partitions.iter().flatten();
            let unique: BTreeSet<i32> = known.map(|p| p.partition_index).collect();
            let indexes: Vec<i32> = unique.into_iter().collect();
            let (each, extra) = (indexes.len() / takers.len(), indexes.len() % takers.len());
            let mut next = 0;
            for (rank, &member) in takers.iter().enumerate() {
                let count = each + usize::from(rank < extra);
                if count > 0 {
                    let partitions = indexes[next..next + count].to_vec();
                    next += count;
                    let id = &by_id[member].member_id;
                    debug!(member_id = id, topic_name = name, assigned = ?partitions);
                    let topic = name.to_owned();
                    owned[member].push(TopicPartition { topic, partitions });
                }
            }
        }

        let mut answers = Vec::with_capacity(by_id.len());
        for (member, runs) in by_id.iter().zip(owned) {
            let protocol =
                ConsumerProtocolAssignment::default().assigned_partitions(runs.into_iter());
            let assignment = MemberAssignment::default().version(3).assignment(protocol);
            let encoded = Bytes::try_from(&assignment)?;
            let answer = SyncGroupRequestAssignment::default().member_id(member.member_id.clone());
            answers.push(answer.assignment(encoded));
        }
        Ok(answers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerProtocolSubscription;
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
        for given in RangeAssignor.assign(joined, &metadata)? {
            let assignment = MemberAssignment::try_from(given.assignment)?;
            decoded.push((given.member_id, assignment.assignment.assigned_partitions));
        }
        Ok(decoded)
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

    /// Two members, two three-partition topics: the first takes the extra
    /// partition of each, `[0, 1]`, and the second `[2]`.
    #[test]
    fn the_first_members_take_the_partitions_left_over() -> Result<(), Error> {
        let both = ["t0", "t1"];
        let joined = [member("C0", &both)?, member("C1", &both)?];
        let expected: Assigned = vec![
            (
                String::from("C0"),
                vec![owned("t0", &[0, 1]), owned("t1", &[0, 1])],
            ),
            (
                String::from("C1"),
                vec![owned("t0", &[2]), owned("t1", &[2])],
            ),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 3), topic("t1", 3)])?
        );
        Ok(())
    }

    /// Each topic is split by its own partition count: t0's four go two and
    /// two, t1's two go one and one. Splitting the union of every topic's
    /// indexes instead would hand the second member t1 partitions 2 and 3,
    /// which do not exist.
    #[test]
    fn each_topic_is_split_by_its_own_partition_count() -> Result<(), Error> {
        let both = ["t0", "t1"];
        let joined = [member("C0", &both)?, member("C1", &both)?];
        let expected: Assigned = vec![
            (
                String::from("C0"),
                vec![owned("t0", &[0, 1]), owned("t1", &[0])],
            ),
            (
                String::from("C1"),
                vec![owned("t0", &[2, 3]), owned("t1", &[1])],
            ),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 4), topic("t1", 2)])?
        );
        Ok(())
    }

    /// A topic is split among its own subscribers only: t1, which only the
    /// first member asked for, goes to it whole.
    #[test]
    fn a_topic_is_split_among_its_own_subscribers_only() -> Result<(), Error> {
        let joined = [member("C0", &["t0", "t1"])?, member("C1", &["t0"])?];
        let expected: Assigned = vec![
            (
                String::from("C0"),
                vec![owned("t0", &[0, 1]), owned("t1", &[0, 1])],
            ),
            (String::from("C1"), vec![owned("t0", &[2, 3])]),
        ];
        assert_eq!(
            expected,
            assigned(&joined, vec![topic("t0", 4), topic("t1", 2)])?
        );
        Ok(())
    }
}
