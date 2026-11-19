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

use super::round_robin::RoundRobinAssignor;

/// Kafka's `cooperative-sticky` assignor, which for now hands out exactly what
/// [`RoundRobinAssignor`] does.
///
/// Still to do is the incremental rebalance of KIP-429: reading each member's
/// `owned_partitions` from its `ConsumerProtocolSubscription`, keeping the
/// assignments that are already balanced, and moving only the partitions that
/// have to be revoked.
#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct CooperativeStickyAssignor;

impl ConsumerAssignor for CooperativeStickyAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let delegate = RoundRobinAssignor;
        delegate.assign(members, metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::CooperativeStickyAssignor;
    use crate::consumer::ConsumerAssignor as _;
    use crate::consumer::{ConsumerProtocolSubscription, MemberAssignment};
    use crate::consumer::{MemberMetadata, TopicPartition};
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

    /// `topic`'s `indexes`, as an assignment lists them.
    fn owned(topic: &str, indexes: &[i32]) -> TopicPartition {
        let topic = topic.to_owned();
        let partitions = indexes.to_vec();
        TopicPartition { topic, partitions }
    }

    /// A group member called `id`, subscribed to `topics` and already owning
    /// `held`.
    fn member(
        id: &str,
        topics: &[&str],
        held: Vec<TopicPartition>,
    ) -> Result<JoinGroupResponseMember, Error> {
        let names = topics.iter().map(|name| (*name).to_owned());
        let subscription = ConsumerProtocolSubscription::default().topics(names);
        let subscription = subscription.owned_partitions(held.into_iter());
        let metadata = MemberMetadata::default()
            .version(3)
            .subscription(subscription);
        let encoded = bytes::Bytes::try_from(&metadata)?;
        let joined = JoinGroupResponseMember::default().member_id(id.to_owned());
        Ok(joined.metadata(encoded))
    }

    /// Each member's id and decoded partitions, in the order assigned.
    fn assigned(
        joined: &[JoinGroupResponseMember],
        cluster: Vec<MetadataResponseTopic>,
    ) -> Result<Assigned, Error> {
        let metadata = MetadataResponse::default().topics(Some(cluster));
        let mut decoded = Vec::new();
        for given in CooperativeStickyAssignor.assign(joined, &metadata)? {
            let assignment = MemberAssignment::try_from(given.assignment)?;
            decoded.push((given.member_id, assignment.assignment.assigned_partitions));
        }
        Ok(decoded)
    }

    /// Three members, one three-partition topic: one partition each, as the
    /// round robin hands them out.
    #[test]
    fn three_members_take_one_partition_each() -> Result<(), Error> {
        let joined = [
            member("C0", &["t0"], vec![])?,
            member("C1", &["t0"], vec![])?,
            member("C2", &["t0"], vec![])?,
        ];
        let one_each = |i: i32| (format!("C{i}"), vec![owned("t0", &[i])]);
        let expected: Assigned = (0..3).map(one_each).collect();
        assert_eq!(expected, assigned(&joined, vec![topic("t0", 3)])?);
        Ok(())
    }

    /// Two members, two three-partition topics: the round robin runs on from
    /// one topic into the next, so the first member takes t0's 0 and 2 and
    /// t1's 1.
    #[test]
    fn the_round_robin_runs_on_across_topics() -> Result<(), Error> {
        let both = ["t0", "t1"];
        let joined = [member("C0", &both, vec![])?, member("C1", &both, vec![])?];
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

    /// Members whose subscriptions say what they already own still decode,
    /// and get an assignment: two members owning one of two partitions each
    /// keep them, because that is also what the round robin hands out.
    #[test]
    fn owned_partitions_already_balanced_are_kept() -> Result<(), Error> {
        let joined = [
            member("C0", &["t0"], vec![owned("t0", &[0])])?,
            member("C1", &["t0"], vec![owned("t0", &[1])])?,
        ];
        let expected: Assigned = vec![
            (String::from("C0"), vec![owned("t0", &[0])]),
            (String::from("C1"), vec![owned("t0", &[1])]),
        ];
        assert_eq!(expected, assigned(&joined, vec![topic("t0", 2)])?);
        Ok(())
    }
}
