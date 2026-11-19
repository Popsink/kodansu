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

//! `ListPartitionReassignments`: always nothing in flight.

use tansu_sans_io::list_partition_reassignments_response::OngoingPartitionReassignment;
use tansu_sans_io::list_partition_reassignments_response::OngoingTopicReassignment;
use tansu_sans_io::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use tansu_sans_io::{ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse};

use crate::TopicId;

storage_service! {
    /// Reports each requested topic (all of them if none are named) with its
    /// current replicas and empty adding and removing lists. Replicas never
    /// move, so a reassignment is never in progress, but a client polling for
    /// one still needs to see the topic.
    ListPartitionReassignmentsService:
        ListPartitionReassignmentsRequest => ListPartitionReassignmentsResponse,
    |ctx, req| {
        let wanted: Option<Vec<TopicId>> = req
            .topics
            .map(|requested| requested.into_iter().map(|topic| topic.name.into()).collect());
        let metadata = ctx.state().metadata(wanted.as_deref()).await?;
        let topics: Vec<_> = metadata.topics().iter().filter_map(settled).collect();
        Ok(ListPartitionReassignmentsResponse::default().topics(topics.into()))
    }
}

/// A named topic with no reassignment under way; `None` for a topic without
/// a name, which the response has no way to identify.
fn settled(topic: &MetadataResponseTopic) -> Option<OngoingTopicReassignment> {
    let name = topic.name.clone()?;
    let partitions = topic
        .partitions
        .as_deref()
        .map(|partitions| partitions.iter().map(unmoved).collect());
    let settled = OngoingTopicReassignment::default().name(name);
    Some(settled.partitions(partitions))
}

/// A partition staying on the replicas it has.
fn unmoved(partition: &MetadataResponsePartition) -> OngoingPartitionReassignment {
    let MetadataResponsePartition {
        partition_index,
        replica_nodes,
        ..
    } = partition;
    let reassignment = OngoingPartitionReassignment::default();
    reassignment
        .partition_index(*partition_index)
        .replicas(replica_nodes.clone())
        .adding_replicas(Some(Vec::new()))
        .removing_replicas(Some(Vec::new()))
}
