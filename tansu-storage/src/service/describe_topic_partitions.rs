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

//! `DescribeTopicPartitions`: topic metadata, paged by partition.

use tansu_sans_io::{DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse};

use crate::TopicId;

storage_service! {
    /// Passes the requested topics, the partition limit and the cursor to
    /// [`Storage::describe_topic_partitions`](crate::Storage::describe_topic_partitions).
    /// No topics means all of them. Every page fits in one response, so there
    /// is never a next cursor.
    ///
    /// `tests/describe_topic_partitions.rs` asks about an unknown topic.
    DescribeTopicPartitionsService:
        DescribeTopicPartitionsRequest => DescribeTopicPartitionsResponse,
    |ctx, req| {
        let DescribeTopicPartitionsRequest {
            topics,
            response_partition_limit,
            cursor,
            ..
        } = req;
        let wanted: Option<Vec<TopicId>> =
            topics.map(|requested| requested.iter().map(TopicId::from).collect());
        let cursor = cursor.map(Into::into);
        let described = ctx
            .state()
            .describe_topic_partitions(wanted.as_deref(), response_partition_limit, cursor)
            .await?;
        Ok(DescribeTopicPartitionsResponse::default().topics(described.into()))
    }
}
