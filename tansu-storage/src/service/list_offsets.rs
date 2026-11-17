// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use rama::Context;
use rama::Service;
use tansu_sans_io::list_offsets_response::ListOffsetsPartitionResponse;
use tansu_sans_io::list_offsets_response::ListOffsetsTopicResponse;
use tansu_sans_io::{ApiKey, IsolationLevel, ListOffset};
use tansu_sans_io::{ListOffsetsRequest, ListOffsetsResponse};

use crate::ListOffsetResponse;
use crate::Topition;
use crate::{Error, Result, Storage};

/// One partition's answer, with `-1` for a timestamp or offset the engine has none of.
fn partition_answer(
    topition: &Topition,
    listed: &ListOffsetResponse,
) -> ListOffsetsPartitionResponse {
    let at = listed.timestamp().ok().flatten().unwrap_or(-1);
    let answer = ListOffsetsPartitionResponse::default().partition_index(topition.partition());
    let answer = answer.error_code(i16::from(listed.error_code()));
    let answer = answer.old_style_offsets(None).timestamp(Some(at));
    // `-1`, not `0`, when the storage layer has no offset to give (#444). It
    // only says `None` for a timestamp query with no record at or after the
    // target — EARLIEST and LATEST always answer — and Kafka's sentinel for
    // that is `-1`, "no such offset". `0` is a valid position meaning the very
    // beginning, so `offsetsForTimes(now + 60s)` turned "resume from later"
    // into "replay the entire topic". The `timestamp` field beside it already
    // answered `-1`.
    let answer = answer.offset(Some(listed.offset().unwrap_or(-1)));
    answer.leader_epoch(Some(0))
}

/// The engine's answers grouped by topic, topics in name order and each
/// topic's partitions in the order the engine answered them.
fn by_topic(listed: &[(Topition, ListOffsetResponse)]) -> Vec<ListOffsetsTopicResponse> {
    let mut names: Vec<&str> = listed
        .iter()
        .map(|(topition, _)| topition.topic())
        .collect();
    names.sort_unstable();
    names.dedup();

    let topic_answer = |name: &str| {
        let ours = listed
            .iter()
            .filter(|(topition, _)| topition.topic() == name);
        let partitions = ours.map(|(topition, offset)| partition_answer(topition, offset));
        let answer = ListOffsetsTopicResponse::default().name(name.to_owned());
        answer.partitions(Some(partitions.collect()))
    };
    names.into_iter().map(topic_answer).collect()
}

/// Lists the earliest, latest or by-time offset of each partition asked about,
/// read uncommitted unless the request says otherwise.
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::ListOffset;
/// use tansu_sans_io::list_offsets_request::ListOffsetsPartition;
/// use tansu_sans_io::list_offsets_request::ListOffsetsTopic;
/// use tansu_storage::{ListOffsetsService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let listing = MapStateLayer::new(|_| engine).into_layer(ListOffsetsService);
///
/// let earliest = ListOffsetsPartition::default().partition_index(0);
/// let earliest = earliest.current_leader_epoch(Some(-1)).max_num_offsets(Some(3));
/// let earliest = earliest.timestamp(ListOffset::Earliest.try_into()?);
/// let topic = ListOffsetsTopic::default().name(String::from("abcba"));
/// let topic = topic.partitions(Some(vec![earliest]));
/// let read_uncommitted = Some(i8::from(tansu_sans_io::IsolationLevel::ReadUncommitted));
/// let asked = tansu_sans_io::ListOffsetsRequest::default().isolation_level(read_uncommitted);
/// let asked = asked.replica_id(111).topics(Some(vec![topic]));
/// let answered = listing.serve(Context::default(), asked).await?;
///
/// let [listed] = answered.topics.as_deref().unwrap_or_default() else {
///     panic!("one topic asked, one listed")
/// };
/// assert_eq!("abcba", listed.name);
/// let [partition] = listed.partitions.as_deref().unwrap_or_default() else {
///     panic!("one partition asked, one listed")
/// };
/// assert_eq!((0, 0), (partition.partition_index, partition.error_code));
/// assert_eq!(None, partition.old_style_offsets);
/// let position = (partition.timestamp, partition.offset, partition.leader_epoch);
/// assert_eq!((Some(-1), Some(0), Some(0)), position);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetsService;

impl ApiKey for ListOffsetsService {
    const KEY: i16 = ListOffsetsRequest::KEY;
}

impl<G> Service<G, ListOffsetsRequest> for ListOffsetsService
where
    G: Storage,
{
    type Response = ListOffsetsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: ListOffsetsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let isolation = match request.isolation_level {
            Some(level) => IsolationLevel::try_from(level)?,
            None => IsolationLevel::ReadUncommitted,
        };

        let mut topics = None;
        if let Some(asked) = request.topics {
            let mut wanted = Vec::new();
            for topic in &asked {
                for partition in topic.partitions.iter().flatten() {
                    let at = ListOffset::try_from(partition.timestamp)?;
                    let topition = Topition::new(topic.name.clone(), partition.partition_index);
                    wanted.push((topition, at));
                }
            }

            let listing = state.state().list_offsets(isolation, &wanted);
            let failed = |err: &Error| tracing::error!(?err, offsets = ?wanted);
            let listed = listing.await.inspect_err(failed)?;
            tracing::debug!(r = ?listed, offsets = ?wanted);
            topics = Some(by_topic(&listed));
        }

        let answer = ListOffsetsResponse::default().throttle_time_ms(Some(0));
        let answer = answer.topics(topics);
        tracing::debug!(r = ?answer);
        Ok(answer)
    }
}
