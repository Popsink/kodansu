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

//! Lists the earliest or latest offset of every partition of a topic.

use clap::Parser as _;
use tansu_client::Error;
use tansu_sans_io::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use tansu_sans_io::metadata_response::MetadataResponseTopic;
use tansu_sans_io::{IsolationLevel, ListOffset, ListOffsetsRequest};

mod common;

/// List Offsets
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, default_value = common::BROKER)]
    broker: url::Url,

    #[arg(id = "topic", value_name = "TOPIC")]
    name: String,

    #[arg(long = "isolation", value_name = "ISOLATION")]
    #[arg(default_value = "read-uncommitted")]
    level: Level,

    #[arg(long = "offset", value_name = "OFFSET", default_value = "earliest")]
    from: Start,

    #[arg(long = "replica-id", value_name = "REPLICA_ID", default_value = "-1")]
    replica: i32,

    #[arg(long = "max-num-offsets", value_name = "MAX_NUM_OFFSETS")]
    most: Option<i32>,

    #[arg(long = "current-leader-epoch", value_name = "CURRENT_LEADER_EPOCH")]
    #[arg(default_value = "-1")]
    leader_epoch: i32,
}

/// `--isolation`: whether offsets past the last stable one are listed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
enum Level {
    #[value(name = "read-uncommitted")]
    Uncommitted,
    #[value(name = "read-committed")]
    Committed,
}

impl Level {
    fn isolation(self) -> IsolationLevel {
        match self {
            Self::Uncommitted => IsolationLevel::ReadUncommitted,
            Self::Committed => IsolationLevel::ReadCommitted,
        }
    }
}

/// `--offset`: which end of each partition to list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
enum Start {
    #[value(name = "earliest")]
    First,
    #[value(name = "latest")]
    Last,
}

impl Start {
    /// The sentinel timestamp `ListOffsets` reads as this end.
    fn timestamp(self) -> Result<i64, Error> {
        let end = match self {
            Self::First => ListOffset::Earliest,
            Self::Last => ListOffset::Latest,
        };
        i64::try_from(end).map_err(Into::into)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    common::start();
    let options = Options::parse();
    let client = common::connect(options.broker).await?;

    let lookup = common::metadata(Some(vec![common::named(options.name.clone())]));
    let topics = client.call(lookup).await?.topics.unwrap_or_default();
    let max_index = |t: &MetadataResponseTopic| {
        let partitions = t.partitions.as_deref()?;
        partitions.iter().map(|p| p.partition_index).max()
    };
    let highest = common::called(&topics, &options.name).find_map(max_index);
    let highest = highest.expect("topic not found in metadata");
    let timestamp = options.from.timestamp()?;

    let partition = |index| {
        let partition = ListOffsetsPartition::default().partition_index(index);
        let partition = partition.max_num_offsets(options.most).timestamp(timestamp);
        partition.current_leader_epoch(Some(options.leader_epoch))
    };
    let partitions = (0..highest).map(partition).collect();
    let topic = ListOffsetsTopic::default().name(options.name);
    let topic = topic.partitions(Some(partitions));
    let isolation = Some(options.level.isolation().into());
    let request = ListOffsetsRequest::default().isolation_level(isolation);
    let request = request
        .replica_id(options.replica)
        .topics(Some(vec![topic]));
    let response = client.call(request).await?;
    println!("{response:?}");
    Ok(())
}
