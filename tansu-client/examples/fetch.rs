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

//! Fetches from one topic partition, from the offset given.

use clap::Parser as _;
use tansu_client::Error;
use tansu_sans_io::FetchRequest;
use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic};

mod common;

/// Fetch
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, default_value = common::BROKER)]
    #[arg(help = "The URL of the broker to fetch messages from")]
    broker: url::Url,

    #[arg(id = "topic", value_name = "TOPIC")]
    #[arg(help = "The topic to fetch messages from")]
    name: String,

    #[arg(long = "partition", value_name = "PARTITION", default_value = "0")]
    #[arg(help = "The partition to fetch messages from")]
    index: i32,

    #[arg(long = "max-wait-time-ms", value_name = "MAX_WAIT_TIME_MS")]
    #[arg(help = "The maximum time in milliseconds to wait for a message")]
    #[arg(default_value = "5000")]
    wait_ms: i32,

    #[arg(long = "min-bytes", value_name = "MIN_BYTES", default_value = "1")]
    #[arg(help = "The minimum number of bytes to wait for")]
    least: i32,

    #[arg(
        long = "max-bytes",
        value_name = "MAX_BYTES",
        default_value = "52428800"
    )]
    #[arg(help = "The maximum bytes to wait for")]
    most: Option<i32>,

    #[arg(
        long = "fetch-offset",
        value_name = "FETCH_OFFSET",
        default_value = "0"
    )]
    #[arg(help = "The fetch offset to start from")]
    from: i64,

    #[arg(long = "partition-max-bytes", value_name = "PARTITION_MAX_BYTES")]
    #[arg(help = "The partition to consume from", default_value = "1048576")]
    partition_limit: i32,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    common::start();
    let options = Options::parse();
    let client = common::connect(options.broker).await?;

    let lookup = common::metadata(Some(vec![common::named(options.name.clone())]));
    let topics = client.call(lookup).await?.topics.unwrap_or_default();
    let wanted = common::called(&topics, &options.name).next();
    let topic_id = wanted.and_then(|topic| topic.topic_id).expect("topic id");

    let partition = FetchPartition::default().partition(options.index);
    let partition = partition.fetch_offset(options.from);
    let partition = partition.current_leader_epoch(Some(0));
    let partition = partition.log_start_offset(Some(-1));
    let partition = partition.last_fetched_epoch(Some(-1));
    let partition = partition.partition_max_bytes(options.partition_limit);
    let partition = partition.replica_directory_id(None);
    let topic = FetchTopic::default().topic(None).topic_id(Some(topic_id));
    let topic = topic.partitions(Some(vec![partition]));

    let request = FetchRequest::default().cluster_id(None).replica_id(None);
    let request = request.replica_state(None).max_wait_ms(options.wait_ms);
    let request = request.min_bytes(options.least).max_bytes(options.most);
    let request = request.isolation_level(Some(0)).session_id(Some(-1));
    let request = request.session_epoch(Some(-1)).topics(Some(vec![topic]));
    let request = request.forgotten_topics_data(Some(vec![]));
    let response = client.call(request.rack_id(Some(String::new()))).await?;
    println!("{response:?}");
    Ok(())
}
