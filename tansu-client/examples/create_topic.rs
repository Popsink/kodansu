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

//! Creates a topic with the partitions and replication factor given.

use clap::Parser as _;
use tansu_client::Error;
use tansu_sans_io::CreateTopicsRequest;
use tansu_sans_io::create_topics_request::CreatableTopic;

mod common;

/// Create Topic
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, default_value = common::BROKER)]
    broker: url::Url,

    #[arg(id = "topic", value_name = "TOPIC")]
    name: String,

    #[arg(long = "num-partitions", value_name = "NUM_PARTITIONS")]
    #[arg(default_value = "3")]
    partitions: i32,

    #[arg(long = "replication-factor", value_name = "REPLICATION_FACTOR")]
    #[arg(default_value = "1")]
    replicas: i16,

    #[arg(long = "timeout", value_name = "TIMEOUT", default_value = "5000")]
    timeout_ms: i32,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    common::start();
    let options = Options::parse();
    let client = common::connect(options.broker).await?;

    let topic = CreatableTopic::default().name(options.name);
    let topic = topic.num_partitions(options.partitions);
    let topic = topic.replication_factor(options.replicas);
    let topic = topic.configs(Some(vec![])).assignments(Some(vec![]));
    let request = CreateTopicsRequest::default().topics(Some(vec![topic]));
    let request = request
        .timeout_ms(options.timeout_ms)
        .validate_only(Some(false));
    let response = client.call(request).await?;
    println!("{response:?}");
    Ok(())
}
