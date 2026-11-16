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

//! Joins a consumer group on a broker and stays a member, following the
//! coordinator through join, sync and heartbeat: `just group-consumer-example`.

use clap::Parser as _;
use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use tansu_client::{BytesConnectionService, Client, ConnectionManager};
use tansu_client::{ConsumerGroupLayer, FrameConnectionLayer, FramePoolLayer};
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::{CoordinatorType, FindCoordinatorRequest};
use tansu_sans_io::{MetadataRequest, NULL_TOPIC_ID};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

const BROKER: &str = "tcp://localhost:9092";

/// Group Consumer
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, help = "The URL of the broker", default_value = BROKER)]
    broker: url::Url,

    #[arg(long = "topics", value_name = "TOPICS")]
    #[arg(help = "The topics within the group")]
    topic: Vec<String>,

    #[arg(long = "group", value_name = "GROUP")]
    #[arg(help = "The group name", default_value = "test")]
    name: String,

    #[arg(long, default_value = "3s", value_parser = humantime::parse_duration)]
    heartbeat_interval: std::time::Duration,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    _ = dotenv::dotenv();
    let output = tracing_subscriber::fmt::layer().with_level(true);
    let output = output.with_line_number(true).with_thread_ids(false);
    let output = output.with_span_events(FmtSpan::NONE);
    let filtered = tracing_subscriber::registry().with(EnvFilter::from_default_env());
    filtered.with(output).init();

    let options = Options::parse();
    let client_id = Some(env!("CARGO_PKG_NAME").into());
    let pool = ConnectionManager::builder(options.broker).client_id(client_id);
    let pool = pool.build().await?;
    tracing::debug!(?pool);
    let client = Client::new(pool.clone());

    let group = options.name;
    let find = FindCoordinatorRequest::default().key(Some(group.clone()));
    let find = find.coordinator_keys(Some(vec![group.clone()]));
    let coordinator = client.call(find.key_type(Some(CoordinatorType::Group.into())));
    let coordinator = coordinator.await?;
    tracing::debug!(?coordinator);

    let named = |name: &String| MetadataRequestTopic::default().name(Some(name.clone()));
    let topics = options.topic.iter().map(named);
    let topics: Vec<_> = topics.map(|t| t.topic_id(Some(NULL_TOPIC_ID))).collect();
    let request = MetadataRequest::default().topics(Some(topics));
    let request = request.allow_auto_topic_creation(Some(false));
    let request = request.include_cluster_authorized_operations(Some(false));
    let request = request.include_topic_authorized_operations(Some(false));
    let metadata = client.call(request).await?;

    let wire =
        (FrameConnectionLayer, tansu_service::FrameBytesLayer).into_layer(BytesConnectionService);
    let member = ConsumerGroupLayer::new(group, options.topic, metadata);
    let member = (member, FramePoolLayer::new(pool)).into_layer(wire);
    let membership = member.serve(Context::default(), ());
    membership.await.map_err(Into::into)
}
