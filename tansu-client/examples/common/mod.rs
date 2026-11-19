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

//! What the request examples share: a subscriber filtered by `RUST_LOG`, a
//! client for the broker, and the `Metadata` request they look topics up with.

#![allow(dead_code, reason = "each example uses part of this")]

use tansu_client::{Client, ConnectionManager, Error};
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::metadata_response::MetadataResponseTopic;
use tansu_sans_io::{MetadataRequest, NULL_TOPIC_ID};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

/// Where every example looks for the broker unless told otherwise.
pub(crate) const BROKER: &str = "tcp://localhost:9092";

/// Loads `.env`, then logs to the terminal at the levels `RUST_LOG` asks for.
pub(crate) fn start() {
    _ = dotenv::dotenv();
    let output = tracing_subscriber::fmt::layer().with_level(true);
    let output = output.with_line_number(true).with_thread_ids(false);
    let output = output.with_span_events(FmtSpan::NONE);
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    tracing_subscriber::registry()
        .with(filter)
        .with(output)
        .init();
}

/// A client for `broker`, identifying itself by this crate's name.
pub(crate) async fn connect(broker: url::Url) -> Result<Client, Error> {
    let client_id = Some(String::from(env!("CARGO_PKG_NAME")));
    let manager = ConnectionManager::builder(broker).client_id(client_id);
    manager.build().await.map(Client::new)
}

/// A topic of a `Metadata` request, asked for by name.
pub(crate) fn named(name: String) -> MetadataRequestTopic {
    let topic = MetadataRequestTopic::default().name(Some(name));
    topic.topic_id(Some(NULL_TOPIC_ID))
}

/// The topics of a `Metadata` response that go by `name`.
pub(crate) fn called<'a>(
    topics: &'a [MetadataResponseTopic],
    name: &'a str,
) -> impl Iterator<Item = &'a MetadataResponseTopic> {
    topics
        .iter()
        .filter(move |t| t.name.as_deref() == Some(name))
}

/// `Metadata` for `topics` (every topic when `None`), creating none and
/// asking for no authorized operations.
pub(crate) fn metadata(topics: Option<Vec<MetadataRequestTopic>>) -> MetadataRequest {
    let request = MetadataRequest::default().topics(topics);
    let request = request.allow_auto_topic_creation(Some(false));
    let request = request.include_cluster_authorized_operations(Some(false));
    request.include_topic_authorized_operations(Some(false))
}
