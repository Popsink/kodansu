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

//! `tansu topic` against a broker that answers.
//!
//! This crate was at 0% (#556). It is a thin shell over code that is covered
//! elsewhere, which is exactly why the useful test is not a unit test: what
//! can break here is the shell no longer delegating — a subcommand that builds
//! the wrong request, reads the wrong field of the response, or stops sending
//! one at all — and every one of those still type-checks.
//!
//! So each case drives the real `Topic::main` against an in-process broker
//! over `memory://`, and asserts the *effect* rather than the return: a
//! `create` that returns `None` but created nothing fails here, because the
//! second create would not come back `TopicAlreadyExists`.

use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, TcpListener as StdTcpListener},
};

use anyhow::Result;
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, ErrorCode,
    describe_configs_request::DescribeConfigsResource,
};
use tansu_storage::ArcDynStorage;
use tansu_topic::Topic;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

/// A port nothing else is on. Bound and released rather than guessed, which is
/// what makes a parallel test run deterministic.
fn free_port() -> Result<u16> {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .map_err(Into::into)
}

/// A broker serving `memory://` on a free port, and the URL to reach it.
///
/// The [`CancellationToken`] is returned rather than dropped: cancelling it is
/// what stops the accept loop, and a test that forgets leaves the task behind.
async fn broker() -> Result<(Url, CancellationToken)> {
    let port = free_port()?;
    let listener = Url::parse(&format!("tcp://{}:{port}", Ipv4Addr::LOCALHOST))?;
    let cancellation = CancellationToken::new();

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(Uuid::now_v7().to_string())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener.clone())
        .cancellation(cancellation.clone())
        .silent(true)
        .build()
        .await?;

    let serving = cancellation.clone();
    _ = tokio::spawn(async move {
        let _ = broker.serve(Instant::now()).await;
        drop(serving);
    });

    Ok((listener, cancellation))
}

fn topic_name() -> String {
    format!("t{}", Uuid::now_v7().simple())
}

#[tokio::test]
async fn create_is_visible_to_a_second_create() -> Result<()> {
    let (broker_url, cancellation) = broker().await?;
    let name = topic_name();

    let created = Topic::create()
        .broker(broker_url.clone())
        .name(name.clone())
        .partitions(3)
        .build()
        .main()
        .await?;

    assert_eq!(ErrorCode::None, created);

    // The assertion that a `create` returning `None` actually created
    // something. Without it this test passes against a subcommand that builds
    // a request the broker ignores — checked by flipping `validate_only`,
    // which fails here and nowhere else (#556).
    let again = Topic::create()
        .broker(broker_url)
        .name(name)
        .partitions(3)
        .build()
        .main()
        .await?;

    assert_eq!(ErrorCode::TopicAlreadyExists, again);

    cancellation.cancel();
    Ok(())
}

/// The configs have to be read back, not inferred from the return.
///
/// The first version of this asserted only `ErrorCode::None` and was vacuous:
/// deleting the whole `configs` map from `Create::main` still passes it,
/// because a create with no configs is just as valid. `DescribeConfigs` is
/// what distinguishes "the map reached the wire" from "the broker was happy".
#[tokio::test]
async fn create_carries_its_configs() -> Result<()> {
    let (broker_url, cancellation) = broker().await?;
    let name = topic_name();

    let created = Topic::create()
        .broker(broker_url.clone())
        .name(name.clone())
        .partitions(1)
        .config(
            [
                ("cleanup.policy".into(), "compact".into()),
                ("retention.ms".into(), "60000".into()),
            ]
            .into(),
        )
        .build()
        .main()
        .await?;

    assert_eq!(ErrorCode::None, created);

    let client = ConnectionManager::builder(broker_url)
        .client_id(Some("subcommands".into()))
        .build()
        .await
        .map(Client::new)?;

    let described = client
        .call(
            DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_type(ConfigResource::Topic.into())
                        .resource_name(name)
                        .configuration_keys(None)]
                    .into(),
                )),
        )
        .await?;

    let stored = described
        .results
        .unwrap_or_default()
        .into_iter()
        .flat_map(|result| result.configs.unwrap_or_default())
        .filter_map(|config| config.value.map(|value| (config.name, value)))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(Some(&"compact".to_owned()), stored.get("cleanup.policy"));
    assert_eq!(Some(&"60000".to_owned()), stored.get("retention.ms"));

    cancellation.cancel();
    Ok(())
}

#[tokio::test]
async fn delete_removes_the_topic_it_names() -> Result<()> {
    let (broker_url, cancellation) = broker().await?;
    let name = topic_name();

    assert_eq!(
        ErrorCode::None,
        Topic::create()
            .broker(broker_url.clone())
            .name(name.clone())
            .partitions(1)
            .build()
            .main()
            .await?
    );

    assert_eq!(
        ErrorCode::None,
        Topic::delete()
            .broker(broker_url.clone())
            .name(name.clone())
            .build()
            .main()
            .await?
    );

    // Deleted, not merely reported deleted: the name is free again (#556).
    assert_eq!(
        ErrorCode::None,
        Topic::create()
            .broker(broker_url)
            .name(name)
            .partitions(1)
            .build()
            .main()
            .await?
    );

    cancellation.cancel();
    Ok(())
}

#[tokio::test]
async fn delete_says_so_when_there_is_nothing_to_delete() -> Result<()> {
    let (broker_url, cancellation) = broker().await?;

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        Topic::delete()
            .broker(broker_url)
            .name(topic_name())
            .build()
            .main()
            .await?
    );

    cancellation.cancel();
    Ok(())
}

#[tokio::test]
async fn list_reaches_the_broker() -> Result<()> {
    let (broker_url, cancellation) = broker().await?;

    assert_eq!(
        ErrorCode::None,
        Topic::create()
            .broker(broker_url.clone())
            .name(topic_name())
            .partitions(1)
            .build()
            .main()
            .await?
    );

    // `List::main` prints its JSON and answers `None` whatever comes back, so
    // the only thing assertable in-process is that it made the call and the
    // call succeeded — the weakest of the seven, and #556 says why it is still
    // worth having: a `list` that stopped sending `MetadataRequest` errors
    // here.
    assert_eq!(
        ErrorCode::None,
        Topic::list().broker(broker_url).build().main().await?
    );

    cancellation.cancel();
    Ok(())
}

/// `start_paused` because the connection manager backs off between attempts,
/// and against a port nothing is listening on that is the whole runtime of the
/// test: 72 s on a real clock, under a second on a paused one, for the same
/// three assertions (#359's rig, #556).
#[tokio::test(start_paused = true)]
async fn a_broker_that_is_not_there_is_an_error_not_a_code() -> Result<()> {
    // The port is free, so nothing answers on it. Each subcommand has to
    // surface that as `Err` rather than an `ErrorCode`, which is the
    // difference between "the broker refused" and "there was no broker"
    // (#556).
    let nowhere = Url::parse(&format!("tcp://{}:{}", Ipv4Addr::LOCALHOST, free_port()?))?;

    assert!(
        Topic::create()
            .broker(nowhere.clone())
            .name(topic_name())
            .partitions(1)
            .build()
            .main()
            .await
            .is_err()
    );

    assert!(
        Topic::delete()
            .broker(nowhere.clone())
            .name(topic_name())
            .build()
            .main()
            .await
            .is_err()
    );

    assert!(Topic::list().broker(nowhere).build().main().await.is_err());

    Ok(())
}

/// The three `Error` members no subcommand can provoke from here.
///
/// `From<io::Error>` and `From<serde_json::Error>` wrap in an `Arc` because
/// neither source is `Clone` and every error type in this tree is
/// (`CLAUDE.md`), and `Display` is `{self:?}`. The invariant worth asserting is
/// that the wrapping does not lose the cause and that the result really does
/// clone — a `From` that dropped its source would still compile, and the
/// subcommand reporting it would print a variant name and nothing else.
#[test]
fn the_error_conversions_keep_their_cause_and_clone() {
    use std::io;

    let io: tansu_topic::Error = io::Error::new(io::ErrorKind::NotFound, "no such broker").into();
    let rendered = io.to_string();
    assert!(rendered.contains("no such broker"), "{rendered}");
    assert_eq!(rendered, io.clone().to_string());

    let json: tansu_topic::Error = serde_json::from_str::<serde_json::Value>("{")
        .unwrap_err()
        .into();
    let rendered = json.to_string();
    assert!(rendered.contains("SerdeJson"), "{rendered}");
    assert_eq!(rendered, json.clone().to_string());
}
