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

//! What the code generator read from the upstream Kafka message descriptors,
//! checked through [`MESSAGE_META`] for a handful of messages: their API key,
//! versions, nested structures and the shape of chosen fields.

use tansu_model::{FieldMeta, MessageKind, MessageMeta, VersionRange};
use tansu_sans_io::MESSAGE_META;

pub mod common;

fn message(name: &str) -> &'static MessageMeta {
    let found = MESSAGE_META.iter().find(|(key, _)| *key == name);
    found.map_or_else(|| panic!("no {name} in MESSAGE_META"), |(_, meta)| *meta)
}

/// The field at a dotted `path` (`results.configs.read_only`) of `message`.
fn field(message: &MessageMeta, path: &str) -> &'static FieldMeta {
    let mut names = path.split('.');
    let top = names.next().and_then(|name| message.field(name));
    let nested = names.fold(top, |parent, name| parent.and_then(|meta| meta.field(name)));
    nested.unwrap_or_else(|| panic!("no {path} in {}", message.name))
}

/// The names of every structure nested in `message`, sorted.
fn structure_names(message: &MessageMeta) -> Vec<&str> {
    let mut names: Vec<_> = message.structures().into_iter().map(|(n, _)| n).collect();
    names.sort_unstable();
    names
}

/// The element type of a sequence field, `None` when it is not one.
fn sequence_of(field: &FieldMeta) -> Option<&'static str> {
    field.kind.kind_of_sequence().map(|element| element.0)
}

/// Whether a field is a sequence, and whether it is a structure.
fn shape(field: &FieldMeta) -> (bool, bool) {
    (field.kind.is_sequence(), field.is_structure())
}

/// Present from version `start` onwards.
fn since(start: i16) -> VersionRange {
    VersionRange {
        start,
        end: i16::MAX,
    }
}

#[test]
fn add_partitions_to_txn_request() {
    let _guard = common::init_tracing().unwrap();
    let request = message("AddPartitionsToTxnRequest");
    assert_eq!(
        (24, MessageKind::Request),
        (request.api_key, request.message_kind)
    );
    assert_eq!(VersionRange { start: 0, end: 5 }, request.version.valid);
    assert_eq!(since(3), request.version.flexible);
    assert_eq!(since(4), field(request, "transactions").version);

    for leaf in [
        "transactional_id",
        "producer_id",
        "producer_epoch",
        "verify_only",
    ] {
        let path = format!("transactions.{leaf}");
        assert_eq!(0, field(request, &path).fields.len(), "{path}");
    }
    let topics = field(request, "transactions.topics").fields.iter();
    let mut members: Vec<_> = topics.map(|(name, _)| *name).collect();
    members.sort_unstable();
    assert_eq!(vec!["name", "partitions"], members);
}

#[test]
fn create_topics_request() {
    let _guard = common::init_tracing().unwrap();
    let request = message("CreateTopicsRequest");
    assert_eq!(
        (19, MessageKind::Request),
        (request.api_key, request.message_kind)
    );

    let nested = [
        "CreatableReplicaAssignment",
        "CreatableTopic",
        "CreatableTopicConfig",
    ];
    assert_eq!(nested.to_vec(), structure_names(request));

    let topics = field(request, "topics");
    assert_eq!((true, true), shape(topics));
    assert!(!topics.kind.is_primitive());
    assert_eq!(Some("CreatableTopic"), sequence_of(topics));

    for scalar in ["timeout_ms", "validate_only"] {
        assert_eq!((false, false), shape(field(request, scalar)), "{scalar}");
    }
}

#[test]
fn describe_configs_response() {
    let _guard = common::init_tracing().unwrap();
    let response = message("DescribeConfigsResponse");
    assert_eq!(
        (32, MessageKind::Response),
        (response.api_key, response.message_kind)
    );

    let nested = [
        "DescribeConfigsResourceResult",
        "DescribeConfigsResult",
        "DescribeConfigsSynonym",
    ];
    assert_eq!(nested.to_vec(), structure_names(response));

    let results = field(response, "results");
    assert_eq!(Some("DescribeConfigsResult"), sequence_of(results));
    let configs = field(response, "results.configs");
    assert_eq!(Some("DescribeConfigsResourceResult"), sequence_of(configs));

    let read_only = field(response, "results.configs.read_only");
    assert_eq!(("bool", since(0)), (read_only.kind.0, read_only.version));
    let is_default = field(response, "results.configs.is_default");
    let only_v0 = VersionRange { start: 0, end: 0 };
    assert_eq!(("bool", only_v0), (is_default.kind.0, is_default.version));
}

#[test]
fn describe_topic_partitions_request() {
    let _guard = common::init_tracing().unwrap();
    let request = message("DescribeTopicPartitionsRequest");
    assert_eq!(
        (75, MessageKind::Request),
        (request.api_key, request.message_kind)
    );
    assert_eq!(vec!["Cursor", "TopicRequest"], structure_names(request));

    let shapes = [
        ("topics", (true, true)),
        ("response_partition_limit", (false, false)),
    ];
    for (name, expected) in shapes.into_iter().chain([("cursor", (false, true))]) {
        assert_eq!(expected, shape(field(request, name)), "{name}");
    }
    assert!(field(request, "cursor").is_nullable(i16::MAX));
}

#[test]
fn fetch_request() {
    let _guard = common::init_tracing().unwrap();
    let request = message("FetchRequest");
    assert_eq!(
        (1, MessageKind::Request),
        (request.api_key, request.message_kind)
    );

    let nested = [
        "FetchPartition",
        "FetchTopic",
        "ForgottenTopic",
        "ReplicaState",
    ];
    assert_eq!(nested.to_vec(), structure_names(request));

    let forgotten = field(request, "forgotten_topics_data");
    assert_eq!(Some("ForgottenTopic"), sequence_of(forgotten));

    let partitions = field(request, "forgotten_topics_data.partitions");
    assert_eq!((true, false), shape(partitions));
    let element = partitions.kind.kind_of_sequence();
    assert_eq!(Some(true), element.map(|kind| kind.is_primitive()));
}

#[test]
fn find_coordinator_request() {
    let _guard = common::init_tracing().unwrap();
    let request = message("FindCoordinatorRequest");
    assert_eq!(
        (10, MessageKind::Request),
        (request.api_key, request.message_kind)
    );
    assert_eq!(VersionRange { start: 0, end: 6 }, request.version.valid);
    assert_eq!(
        VersionRange { start: 0, end: 3 },
        field(request, "key").version
    );

    for (optional, from) in [("key_type", 1), ("coordinator_keys", 4)] {
        let added = field(request, optional);
        assert_eq!(since(from), added.version, "{optional}");
        assert!(!added.is_mandatory(None), "{optional}");
    }
}
