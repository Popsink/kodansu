// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! What a topic may be called and how many partitions it may have (#443).
//!
//! Kafka refuses a malformed creation at the door with a precise error code;
//! this broker accepted them, so the mistake travelled. A name is not just a
//! label here — it is an object-store key component, a segment footer entry, a
//! routing prefix and a metric label — so an unrepresentable name breaks far
//! from the client that chose it, and by then nothing points back.
//!
//! Applied at [`crate::Storage::create_topic`], which is the single creation
//! choke point (#225): `CreateTopics` and metadata auto-create both land there,
//! and auto-create takes its name straight off the wire.

use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};

use crate::{Error, Result};

/// Kafka's `Topic.MAX_NAME_LENGTH`. The limit exists because a topic name is
/// embedded in filesystem paths there and in object keys here.
const MAX_NAME_LENGTH: usize = 249;

/// Whether `c` may appear in a topic name, matching Kafka's
/// `Topic.containsValidPattern`: `[a-zA-Z0-9._-]`.
fn legal(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
}

/// Kafka's `Topic.validate`, as an [`ErrorCode`].
///
/// `.` and `..` are refused for the reason they are refused in a filesystem:
/// they are not names, they are traversal. Kafka keeps the rule even where the
/// storage would tolerate them, and so does this — a topic called `..` would be
/// a path component in every key it appears in.
pub(crate) fn topic_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(Error::Api(ErrorCode::InvalidTopicException));
    }

    // Characters, not bytes: the legal set is ASCII, so a name that is not
    // ASCII fails the pattern before the length ever matters.
    if name.chars().count() > MAX_NAME_LENGTH || !name.chars().all(legal) {
        return Err(Error::Api(ErrorCode::InvalidTopicException));
    }

    Ok(())
}

/// A topic must have at least one partition.
///
/// `-1` — Kafka's "use the broker default" — is resolved by the `CreateTopics`
/// service before a creation reaches storage, so it is not a value this layer
/// accepts: a partition count that has not been resolved is a caller that has
/// not finished deciding, and creating a topic with it would bake the
/// unresolved value into its metadata.
///
/// `replication_factor` is deliberately not checked. There is no replication in
/// this broker, so every value is equally unhonoured; refusing `0` while
/// accepting `3` would assert a distinction that does not exist here.
pub(crate) fn num_partitions(num_partitions: i32) -> Result<()> {
    if num_partitions < 1 {
        return Err(Error::Api(ErrorCode::InvalidPartitions));
    }

    Ok(())
}

/// Everything a creation must satisfy before anything is written.
pub(crate) fn creatable_topic(topic: &CreatableTopic) -> Result<()> {
    topic_name(topic.name.as_str())?;
    num_partitions(topic.num_partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(result: Result<()>) -> Option<ErrorCode> {
        match result {
            Ok(()) => None,
            Err(Error::Api(code)) => Some(code),
            Err(otherwise) => panic!("unexpected {otherwise:?}"),
        }
    }

    #[test]
    fn a_name_kafka_accepts_is_accepted() {
        for name in [
            "abcba",
            "org.env.conn.tab_a",
            "a",
            "_",
            "-",
            "..a",
            "with-dash_and.dot0123",
            &"a".repeat(MAX_NAME_LENGTH),
        ] {
            assert_eq!(None, code(topic_name(name)), "{name} must be accepted");
        }
    }

    #[test]
    fn a_name_kafka_refuses_is_refused() {
        for name in [
            "",
            ".",
            "..",
            // The report's own example: a space and a slash, either of which
            // would land in an object key.
            "some topic/bad!",
            "with space",
            "with/slash",
            "with:colon",
            "with\\backslash",
            "naïve",
            "emoji🙂",
            &"a".repeat(MAX_NAME_LENGTH + 1),
        ] {
            assert_eq!(
                Some(ErrorCode::InvalidTopicException),
                code(topic_name(name)),
                "{name} must be refused",
            );
        }
    }

    #[test]
    fn a_topic_needs_at_least_one_partition() {
        assert_eq!(None, code(num_partitions(1)));
        assert_eq!(None, code(num_partitions(1_000)));

        for count in [0, -1, -2, i32::MIN] {
            assert_eq!(
                Some(ErrorCode::InvalidPartitions),
                code(num_partitions(count)),
                "{count} partitions must be refused",
            );
        }
    }
}
