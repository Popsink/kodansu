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

//! Regression for #7 ("Tag 0 is not valid for version" on the consumer read
//! path). `NodeEndpoints` is a tagged field (tag 0) on `FetchResponse` that is
//! only valid from v16 (KIP-951). The serializer encodes a tagged field as soon
//! as its value is `Some` — it does not gate on the field's `taggedVersions` —
//! so populating `node_endpoints` (even with an empty list) emits a top-level
//! tag 0 even on a response whose negotiated version is < 16. A modern Kafka
//! client then fails to decode it with "Tag 0 is not valid for version <V>".
//!
//! The broker therefore must leave `node_endpoints` unset (`None`) for any
//! response it might serve below v16; these tests lock both that contract and
//! the underlying serializer behaviour that makes it necessary.

use tansu_sans_io::{
    FetchResponse, Frame, Header, NULL_TOPIC_ID, Result,
    fetch_response::{FetchableTopicResponse, NodeEndpoint, PartitionData},
};

fn encode(api_version: i16, node_endpoints: Option<Vec<NodeEndpoint>>) -> Result<Vec<u8>> {
    let header = Header::Response { correlation_id: 8 };

    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(0))
        .responses(Some(
            [FetchableTopicResponse::default()
                // `topic` is 0-12 and `topic_id` 13+, neither nullable, and this
                // encodes across that boundary — so both have to be present for
                // the frame to be legal at every version swept (#351).
                .topic(Some("t".into()))
                .topic_id(Some(NULL_TOPIC_ID))
                .partitions(Some(
                    [PartitionData::default()
                        .partition_index(0)
                        .error_code(0)
                        .high_watermark(0)
                        .last_stable_offset(Some(0))
                        .log_start_offset(Some(0))
                        .diverging_epoch(None)
                        .current_leader(None)
                        .snapshot_id(None)
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(-1))
                        .records(None)]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(node_endpoints);

    Frame::response(header, body.into(), 1, api_version).map(|frame| frame.to_vec())
}

/// With `node_endpoints` unset (what the broker sends), no top-level tagged
/// field is emitted at any version, so a client on any Fetch version decodes
/// the response. This is the contract `fetch.rs` relies on.
#[test]
fn none_emits_no_top_level_tagged_field() -> Result<()> {
    for api_version in [12i16, 13, 15, 16, 17] {
        let encoded = encode(api_version, None)?;
        // The trailing byte is the FetchResponse top-level tagged-field count,
        // which must be 0 (no NodeEndpoints).
        assert_eq!(
            Some(&0u8),
            encoded.last(),
            "v{api_version}: expected empty top-level tagged-field section"
        );
    }

    Ok(())
}

/// Populating `node_endpoints` below v16 emits a stray top-level tag 0 — the
/// exact framing that breaks clients (#7). This documents *why* the broker must
/// keep it `None`; if the serializer is ever taught to gate tagged fields by
/// their `taggedVersions`, this expectation will flag that the workaround in
/// `fetch.rs` can be removed.
#[test]
fn some_below_v16_emits_stray_tag_zero() -> Result<()> {
    for api_version in [12i16, 13, 15] {
        let none = encode(api_version, None)?;
        let some_empty = encode(api_version, Some([].into()))?;

        assert!(
            some_empty.len() > none.len(),
            "v{api_version}: populating node_endpoints below v16 should add a tagged field"
        );
        // tag 0, length 1, value 1 (empty compact array): the stray NodeEndpoints.
        assert_eq!(
            &[0u8, 1, 1],
            &some_empty[some_empty.len() - 3..],
            "v{api_version}: expected a stray tag-0 NodeEndpoints tagged field"
        );
    }

    Ok(())
}

/// From v16 NodeEndpoints is legitimately a valid tagged field.
#[test]
fn some_from_v16_is_valid() -> Result<()> {
    for api_version in [16i16, 17] {
        let some_empty = encode(api_version, Some([].into()))?;
        assert_eq!(
            &[0u8, 1, 1],
            &some_empty[some_empty.len() - 3..],
            "v{api_version}: NodeEndpoints is valid here"
        );
    }

    Ok(())
}
