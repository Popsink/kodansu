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

//! `ConsumerGroupHeartbeat` on the wire (#612).
//!
//! Its request is the one message here with a *nullable array of strings*
//! (`SubscribedTopicNames`), and its response the one with a *nullable
//! structure* a broker actually sends (`Assignment`). Neither survived a round
//! trip until #612: the array's elements were read without their length, and
//! the structure was written without the presence byte the decoder reads.

use bytes::Bytes;
use tansu_sans_io::{
    Body, ConsumerGroupHeartbeatRequest, Frame, Header, Result,
    consumer_group_heartbeat_response::{
        Assignment, ConsumerGroupHeartbeatResponse, TopicPartitions,
    },
};

const API_KEY: i16 = 68;

#[test]
fn a_join_request_decodes() -> Result<()> {
    let body = ConsumerGroupHeartbeatRequest::default()
        .group_id("g".into())
        .member_id("m".into())
        .member_epoch(0)
        .rebalance_timeout_ms(1)
        .subscribed_topic_names(Some(vec!["t".into(), "u".into()]))
        .topic_partitions(Some(vec![]));

    let encoded = Frame::request(
        Header::Request {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: 7,
            client_id: Some("c".into()),
        },
        body.clone().into(),
    )?;

    let decoded = Frame::request_from_bytes(encoded)?;
    assert_eq!(Body::from(body), decoded.body);

    Ok(())
}

/// Version 1 carries the subscription pattern (#625); version 0 has no room
/// for it and drops it.
#[test]
fn a_pattern_subscription_round_trips_at_v1_only() -> Result<()> {
    let body = ConsumerGroupHeartbeatRequest::default()
        .group_id("g".into())
        .member_id("m".into())
        .member_epoch(0)
        .rebalance_timeout_ms(1)
        .subscribed_topic_names(Some(vec![]))
        .subscribed_topic_regex(Some("orders-.*".into()))
        .topic_partitions(Some(vec![]));

    let at = |api_version| {
        Frame::request(
            Header::Request {
                api_key: API_KEY,
                api_version,
                correlation_id: 7,
                client_id: Some("c".into()),
            },
            body.clone().into(),
        )
        .and_then(Frame::request_from_bytes)
        .and_then(|frame| ConsumerGroupHeartbeatRequest::try_from(frame.body))
    };

    assert_eq!(Some("orders-.*".into()), at(1)?.subscribed_topic_regex);
    assert_eq!(None, at(0)?.subscribed_topic_regex);

    Ok(())
}

#[test]
fn an_assignment_carries_its_presence_byte() -> Result<()> {
    let body = ConsumerGroupHeartbeatResponse::default()
        .member_id(Some("m".into()))
        .member_epoch(1)
        .heartbeat_interval_ms(5_000)
        .assignment(Some(Assignment::default().topic_partitions(Some(vec![
            TopicPartitions::default()
                .topic_id([1; 16])
                .partitions(Some(vec![0, 1])),
        ]))));

    let encoded = Frame::response(
        Header::Response { correlation_id: 7 },
        body.clone().into(),
        API_KEY,
        0,
    )?;

    let interval = 5_000i32.to_be_bytes();
    let assignment_at = encoded
        .windows(interval.len())
        .position(|window| window == interval)
        .map(|position| position + interval.len());

    assert_eq!(
        Some(&[1u8, 2][..]),
        assignment_at.and_then(|at| encoded.get(at..at + 2)),
        "present (1), then one topic (compact length 2)"
    );

    let decoded = Frame::response_from_bytes(Bytes::from(encoded.to_vec()), API_KEY, 0)?;
    assert_eq!(Body::from(body), decoded.body);

    Ok(())
}

#[test]
fn a_missing_assignment_is_null() -> Result<()> {
    let body = ConsumerGroupHeartbeatResponse::default()
        .member_id(Some("m".into()))
        .member_epoch(1)
        .heartbeat_interval_ms(5_000)
        .assignment(None);

    let encoded = Frame::response(
        Header::Response { correlation_id: 7 },
        body.clone().into(),
        API_KEY,
        0,
    )?;

    let decoded = Frame::response_from_bytes(encoded, API_KEY, 0)?;
    assert_eq!(Body::from(body), decoded.body);

    Ok(())
}
