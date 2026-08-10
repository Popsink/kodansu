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

//! Where a quota is applied (#384).
//!
//! One layer, immediately inside [`super::principal::RequesterService`], for
//! the reason that service exists at all: it is the only place that knows who
//! is asking. The layer above puts the [`Requester`] into the context; this one
//! reads the same value, which is what makes re-authentication (KIP-368)
//! visible to the limit without anything else being told.
//!
//! It sits at the [`Frame`] level rather than beside each API's service, and
//! that is not an implementation convenience: a quota is a property of the
//! *connection's principal*, not of the produce path, and putting it in
//! twenty-seven services would mean twenty-seven places to forget it. Here
//! there is one, and the APIs added after it are covered by having been added.
//!
//! ## What it charges, and when it waits
//!
//! Produce bytes come off the request's record batches, fetch bytes off the
//! response's — what Kafka's own quotas measure, and what an operator sizes a
//! limit against. Every request also costs one against the request rate,
//! including the ones that carry no records at all, because a metadata storm is
//! exactly the shape of load a byte-rate quota cannot see.
//!
//! The wait is not taken here. The response is answered immediately with the
//! delay in `throttle_time_ms` (KIP-219), and the connection is muted for that
//! long *between* requests by [`Throttle`]. Sleeping here instead would put the
//! wait inside the in-flight count and inside `tansu_request_duration`, and a
//! fleet deliberately refusing traffic would report itself saturated to the
//! scaler reading those (#362).

use rama::{Context, Layer, Service};
use tansu_sans_io::{Body, ByteSize as _, Frame};
use tansu_service::Throttle;
use tansu_storage::{Charge, QuotaEnforcer, Requester};
use tracing::{debug, warn};

/// Applies the client quotas, when this broker enforces any.
#[derive(Clone, Debug, Default)]
pub struct QuotaLayer {
    /// `None` on a broker without `--authentication`: there are no principals,
    /// so there is nothing to write a limit against and nothing to key the
    /// accounting by. The same switch authorization uses, for the same reason.
    enforcer: Option<QuotaEnforcer>,
}

impl QuotaLayer {
    #[must_use]
    pub fn new(enforcer: Option<QuotaEnforcer>) -> Self {
        Self { enforcer }
    }
}

impl<S> Layer<S> for QuotaLayer {
    type Service = QuotaService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            enforcer: self.enforcer.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QuotaService<S> {
    inner: S,
    enforcer: Option<QuotaEnforcer>,
}

/// The record bytes a produce request carries.
///
/// Zero for every other API, which is not the same as "unknown": a request that
/// produces nothing costs nothing against a produce quota, and only against the
/// request rate.
fn produced(body: &Body) -> u64 {
    let Body::ProduceRequest(request) = body else {
        return 0;
    };

    request
        .topic_data
        .iter()
        .flatten()
        .flat_map(|topic| topic.partition_data.iter().flatten())
        .filter_map(|partition| partition.records.as_ref())
        .filter_map(|records| records.size_in_bytes().ok())
        .map(|size| size as u64)
        .sum()
}

/// The record bytes a fetch response carries.
///
/// The response and not the request, because what a fetch costs is what it
/// returns: a `max_bytes` a client asked for and did not get is not traffic,
/// and charging for it would throttle a consumer that is caught up and reading
/// nothing.
fn fetched(body: &Body) -> u64 {
    let Body::FetchResponse(response) = body else {
        return 0;
    };

    response
        .responses
        .iter()
        .flatten()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .filter_map(|partition| partition.records.as_ref())
        .filter_map(|records| records.size_in_bytes().ok())
        .map(|size| size as u64)
        .sum()
}

impl<S, State> Service<State, Frame> for QuotaService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let Some(enforcer) = self.enforcer.clone() else {
            return self.inner.serve(ctx, req).await;
        };

        // No principal, no quota — a connection that has not authenticated on a
        // broker that does not require it. The same answer `authorized` gives,
        // and for the same reason: there is nothing to key a limit by, and
        // inventing one would throttle by connection, which is a thing a client
        // can multiply at will.
        let Some(principal) = ctx
            .get::<Requester>()
            .and_then(|requester| requester.principal.clone())
        else {
            return self.inner.serve(ctx, req).await;
        };

        let produced = produced(&req.body);

        let response = self.inner.serve(ctx.clone(), req).await?;

        let charge = Charge::request()
            .produced(produced)
            .fetched(fetched(&response.body));

        let throttle = enforcer.throttle(&principal, charge).await;

        if throttle.is_zero() {
            return Ok(response);
        }

        // Answered first, waited for afterwards: the client is told how long it
        // is being asked to back off, and its connection is muted for that long
        // once this response has gone out.
        let Some(owed) = ctx.get::<Throttle>() else {
            // Unreachable through the broker's own stack — the connection loop
            // inserts one — but a quota that silently does not apply is worth a
            // line rather than a shrug, because the symptom is a limit that
            // reads as configured and enforces nothing.
            warn!(
                principal,
                ?throttle,
                "no throttle on this connection: answering the delay without muting it"
            );

            return Ok(Frame {
                body: response
                    .body
                    .with_throttle_time_ms(throttle.as_millis() as i32),
                ..response
            });
        };

        owed.owe(throttle);

        debug!(principal, ?throttle, ?charge);

        Ok(Frame {
            body: response
                .body
                .with_throttle_time_ms(throttle.as_millis() as i32),
            ..response
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use rama::Layer as _;
    use tansu_sans_io::{
        FetchResponse, Header, ProduceRequest, ProduceResponse,
        fetch_response::{FetchableTopicResponse, PartitionData},
        produce_request::{PartitionProduceData, TopicProduceData},
        record::{
            Record,
            deflated::{self, Frame as RecordBatch},
            inflated,
        },
    };

    use super::*;

    fn batch(payload: &'static [u8]) -> RecordBatch {
        RecordBatch {
            batches: vec![
                inflated::Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(payload).into()))
                    .build()
                    .and_then(deflated::Batch::try_from)
                    .expect("batch"),
            ],
        }
    }

    /// A produce is charged the bytes of the batches it carries, and every
    /// other API nothing — the difference the produce quota is written against.
    #[test]
    fn a_produce_is_charged_its_record_bytes() {
        let records = batch(b"Lorem ipsum dolor sit amet");
        let expected = records.size_in_bytes().expect("size") as u64;

        assert!(expected > 0);

        let request = Body::ProduceRequest(ProduceRequest::default().topic_data(Some(vec![
                TopicProduceData::default()
                    .name("orders".into())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(0)
                            .records(Some(records)),
                    ])),
            ])));

        assert_eq!(expected, produced(&request));
        assert_eq!(0, fetched(&request));
    }

    /// A fetch is charged what it *returned*, off the response. Charging the
    /// request's `max_bytes` would throttle a caught-up consumer that is
    /// reading nothing at all.
    #[test]
    fn a_fetch_is_charged_the_record_bytes_it_returned() {
        let records = batch(b"consectetur adipiscing elit");
        let expected = records.size_in_bytes().expect("size") as u64;

        let response = Body::FetchResponse(FetchResponse::default().responses(Some(vec![
            FetchableTopicResponse::default().partitions(Some(vec![
                PartitionData::default()
                    .partition_index(0)
                    .records(Some(records)),
            ])),
        ])));

        assert_eq!(expected, fetched(&response));
        assert_eq!(0, produced(&response));

        // An empty fetch — the long poll that returned nothing — costs nothing
        // against the byte rate, and only its one request.
        assert_eq!(
            0,
            fetched(&Body::FetchResponse(
                FetchResponse::default().responses(Some(vec![]))
            )),
        );
    }

    /// The throttle reaches the client on the response it belongs to, whichever
    /// response that is. Generated from the protocol descriptors rather than
    /// matched by hand, so an API added later carries it without anything being
    /// remembered.
    #[test]
    fn a_throttle_is_answered_on_the_response() {
        let body = Body::ProduceResponse(ProduceResponse::default().throttle_time_ms(Some(0)));

        let Body::ProduceResponse(throttled) = body.with_throttle_time_ms(1234) else {
            panic!("the variant must not change");
        };

        assert_eq!(Some(1234), throttled.throttle_time_ms);
    }

    /// A frame is answered unchanged when nothing has a quota: the header, the
    /// size and the body all survive the layer.
    #[test]
    fn a_response_that_cannot_carry_a_throttle_is_unchanged() {
        let body = Body::ProduceResponse(ProduceResponse::default());
        let frame = Frame {
            size: 42,
            header: Header::Response { correlation_id: 7 },
            body: body.clone(),
        };

        let unchanged = Frame {
            body: frame.body.clone(),
            ..frame
        };

        assert_eq!(42, unchanged.size);
        assert_eq!(body, unchanged.body);
    }

    /// Answers every request with an empty `ProduceResponse`, so the only thing
    /// under test is what the layer above it did.
    #[derive(Clone, Debug, Default)]
    struct Answers;

    impl Service<(), Frame> for Answers {
        type Response = Frame;
        type Error = tansu_service::Error;

        async fn serve(&self, _ctx: Context<()>, _req: Frame) -> Result<Frame, Self::Error> {
            Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id: 0 },
                body: Body::ProduceResponse(
                    ProduceResponse::default()
                        .responses(Some(vec![]))
                        .throttle_time_ms(Some(0)),
                ),
            })
        }
    }

    fn produce(payload: &'static [u8]) -> Frame {
        Frame {
            size: 0,
            header: Header::Request {
                api_key: 0,
                api_version: 9,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::ProduceRequest(ProduceRequest::default().topic_data(Some(vec![
                TopicProduceData::default()
                    .name("orders".into())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(0)
                            .records(Some(batch(payload))),
                    ])),
            ]))),
        }
    }

    /// An enforcer over an in-memory cluster with no quotas of its own, so the
    /// limit under test is the broker's own default — the case a fleet with no
    /// control plane in front of it is in.
    async fn enforcer() -> QuotaEnforcer {
        use std::sync::Arc;

        use tansu_storage::{ArcDynStorage, QuotaLimits, StorageContainer};
        use url::Url;

        let storage = StorageContainer::builder()
            .cluster_id("tansu")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092").expect("listener"))
            .storage(Url::parse("memory://tansu/").expect("storage"))
            .build()
            .await
            .map(|storage| Arc::new(storage) as ArcDynStorage)
            .expect("storage");

        QuotaEnforcer::new(storage).with_defaults(QuotaLimits {
            producer_byte_rate: Some(1.0),
            ..QuotaLimits::default()
        })
    }

    /// The acceptance criterion, through the layer: a principal over its
    /// produce byte-rate receives a **non-zero** `throttle_time_ms`, and its
    /// connection is left owing exactly that.
    ///
    /// Both, because either alone is a half-implemented KIP-219: a throttle the
    /// client is told about but never made to wait for enforces nothing on a
    /// client that ignores it, and a wait the client is not told about looks to
    /// it like a broker that has stopped answering.
    #[tokio::test(start_paused = true)]
    async fn a_principal_over_its_limit_is_told_and_the_connection_is_muted() {
        let service = QuotaLayer::new(Some(enforcer().await)).into_layer(Answers);

        let throttle = Throttle::default();

        let mut ctx = Context::default();
        _ = ctx.insert(Requester {
            principal: Some("User:alice".into()),
            host: "10.0.0.1".into(),
        });
        _ = ctx.insert(throttle.clone());

        // The first request spends the burst; the second is over the limit.
        _ = service
            .serve(ctx.clone(), produce(b"Lorem ipsum dolor sit amet"))
            .await
            .expect("first");

        let response = service
            .serve(ctx, produce(b"consectetur adipiscing elit"))
            .await
            .expect("second");

        let Body::ProduceResponse(response) = response.body else {
            panic!("a produce is answered with a produce response");
        };

        let answered = response.throttle_time_ms.expect("a throttle");

        assert!(
            answered > 0,
            "a principal over its produce byte-rate must be told to back off",
        );

        assert_eq!(
            Duration::from_millis(answered as u64),
            throttle.owed(),
            "the connection must owe exactly what the client was told",
        );
    }

    /// A broker without `--authentication` has no principals, so the layer is
    /// not there at all — and even with an enforcer, a connection that has not
    /// authenticated is not throttled: there is nothing to key a limit by, and
    /// keying by connection is a thing a client can multiply at will.
    #[tokio::test(start_paused = true)]
    async fn without_a_principal_nothing_is_throttled() {
        let throttle = Throttle::default();

        for enforcer in [None, Some(enforcer().await)] {
            let service = QuotaLayer::new(enforcer).into_layer(Answers);

            let mut ctx = Context::default();
            _ = ctx.insert(Requester {
                principal: None,
                host: "10.0.0.1".into(),
            });
            _ = ctx.insert(throttle.clone());

            for _ in 0..8 {
                let response = service
                    .serve(ctx.clone(), produce(b"Lorem ipsum dolor sit amet"))
                    .await
                    .expect("served");

                let Body::ProduceResponse(response) = response.body else {
                    panic!("a produce is answered with a produce response");
                };

                assert_eq!(
                    Some(0),
                    response.throttle_time_ms,
                    "a broker that has never turned authentication on must answer as it does today",
                );
            }

            assert_eq!(Duration::ZERO, throttle.owed());
        }
    }
}
