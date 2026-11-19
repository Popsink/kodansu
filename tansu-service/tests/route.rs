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

//! Typed requests routed through the frame and byte layers, and back.

use rama::layer::{HijackLayer, MapResponseLayer};
use rama::{Context, Layer, Service};
use tansu_sans_io::metadata_response::MetadataResponseBroker;
use tansu_sans_io::{ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, ErrorCode};
use tansu_sans_io::{Frame, Header, MetadataRequest, MetadataResponse};
use tansu_sans_io::{RequestApiVersion, RootMessageMeta};
use tansu_service::{BytesFrameLayer, BytesLayer, FrameBytesLayer, RequestFrameLayer};
use tansu_service::{FrameApiKeyMatcher, FrameRequestLayer, FrameRouteService, FrameService};
use tansu_service::{RequestLayer, ResponseService};

use self::common::Error;
use self::common::init_tracing;

mod common;

/// What a client puts between itself and a router, outermost first.
type Bytes = (BytesLayer, BytesFrameLayer);
type Wire = (RequestFrameLayer, (FrameBytesLayer, Bytes));

/// `router` as a client sees it: a typed request in, through a frame and its
/// bytes and back, and a typed response out.
fn over_the_wire<S>(router: S) -> <Wire as Layer<S>>::Service
where
    Wire: Layer<S>,
{
    let bytes = (BytesLayer, BytesFrameLayer::default());
    let wire: Wire = (RequestFrameLayer, (FrameBytesLayer, bytes));
    wire.into_layer(router)
}

/// A `Metadata` answer naming `cluster`, with no brokers or topics.
fn answer(cluster: &str) -> MetadataResponse {
    let empty = MetadataResponse::default().brokers(Some(vec![]));
    let empty = empty
        .topics(Some(vec![]))
        .cluster_authorized_operations(Some(-1));
    let named = empty.cluster_id(Some(cluster.to_owned()));
    named.controller_id(Some(111)).throttle_time_ms(Some(0))
}

/// A `Metadata` request for no topics, creating nothing.
fn brokers_only() -> MetadataRequest {
    let asked = MetadataRequest::default().topics(Some(vec![]));
    let asked = asked.allow_auto_topic_creation(Some(false));
    let asked = asked.include_cluster_authorized_operations(Some(false));
    asked.include_topic_authorized_operations(Some(false))
}

/// The one broker a mapped response names in place of none.
fn broker(node: i32, host: &str, port: i32) -> MetadataResponseBroker {
    let named = MetadataResponseBroker::default()
        .node_id(node)
        .host(host.to_owned());
    named.port(port).rack(None)
}

/// A router with a single route, `Metadata`, answering `cluster`.
fn metadata_router(cluster: &'static str) -> Result<FrameRouteService<(), Error>, Error> {
    let handler = ResponseService::new(move |_: Context<()>, _: MetadataRequest| {
        Ok::<_, Error>(answer(cluster))
    });
    let route = RequestLayer::<MetadataRequest>::new().into_layer(handler);
    Ok(FrameRouteService::builder().with_service(route)?.build()?)
}

/// The API keys an `ApiVersions` answer lists, once it has answered `NONE`.
async fn advertised<S>(client: &S) -> Result<Vec<i16>, Error>
where
    S: Service<(), ApiVersionsRequest, Response = ApiVersionsResponse, Error = Error>,
{
    let asked = ApiVersionsRequest::default().client_software_name(Some(String::from("abcba")));
    let asked = asked.client_software_version(Some(String::from("12321")));
    let answered = client.serve(Context::default(), asked).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(answered.error_code)?);
    let versions = answered.api_keys.unwrap_or_default().into_iter();
    Ok(versions.map(|version| version.api_key).collect())
}

/// `Metadata` and `ApiVersions` itself, and nothing else.
fn assert_metadata_and_versions(keys: &[i16]) {
    assert_eq!(2, keys.len());
    assert!(keys.contains(&ApiVersionsRequest::KEY));
    assert!(keys.contains(&MetadataRequest::KEY));
}

#[tokio::test]
async fn a_routed_metadata_request_is_answered() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let client = over_the_wire(metadata_router("tansu")?);
    let answered = client.serve(Context::default(), brokers_only()).await?;
    assert_eq!(Some(String::from("tansu")), answered.cluster_id);
    Ok(())
}

#[tokio::test]
async fn api_versions_lists_each_route_and_itself() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let client = over_the_wire(metadata_router("abc")?);
    assert_metadata_and_versions(&advertised(&client).await?);

    let answered = client.serve(Context::default(), brokers_only()).await?;
    assert_eq!(Some(String::from("abc")), answered.cluster_id);
    Ok(())
}

#[tokio::test]
async fn a_route_can_map_its_response() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (node, host, port) = (12321, "defgfed", 32123);
    let replacement = broker(node, host, port);
    let replace = MapResponseLayer::new(move |response: MetadataResponse| {
        response.brokers(Some(vec![replacement.clone()]))
    });
    let handler =
        ResponseService::new(|_: Context<()>, _: MetadataRequest| Ok::<_, Error>(answer("abc")));
    let route = (RequestLayer::<MetadataRequest>::new(), replace).into_layer(handler);
    let router = FrameRouteService::<(), Error>::builder()
        .with_service(route)?
        .build()?;
    let client = over_the_wire(router);
    assert_metadata_and_versions(&advertised(&client).await?);

    let answered = client.serve(Context::default(), brokers_only()).await?;
    let identity = (answered.cluster_id.as_deref(), answered.controller_id);
    assert_eq!((Some("abc"), Some(111)), identity);
    let [only] = &answered.brokers.unwrap_or_default()[..] else {
        panic!("the mapped response names exactly one broker")
    };
    assert_eq!(
        (node, host, port),
        (only.node_id, only.host.as_str(), only.port)
    );
    Ok(())
}

#[tokio::test]
async fn a_hijack_takes_the_frames_its_api_key_matches() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let replacement = broker(111, "localhost", 9092);
    let replace = MapResponseLayer::new(move |response: MetadataResponse| {
        response.brokers(Some(vec![replacement.clone()]))
    });
    let handler = ResponseService::new(|_, _: MetadataRequest| Ok::<_, Error>(answer("abc")));
    let metadata = (FrameRequestLayer::<MetadataRequest>::new(), replace).into_layer(handler);
    let unmatched = FrameService::new(|_, req: Frame| {
        tracing::debug!(?req);
        Err(Error::Message(Box::from("unmapped")))
    });
    let matcher = FrameApiKeyMatcher(MetadataRequest::KEY);
    let hijacked = HijackLayer::new(matcher, metadata).into_layer(unmatched);

    let (api_key, api_version, correlation_id) = (MetadataRequest::KEY, 12, 0);
    let client_id = Some(String::from("tansu"));
    let header = Header::Request {
        client_id,
        correlation_id,
        api_version,
        api_key,
    };
    let (size, body) = (0, MetadataRequest::default().into());
    let request = Frame { size, header, body };
    let answered = hijacked.serve(Context::default(), request).await?;

    let response = MetadataResponse::try_from(answered.body)?;
    assert_eq!(response.controller_id, Some(111));
    let [only] = &response.brokers.unwrap_or_default()[..] else {
        panic!("the hijacking route names exactly one broker")
    };
    assert_eq!(111, only.node_id);
    Ok(())
}

/// A typed handler learns the version its request was decoded at (#593):
/// `Metadata`'s empty topic list means opposite things at v0 and from v1, and
/// nothing in the decoded request says which.
#[tokio::test]
async fn a_handler_sees_the_version_its_request_arrived_at() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let handler = ResponseService::new(|ctx: Context<()>, _: MetadataRequest| {
        let arrived = ctx.get::<RequestApiVersion>();
        let version = arrived.map(|RequestApiVersion(version)| i32::from(*version));
        Ok::<_, Error>(answer("abc").controller_id(version))
    });
    let route = RequestLayer::<MetadataRequest>::new().into_layer(handler);
    let router = FrameRouteService::<(), Error>::builder()
        .with_service(route)?
        .build()?;
    let client = over_the_wire(router);

    let declared = RootMessageMeta::messages()
        .requests()
        .get(&MetadataRequest::KEY);
    let latest = declared.map(|meta| i32::from(meta.version.valid().end));
    let answered = client.serve(Context::default(), brokers_only()).await?;

    assert!(latest.is_some());
    assert_eq!(latest, answered.controller_id);
    Ok(())
}
