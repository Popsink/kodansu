// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use rama::service::BoxService;
use rama::{Context, Service};
use tansu_sans_io::api_versions_response::ApiVersion;
use tansu_sans_io::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, RootMessageMeta};
use tansu_sans_io::{Body, ErrorCode, FetchRequest, Frame, Header, ProduceRequest};

use super::Error;

/// The lowest version to advertise for an API whose low versions imply a record
/// format this broker does not speak (#322).
///
/// This broker reads and writes exactly one record format, the v2 RecordBatch. The
/// API version and the record format move together, which is what makes this a
/// table of facts rather than a matter of taste: a producer only sends a pre-v2
/// MessageSet on `Produce` v0/v1/v2, and a consumer only expects one back on
/// `Fetch` v0/v1/v2/v3. Kafka serves those versions by down-converting; we do not
/// implement that and should not advertise as though we did.
///
/// Advertising them was not harmless in either direction:
///
/// - on **produce**, a magic-0/1 batch is refused `UNSUPPORTED_FOR_MESSAGE_FORMAT`
///   (#320) — a clean answer, but to a version we invited;
/// - on **fetch**, there is no equivalent. `fetch.rs` has no `magic` handling and
///   no down-conversion, so a consumer that negotiated a legacy version is handed
///   v2 batches and misreads them, with no error code anywhere. That is the worse
///   half, and it is why the floor is the only protection.
///
/// A floor rather than an edit to `validVersions` in the message descriptors, and
/// that choice is the substance of #322. `validVersions` also gates request
/// *decoding*: shrinking it would make a v0 request fail to decode, and a frame
/// the broker cannot decode ends the connection with no response at all. A
/// non-compliant client that skipped `ApiVersions` would get a dropped socket
/// instead of an error code — harder to diagnose than the state being fixed. The
/// cost of a floor is a second source of truth next to `MessageMeta`, which is why
/// it lives here, beside the only code that reads it, and is asserted by name in
/// tests rather than re-derived.
const RECORD_FORMAT_FLOOR: &[(i16, i16)] = &[
    // v3 (0.11) is the first Produce carrying a v2 RecordBatch.
    (ProduceRequest::KEY, 3),
    // v4 (0.11) is the first Fetch a client expects v2 RecordBatches back on.
    (FetchRequest::KEY, 4),
];

/// `valid_start` raised to this API's record-format floor, never above
/// `valid_end`.
///
/// The clamp matters: a floor above the highest supported version would advertise
/// an inverted range, which a client is entitled to read as "no common version"
/// for an API that does in fact work. If that ever happens it means the descriptors
/// moved under the table, and the honest degenerate answer is the API's real
/// maximum.
fn advertised_min_version(api_key: i16, valid_start: i16, valid_end: i16) -> i16 {
    RECORD_FORMAT_FLOOR
        .iter()
        .find(|(key, _)| *key == api_key)
        .map_or(valid_start, |(_, floor)| valid_start.max(*floor))
        .min(valid_end)
}

/// Answers `ApiVersions` with every routed API key, each at the version range
/// its [`RootMessageMeta`] entry declares, floored where the record format
/// requires it..
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    supported: Vec<i16>,
    error: PhantomData<E>,
}

impl<State, E> Service<State, ApiVersionsRequest> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Response = ApiVersionsResponse;
    type Error = E;

    async fn serve(
        &self,
        _ctx: Context<State>,
        _req: ApiVersionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let routed = RootMessageMeta::messages().requests().iter();
        let routed = routed.filter(|(key, _)| self.supported.contains(key));
        let advertised = routed.map(|(_, meta)| {
            let (lowest, highest) = (meta.version.valid.start, meta.version.valid.end);
            let floored = advertised_min_version(meta.api_key, lowest, highest);
            let version = ApiVersion::default().api_key(meta.api_key);
            version.min_version(floored).max_version(highest)
        });

        let answer = ApiVersionsResponse::default().finalized_features(Some(vec![]));
        let answer = answer.finalized_features_epoch(Some(-1));
        let answer = answer
            .supported_features(Some(vec![]))
            .zk_migration_ready(Some(false));
        let answer = answer.error_code(i16::from(ErrorCode::None));
        let answer = answer.api_keys(Some(advertised.collect()));
        Ok(answer.throttle_time_ms(Some(0)))
    }
}

impl<State, E> Service<State, Body> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let request = ApiVersionsRequest::try_from(req)?;
        let answered = self.serve(ctx, request).await?;
        Ok(Body::from(answered))
    }
}

impl<State, E> Service<State, Frame> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let answered: Body = self.serve(ctx, req.body).await?;
        let header = Header::Response { correlation_id };
        Ok(Frame {
            size: 0,
            header,
            body: answered,
        })
    }
}

/// Hands each [`Frame`] to the service routed for its API key, answering
/// `ApiVersions` itself for every key it routes.
///
/// ```
/// use rama::Layer as _;
/// use tansu_sans_io::{CreateTopicsRequest, CreateTopicsResponse};
/// use tansu_sans_io::{MetadataRequest, MetadataResponse};
/// use tansu_service::{Error, FrameRouteService, RequestLayer, ResponseService};
///
/// # fn main() -> Result<(), Error> {
/// let metadata = ResponseService::new(|_, _| {
///     let answer = MetadataResponse::default().brokers(Some(vec![])).topics(Some(vec![]));
///     let answer = answer.cluster_id(Some(String::from("tansu"))).controller_id(Some(111));
///     Ok(answer.throttle_time_ms(Some(0)).cluster_authorized_operations(Some(-1)))
/// });
/// let create = ResponseService::new(|_, _| {
///     let answer = CreateTopicsResponse::default().throttle_time_ms(Some(0));
///     Ok(answer.topics(Some(vec![])))
/// });
///
/// let routes = FrameRouteService::<(), Error>::builder();
/// let routes = routes.with_service(RequestLayer::<MetadataRequest>::new().into_layer(metadata))?;
/// let routes = routes.with_service(RequestLayer::<CreateTopicsRequest>::new().into_layer(create))?;
/// let _router = routes.build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct FrameRouteService<State = (), E = Error> {
    routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>,
}

impl<State, E> FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    pub fn new(routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>) -> Self {
        Self { routes }
    }

    pub fn builder() -> FrameRouteBuilder<State, E> {
        FrameRouteBuilder::new()
    }
}

impl<State, E> Service<State, Frame> for FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let key = req.api_key()?;
        match self.routes.get(&key) {
            Some(route) => route.serve(ctx, req).await,
            None => Err(E::from(Error::UnknownServiceFrame(Box::new(req)))),
        }
    }
}

/// A [`Frame`] route builder providing an [`ApiVersionsResponse`] for all available routes
#[derive(Debug)]
pub struct FrameRouteBuilder<State, E> {
    routes: BTreeMap<i16, BoxService<State, Frame, Frame, E>>,
}

impl<State, E> FrameRouteBuilder<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        let routes = BTreeMap::default();
        Self { routes }
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<State, Frame, Frame, E>> + ApiKey,
    {
        let boxed: BoxService<State, Frame, Frame, E> = service.into();
        self.with_route(S::KEY, boxed)
    }

    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<State, Frame, Frame, E>,
    ) -> Result<Self, Error> {
        match self.routes.insert(api_key, service) {
            None => Ok(self),
            Some(_displaced) => Err(Error::DuplicateRoute(api_key)),
        }
    }

    pub fn build(self) -> Result<FrameRouteService<State, E>, Error> {
        let versions_key = ApiVersionsRequest::KEY;
        let mut routed: Vec<i16> = self.routes.keys().copied().collect();
        routed.push(versions_key);

        let versions = ApiVersionsService {
            error: PhantomData,
            supported: routed,
        };
        let with_versions = self.with_route(versions_key, versions.boxed())?;
        let routes = Arc::new(with_versions.routes);
        Ok(FrameRouteService { routes })
    }
}

#[cfg(test)]
mod record_format_floor {
    use super::{RECORD_FORMAT_FLOOR, advertised_min_version};
    use tansu_sans_io::{
        ApiKey as _, FetchRequest, MetadataRequest, ProduceRequest, RootMessageMeta,
    };

    /// The floors are asserted by value, not re-derived from `MESSAGE_META` — a
    /// test that computed them the way the code does would pass whatever the code
    /// said (#322).
    #[test]
    fn produce_and_fetch_are_floored_at_their_v2_record_batch_versions() {
        assert_eq!(3, advertised_min_version(ProduceRequest::KEY, 0, 11));
        assert_eq!(4, advertised_min_version(FetchRequest::KEY, 0, 17));
    }

    /// Every other API keeps the descriptor's own minimum: this is a targeted
    /// floor, not a blanket bump of the protocol surface.
    #[test]
    fn an_unfloored_api_keeps_its_descriptor_minimum() {
        assert_eq!(0, advertised_min_version(MetadataRequest::KEY, 0, 13));
        assert_eq!(7, advertised_min_version(MetadataRequest::KEY, 7, 13));
    }

    /// A descriptor that already starts above the floor is left alone rather than
    /// pulled down to it.
    #[test]
    fn a_floor_never_lowers_an_advertised_minimum() {
        assert_eq!(5, advertised_min_version(ProduceRequest::KEY, 5, 11));
    }

    /// A floor above the API's maximum would advertise an inverted range, which a
    /// client may read as "no common version" for an API that works.
    #[test]
    fn a_floor_is_clamped_to_the_advertised_maximum() {
        assert_eq!(2, advertised_min_version(ProduceRequest::KEY, 0, 2));
    }

    /// The floors must be reachable in the versions this build actually supports,
    /// or the clamp above would be silently doing the work and the advertisement
    /// would still be wrong. Guards against the descriptors moving underneath the
    /// table.
    #[test]
    fn every_floor_is_within_its_api_supported_range() {
        let requests = RootMessageMeta::messages().requests();

        for (api_key, floor) in RECORD_FORMAT_FLOOR {
            let meta = requests
                .get(api_key)
                .unwrap_or_else(|| panic!("api key {api_key} has no request metadata"));

            assert!(
                *floor >= meta.version.valid.start && *floor <= meta.version.valid.end,
                "floor {floor} for api key {api_key} is outside its supported range {}..={}",
                meta.version.valid.start,
                meta.version.valid.end,
            );
        }
    }
}
