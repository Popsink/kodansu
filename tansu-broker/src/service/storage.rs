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

use rama::layer::{MapErrLayer, MapStateLayer};
use rama::{Context, Layer as _, Service};
use tansu_sans_io::{AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AlterClientQuotasRequest};
use tansu_sans_io::{AlterUserScramCredentialsRequest, ConsumerGroupDescribeRequest};
use tansu_sans_io::{CreateAclsRequest, CreateTopicsRequest, DeleteAclsRequest};
use tansu_sans_io::{DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest};
use tansu_sans_io::{DescribeAclsRequest, DescribeClientQuotasRequest, DescribeClusterRequest};
use tansu_sans_io::{DescribeConfigsRequest, DescribeGroupsRequest};
use tansu_sans_io::{DescribeTopicPartitionsRequest, DescribeUserScramCredentialsRequest};
use tansu_sans_io::{EndTxnRequest, FetchRequest, FindCoordinatorRequest};
use tansu_sans_io::{IncrementalAlterConfigsRequest, InitProducerIdRequest, ListGroupsRequest};
use tansu_sans_io::{ListOffsetsRequest, ListPartitionReassignmentsRequest, MetadataRequest};
use tansu_sans_io::{ProduceRequest, TxnOffsetCommitRequest};
use tansu_service::{Admission, FrameRequestLayer, FrameRouteBuilder, NoResponse};
use tansu_storage::{AlterClientQuotasService, AlterUserScramCredentialsService};
use tansu_storage::{ConsumerGroupDescribeService, CreateAclsService, CreateTopicsService};
use tansu_storage::{DeleteAclsService, DeleteGroupsService, DeleteRecordsService};
use tansu_storage::{DeleteTopicsService, DescribeAclsService, DescribeClientQuotasService};
use tansu_storage::{DescribeClusterService, DescribeConfigsService, DescribeGroupsService};
use tansu_storage::{DescribeTopicPartitionsService, DescribeUserScramCredentialsService};
use tansu_storage::{FetchService, FindCoordinatorService, IncrementalAlterConfigsService};
use tansu_storage::{InitProducerIdService, ListGroupsService, ListOffsetsService};
use tansu_storage::{ListPartitionReassignmentsService, MetadataService, OnAdmitted};
use tansu_storage::{ProduceService, Storage, TxnAddOffsetsService, TxnAddPartitionService};
use tansu_storage::{TxnEndService, TxnOffsetCommitService};
use tracing::warn;

use crate::Error;

/// `builder` with a route for `$request`, served by `$service` over
/// `storage`, its errors made this crate's.
macro_rules! storage_route {
    ($builder:expr, $storage:expr, $request:ty, $service:expr) => {{
        let state = MapStateLayer::new(|_| $storage);
        let layers = (MapErrLayer::new(Error::from), state);
        let decoded = (layers, FrameRequestLayer::<$request>::new());
        let route = decoded.into_layer($service).boxed();
        let key = <$request as tansu_sans_io::ApiKey>::KEY;
        $builder.with_route(key, route).map_err(Error::from)
    }};
}

/// The [`Storage`]-backed routes, and with them the API keys this broker
/// advertises: `ApiVersionsService::supported` is derived from the routes
/// registered here (`FrameRouteBuilder::build`), so this list *is* the
/// advertisement.
///
/// **`GetTelemetrySubscriptions` (api_key 71) is deliberately absent** (#515).
/// KIP-714 is a two-call protocol and only the handshake was ever implemented:
/// `PushTelemetry` (api_key 72) has no route, `requested_metrics` was answered
/// empty and always would be, because nothing in this fork configures a
/// subscription. Advertising 71 bought a capability the broker does not have,
/// 20 % of all Kafka API traffic at its peak (#410), and one class of bug with
/// a real blast radius — answering the field wrong threw
/// `IllegalArgumentException` out of every Java client's `poll()`.
///
/// Not advertising it is the path clients are written for, and it is silent:
/// `NetworkClient.doSend` aborts the internal request locally with
/// `UnsupportedVersionException`, `ClientTelemetryReporter.handleFailedRequest`
/// takes the non-retryable branch, suppresses the warn log for exactly that
/// exception, and sets the retry interval to `Integer.MAX_VALUE`. No request
/// reaches the wire and telemetry stays off for the life of the client.
///
/// [`tansu_storage::GetTelemetrySubscriptionsService`] is still correct and
/// still tested; it is what anything re-adding the route should inherit. Re-add
/// it with api_key 72, or not at all.
pub fn services<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    let registrations = [
        add_offsets_to_txn,
        add_partitions_to_txn,
        alter_client_quotas,
        alter_user_scram_credentials,
        consumer_group_describe,
        create_acls,
        create_topics,
        delete_acls,
        delete_groups,
        delete_records,
        delete_topics,
        describe_acls,
        describe_client_quotas,
        describe_cluster,
        describe_configs,
        describe_groups,
        describe_topic_partitions,
        describe_user_scram_credentials,
        end_txn,
        fetch,
        find_coordinator,
        incremental_alter_configs,
        init_producer_id,
        list_groups,
        list_offsets,
        list_partition_reassignments,
        metadata,
        produce,
        txn_offset_commit_request,
    ];
    let mut routes = builder;
    for register in registrations {
        routes = register(routes, storage.clone())?;
    }
    Ok(routes)
}

pub fn alter_client_quotas<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        AlterClientQuotasRequest,
        AlterClientQuotasService
    )
}

pub fn describe_client_quotas<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeClientQuotasRequest,
        DescribeClientQuotasService
    )
}

pub fn alter_user_scram_credentials<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        AlterUserScramCredentialsRequest,
        AlterUserScramCredentialsService
    )
}

pub fn describe_user_scram_credentials<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeUserScramCredentialsRequest,
        DescribeUserScramCredentialsService
    )
}

pub fn create_acls<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, CreateAclsRequest, CreateAclsService)
}

pub fn delete_acls<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, DeleteAclsRequest, DeleteAclsService)
}

pub fn describe_acls<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, DescribeAclsRequest, DescribeAclsService)
}

pub fn consumer_group_describe<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        ConsumerGroupDescribeRequest,
        ConsumerGroupDescribeService
    )
}

pub fn create_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, CreateTopicsRequest, CreateTopicsService)
}

pub fn delete_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, DeleteGroupsRequest, DeleteGroupsService)
}

pub fn delete_records<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, DeleteRecordsRequest, DeleteRecordsService)
}

pub fn delete_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, DeleteTopicsRequest, DeleteTopicsService)
}

pub fn describe_cluster<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeClusterRequest,
        DescribeClusterService
    )
}

pub fn describe_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeConfigsRequest,
        DescribeConfigsService
    )
}

pub fn describe_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeGroupsRequest,
        DescribeGroupsService
    )
}

pub fn describe_topic_partitions<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        DescribeTopicPartitionsRequest,
        DescribeTopicPartitionsService
    )
}

pub fn fetch<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, FetchRequest, FetchService)
}

pub fn find_coordinator<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        FindCoordinatorRequest,
        FindCoordinatorService
    )
}

pub fn incremental_alter_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        IncrementalAlterConfigsRequest,
        IncrementalAlterConfigsService
    )
}

pub fn init_producer_id<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        InitProducerIdRequest,
        InitProducerIdService
    )
}

pub fn list_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, ListGroupsRequest, ListGroupsService)
}

pub fn list_offsets<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, ListOffsetsRequest, ListOffsetsService)
}

pub fn list_partition_reassignments<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        ListPartitionReassignmentsRequest,
        ListPartitionReassignmentsService
    )
}

pub fn metadata<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, MetadataRequest, MetadataService)
}

/// Kafka's `acks=0`: the request is served and **not answered** (#440).
///
/// The client registers no handler for it, does not wait for it, and considers
/// the record delivered the moment the request is written to the socket. A
/// response it never asked for arrives carrying a correlation id it has no
/// in-flight request for, and a client that meets one drops the connection —
/// taking everything still queued on it with it, while the delivery reports it
/// already fired say the records were sent. Nothing reports a loss.
///
/// A layer here rather than inside `ProduceService` because the decision
/// belongs to the *protocol*, not to storage: `tansu-storage` cannot see
/// [`NoResponse`], which lives with the connection loop that acts on it. The
/// records are still written and still awaited — `acks=0` withholds the answer,
/// it does not withhold the durability.
///
/// The same layer decides whether a produce may be **pipelined** (#588): the
/// connection reads its next request once this one's batches are in their
/// windows, rather than once they are durable. Every produce with `acks != 0`
/// is. Pipelining puts two requests for one partition into different windows,
/// and the earlier window can fail while the later one succeeds — but a client
/// can only have sent the later request while the earlier was unanswered, and
/// on that connection the broker could never have stopped it: unpipelined, the
/// later request is read after the earlier fails and lands ahead of its retry
/// all the same. An idempotent batch is refused `OUT_OF_ORDER_SEQUENCE_NUMBER`
/// behind the gap either way; a batch without a sequence is reordered either
/// way, which is Kafka's contract for `max.in.flight` above one without
/// idempotence, and what a snapshot producer that reads each key once asks for.
/// `acks=0` does not pipeline: its answer is silence either way.
#[derive(Clone, Debug)]
struct Acks<S>(S);

impl<State, S> Service<State, ProduceRequest> for Acks<S>
where
    S: Service<State, ProduceRequest>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        mut ctx: Context<State>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        if req.acks != 0
            && let Some(admission) = ctx.get::<Admission>().cloned()
        {
            _ = ctx.insert(OnAdmitted::new(move || admission.admitted()));
        }

        if req.acks == 0 {
            if let Some(no_response) = ctx.get::<NoResponse>() {
                no_response.set();
            } else {
                // Unreachable through the broker's own stack — the connection
                // loop inserts one — but worth a line rather than a shrug: the
                // symptom is a client that keeps dropping its connection for no
                // reason a broker log would explain.
                warn!(
                    "no acks=0 signal on this connection: answering a request that gets no answer"
                );
            }
        }

        self.0.serve(ctx, req).await
    }
}

pub fn produce<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, ProduceRequest, Acks(ProduceService))
}

pub fn add_offsets_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        AddOffsetsToTxnRequest,
        TxnAddOffsetsService
    )
}

pub fn add_partitions_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        AddPartitionsToTxnRequest,
        TxnAddPartitionService
    )
}

/// `EndTxn` (api key 26): commit or abort a transaction.
///
/// Absent until #441. [`Storage::txn_end`] was implemented and tested, but with
/// no route the API was not advertised — so `commit_transaction` and
/// `abort_transaction` both failed `_UNSUPPORTED_FEATURE` on a client that the
/// *other* four transaction APIs had already let write records. The route is
/// what makes the transaction machinery reachable over the wire.
pub fn end_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(builder, storage, EndTxnRequest, TxnEndService)
}

pub fn txn_offset_commit_request<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    storage_route!(
        builder,
        storage,
        TxnOffsetCommitRequest,
        TxnOffsetCommitService
    )
}
