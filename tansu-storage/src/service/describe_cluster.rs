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

//! `DescribeCluster`: the brokers, the controller and the cluster id.

use tansu_sans_io::{DescribeClusterRequest, DescribeClusterResponse};

storage_service! {
    /// Lists the brokers [`Storage::brokers`](crate::Storage::brokers) knows,
    /// names the first as controller (`-1` if there is none) and echoes back
    /// the endpoint type the client asked about. Authorized operations are
    /// never computed, so they go back as Kafka's "not requested" sentinel,
    /// [`i32::MIN`].
    ///
    /// `tests/describe_cluster.rs` serves a request end to end.
    DescribeClusterService: DescribeClusterRequest => DescribeClusterResponse,
    |ctx, req| {
        let DescribeClusterRequest { endpoint_type, .. } = req;
        let storage = ctx.state();
        let brokers = storage.brokers().await?;
        let cluster_id = storage.cluster_id().await?;
        tracing::debug!(?brokers, cluster_id);

        let controller = brokers.first().map_or(-1, |broker| broker.broker_id);
        let response = DescribeClusterResponse::default()
            .endpoint_type(endpoint_type)
            .controller_id(controller)
            .cluster_id(cluster_id)
            .brokers(brokers.into())
            .cluster_authorized_operations(i32::MIN);
        Ok(response)
    }
}
