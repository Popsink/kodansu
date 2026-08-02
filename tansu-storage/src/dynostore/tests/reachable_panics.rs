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

//! Sites that used to panic the request task on input or conditions a client
//! controls (#276). Each test here panics rather than fails without the fix,
//! which is the point: an `Err` is a bounded answer, a panic is not.
//!
//! With neither `--authentication` nor TLS enabled, anything reachable from a
//! connection is reachable by anyone who can reach the port.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use std::fmt::{self, Debug, Display};
use tansu_sans_io::{
    ConfigResource, ErrorCode, OpType, incremental_alter_configs_request::AlterableConfig,
};

use crate::{
    Error, Result, Storage, TxnAddPartitionsRequest,
    dynostore::{DynoStore, TopicMetadata, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A foreign object under the committed-offsets prefix — shorter than the
/// 10-digit zero-padded partition names the broker writes — must be skipped,
/// not sliced.
///
/// `committed_offset_topitions` took `&component[0..10]` unconditionally, so any
/// such object panicked the request task. The broker never writes one, so this
/// needs a foreign or truncated object in the bucket rather than client input —
/// but a bucket is not a private namespace, and the panic is free to make
/// impossible.
#[tokio::test]
async fn a_short_partition_component_is_skipped() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let group = "group-a";

    // What the broker writes, and one foreign object beside it.
    for name in ["0000000000.json", "1.json"] {
        _ = bucket
            .put(
                &Path::from(format!(
                    "clusters/{CLUSTER}/groups/consumers/{group}/offsets/a-topic/partitions/{name}"
                )),
                PutPayload::from_static(b"{\"offset\":5}"),
            )
            .await?;
    }

    let store = DynoStore::new(CLUSTER, NODE, bucket);

    // The call completes. Before the fix it panicked on the short component.
    let committed = store.committed_offset_topitions(group).await?;

    assert!(
        committed.keys().all(|topition| topition.partition() == 0),
        "only the well-formed object may yield a topition, got {committed:?}"
    );

    Ok(())
}

/// Fails every read, the way a throttled or unavailable store does.
struct AlwaysUnavailable;

impl AlwaysUnavailable {
    fn unavailable() -> object_store::Error {
        object_store::Error::Generic {
            store: "AlwaysUnavailable",
            source: "unavailable".into(),
        }
    }
}

impl Debug for AlwaysUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlwaysUnavailable").finish()
    }
}

impl Display for AlwaysUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlwaysUnavailable").finish()
    }
}

#[async_trait]
impl ObjectStore for AlwaysUnavailable {
    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        Err(Self::unavailable())
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        Err(Self::unavailable())
    }

    async fn get_opts(&self, _: &Path, _: GetOptions) -> Result<GetResult, object_store::Error> {
        Err(Self::unavailable())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        locations
    }

    fn list(
        &self,
        _: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        Box::pin(futures::stream::empty())
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        Err(Self::unavailable())
    }

    async fn copy_opts(
        &self,
        _: &Path,
        _: &Path,
        _: CopyOptions,
    ) -> Result<(), object_store::Error> {
        Err(Self::unavailable())
    }
}

/// A transient object-store error while reading topic metadata must surface as
/// an error, not a panic.
///
/// This is the one that matters most: `describe_config` is not an admin-only
/// path. `topic_is_compacted` calls it, and that runs on **produce and fetch**
/// via `routed_prefix_of` whenever the memo misses — first use of a topic in a
/// process, or TTL expiry. Transient storage errors are routine, so this arm
/// was reachable in ordinary operation.
#[tokio::test]
async fn a_failed_topic_metadata_read_is_an_error() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, AlwaysUnavailable);

    assert!(
        store
            .describe_config("a-topic", ConfigResource::Topic, None)
            .await
            .is_err(),
        "a failed metadata read must be an error, not a panic"
    );

    Ok(())
}

/// `AddPartitionsToTxn` v4+ is not implemented, and must answer
/// `UnsupportedVersion` rather than panic.
///
/// No error condition was needed to reach it: a client picks the API version it
/// sends and is not bound by the advertised range.
#[tokio::test]
async fn add_partitions_to_txn_v4_is_unsupported() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    match store
        .txn_add_partitions(TxnAddPartitionsRequest::VersionFourPlus {
            transactions: vec![],
        })
        .await
    {
        Err(Error::Api(code)) => assert_eq!(ErrorCode::UnsupportedVersion, code),
        otherwise => panic!("expected UnsupportedVersion, got {otherwise:?}"),
    }

    Ok(())
}

/// `IncrementalAlterConfigs` APPEND/SUBTRACT are not implemented, and must be
/// refused rather than panic. `config_operation` is a wire field, so the client
/// chooses it.
#[test]
fn alter_configs_append_and_subtract_are_refused() {
    let mut metadata = TopicMetadata::default();

    // `OpType` has no `Debug`, so the wire value names the case on failure.
    for operation in [i8::from(OpType::Append), i8::from(OpType::Subtract)] {
        let change = AlterableConfig::default()
            .name("cleanup.policy".into())
            .config_operation(operation)
            .value(Some("compact".into()));

        match metadata.alter_configs(&[change]) {
            Err(Error::Api(code)) => assert_eq!(ErrorCode::InvalidConfig, code),
            otherwise => {
                panic!("expected InvalidConfig for config_operation {operation}, got {otherwise:?}")
            }
        }
    }
}
