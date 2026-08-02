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

//! `DescribeGroups` over many groups (#240).

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tansu_sans_io::ErrorCode;
use tokio::time::sleep;

use crate::{
    Error, GroupDetailResponse, Result, Storage,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Records how many `get_opts` calls are in flight at once, and holds each one
/// long enough for overlap to be observable.
///
/// Concurrency is asserted by counting rather than by timing: a wall-clock
/// assertion on a shared CI runner measures contention as much as it measures
/// the code, which is how a green test hides a serial fan-out.
#[derive(Clone)]
struct ConcurrencyRecording<O> {
    inner: O,
    hold: Duration,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl<O> ConcurrencyRecording<O> {
    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        _ = self.peak.fetch_max(now, SeqCst);
    }

    fn leave(&self) {
        _ = self.in_flight.fetch_sub(1, SeqCst);
    }
}

impl<O> Debug for ConcurrencyRecording<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrencyRecording").finish()
    }
}

impl<O> Display for ConcurrencyRecording<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrencyRecording").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for ConcurrencyRecording<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.enter();
        sleep(self.hold).await;
        let outcome = self.inner.get_opts(location, options).await;
        self.leave();
        outcome
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// #240: describing many groups must not serialize one round-trip per group.
///
/// The production symptom was `context deadline exceeded` on `group describe`,
/// and `listConsumerGroupOffsets` timing out inside a consumer's rebalance
/// callback — which threw, made Kafka discard the assignment, and left the group
/// re-joining forever. One awaited GET per group is tens of seconds at a few
/// hundred groups, past any admin client's deadline.
#[tokio::test]
async fn describing_many_groups_is_concurrent() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const GROUPS: usize = 64;

    let peak = Arc::new(AtomicUsize::new(0));
    let bucket = ConcurrencyRecording {
        inner: InMemory::new(),
        hold: Duration::from_millis(20),
        in_flight: Arc::new(AtomicUsize::new(0)),
        peak: peak.clone(),
    };

    let storage = DynoStore::new(CLUSTER, NODE, bucket);

    let group_ids: Vec<String> = (0..GROUPS).map(|i| format!("group-{i}")).collect();

    let described = storage.describe_groups(Some(&group_ids), false).await?;

    assert_eq!(GROUPS, described.len(), "one answer per group asked about");

    let peak = peak.load(SeqCst);
    assert!(
        peak > 1,
        "reads were serialized: peak in-flight was {peak}, so N groups cost N round-trips",
    );

    Ok(())
}

/// A store error that is not `NotFound` must be reported as retriable, not as an
/// empty group.
///
/// This used to answer `GroupDetail::default()` for any read failure, so a
/// throttle or a 5xx made a live group with members describe as empty — the same
/// shape as #214, where an unresolvable topic was reported absent and a client
/// could not tell it from a deleted one. Here it would tell an operator, or an
/// admin client mid-migration, that a group has no members and no offsets.
///
/// `NotFound` keeps answering with an empty group: that one is a fact.
#[tokio::test]
async fn a_failed_read_is_retriable_not_an_empty_group() -> Result<(), Error> {
    let _guard = init_tracing()?;

    /// Fails every read the way a throttled or unavailable store does.
    #[derive(Clone)]
    struct AlwaysUnavailable;

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
            Err(object_store::Error::Generic {
                store: "AlwaysUnavailable",
                source: "unavailable".into(),
            })
        }

        async fn put_multipart_opts(
            &self,
            _: &Path,
            _: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
            Err(object_store::Error::Generic {
                store: "AlwaysUnavailable",
                source: "unavailable".into(),
            })
        }

        async fn get_opts(
            &self,
            _: &Path,
            _: GetOptions,
        ) -> Result<GetResult, object_store::Error> {
            Err(object_store::Error::Generic {
                store: "AlwaysUnavailable",
                source: "unavailable".into(),
            })
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
            Err(object_store::Error::Generic {
                store: "AlwaysUnavailable",
                source: "unavailable".into(),
            })
        }

        async fn copy_opts(
            &self,
            _: &Path,
            _: &Path,
            _: CopyOptions,
        ) -> Result<(), object_store::Error> {
            Err(object_store::Error::Generic {
                store: "AlwaysUnavailable",
                source: "unavailable".into(),
            })
        }
    }

    let storage = DynoStore::new(CLUSTER, NODE, AlwaysUnavailable);

    let described = storage
        .describe_groups(Some(&[String::from("actively-consuming")]), false)
        .await?;

    assert_eq!(1, described.len());

    match &described[0].response {
        GroupDetailResponse::ErrorCode(code) => assert_eq!(
            ErrorCode::CoordinatorLoadInProgress,
            *code,
            "a read failure must be answered retriably",
        ),

        GroupDetailResponse::Found(detail) => panic!(
            "a live group was reported as an existing group with {} members \
             on a failed read: {detail:?}",
            detail.members.len(),
        ),
    }

    Ok(())
}
