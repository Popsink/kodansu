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

use std::{
    fmt::{self, Debug, Display},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::{StreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};

use crate::{
    GroupDetail, OffsetCommitRequest, Result, Storage, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A group state object untouched for longer than the retention window is
/// expired; a fresh one is left alone; and the expiry actually deletes the
/// object (a second sweep finds nothing).
#[tokio::test]
async fn expires_only_stale_groups() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for group in ["group-a", "group-b"] {
        _ = store
            .update_group(group, GroupDetail::default(), None)
            .await
            .expect("seed group state");
    }

    let now = SystemTime::now();

    // Freshly written groups are within the retention window → survive.
    assert_eq!(0, store.expire_groups(now).await?);

    // Eight days later both are past the 7-day retention window → expired.
    let later = now + Duration::from_secs(8 * 24 * 60 * 60);
    assert_eq!(2, store.expire_groups(later).await?);

    // They were actually removed: a second sweep finds nothing to expire.
    assert_eq!(0, store.expire_groups(later).await?);

    Ok(())
}

/// `read_group` returns the persisted group detail and the **same version**
/// `update_group` last returned, and `None` for an absent group (#111). The
/// coordinator's GET-first heartbeat/commit path relies on this version
/// matching: it is how a stale replica is detected (version differs → refresh)
/// and how a no-op is recognised (version equal + detail unchanged → skip the
/// PUT).
#[tokio::test]
async fn read_group_returns_persisted_detail_and_matching_version() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    // Absent group → None (not a default-valued Some).
    assert!(store.read_group("absent").await?.is_none());

    // Seed a group with a distinctive field.
    let detail = GroupDetail {
        generation_id: 7,
        ..GroupDetail::default()
    };
    let written = store
        .update_group("group-a", detail.clone(), None)
        .await
        .expect("seed group state");

    // read_group round-trips the detail and returns the version update_group gave.
    let (read_detail, read_version) = store.read_group("group-a").await?.expect("group present");
    assert_eq!(detail, read_detail);
    assert_eq!(
        written, read_version,
        "read_group version must equal the one update_group returned"
    );

    Ok(())
}

/// Reports every `{group}.json` directly under the consumer root as written
/// `age` ago, leaving every other object's timestamp alone.
///
/// This is the state a commit-only consumer is in, and real mtimes in a test
/// cannot express it: the state object is created by the first commit and then
/// never rewritten, while the offsets objects are rewritten on every commit. The
/// two are milliseconds apart when a test writes them, and eight days apart in
/// production.
#[derive(Clone)]
struct BackdateGroupState<O> {
    inner: O,
    age: Duration,
}

impl<O> BackdateGroupState<O> {
    /// Rewrite the timestamp of a group state object, and only that.
    fn backdate(&self, mut meta: ObjectMeta) -> ObjectMeta {
        let is_group_state = meta
            .location
            .parts()
            .next_back()
            .is_some_and(|name| name.as_ref().ends_with(".json"))
            && meta.location.parts().count() == 5;

        if is_group_state {
            meta.last_modified -= chrono::Duration::from_std(self.age).expect("representable");
        }

        meta
    }
}

impl<O> Debug for BackdateGroupState<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackdateGroupState").finish()
    }
}

impl<O> Display for BackdateGroupState<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackdateGroupState").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for BackdateGroupState<O>
where
    O: ObjectStore + Clone + 'static,
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
        self.inner.get_opts(location, options).await
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
        let this = self.clone();
        self.inner
            .list(prefix)
            .map(move |meta| meta.map(|meta| this.backdate(meta)))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await.map(|listed| {
            let objects = listed
                .objects
                .into_iter()
                .map(|meta| self.backdate(meta))
                .collect();

            ListResult { objects, ..listed }
        })
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

/// Seed a group with a committed offset, so it has both a state object and an
/// offsets subtree.
async fn seed_committing_group(store: &DynoStore, group: &str, topic: &str) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    _ = store
        .update_group(group, GroupDetail::default(), None)
        .await
        .expect("seed group state");

    let committed = store
        .offset_commit(
            group,
            None,
            &[(
                Topition::new(topic, 0),
                OffsetCommitRequest::default().offset(42),
            )],
        )
        .await?;

    assert_eq!(vec![(Topition::new(topic, 0), ErrorCode::None)], committed);

    Ok(())
}

/// **The converse test.** A group whose only activity is offset commits must
/// survive the retention window (#272).
///
/// The state object's mtime freezes at the first commit — subsequent commits
/// write the offsets objects and leave it alone — so age on that object alone
/// said "abandoned" about a consumer that was still committing. Expiry then
/// deleted the group state *and every committed offset under it*. An actively
/// committing consumer re-created them within one commit interval, so the loss
/// window was small; a paused or idle one lost its offsets outright.
#[tokio::test]
async fn a_group_that_only_commits_offsets_survives() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        BackdateGroupState {
            inner: bucket.clone(),
            // Past the 7-day window, so the state object is a candidate.
            age: Duration::from_secs(8 * 24 * 60 * 60),
        },
    );

    seed_committing_group(&store, "group-committing", "committed-topic").await?;

    // The state object reads as 8 days old, the committed offsets as current.
    // Before the fix the first fact decided and the offsets went with it.
    assert_eq!(
        0,
        store.expire_groups(SystemTime::now()).await?,
        "a group still committing offsets must not be expired",
    );

    // And it really is intact — both halves.
    assert!(
        store.read_group("group-committing").await?.is_some(),
        "the group state must survive",
    );
    assert_eq!(
        Some(&42),
        store
            .offset_fetch(
                Some("group-committing"),
                &[Topition::new("committed-topic", 0)],
                Some(false),
            )
            .await?
            .get(&Topition::new("committed-topic", 0)),
        "the committed offset must survive — losing it resets the consumer",
    );

    Ok(())
}

/// The other half: the fix must not disable expiry. A group with nothing under
/// it is still reclaimed, which is #45's original purpose.
#[tokio::test]
async fn an_abandoned_group_is_still_reclaimed() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        BackdateGroupState {
            inner: bucket.clone(),
            age: Duration::from_secs(8 * 24 * 60 * 60),
        },
    );

    // State object only: no commits, so no offsets subtree to vouch for it.
    _ = store
        .update_group("group-abandoned", GroupDetail::default(), None)
        .await
        .expect("seed group state");

    assert_eq!(
        1,
        store.expire_groups(SystemTime::now()).await?,
        "a group with no committed-offset activity is still reclaimed",
    );

    assert!(store.read_group("group-abandoned").await?.is_none());

    Ok(())
}
