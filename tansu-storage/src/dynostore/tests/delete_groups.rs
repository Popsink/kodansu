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

use object_store::memory::InMemory;
use tansu_sans_io::ErrorCode;

use crate::{
    GenerationDoc, Result, Storage,
    dynostore::{
        DynoStore,
        tests::{init_tracing, legacy_group_exists, seed_legacy_group},
    },
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Every spelling that normalises away — and only those — is refused a prefix.
///
/// The guard is structural rather than `is_empty()`, because normalisation is
/// what does the widening: [`Path`] drops empty components, so `"/"` and `"///"`
/// collapse onto the consumer tree root exactly as `""` does (#277).
///
/// [`Path`]: object_store::path::Path
#[test]
fn group_prefix_refuses_ids_that_normalise_to_the_root() {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for group_id in ["", "/", "///"] {
        assert_eq!(
            None,
            store.group_prefix(group_id),
            "{group_id:?} collapses onto the consumer tree root and must be refused"
        );
    }

    // A real id still yields the prefix that holds only that group.
    let prefix = store
        .group_prefix("group-a")
        .expect("a real id has a prefix");
    assert_eq!(
        format!("clusters/{CLUSTER}/groups/consumers/group-a"),
        prefix.as_ref()
    );
    assert_ne!(store.groups_root(), prefix);
}

/// `delete_groups` with an id that resolves to the consumer tree root deletes
/// nothing and reports `InvalidGroupId`.
///
/// Before #277 the prefix collapsed onto the root, so the scan enumerated every
/// group's state object and every committed offset in the cluster and
/// `delete_stream` deleted them all — from one `DeleteGroups` call, or from the
/// maintenance loop, since `expire_groups` derives its ids by stripping `.json`
/// off a listing and a stray object named exactly `.json` yields an empty one.
#[tokio::test]
async fn empty_group_id_deletes_nothing() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    // One of each: a group in the layout this binary writes, and a legacy
    // leftover. The refusal must take neither.
    let bystanders = ["group-a", "group-b"];
    for group in bystanders {
        _ = store
            .update_group_generation(group, GenerationDoc::default(), None)
            .await
            .expect("seed a generation");

        seed_legacy_group(&bucket, CLUSTER, group).await?;
    }

    let refused = ["".to_owned(), "/".to_owned(), "///".to_owned()];
    let results = store.delete_groups(Some(&refused)).await?;

    assert_eq!(refused.len(), results.len());
    for (index, result) in results.iter().enumerate() {
        assert_eq!(refused[index], result.group_id.as_str());
        assert_eq!(
            i16::from(ErrorCode::InvalidGroupId),
            result.error_code,
            "{:?} must be refused",
            refused[index]
        );
    }

    // The bystanders are still there: nothing was taken with the refusal.
    for group in bystanders {
        assert!(
            store.read_group_generation(group).await?.is_some(),
            "{group} was deleted by a refused group id"
        );
        assert!(
            legacy_group_exists(&bucket, CLUSTER, group).await,
            "{group}'s legacy object was deleted by a refused group id"
        );
    }

    Ok(())
}

/// A real group is still deleted — the guard rejects the widening case only.
#[tokio::test]
async fn a_named_group_is_still_deleted() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    for group in ["group-a", "group-b"] {
        _ = store
            .update_group_generation(group, GenerationDoc::default(), None)
            .await
            .expect("seed a generation");

        seed_legacy_group(&bucket, CLUSTER, group).await?;
    }

    let results = store.delete_groups(Some(&["group-a".to_owned()])).await?;

    assert_eq!(1, results.len());
    assert_eq!(
        i16::from(ErrorCode::None),
        results[0].error_code,
        "a named group must still be deletable"
    );

    assert!(store.read_group_generation("group-a").await?.is_none());
    assert!(
        !legacy_group_exists(&bucket, CLUSTER, "group-a").await,
        "deleting a group must take its legacy leftover with it"
    );

    assert!(store.read_group_generation("group-b").await?.is_some());
    assert!(legacy_group_exists(&bucket, CLUSTER, "group-b").await);

    Ok(())
}
