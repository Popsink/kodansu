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

//! The decomposed group objects through the storage trait (#359).

use std::collections::BTreeMap;

use bytes::Bytes;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use crate::{
    AssignmentDoc, AssignmentOutcome, Error, GenerationDoc, MemberDoc, Result, Storage,
    UpdateError,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

fn member(last_contact_ms: i64) -> MemberDoc {
    MemberDoc {
        last_contact_ms,
        session_timeout_ms: 45_000,
        ..Default::default()
    }
}

fn generation(generation_id: i32) -> GenerationDoc {
    GenerationDoc {
        generation_id,
        session_timeout_ms: 45_000,
        ..Default::default()
    }
}

fn assignment(generation_id: i32) -> AssignmentDoc {
    AssignmentDoc {
        generation_id,
        leader: "m-1".into(),
        protocol_type: "consumer".into(),
        protocol_name: "range".into(),
        assignments: BTreeMap::from([("m-1".to_owned(), Bytes::from_static(&[1, 2]))]),
        assigned_at_ms: 9,
    }
}

/// Each document lands on the key the layout says it does. Pinned as literal
/// paths because the layout is the contract with every other reader of the
/// bucket — a rename is a migration, not a refactor.
#[tokio::test]
async fn each_document_lands_where_the_layout_says() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = DynoStore::new(CLUSTER, NODE, bucket.clone());

    _ = storage
        .write_group_member("g-1", "m-1", member(1_000), None)
        .await
        .expect("member");

    _ = storage
        .update_group_generation("g-1", generation(7), None)
        .await
        .expect("generation");

    _ = storage
        .create_group_assignment("g-1", 7, assignment(7))
        .await?;

    for expected in [
        format!("clusters/{CLUSTER}/groups/consumers/g-1/members/m-1.json"),
        format!("clusters/{CLUSTER}/groups/consumers/g-1/generation.json"),
        format!("clusters/{CLUSTER}/groups/consumers/g-1/assignment/0000000007.json"),
    ] {
        assert!(
            bucket.get(&Path::from(expected.clone())).await.is_ok(),
            "{expected} is missing",
        );
    }

    Ok(())
}

/// A member document round-trips, and the CAS is what stops a stale writer:
/// the second write with the same version loses and is handed what is stored,
/// rather than clobbering a subscription a join has just changed.
#[tokio::test]
async fn a_member_write_with_a_spent_version_loses_and_is_handed_the_current() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let first = storage
        .write_group_member("g-1", "m-1", member(1_000), None)
        .await
        .expect("create");

    assert_eq!(
        Some(member(1_000)),
        storage
            .read_group_member("g-1", "m-1")
            .await?
            .map(|(doc, _)| doc)
    );

    let second = member(2_000).bumped();
    let version = storage
        .write_group_member("g-1", "m-1", second.clone(), Some(first.clone()))
        .await
        .expect("cas");

    match storage
        .write_group_member("g-1", "m-1", member(3_000), Some(first))
        .await
    {
        Err(UpdateError::Outdated { current, .. }) => assert_eq!(second, *current),
        otherwise => panic!("a spent version must lose the CAS: {otherwise:?}"),
    }

    // The winner's document is what is stored.
    assert_eq!(
        Some((second, version)),
        storage.read_group_member("g-1", "m-1").await?
    );

    Ok(())
}

/// Absent is an answer, not a failure: a group with no generation, a member
/// with no document, a generation with no assignment.
#[tokio::test]
async fn what_was_never_written_reads_as_absent() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert_eq!(None, storage.read_group_generation("g-1").await?);
    assert_eq!(None, storage.read_group_member("g-1", "m-1").await?);
    assert_eq!(None, storage.read_group_assignment("g-1", 7).await?);
    assert!(storage.list_group_members("g-1").await?.is_empty());

    // Deleting one is the outcome asked for, not an error.
    storage.delete_group_member("g-1", "m-1").await?;

    Ok(())
}

/// Members are listed by id, and a deleted one leaves the listing.
#[tokio::test]
async fn members_are_listed_by_id() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for member_id in ["m-1", "m-2"] {
        _ = storage
            .write_group_member("g-1", member_id, member(1_000), None)
            .await
            .expect("member");
    }

    // A second group's members must not appear in the first group's listing.
    _ = storage
        .write_group_member("g-2", "m-3", member(1_000), None)
        .await
        .expect("member");

    assert_eq!(
        vec!["m-1".to_owned(), "m-2".to_owned()],
        storage
            .list_group_members("g-1")
            .await?
            .into_keys()
            .collect::<Vec<_>>()
    );

    storage.delete_group_member("g-1", "m-1").await?;

    assert_eq!(
        vec!["m-2".to_owned()],
        storage
            .list_group_members("g-1")
            .await?
            .into_keys()
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// The generation CAS: a create wins once, and a stale version is handed the
/// current document so the caller can re-apply its change to it.
#[tokio::test]
async fn a_generation_is_created_once_and_then_only_cas_moves_it() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let version = storage
        .update_group_generation("g-1", generation(0), None)
        .await
        .expect("create");

    // A second create loses: this is how two replicas racing to birth the same
    // group cannot both win.
    match storage
        .update_group_generation("g-1", generation(0), None)
        .await
    {
        Err(UpdateError::Outdated { current, .. }) => assert_eq!(generation(0), *current),
        otherwise => panic!("a second create must lose: {otherwise:?}"),
    }

    let next = generation(1).bumped();
    _ = storage
        .update_group_generation("g-1", next.clone(), Some(version.clone()))
        .await
        .expect("cas");

    match storage
        .update_group_generation("g-1", generation(1), Some(version))
        .await
    {
        Err(UpdateError::Outdated { current, .. }) => assert_eq!(next, *current),
        otherwise => panic!("a spent version must lose the CAS: {otherwise:?}"),
    }

    Ok(())
}

/// The assignment is create-only, and the writer that finds it already there
/// adopts it. Overwriting would break the memoization every non-leader relies
/// on — an object that is immutable can be cached forever.
#[tokio::test]
async fn an_assignment_is_adopted_rather_than_overwritten() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert!(matches!(
        storage
            .create_group_assignment("g-1", 7, assignment(7))
            .await?,
        AssignmentOutcome::Created(_)
    ));

    let usurper = AssignmentDoc {
        leader: "m-2".into(),
        ..assignment(7)
    };

    assert_eq!(
        AssignmentOutcome::AlreadyExists(Box::new(assignment(7))),
        storage.create_group_assignment("g-1", 7, usurper).await?
    );

    assert_eq!(
        Some(assignment(7)),
        storage.read_group_assignment("g-1", 7).await?
    );

    Ok(())
}

/// Housekeeping removes the assignments below a generation and leaves the rest.
#[tokio::test]
async fn assignments_below_a_generation_are_swept() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for generation_id in 0..5 {
        _ = storage
            .create_group_assignment("g-1", generation_id, assignment(generation_id))
            .await?;
    }

    // Another group's assignments are not this group's to remove.
    _ = storage
        .create_group_assignment("g-2", 0, assignment(0))
        .await?;

    assert_eq!(3, storage.delete_group_assignments_before("g-1", 3).await?);

    for generation_id in 0..3 {
        assert_eq!(
            None,
            storage.read_group_assignment("g-1", generation_id).await?
        );
    }

    for generation_id in 3..5 {
        assert!(
            storage
                .read_group_assignment("g-1", generation_id)
                .await?
                .is_some()
        );
    }

    assert!(storage.read_group_assignment("g-2", 0).await?.is_some());

    // Idempotent: a second sweep of the same floor removes nothing.
    assert_eq!(0, storage.delete_group_assignments_before("g-1", 3).await?);

    Ok(())
}

/// A group id that normalises away is refused a write and reads as absent,
/// exactly as the state object's prefix is (#277). Writing one would put a
/// member document, a generation and an assignment at the root of the consumer
/// tree, where a group-id-shaped listing would then find them.
#[tokio::test]
async fn a_group_id_that_normalises_away_owns_no_documents() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for group_id in ["", "/", "///"] {
        assert!(matches!(
            storage
                .write_group_member(group_id, "m-1", member(1_000), None)
                .await,
            Err(UpdateError::Error(Error::Api(
                tansu_sans_io::ErrorCode::InvalidGroupId
            )))
        ));

        assert!(matches!(
            storage
                .update_group_generation(group_id, generation(0), None)
                .await,
            Err(UpdateError::Error(Error::Api(
                tansu_sans_io::ErrorCode::InvalidGroupId
            )))
        ));

        assert!(matches!(
            storage
                .create_group_assignment(group_id, 7, assignment(7))
                .await,
            Err(Error::Api(tansu_sans_io::ErrorCode::InvalidGroupId))
        ));

        assert_eq!(None, storage.read_group_generation(group_id).await?);
        assert_eq!(None, storage.read_group_member(group_id, "m-1").await?);
        assert!(storage.list_group_members(group_id).await?.is_empty());
        assert_eq!(
            0,
            storage.delete_group_assignments_before(group_id, 7).await?
        );
    }

    Ok(())
}

/// The same treatment for a member id: one that contributes no path component
/// would otherwise write the group's `members` prefix as an object, which then
/// shadows the folder every member document lives in.
#[tokio::test]
async fn a_member_id_that_normalises_away_owns_no_document() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for member_id in ["", "/", "///"] {
        assert!(matches!(
            storage
                .write_group_member("g-1", member_id, member(1_000), None)
                .await,
            Err(UpdateError::Error(Error::Api(
                tansu_sans_io::ErrorCode::InvalidGroupId
            )))
        ));

        assert_eq!(None, storage.read_group_member("g-1", member_id).await?);
    }

    Ok(())
}
