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

//! KIP-848 liveness stamps, `liveness/{member}-{stamp}` (#627).

use std::collections::{BTreeMap, BTreeSet};

use object_store::memory::InMemory;

use crate::{
    ConsumerGroupDoc, Result, Storage,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A member id is the client's to choose (KIP-1082), so one with a delimiter
/// in it, or the separator the key uses, lists back as itself.
#[tokio::test]
async fn stamps_list_back_by_member_and_are_deleted_by_stamp() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let awkward = "a/b-c";

    storage.renew_consumer_member("g", "m-1", 1_000).await?;
    storage.renew_consumer_member("g", "m-1", 2_000).await?;
    storage.renew_consumer_member("g", awkward, 1_500).await?;
    storage.renew_consumer_member("other", "m-1", 9_000).await?;

    assert_eq!(
        BTreeMap::from([
            ("m-1".to_owned(), BTreeSet::from([1_000, 2_000])),
            (awkward.to_owned(), BTreeSet::from([1_500])),
        ]),
        storage.list_consumer_member_stamps("g").await?
    );

    storage
        .delete_consumer_member_stamps(
            "g",
            &[("m-1".to_owned(), 1_000), (awkward.to_owned(), 1_500)],
        )
        .await?;

    assert_eq!(
        BTreeMap::from([("m-1".to_owned(), BTreeSet::from([2_000]))]),
        storage.list_consumer_member_stamps("g").await?
    );

    Ok(())
}

/// What a stamp proves is folded into the document's view, and only forward.
#[tokio::test]
async fn a_stamp_only_moves_a_member_forward() -> Result<()> {
    let _guard = init_tracing()?;

    let mut group = ConsumerGroupDoc::default();
    _ = group.members.insert(
        "m-1".into(),
        crate::ConsumerMember {
            last_contact_ms: 5_000,
            ..Default::default()
        },
    );
    _ = group.members.insert(
        "m-2".into(),
        crate::ConsumerMember {
            last_contact_ms: 5_000,
            ..Default::default()
        },
    );

    let seen = group.with_contacts(&BTreeMap::from([
        ("m-1".to_owned(), BTreeSet::from([1_000, 9_000])),
        ("m-2".to_owned(), BTreeSet::from([1_000])),
        ("gone".to_owned(), BTreeSet::from([9_000])),
    ]));

    assert_eq!(9_000, seen.members["m-1"].last_contact_ms);
    assert_eq!(5_000, seen.members["m-2"].last_contact_ms);
    assert!(!seen.members.contains_key("gone"));

    Ok(())
}
