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

use std::time::{Duration, SystemTime};

use object_store::memory::InMemory;

use crate::{
    GroupDetail, Result, Storage,
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
