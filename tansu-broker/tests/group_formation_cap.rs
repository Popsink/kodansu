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

//! A consumer group forms under the GCS per-object write cap (#427).
//!
//! GCS caps updates to a single object at about one write per second, and the
//! `gs` arm of `StorageContainer::builder` holds the fleet to that with
//! [`PutRateLimiter`] (#13). `generation.json` is the one object a group's
//! members contend on, and until #427 every member admitted itself with its own
//! CAS: measured over exactly this arrangement, sixteen members racing cost ~57
//! attempts and **~52 seconds**, which is past the 45 s default
//! `session.timeout.ms` of a Kafka 3.x/4.x client. The dead-member sweep then
//! evicted members that had not been admitted yet and the group re-formed into
//! the same wall — a permanent rebalance loop, not a slow one.
//!
//! `dynostore::tests::gcs` measured that against the store directly, which is
//! where the cost is but not where the fix is. This drives
//! [`Controller::join`], so what it bounds is the thing a client waits for.
//!
//! The limiter is a **local delay** — it waits rather than sending — so
//! composing it over `InMemory` reproduces the cap exactly, with no bucket, no
//! credentials and no Docker. What it does not reproduce is GCS itself: the 429
//! body and the per-bucket write ramp still need a real bucket (#429).
//!
//! Wall clock throughout, and a real one: `governor` keeps its own monotonic
//! clock, which `tokio`'s paused time never reaches.
//!
//! Gated on `dynostore`, which is what `DynoStore` and [`PutRateLimiter`] are
//! behind: `check-no-default-features` builds every target of every crate
//! without it, and an integration test is a target.

#![cfg(feature = "dynostore")]

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_broker::{
    Result,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use tansu_sans_io::{Body, ErrorCode, join_group_request::JoinGroupRequestProtocol};
use tansu_storage::{DynoStore, PutRateLimiter, Storage};
use tokio::{task::JoinSet, time::Instant};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const GROUP_ID: &str = "capped";
const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";
const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: Option<i32> = Some(300_000);

/// The default `session.timeout.ms` of a Kafka 3.x/4.x consumer, which is the
/// deadline a formation has to beat: past it the sweep evicts members that have
/// not been admitted yet, and the group re-forms into the same wall.
const SESSION_TIMEOUT: Duration = Duration::from_millis(SESSION_TIMEOUT_MS as u64);

/// Members racing to join one group. Sixteen because that is the size #427 was
/// filed on and the size the ~52 s was measured at.
const MEMBERS: usize = 16;

/// Exactly what `StorageContainer::builder`'s `gs` arm wraps the store in.
fn gcs_shaped<O>(inner: O) -> PutRateLimiter<O> {
    PutRateLimiter::new(inner, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
}

fn protocols() -> Vec<JoinGroupRequestProtocol> {
    vec![
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"metadata")),
    ]
}

/// A group of [`MEMBERS`] forms inside a client's session timeout over a store
/// capped as GCS caps one (#427).
///
/// The bound is the session timeout because that is what breaking it costs: not
/// a slow join, but a group that cannot converge. The margin is large on purpose
/// — what the assertion has to survive is a loaded CI runner, and what it has to
/// catch is a regression of the *shape*, which is a factor of ten away and not a
/// few hundred milliseconds.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_forms_inside_a_session_under_the_per_object_cap() -> Result<()> {
    let storage = Arc::new(DynoStore::new(CLUSTER, NODE, gcs_shaped(InMemory::new())));
    let controller = Controller::with_storage(storage.clone())?;

    let started = Instant::now();
    let mut joining = JoinSet::new();

    for member in 0..MEMBERS {
        let controller = controller.clone();

        _ = joining.spawn(async move {
            let member_id = format!("m-{member:02}");

            let body = controller
                .join(
                    Some("console-consumer"),
                    GROUP_ID,
                    SESSION_TIMEOUT_MS,
                    REBALANCE_TIMEOUT_MS,
                    &member_id,
                    None,
                    PROTOCOL_TYPE,
                    Some(&protocols()[..]),
                    None,
                )
                .await?;

            let Body::JoinGroupResponse(join) = body else {
                panic!("{body:?}")
            };

            assert_eq!(i16::from(ErrorCode::None), join.error_code, "{member_id}");
            assert_eq!(member_id, join.member_id);

            Ok::<_, tansu_broker::Error>(())
        });
    }

    while let Some(joined) = joining.join_next().await {
        joined.expect("member")?;
    }

    let elapsed = started.elapsed();

    let generation = storage
        .read_group_generation(GROUP_ID)
        .await?
        .expect("the group has a generation")
        .0;

    assert_eq!(
        MEMBERS,
        generation.members.len(),
        "every member is in the group it joined: {:?}",
        generation.members.keys().collect::<Vec<_>>()
    );

    assert!(
        elapsed < SESSION_TIMEOUT,
        "{MEMBERS} members took {elapsed:?} to form a group, against a client \
         session timeout of {SESSION_TIMEOUT:?}"
    );

    // Why it fits, stated as the count rather than left to the clock. `seq` is
    // bumped by every CAS that lands on `generation.json` and the group is
    // fresh, so nothing else has touched it: **two** writes for sixteen
    // members, measured — the create, and one batch admitting everyone whose
    // document had not been written when the creator read the listing.
    //
    // The clock assertion above passes at one write per member too, at 16s;
    // this one does not, and 16s is what a group of fifty cannot afford.
    let budget = (MEMBERS / 4) as u64;

    assert!(
        generation.seq <= budget,
        "{MEMBERS} members cost {} writes of generation.json against a budget \
         of {budget}, in {elapsed:?}",
        generation.seq,
    );

    Ok(())
}
