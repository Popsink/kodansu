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

//! The cluster-global `meta.json` producer and transaction tables are never
//! pruned (#283), and this is the measurement that has to come before deciding
//! whether they need to be.
//!
//! Nothing here asserts a bound, because there is no policy yet to assert one
//! against — #81 retains aborted transactions on purpose, so the transaction half
//! needs a design decision rather than a prune. What these tests pin is that the
//! growth is real, that it is *linear in registrations rather than in producers
//! alive*, and that the numbers the maintenance tick reports are the ones an
//! operator would read off the production bucket.

use object_store::memory::InMemory;

use crate::{Error, Result, Storage, dynostore::DynoStore, dynostore::tests::init_tracing};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Register a producer as a plain idempotent producer does: `InitProducerId`
/// with no transactional id and `(-1, -1)`.
async fn init_producer(storage: &DynoStore) -> Result<i64> {
    storage
        .init_producer(None, 0, Some(-1), Some(-1))
        .await
        .map(|response| response.id)
}

/// Every `InitProducerId` appends a producer entry, and nothing ever removes
/// one — so the table counts registrations, not live producers. A connector that
/// restarts a hundred times leaves a hundred entries behind, and the cost that
/// matters is that `init_producer` round-trips the whole object: registration
/// gets more expensive the more producers the cluster has ever seen, which is
/// exactly backwards for the mass reconnect after an incident.
#[tokio::test]
async fn the_producer_table_counts_registrations_not_live_producers() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let (producers, transactions, empty_bytes) = storage.measure_meta().await?;
    assert_eq!(0, producers);
    assert_eq!(0, transactions);

    const REGISTRATIONS: u64 = 32;

    let mut ids = Vec::new();
    for _ in 0..REGISTRATIONS {
        ids.push(init_producer(&storage).await?);
    }

    // Distinct ids: each registration is a new entry, not a reuse of an old one.
    // This is what makes the growth monotonic rather than bounded by the number
    // of producers actually connected.
    let distinct = ids.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(REGISTRATIONS as usize, distinct.len());

    let (producers, transactions, bytes) = storage.measure_meta().await?;
    assert_eq!(REGISTRATIONS, producers);
    assert_eq!(0, transactions);

    // The bytes are the point: this is the payload every subsequent
    // `InitProducerId` GETs, parses, re-serialises and CAS-PUTs.
    assert!(
        bytes > empty_bytes,
        "meta.json must have grown: {bytes} vs {empty_bytes}"
    );

    // Per-entry cost, recorded so the growth math is in the tree rather than in
    // an issue comment: a lower bound of a few bytes per producer, which at the
    // observed restart rates is what makes this a measurement rather than an
    // incident. The assertion is deliberately loose — the exact serialisation is
    // not the contract, the linearity is.
    let per_entry = (bytes - empty_bytes) / REGISTRATIONS;
    assert!(
        per_entry > 0,
        "each producer entry must cost something: {per_entry}"
    );

    Ok(())
}

/// A transactional producer appends to *both* tables, and re-initialising the
/// same transactional id bumps its epoch in place rather than adding an entry —
/// so the transaction table is bounded by distinct transactional ids where the
/// producer table is not. That asymmetry is why the two are measured separately.
#[tokio::test]
async fn the_transaction_table_is_keyed_by_transactional_id() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for _ in 0..4 {
        _ = storage
            .init_producer(Some("txn-1"), 60_000, Some(-1), Some(-1))
            .await?;
    }

    let (producers, transactions, _) = storage.measure_meta().await?;
    assert_eq!(1, transactions, "one transactional id, one entry");
    assert_eq!(
        1, producers,
        "re-init of a known transactional id reuses its producer"
    );

    _ = storage
        .init_producer(Some("txn-2"), 60_000, Some(-1), Some(-1))
        .await?;

    let (producers, transactions, _) = storage.measure_meta().await?;
    assert_eq!(2, transactions);
    assert_eq!(2, producers);

    Ok(())
}
