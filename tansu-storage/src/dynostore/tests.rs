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

use dotenv::dotenv;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, sync::Arc, thread};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use crate::{Error, Result, Topition};

use super::{DynoStore, Watermark};

mod compact_segments;
mod delete_groups;
mod group_describe;
mod group_expiry;
mod idempotent;
mod latency;
mod metadata_visibility;
mod offset_assignment;
mod offset_commit;
mod prefix_coalesce;
mod reachable_panics;
mod scaling;
mod segment;

pub(crate) fn init_tracing() -> Result<DefaultGuard, Error> {
    _ = dotenv().ok();

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[test]
fn range_check() {
    let map = BTreeMap::from([(3, "a"), (5, "b"), (8, "c")]);

    assert_eq!(Some((&3, &"a")), map.range(2..).next());
    assert_eq!(Some((&5, &"b")), map.range(4..).next());
    assert_eq!(None, map.range(9..).next());
}

#[test]
fn schema_change() -> Result<()> {
    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X0 {
        low: Option<i64>,
        high: Option<i64>,
    }

    let low = Some(6);
    let high = Some(66);

    let x0 = X0 { low, high };

    let encoded = serde_json::to_string(&x0)?;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X1 {
        low: Option<i64>,
        high: Option<i64>,
        timestamps: Option<BTreeMap<i64, i64>>,
    }

    let x1: X1 = serde_json::from_str(&encoded[..])?;

    assert_eq!(low, x1.low);
    assert_eq!(high, x1.high);
    assert!(x1.timestamps.is_none());

    Ok(())
}

/// `watermark.json` is round-tripped through `OptiCon::with_mut` on the
/// maintainer path every maintenance interval (`expire_prefix_segments`
/// persists `high`, `delete_records_before` the truncation floor), so a binary
/// whose [`Watermark`] does not model a field would silently erase it on
/// that round-trip: during a rolling deploy, an old maintainer would drop
/// what a newer process had just written. For the truncation floor (#176)
/// that erasure would resurrect records a user deleted — which is why the
/// `#[serde(flatten)]` catch-all had to be fleet-wide **before** the field
/// carried meaning. This pins the mechanism: keys seeded into the raw object
/// survive a `with_mut` that touches `high` — `truncate`, now a modeled
/// field, and `future`, standing in for whatever comes after #176 — and a
/// watermark with no extra fields still serialises byte-identically to the
/// pre-catch-all layout (`truncate` is `skip_serializing_if`), so the change
/// does not rewrite every existing object on first touch.
///
/// Since #180 it also pins that removal's migration guarantee: `low` is no longer a
/// modeled field, so an object written before it was dropped carries `"low"` as an
/// unknown key — and the catch-all is what keeps that key intact instead of erasing
/// it, which is why dropping the field needed no data migration.
#[tokio::test]
async fn watermark_with_mut_preserves_unknown_fields() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Byte identity first: an empty catch-all map must flatten to nothing,
    // and an absent truncation floor must serialise to nothing (#176) — a
    // fleet with no floors must not have every watermark object rewritten
    // with a `"truncate":null` key (etag churn across the whole cost plane).
    assert_eq!(
        r#"{"high":5}"#,
        serde_json::to_string(&Watermark {
            high: Some(5),
            ..Default::default()
        })?
    );

    // A held floor serialises as the named field, after `high`.
    assert_eq!(
        r#"{"high":5,"truncate":3}"#,
        serde_json::to_string(&Watermark {
            high: Some(5),
            truncate: Some(3),
            ..Default::default()
        })?
    );

    // Seed the raw object with the truncation floor plus a key this binary
    // does not model, exactly as a newer release would have written them.
    let bucket = InMemory::new();
    let topition = Topition::new("unknown-fields", 0);
    let path = Path::from(format!(
        "clusters/tansu/topics/{}/partitions/{:0>10}/watermark.json",
        topition.topic, topition.partition,
    ));
    _ = bucket
        .put(
            &path,
            PutPayload::from_static(br#"{"low":null,"high":5,"truncate":42,"future":17}"#),
        )
        .await?;

    let storage = DynoStore::new("tansu", 111, bucket.clone());
    storage
        .watermark(&topition)?
        .with_mut(&storage.object_store, |watermark| {
            watermark.high = Some(7);
            Ok(())
        })
        .await?;

    // Read the raw bytes back from the bucket, bypassing the store: the
    // mutation must have landed and both extra keys must have survived it
    // with their values intact.
    let raw = bucket.get(&path).await?.bytes().await?;
    let object = serde_json::from_slice::<serde_json::Value>(&raw)?;
    assert_eq!(Some(7), object["high"].as_i64());
    assert_eq!(Some(42), object["truncate"].as_i64());
    assert_eq!(Some(17), object["future"].as_i64());
    // The historic `low` is one of those unknown keys now (#180): present, and
    // preserved rather than erased.
    assert!(
        object.get("low").is_some(),
        "a pre-#180 object's `low` must survive the round-trip"
    );

    Ok(())
}

/// A `watermark.json` carrying a truncation floor — written by a #176-aware
/// pod, possibly round-tripped through a beta.23 pod's catch-all in between —
/// deserialises into the **named** `truncate` field (serde routes a named key
/// to the field, never to the `#[serde(flatten)]` catch-all).
///
/// Byte identity holds for the fields this binary models. It does **not** hold for
/// an object still carrying the `low` that #180 dropped: an unmodeled key lands in
/// the catch-all, which serialises after the named fields, so the key moves to the
/// end. The value survives — that is the guarantee that made dropping the field
/// safe — but the bytes are reordered, so such an object gets one rewrite the next
/// time something else on it changes. Bounded: `watermark.json` is only written when
/// a floor or a high watermark actually moves, never on a read.
#[test]
fn watermark_truncate_round_trips_as_a_named_field() -> Result<()> {
    let raw = r#"{"low":null,"high":5,"truncate":42}"#;

    let watermark = serde_json::from_str::<Watermark>(raw)?;
    assert_eq!(Some(42), watermark.truncate);
    // `truncate` is modeled so it does not land in the catch-all; `low` is not
    // modeled any more (#180) so it does, which is what preserves it.
    assert_eq!(
        vec!["low"],
        watermark.rest.keys().collect::<Vec<_>>(),
        "only the dropped `low` may land in rest"
    );

    // Same content, `low` relocated to the end by the catch-all (#180).
    assert_eq!(
        r#"{"high":5,"truncate":42,"low":null}"#,
        serde_json::to_string(&watermark)?
    );

    Ok(())
}
