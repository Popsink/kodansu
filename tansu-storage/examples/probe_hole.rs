//! Verify a reported offset hole through the **read path** rather than through
//! the audit's coverage sweep (#399/#461).
//!
//! The audit walks a point-in-time listing. While compaction is draining a
//! backlog it deletes originals and creates merged segments underneath that
//! walk, so a segment created after the LIST is invisible while the originals
//! it replaced were listed — both sides missing reads as a hole that does not
//! exist. Two audit runs four minutes apart agreed on only 50% of their new
//! holes, and 170 of them "healed", which a real hole cannot do.
//!
//! `Storage::fetch` is the same call the broker serves `Fetch` with, and it
//! resolves against a *freshly refreshed* index each time, so it answers the
//! question the audit cannot while the bucket moves.
//!
//! Not committed: a scratch probe run against production with read-only
//! credentials.

use std::env;

use tansu_sans_io::IsolationLevel;
use tansu_storage::{Storage, StorageContainer, Topition};
use url::Url;

#[derive(serde::Deserialize)]
struct Hole {
    topic: String,
    partition: i32,
    lost_from: i64,
    lost_to: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = env::var("PROBE_BUCKET")?;
    let holes: Vec<Hole> = serde_json::from_reader(std::fs::File::open("/tmp/probe.json")?)?;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse(&bucket)?)
        .build()
        .await?;

    for hole in holes {
        let tp = Topition::new(hole.topic.as_str(), hole.partition);
        let short = hole.topic.rsplit('.').take(2).collect::<Vec<_>>().join(".");

        // Read from the last offset the audit says IS held, and from the first
        // it says is NOT. A hole that exists shows as the second fetch
        // returning offsets at/after `lost_to + 1` (or nothing at all).
        let mut line = format!(
            "{short}-{} [{}, {}]",
            hole.partition, hole.lost_from, hole.lost_to
        );

        for (label, at) in [("before", hole.lost_from - 1), ("inside", hole.lost_from)] {
            let served = storage
                .fetch(
                    &tp,
                    at,
                    1,
                    1024 * 1024,
                    IsolationLevel::ReadUncommitted,
                    std::time::Duration::from_millis(2_000),
                )
                .await;

            let span = match served {
                Ok(batches) if batches.is_empty() => "empty".to_owned(),
                Ok(batches) => {
                    let lo = batches.first().map(|b| b.base_offset).unwrap_or(-1);
                    let hi = batches
                        .last()
                        .map(|b| b.base_offset + b.last_offset_delta as i64)
                        .unwrap_or(-1);
                    format!("{lo}..{hi}")
                }
                Err(error) => format!("ERR {error}"),
            };

            line.push_str(&format!("   {label}@{at} -> {span}"));
        }

        println!("{line}");
    }

    Ok(())
}
