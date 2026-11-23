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

//! A group's concurrent offset commits, folded into one write of its
//! `offsets.json`.
//!
//! Every `OffsetCommit` is one conditional write of the group's single offsets
//! object (#406). On `gs://` writes to one object name are paced at one per
//! second ([`crate::PutRateLimiter`], GCS's documented per-object cap), and a
//! large group's members commit on the same auto-commit interval, so their
//! commits queue on one key. They did worse than queue: each was its own CAS, a
//! loser re-read and re-applied, and every attempt waited out the pacing again.
//! Sixteen members committing at once took **~66 s**, p50 ~39 s, past a Kafka
//! client's 30 s `request.timeout.ms` (`tests/offset_commit_cap.rs`).
//!
//! This is #427's answer applied to the other per-group object: the commits
//! that arrive while a write of the group is in flight are written together by
//! the next one. Within a replica, one write is in flight per group at a time;
//! whoever takes the turn writes everything pending, in arrival order, and
//! every commit it carried is answered with that write's outcome. A burst of
//! any size costs two writes — the one already in flight and the one that
//! carries the rest.
//!
//! Replicas do not coordinate, and on `gs://` that was the rest of the problem
//! (DEV-663). Sixty-four members committing every second through three
//! replicas measured p99 35 s here and 29–80 s on a real bucket, with up to 105
//! retries for one write. Each replica's turn met the others' in the CAS, and
//! the loser paid twice: [`OptiCon::with_mut`] re-read the object *then* sat out
//! a second of [`crate::PutRateLimiter`] pacing, so the etag it carried was a
//! second old when it reached the bucket — long enough for another replica to
//! have moved it. On a hot object that is a loop, not a retry.
//!
//! So on a store that paces writes of one object (`object_write_interval`,
//! [`super::config::Tuning`]), the turn owns its retry:
//!
//! - **a loser waits, then re-reads.** The pacing is sat out before the re-read,
//!   so the etag a retry presents is milliseconds old rather than a second;
//! - **a retry carries what arrived meanwhile.** Commits queued on this replica
//!   while the turn waited are folded into its next attempt instead of waiting
//!   for a turn of their own behind it;
//! - **a contended replica backs off.** Each conflict lengthens the spacing
//!   between this replica's writes of the group by one interval, up to
//!   [`MAX_SPACING`] intervals, and each write that lands at the first attempt
//!   shortens it by a quarter of one. Replicas that keep meeting spread their
//!   turns out until they stop meeting — `n` replicas settle near `n` intervals
//!   apart, the object's one write per interval shared between them — and a
//!   loser, retrying after one interval, goes ahead of a winner spaced further
//!   out;
//! - **a contended replica re-reads before its turn.** The etag it holds is
//!   from its own last write, which other replicas have since moved: presenting
//!   it would lose for nothing, and cost an interval.
//!
//! The alternatives were weighed in DEV-663. Routing a group's commits to one
//! replica is group forwarding, which #360 removed because it made replicas
//! non-interchangeable (peer DNS, an internal listener, pod identity); sharding
//! `offsets.json` per writer gives back the reads #406 and #415 traded away and
//! needs a cross-shard order a stateless fleet does not have. This keeps one
//! object and stateless replicas, and bounds the cost of a hot group at about
//! one write per interval per replica rather than removing it.
//!
//! On `s3://` and `memory://` there is no spacing: a loser re-reads and retries
//! at once, as [`OptiCon::with_mut`] did, and still carries what arrived.

use std::{sync::Mutex, time::Duration};

use object_store::ObjectStore;
use tokio::{
    sync::oneshot::{self, error::TryRecvError},
    time::Instant,
};
use tracing::warn;

use super::{GroupOffsets, OptiCon};
use crate::{OffsetCommitRequest, Result, Topition};

/// The longest spacing a contended replica keeps between its writes of one
/// group, in object write intervals: four seconds on `gs://`, so a commit that
/// arrives just after its replica's turn waits at most about that plus the
/// write. Four replicas share the object's write rate without meeting; more
/// than that meet more often, and each meeting costs a loser one interval.
const MAX_SPACING: u32 = 4;

/// Retries of one turn past which the group is logged as contended, as
/// [`OptiCon::with_mut`] logs a hot object.
const RETRY_WARN_THRESHOLD: u32 = 8;

/// One commit waiting for a turn: what it writes, and who it answers.
#[derive(Debug)]
struct Pending {
    offsets: Vec<(Topition, OffsetCommitRequest)>,
    done: oneshot::Sender<Result<()>>,
}

/// How this replica spaces its writes of one group's `offsets.json`.
#[derive(Debug, Default)]
struct Pacing {
    /// The spacing between this replica's writes of the group: one interval
    /// while uncontended, up to [`MAX_SPACING`] intervals while other replicas
    /// keep winning the object first.
    spacing: Duration,

    /// When this replica last wrote the group.
    written_at: Option<Instant>,
}

impl Pacing {
    /// When this replica may next write the group.
    fn next_write(&self, interval: Duration) -> Option<Instant> {
        self.written_at
            .map(|written_at| written_at + self.spacing.max(interval))
    }

    /// Whether other replicas have been writing the group: then the etag this
    /// replica holds from its own last write is almost surely stale, and a
    /// write presenting it would lose for nothing.
    fn contended(&self, interval: Duration) -> bool {
        self.spacing > interval
    }

    /// Record a conflict: lengthen the spacing now, not when the write lands,
    /// so the next turn is spaced out even while this one is still retrying.
    fn conflicted(&mut self, interval: Duration) {
        self.spacing = (self.spacing.max(interval) + interval).min(interval * MAX_SPACING);
    }

    /// Record a write that landed, after `retries` conflicts: a clean one
    /// shortens the spacing by a quarter interval.
    fn written(&mut self, interval: Duration, retries: u32) {
        if retries == 0 {
            self.spacing = self.spacing.saturating_sub(interval / 4).max(interval);
        }
        self.written_at = Some(Instant::now());
    }
}

/// The commit queue of one group, on this replica.
#[derive(Debug, Default)]
pub(super) struct GroupCommits {
    pending: Mutex<Vec<Pending>>,

    /// Held for the length of a write, and what it holds is the group's pacing,
    /// which only a writer reads. A `tokio` mutex because it is held across the
    /// write's `await`, and because it is fair: a commit waiting for the turn
    /// gets it in arrival order.
    turn: tokio::sync::Mutex<Pacing>,
}

impl GroupCommits {
    /// Commit `offsets` to the group's `offsets.json`, together with whatever
    /// else is pending for the group when this commit's turn comes — or answer
    /// with the outcome of the write that already carried it.
    ///
    /// `interval` is the store's spacing between two writes of one object, if
    /// it has one; see the module documentation for what the turn does with it.
    ///
    /// Safe to cancel. A commit whose writer is dropped mid-write gets a closed
    /// channel instead of an outcome and queues itself again; applying an
    /// offset twice stores it once.
    pub(super) async fn commit(
        &self,
        offsets: &[(Topition, OffsetCommitRequest)],
        group: &OptiCon<GroupOffsets>,
        object_store: &impl ObjectStore,
        interval: Option<Duration>,
    ) -> Result<()> {
        loop {
            let (done, mut outcome) = oneshot::channel();
            self.pending.lock()?.push(Pending {
                offsets: offsets.to_vec(),
                done,
            });

            let mut pacing = self.turn.lock().await;

            match outcome.try_recv() {
                Ok(written) => return written,
                Err(TryRecvError::Closed) => continue,
                Err(TryRecvError::Empty) => (),
            }

            let mut carried = vec![];
            let written = self
                .write(&mut carried, &mut pacing, group, object_store, interval)
                .await;

            for pending in carried {
                _ = pending.done.send(written.clone());
            }

            if let Ok(written) = outcome.try_recv() {
                return written;
            }
        }
    }

    /// One turn: write everything pending into `carried`, retrying on conflict
    /// until it lands or fails, and folding in what arrives while it waits.
    async fn write(
        &self,
        carried: &mut Vec<Pending>,
        pacing: &mut Pacing,
        group: &OptiCon<GroupOffsets>,
        object_store: &impl ObjectStore,
        interval: Option<Duration>,
    ) -> Result<()> {
        if let Some(interval) = interval {
            if let Some(next_write) = pacing.next_write(interval) {
                tokio::time::sleep_until(next_write).await;
            }

            if pacing.contended(interval) {
                group.reload(object_store).await?;
            }
        }

        let mut retries = 0;
        loop {
            carried.append(&mut *self.pending.lock()?);

            let attempt = group
                .try_mut(object_store, |stored| {
                    for (topition, commit) in carried.iter().flat_map(|pending| &pending.offsets) {
                        stored.insert(topition, commit.clone());
                    }

                    Ok(())
                })
                .await?;

            if let Some(written) = attempt {
                if let Some(interval) = interval {
                    pacing.written(interval, retries);
                }
                return written;
            }

            retries += 1;
            if retries >= RETRY_WARN_THRESHOLD {
                warn!(
                    retries,
                    carried = carried.len(),
                    "offset commits contending"
                );
            }

            if let Some(interval) = interval {
                pacing.conflicted(interval);
                tokio::time::sleep(interval).await;
            }
            group.reload(object_store).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, sync::Arc, time::Duration};

    use object_store::{memory::InMemory, path::Path};

    use super::*;
    use crate::PutRateLimiter;

    fn commit(partition: i32, offset: i64) -> Vec<(Topition, OffsetCommitRequest)> {
        vec![(
            Topition::new("t", partition),
            OffsetCommitRequest::default().offset(offset),
        )]
    }

    /// Conflicts space a replica's writes out to [`MAX_SPACING`] intervals and
    /// no further; clean writes bring it back to one interval and no closer.
    #[test]
    fn spacing_grows_with_conflicts_and_shrinks_with_clean_writes() {
        let interval = Duration::from_secs(1);
        let mut pacing = Pacing::default();
        assert!(!pacing.contended(interval));

        for _ in 0..10 {
            pacing.conflicted(interval);
        }
        pacing.written(interval, 10);
        assert_eq!(interval * MAX_SPACING, pacing.spacing);
        assert!(pacing.contended(interval));

        pacing.written(interval, 0);
        assert_eq!(interval * MAX_SPACING - interval / 4, pacing.spacing);

        for _ in 0..20 {
            pacing.written(interval, 0);
        }
        assert_eq!(interval, pacing.spacing);
        assert!(!pacing.contended(interval));
    }

    /// A commit carried by a writer that is dropped mid-write is not lost: the
    /// batch went with the writer, and the commit's closed channel sends it
    /// round again.
    ///
    /// `first` holds the turn, waiting out the pacing, while `carrier` and
    /// `carried` queue. `carrier` then takes the turn and both commits into its
    /// batch, waits out the pacing in turn, and is aborted there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_carried_by_a_cancelled_writer_is_written() -> Result<()> {
        let store = Arc::new(
            PutRateLimiter::new(InMemory::new(), Duration::from_mins(5))
                .with_rate_per_second(NonZeroU32::new(1)),
        );
        let group = OptiCon::<GroupOffsets>::path(Path::from("g/offsets.json"));
        let commits = Arc::new(GroupCommits::default());

        commits.commit(&commit(0, 1), &group, &*store, None).await?;

        let spawn = |partition| {
            let (commits, group, store) = (commits.clone(), group.clone(), store.clone());
            tokio::spawn(async move {
                commits
                    .commit(&commit(partition, 1), &group, &*store, None)
                    .await
            })
        };
        let pause = || tokio::time::sleep(Duration::from_millis(200));

        let first = spawn(1);
        pause().await;
        let carrier = spawn(2);
        pause().await;
        let carried = spawn(3);

        first.await.expect("first")?;
        pause().await;
        carrier.abort();
        carried.await.expect("carried")?;

        let stored = group.get_opt(&*store).await?.unwrap_or_default();
        assert_eq!(
            Some(1),
            stored.get(&Topition::new("t", 3)).map(|c| c.offset)
        );
        Ok(())
    }
}
