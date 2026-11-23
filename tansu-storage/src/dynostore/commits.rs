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
//! Replicas do not coordinate: two replicas committing for one group still meet
//! in the CAS, as before, so the fleet's bound is one write per replica per
//! turn rather than one per commit.

use std::sync::Mutex;

use object_store::ObjectStore;
use tokio::sync::oneshot::{self, error::TryRecvError};

use super::{GroupOffsets, OptiCon};
use crate::{OffsetCommitRequest, Result, Topition};

/// One commit waiting for a turn: what it writes, and who it answers.
#[derive(Debug)]
struct Pending {
    offsets: Vec<(Topition, OffsetCommitRequest)>,
    done: oneshot::Sender<Result<()>>,
}

/// The commit queue of one group, on this replica.
#[derive(Debug, Default)]
pub(super) struct GroupCommits {
    pending: Mutex<Vec<Pending>>,

    /// Held for the length of a write. A `tokio` mutex because it is held
    /// across the write's `await`, and because it is fair: a commit waiting for
    /// the turn gets it in arrival order.
    turn: tokio::sync::Mutex<()>,
}

impl GroupCommits {
    /// Commit `offsets` to the group's `offsets.json`, together with whatever
    /// else is pending for the group when this commit's turn comes — or answer
    /// with the outcome of the write that already carried it.
    ///
    /// Safe to cancel. A commit whose writer is dropped mid-write gets a closed
    /// channel instead of an outcome and queues itself again; applying an
    /// offset twice stores it once.
    pub(super) async fn commit(
        &self,
        offsets: &[(Topition, OffsetCommitRequest)],
        group: &OptiCon<GroupOffsets>,
        object_store: &impl ObjectStore,
    ) -> Result<()> {
        loop {
            let (done, mut outcome) = oneshot::channel();
            self.pending.lock()?.push(Pending {
                offsets: offsets.to_vec(),
                done,
            });

            let _turn = self.turn.lock().await;

            match outcome.try_recv() {
                Ok(written) => return written,
                Err(TryRecvError::Closed) => continue,
                Err(TryRecvError::Empty) => (),
            }

            let carried = std::mem::take(&mut *self.pending.lock()?);

            let written = group
                .with_mut(object_store, |stored| {
                    for (topition, commit) in carried.iter().flat_map(|pending| &pending.offsets) {
                        stored.insert(topition, commit.clone());
                    }

                    Ok(())
                })
                .await;

            for pending in carried {
                _ = pending.done.send(written.clone());
            }

            if let Ok(written) = outcome.try_recv() {
                return written;
            }
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

        commits.commit(&commit(0, 1), &group, &*store).await?;

        let spawn = |partition| {
            let (commits, group, store) = (commits.clone(), group.clone(), store.clone());
            tokio::spawn(
                async move { commits.commit(&commit(partition, 1), &group, &*store).await },
            )
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
