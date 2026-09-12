// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Consumer-group documents (#359): the decomposed objects under
//! `consumers/`, the view that composes them back, and the expiry and
//! member-document reclaim that bound them.

use super::*;

impl GroupOffsets {
    pub(super) fn get(&self, topition: &Topition) -> Option<&OffsetCommitRequest> {
        self.committed
            .get(topition.topic())
            .and_then(|partitions| partitions.get(&topition.partition()))
    }

    pub(super) fn insert(&mut self, topition: &Topition, commit: OffsetCommitRequest) {
        _ = self
            .committed
            .entry(topition.topic().to_owned())
            .or_default()
            .insert(topition.partition(), commit);
    }

    /// Drop every partition of `topic`, answering whether anything went. Called
    /// when a topic is deleted: a committed offset that outlives its topic is
    /// served against the recreated one, which is #241's shape.
    pub(super) fn remove_topic(&mut self, topic: &str) -> bool {
        self.committed.remove(topic).is_some()
    }

    pub(super) fn topitions(&self) -> impl Iterator<Item = Topition> + '_ {
        self.committed.iter().flat_map(|(topic, partitions)| {
            partitions
                .keys()
                .map(move |partition| Topition::new(topic.clone(), *partition))
        })
    }
}

impl DynoStore {
    /// The root of the consumer tree: every group's state object and every
    /// committed offset in the cluster lives under this prefix.
    pub(super) fn groups_root(&self) -> Path {
        Path::from(format!(
            "clusters/{}/groups/consumers/",
            self.identity.cluster
        ))
    }

    /// The prefix holding everything owned by `group_id`, or `None` when
    /// `group_id` contributes no path component of its own.
    ///
    /// [`Path`] drops empty components on normalisation, so an empty group id —
    /// or one made only of delimiters — does not narrow the prefix at all: it
    /// collapses onto [`Self::groups_root`]. Handing that to a `delete_stream`
    /// deletes every group and every committed offset in the cluster (#277), so
    /// this returns the widening case as `None` rather than a prefix that
    /// silently means "everything".
    ///
    /// The check is structural — built prefix against root — rather than
    /// `group_id.is_empty()`, because normalisation is what does the widening:
    /// `""`, `"/"` and `"///"` all produce the same root.
    pub(super) fn group_prefix(&self, group_id: &str) -> Option<Path> {
        let prefix = Path::from(format!(
            "clusters/{}/groups/consumers/{}",
            self.identity.cluster, group_id,
        ));

        (prefix != self.groups_root()).then_some(prefix)
    }

    /// The optimistic-concurrency handle on a group's `offsets.json` (#406), or
    /// `None` for the widening group id [`Self::group_prefix`] refuses (#277).
    ///
    /// Memoized per group so the conditional GET a commit pays is answered from
    /// the etag memo. It sits *beside* the `offsets/` prefix rather than under it,
    /// so every listing that walks that prefix — the topition-set discovery, and
    /// `delete_topic`'s per-topic sweep — is unchanged by its existence.
    pub(super) fn group_offsets(&self, group_id: &str) -> Result<Option<OptiCon<GroupOffsets>>> {
        let Some(prefix) = self.group_prefix(group_id) else {
            return Ok(None);
        };

        self.clients.group_offsets(group_id, &prefix).map(Some)
    }

    /// `members/` under a group's prefix, or `None` for the widening group id
    /// [`Self::group_prefix`] refuses (#277).
    pub(super) fn group_members_prefix(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/members")))
    }

    /// A member's own document (#359). `None` when either id contributes no
    /// path component: a member id that normalises away would otherwise write
    /// the group's `members` prefix itself as an object.
    pub(super) fn group_member_location(&self, group_id: &str, member_id: &str) -> Option<Path> {
        let prefix = self.group_members_prefix(group_id)?;
        let location = Path::from(format!("{prefix}/{member_id}.json"));

        (location != Path::from(format!("{prefix}/.json"))).then_some(location)
    }

    /// The member id a listed `members/` object names, or `None` for anything
    /// under that prefix which is not a member document.
    pub(super) fn listed_member_id(location: &Path) -> Option<String> {
        location
            .parts()
            .next_back()
            .and_then(|name| name.as_ref().strip_suffix(".json").map(ToOwned::to_owned))
    }

    /// A group's composition document (#359).
    pub(super) fn group_generation_location(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/generation.json")))
    }

    /// `assignment/` under a group's prefix (#359).
    pub(super) fn group_assignments_prefix(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/assignment")))
    }

    /// A generation's immutable assignment (#359). Zero-padded so the listing
    /// is in generation order, as `{seq}.seg` is for segments.
    ///
    /// `None` for a negative generation: zero-padding one yields `00000000-1`,
    /// which neither sorts nor parses back, so the housekeeping sweep could
    /// never remove it. Generations are minted from zero upward, so this is a
    /// guard against a caller bug rather than a reachable state.
    pub(super) fn group_assignment_location(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Option<Path> {
        if generation_id < 0 {
            return None;
        }

        self.group_assignments_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/{generation_id:0>10}.json")))
    }

    /// The legacy `{group}.json` a pre-#359 cutover may have left, by existence
    /// alone (#445).
    ///
    /// `head`, never `delete`: a delete of an absent key **succeeds** on S3 and
    /// on `InMemory` alike, so using a delete's outcome as an existence signal —
    /// which `delete_groups` did — makes every group look like it existed.
    ///
    /// And never for its *contents*: the object records a membership that the
    /// cutover's quiesce made vacuous, so reading it would answer with something
    /// less true than "empty". This asks only whether the group is there at all,
    /// which is the one thing the object can still be trusted about.
    pub(super) async fn legacy_group_exists(&self, group_id: &str) -> bool {
        self.object_store
            .head(&Path::from(format!(
                "clusters/{}/groups/consumers/{}.json",
                self.identity.cluster, group_id,
            )))
            .await
            .is_ok()
    }

    /// Whether a group with no generation nevertheless exists (#445).
    ///
    /// **A group exists if anything of it does.** A generation is the usual
    /// evidence and the caller has already looked for it; this is the rest —
    /// a legacy object, or committed offsets. That last one is not a corner
    /// case: a client may commit for a group id without ever joining, and such
    /// a group is one `kafka-consumer-groups --describe` lists and one
    /// `DeleteGroups` must be able to reap. Answering "never existed" for it
    /// would leak its offsets forever.
    ///
    /// Only reached when there is no generation, so a healthy group never pays
    /// for it — which is what keeps the 404-per-describe cost this deliberately
    /// avoids off the common path.
    pub(super) async fn group_remnants_exist(&self, group_id: &str) -> Result<bool> {
        if self.legacy_group_exists(group_id).await {
            return Ok(true);
        }

        self.committed_offset_topitions(group_id)
            .await
            .map(|topitions| !topitions.is_empty())
    }

    /// A group's decomposed objects, composed back into the [`GroupDetail`]
    /// every reader already projects from (#359).
    ///
    /// `None` when the group has no `generation.json`, which is how a group
    /// that does not exist and a group still in the legacy layout both read —
    /// the caller falls back to `{group}.json`.
    ///
    /// Deliberately **not** a trait method: the composition is a property of
    /// this layout, not of storage, and putting it on the trait would oblige
    /// every engine to reproduce a fan-out it has no objects for.
    ///
    /// Torn reads are answered, never failed. Between reading the generation
    /// and reading the member documents a member can leave, and between
    /// observing a leader and reading the assignment a rebalance can start.
    /// So a member the generation names whose document is gone is reported
    /// with empty metadata rather than dropped — it *is* a member, the
    /// generation is what says so — and a generation with a leader but no
    /// assignment reports `CompletingRebalance`, which is true, rather than a
    /// phantom `Stable`.
    ///
    /// **A member whose document says its session lapsed is not reported at
    /// all (#523).** Expiry is otherwise reachable only from `join`, `sync` and
    /// `heartbeat` — the dead-member sweep is driven entirely by traffic from
    /// the very group being expired — so a group whose members all fall silent
    /// at once, which is what a consumer being stopped, redeployed or
    /// OOMKilled looks like, has nobody left to trigger the sweep that would
    /// notice. It reported `Stable` with its full member list indefinitely:
    /// observed in production 52 minutes past a 45s session timeout. That
    /// blocks *every* offset-admin operation, because `kafka-consumer-groups`
    /// refuses `--reset-offsets`, `--delete-offsets` and `--delete` on a group
    /// that is not inactive, and `delete_groups` below answers `NonEmptyGroup`
    /// from this same member set — so the standard recovery for a consumer
    /// stuck on a poison record was unavailable, with no client-side
    /// workaround.
    ///
    /// The verdict is [`MemberDoc::is_expired`], the same pure function of the
    /// document and the clock the sweep takes, so a describe reports the
    /// membership the next sweep would leave rather than a second opinion. It
    /// costs nothing: the documents carrying `last_contact_ms` are already
    /// read here.
    ///
    /// Only a document that *was read* can condemn its member. Absent or
    /// unreadable stays a member, as above — a throttle or a 5xx must not
    /// report a live group as reapable, and this read feeds `delete_groups`.
    pub(super) async fn group_view(&self, group_id: &str) -> Result<Option<GroupDetail>> {
        /// As `describe_groups`' own fan-out: one round trip per member, and a
        /// group is tens of members.
        const MEMBER_FETCH_CONCURRENCY: usize = 32;

        let Some((generation, _)) = self.read_group_generation(group_id).await? else {
            return Ok(None);
        };

        let assignment = self
            .read_group_assignment(group_id, generation.generation_id)
            .await?;

        let now_ms = Self::now_ms();

        // From the generation's member set, not from a listing: the set is
        // authoritative, and a LIST here would put one on the describe path for
        // every group an admin client asks about.
        let members = futures::stream::iter(generation.members.keys().cloned().map(|member_id| {
            let generation = &generation;

            async move {
                let held = self
                    .read_group_member(group_id, &member_id)
                    .await
                    .inspect_err(|err| debug!(?err, group_id, member_id))
                    .ok()
                    .flatten();

                // The clock's verdict, taken before anything is composed (#523):
                // a member that has not been heard from within its session is
                // not one this group has, whether or not anything has since
                // asked the coordinator to notice.
                if held.as_ref().is_some_and(|(doc, _)| doc.is_expired(now_ms)) {
                    debug!(group_id, member_id, "not reporting a lapsed member");
                    return None;
                }

                let join_response = held
                    .as_ref()
                    .map(|(doc, _)| doc.join_response.clone())
                    .unwrap_or_else(|| {
                        JoinGroupResponseMember::default()
                            .member_id(member_id.clone())
                            .group_instance_id(
                                generation
                                    .members
                                    .get(&member_id)
                                    .and_then(|held| held.group_instance_id.clone()),
                            )
                    });

                let last_contact = held
                    .as_ref()
                    .and_then(|(doc, _)| to_system_time(doc.last_contact_ms).ok());

                Some((
                    member_id,
                    GroupMember {
                        join_response,
                        last_contact,
                    },
                ))
            }
        }))
        .buffered(MEMBER_FETCH_CONCURRENCY)
        .filter_map(|member| async move { member })
        .collect::<BTreeMap<_, _>>()
        .await;

        Ok(Some(GroupDetail {
            session_timeout_ms: generation.session_timeout_ms,
            rebalance_timeout_ms: generation.rebalance_timeout_ms,
            members,
            generation_id: generation.generation_id,
            skip_assignment: generation.skip_assignment,
            inception: to_system_time(generation.inception_ms).unwrap_or(SystemTime::UNIX_EPOCH),
            state: Self::composed_group_state(&generation, assignment),
        }))
    }

    /// The state a generation and the assignment of that same generation put a
    /// group in.
    ///
    /// Shared by the full view above and the state-only read
    /// [`Self::group_state`], so a group cannot be listed in one state and
    /// described in another (#475).
    pub(super) fn composed_group_state(
        generation: &GenerationDoc,
        assignment: Option<AssignmentDoc>,
    ) -> GroupState {
        match (generation.leader.clone(), assignment) {
            (Some(leader), Some(assignment))
                if assignment.generation_id == generation.generation_id =>
            {
                GroupState::Formed {
                    protocol_type: assignment.protocol_type,
                    protocol_name: assignment.protocol_name,
                    leader,
                    assignments: assignment.assignments,
                }
            }

            (leader, _) => GroupState::Forming {
                protocol_type: generation.protocol_type.clone(),
                protocol_name: generation.protocol_name.clone(),
                leader,
            },
        }
    }

    /// The state of a group the listing has already found, without reading its
    /// members' documents (#475).
    ///
    /// One GET, or two for a group that has a leader — never one per member,
    /// which is what [`Self::group_view`] costs. The state turns on whether the
    /// member set is *empty* and never on what a member document holds, and the
    /// generation names the set, so the documents themselves are not read here.
    ///
    /// A group with no `generation.json` is `Empty`, the same answer
    /// `describe_groups` gives it (#445): only a group that owns something under
    /// the consumer root is listed in the first place, so reaching here means
    /// the group is there with nobody in it — committed offsets it never joined
    /// to write, or the legacy object the cutover left behind. A group deleted
    /// between the listing and this read has the same shape and is reported the
    /// same way; a listing is a snapshot, and the next one does not name it.
    ///
    /// A member set that is non-empty is not the same as a group that has
    /// members (#523): the set only says whom the last rebalance admitted, and
    /// nothing retires an entry until something asks the coordinator to sweep.
    /// So the set is taken as empty when the clock says every one of them has
    /// lapsed, which is what [`Self::group_view`] reports on the describe path —
    /// a group listed `Stable` and described `Empty` is exactly the
    /// disagreement #475 closed. [`Self::any_member_live`] is what keeps that
    /// agreement affordable here.
    pub(super) async fn group_state(&self, group_id: &str) -> Result<ConsumerGroupState> {
        let Some((generation, _)) = self.read_group_generation(group_id).await? else {
            return Ok(ConsumerGroupState::Empty);
        };

        let assignment = self
            .read_group_assignment(group_id, generation.generation_id)
            .await?;

        let members = if self
            .any_member_live(group_id, &generation, Self::now_ms())
            .await
        {
            generation
                .members
                .keys()
                .map(|member_id| (member_id.clone(), GroupMember::default()))
                .collect()
        } else {
            BTreeMap::new()
        };

        Ok(ConsumerGroupState::from(&GroupDetail {
            members,
            state: Self::composed_group_state(&generation, assignment),
            ..Default::default()
        }))
    }

    /// Whether any member this generation names has been heard from inside its
    /// session (#523).
    ///
    /// The cheap half of the verdict [`Self::group_view`] takes in full. The
    /// listing needs one bit — is this group empty by the clock — and the first
    /// live member settles it, so `any` short-circuits and a healthy group pays
    /// at most `PROBE_CONCURRENCY` reads however many members it has. Composing
    /// the whole view instead would cost one read per member for every group in
    /// the cluster, which is the price #475 deliberately kept off this path.
    ///
    /// A group whose members have *all* lapsed pays one read each, once per
    /// listing, until an operator reaps it — which is the pathology this is
    /// here to make visible, and reaping it is now possible again.
    ///
    /// A document that is absent or unreadable counts as **live**, exactly as
    /// `group_view` keeps such a member: not knowing is not a verdict, and the
    /// two reads must not disagree about the same group. The failure direction
    /// is the one every group rule takes — a throttle keeps a group, it never
    /// invents an empty one.
    pub(super) async fn any_member_live(
        &self,
        group_id: &str,
        generation: &GenerationDoc,
        now_ms: i64,
    ) -> bool {
        /// Narrower than the describe fan-out on purpose: this stream is
        /// short-circuited, so the width is what a *live* group pays rather
        /// than how fast a dead one drains.
        const PROBE_CONCURRENCY: usize = 8;

        futures::stream::iter(
            generation
                .members
                .keys()
                .cloned()
                .map(|member_id| async move {
                    match self.read_group_member(group_id, &member_id).await {
                        Ok(Some((member, _))) => !member.is_expired(now_ms),

                        Ok(None) => true,

                        Err(error) => {
                            debug!(?error, group_id, member_id, "probing member liveness");
                            true
                        }
                    }
                }),
        )
        .buffered(PROBE_CONCURRENCY)
        .any(|live| async move { live })
        .await
    }

    /// Which group owns an object under [`Self::groups_root`], or `None` for
    /// one that belongs to no group.
    ///
    /// Layout-agnostic on purpose (#359): a group owns `{group}.json` directly
    /// under the root — the legacy state object — *and* everything under
    /// `{group}/`: its committed offsets, and now its member documents, its
    /// generation and its assignments. Keying on the *prefix* rather than on
    /// any one object is what lets expiry outlive the layout.
    ///
    /// An id that normalises to nothing (a stray object named exactly `.json`)
    /// is refused here rather than passed to `delete_groups`, which would
    /// otherwise report `InvalidGroupId` once per maintenance tick, forever.
    pub(super) fn group_of(root: &Path, location: &Path) -> Option<String> {
        let mut parts = location.prefix_match(root)?;
        let first = parts.next()?;

        let group_id = if parts.next().is_some() {
            first.as_ref().to_owned()
        } else {
            first.as_ref().strip_suffix(".json")?.to_owned()
        };

        (!group_id.is_empty()).then_some(group_id)
    }

    /// Enforce the `delete` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `delete`, drop the batches whose records are
    /// older than `retention.ms` (defaulting to 7 days, matching the SQL
    /// backends). Returns the number of batches removed.
    #[instrument(skip(self), ret)]
    /// Expire consumer groups with no activity anywhere under their prefix
    /// within [`GROUP_RETENTION`].
    ///
    /// One streaming listing of `clusters/{cluster}/groups/consumers/`, folded
    /// into the most recent `last_modified` per group. A group is condemned
    /// when *nothing* it owns has been touched inside the window — its state
    /// object, its committed offsets, its member documents, its generation.
    ///
    /// **The signal is the whole prefix, not one object (#272, #359).** The
    /// legacy state object's mtime freezes once a group stops rewriting it,
    /// which a commit-only consumer does after its first commit: age on that
    /// object alone said "abandoned" about a consumer that was still
    /// committing, and expiry then deleted the group state *and every committed
    /// offset under it*. The decomposed layout (#359) has no `{group}.json` at
    /// all, so a rule keyed on that object would have stopped condemning
    /// anything — the same reasoning, failing the other way, into an unbounded
    /// leak. Folding the prefix answers both, and is why the rule survives the
    /// layout change without a flag: a member's liveness write is now activity,
    /// which is what it always should have been.
    ///
    /// The failure direction is deliberate: every way this can be wrong keeps a
    /// group that would have been deleted. Expiry reclaiming late is #45's
    /// complaint; expiry reclaiming a live consumer's offsets is data loss.
    ///
    /// Cost is one listing page per thousand objects under the consumer tree,
    /// per tick — where the previous shape paid one delimited listing plus one
    /// listing *per candidate*, and a candidate was any group old enough to be
    /// considered.
    ///
    /// Deletions are capped at [`GROUP_EXPIRE_CHUNK`] per tick so a large
    /// accumulated backlog (e.g. groups leaked by a one-group-per-subscription
    /// client model) drains gradually rather than issuing tens of thousands of
    /// deletes at once — the concentrated object-store pressure that degraded
    /// the broker in #8. See #45. The oldest go first, so a backlog drains in
    /// the order it accumulated rather than in listing order.
    pub(super) async fn expire_groups(&self, now: SystemTime) -> Result<(u64, u64)> {
        /// Kafka's default `offsets.retention.minutes` is 7 days; match it.
        const GROUP_RETENTION: Duration = Duration::from_hours(7 * 24);
        /// Maximum number of groups expired per maintenance tick.
        const GROUP_EXPIRE_CHUNK: usize = 1_000;

        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        let threshold_ms = now_ms.saturating_sub(GROUP_RETENTION.as_millis() as i64);
        let orphan_ms = now_ms.saturating_sub(Self::GROUP_MEMBER_ORPHAN_AGE.as_millis() as i64);

        let root = self.groups_root();

        // Folded as the listing streams: one entry per group, not one per
        // object, so the tick's memory is the group count however many objects
        // each group owns.
        let mut latest: BTreeMap<String, i64> = BTreeMap::new();

        // Which groups hold a member document old enough to be worth a second
        // look (#486) — a group id, not a document, so the memory discipline
        // above is untouched. Empty in steady state: a member the generation
        // names renews inside `session/2`, so a document an hour old is one
        // nothing is renewing, and a group with none costs this pass nothing.
        let mut aged_members: BTreeSet<String> = BTreeSet::new();

        let mut listing = self.scan(Scan::Group, &root);

        while let Some(meta) = listing
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, cluster = self.identity.cluster))?
        {
            let Some(group_id) = Self::group_of(&root, &meta.location) else {
                continue;
            };

            let modified = meta.last_modified.timestamp_millis();

            if modified < orphan_ms
                && let Some((member_group, _)) = self.member_document_of(&root, &meta.location)
            {
                _ = aged_members.insert(member_group);
            }

            _ = latest
                .entry(group_id)
                .and_modify(|held| *held = (*held).max(modified))
                .or_insert(modified);
        }

        let mut stale = latest
            .into_iter()
            .filter(|(_, latest_ms)| *latest_ms < threshold_ms)
            .collect::<Vec<_>>();

        if stale.is_empty() {
            // The group set is unchanged, so every group with an aged member
            // document survives this tick and is the reclaim's to judge.
            return self
                .reclaim_member_documents(orphan_ms, aged_members)
                .await
                .map(|reclaimed| (0, reclaimed));
        }

        // Oldest first, so a backlog drains in the order it accumulated.
        stale.sort_by_key(|(_, latest_ms)| *latest_ms);

        let capped = stale.len() > GROUP_EXPIRE_CHUNK;
        stale.truncate(GROUP_EXPIRE_CHUNK);

        let stale = stale
            .into_iter()
            .map(|(group_id, _)| group_id)
            .collect::<Vec<_>>();

        let expired = stale.len() as u64;

        // `delete_groups` removes each state object and every object under the
        // group prefix, logging the per-group outcome.
        _ = self.delete_groups(Some(&stale)).await?;

        if capped {
            info!(
                expired,
                cluster = self.identity.cluster,
                "expire_groups hit the per-tick cap; more stale groups remain for the next tick"
            );
        } else {
            debug!(expired, cluster = self.identity.cluster, "expire_groups");
        }

        // A group that has just been deleted took its member documents with it,
        // so looking for its orphans would be a generation read and a listing of
        // objects that are already gone.
        let expired_ids = stale.into_iter().collect::<BTreeSet<_>>();
        aged_members.retain(|group_id| !expired_ids.contains(group_id));

        let reclaimed = self
            .reclaim_member_documents(orphan_ms, aged_members)
            .await?;

        Ok((expired, reclaimed))
    }

    /// Delete member documents no generation names (#486).
    ///
    /// A member document is written **before** the generation admits the member —
    /// the sweep reads a missing document as a lapsed session, so a member the
    /// generation names must always have one — and nothing deletes the document
    /// of an id the generation never admitted. The sweep only iterates
    /// `generation.members`, `leave` only deletes the member that left, and
    /// `expire_groups` only fires after seven days of a group being *entirely*
    /// idle, which a busy group never is. So such a group accumulates one object
    /// per abandoned id for ever: the production bucket held **46 686 documents
    /// for 348 live members**, growing ~24 k/day, and every one of them was
    /// walked by the listing above on every tick.
    ///
    /// Three things make deleting safe rather than merely likely to be safe:
    ///
    /// - **age.** A document has not been touched for
    ///   [`Self::GROUP_MEMBER_ORPHAN_AGE`], where a member the generation names
    ///   renews inside `session/2`. That is also why the caller can hand this
    ///   nothing but a *group* id: in steady state no group qualifies.
    /// - **identity.** The member id is derived from the object's own path and
    ///   then rebuilt into a location that must equal the one listed
    ///   ([`Self::member_document_of`]), so what is compared against the
    ///   generation's member set is the same string the coordinator writes. A
    ///   name that does not round-trip is left alone rather than guessed at.
    /// - **freshness.** The mtimes are read here, from this group's own listing,
    ///   *after* its generation — not from the whole-tree listing that chose the
    ///   group, which by now is as old as the tick. A join admitting a member
    ///   writes its document first, so a document being re-admitted has a fresh
    ///   mtime by the time it is looked at.
    ///
    /// What that leaves is a join whose document write lands in the moment
    /// between this listing and the delete. Nothing in an object store closes
    /// that — there is no conditional delete — and the cost if it happens is one
    /// rebalance: the member is in the generation with no document, which the
    /// sweep reads as a lapsed session and the member rejoins.
    ///
    /// A generation that cannot be read leaves the group alone. The failure
    /// direction is expiry's: every way this can be wrong keeps a document, and
    /// the next tick reconsiders it.
    pub(super) async fn reclaim_member_documents(
        &self,
        orphan_ms: i64,
        groups: BTreeSet<String>,
    ) -> Result<u64> {
        let root = self.groups_root();
        let mut reclaimed = 0u64;

        for group_id in groups {
            let Some(budget) = Self::GROUP_MEMBER_RECLAIM_CHUNK.checked_sub(reclaimed) else {
                info!(
                    reclaimed,
                    cluster = self.identity.cluster,
                    "member reclaim hit the per-tick cap; more orphans remain for the next tick"
                );
                break;
            };

            // Absent is a verdict: a group with no generation names no member,
            // and a document older than the orphan age is not one a group still
            // forming is about to admit.
            let named = match self.read_group_generation(&group_id).await {
                Ok(held) => held
                    .map(|(generation, _)| generation.members)
                    .unwrap_or_default(),

                Err(error) => {
                    debug!(?error, group_id, "not reclaiming without a generation");
                    continue;
                }
            };

            let Some(prefix) = self.group_members_prefix(&group_id) else {
                continue;
            };

            let mut doomed = Vec::new();
            let mut listing = self.scan(Scan::Group, &prefix);

            while let Some(meta) = listing
                .next()
                .await
                .transpose()
                .inspect_err(|err| error!(?err, group_id))?
            {
                if meta.last_modified.timestamp_millis() >= orphan_ms {
                    continue;
                }

                let Some((_, member_id)) = self.member_document_of(&root, &meta.location) else {
                    continue;
                };

                if named.contains_key(&member_id) {
                    continue;
                }

                doomed.push(meta.location);

                if doomed.len() as u64 >= budget {
                    break;
                }
            }

            if doomed.is_empty() {
                continue;
            }

            let deleted = self
                .object_store
                .delete_stream(futures::stream::iter(doomed.into_iter().map(Ok)).boxed())
                .try_collect::<Vec<Path>>()
                .await
                .inspect_err(|err| warn!(?err, group_id, "reclaiming member documents"))
                .unwrap_or_default();

            if !deleted.is_empty() {
                MEMBER_DOCUMENTS_RECLAIMED.add(deleted.len() as u64, &[]);
                info!(
                    group_id,
                    reclaimed = deleted.len(),
                    "reclaimed member documents no generation names"
                );
            }

            reclaimed += deleted.len() as u64;
        }

        Ok(reclaimed)
    }

    /// The `(group, member)` a listed object addresses, iff it is a member
    /// document *and* that pair rebuilds the very location listed (#486).
    ///
    /// The round trip is the point: a member id is client-chosen and may contain
    /// anything a path component may, so a name parsed one way and compared
    /// against a member set written another way is how a live member's document
    /// gets deleted. What does not rebuild is not judged.
    pub(super) fn member_document_of(
        &self,
        root: &Path,
        location: &Path,
    ) -> Option<(String, String)> {
        let mut parts = location.prefix_match(root)?;

        let group_id = parts.next()?.as_ref().to_owned();

        if parts.next()?.as_ref() != "members" {
            return None;
        }

        let member_id = parts
            .map(|part| part.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join("/")
            .strip_suffix(".json")?
            .to_owned();

        (self.group_member_location(&group_id, &member_id).as_ref() == Some(location))
            .then_some((group_id, member_id))
    }
}
