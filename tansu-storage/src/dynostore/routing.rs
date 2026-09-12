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

//! Where a topition's records live: the coalescing prefix (#57), the pinned
//! routing object (#236/#242), the sealed prefix shape (#464) and the
//! retired-prefix markers (#532).

use super::*;

impl Display for PrefixShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "depth {} separator {:?}", self.depth, self.separator)
    }
}

impl TopicRouting {
    /// How this topic's sub-streams are identified in a segment footer.
    pub(super) fn substream(&self, topic: &str) -> Substream {
        self.substream_id
            .map_or_else(|| Substream::Name(topic.to_owned()), Substream::Id)
    }
}

impl RetiredPrefix {
    /// Fold another retiring topic's obligation into this marker.
    ///
    /// `Default::default()` is `retention_ms: 0` — expire everything — which is
    /// safe only because this is the sole writer and every call passes a real
    /// obligation: `max` over `0` is that obligation. A marker that could be
    /// created empty would time-expire the prefix on the next tick.
    pub(super) fn retire(&mut self, topic: &str, retention_ms: i64, now_ms: i64) {
        self.retention_ms = self.retention_ms.max(retention_ms);
        self.topic = topic.to_owned();
        self.retired_at_ms = now_ms;
    }
}

impl DynoStore {
    /// The pinned routing prefix object for `name` (see [`TopicRouting`]).
    ///
    /// A prefix of its own, not a field of `topic-metadata/{name}.json`: that
    /// object carries genuinely mutable config, so a permanent cache of it would
    /// be wrong, and a cache of one field of it would be a discipline someone has
    /// to maintain. A separate immutable object makes it structural. It also keeps
    /// the pin out of [`Self::all_topics`]'s listing of `topic-metadata/`.
    pub(super) fn topic_routing_path(&self, name: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-routing/{}.json",
            self.identity.cluster, name
        ))
    }

    /// The retired-prefix marker object for `prefix` (see [`RetiredPrefix`]).
    pub(super) fn retired_prefix_path(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/retired-prefixes/{}.json",
            self.identity.cluster, prefix
        ))
    }

    /// Optimistic-concurrency handle on `retired-prefixes/{prefix}.json`.
    ///
    /// Built per call rather than memoized like [`Self::topic_meta`]: it is
    /// written once per topic deletion and read through the etag-delta cache
    /// ([`PrefixCaches`]), so a permanent per-prefix handle would hold
    /// a second copy of the value for no read it serves. A fresh handle has no
    /// cached version, so its first `with_mut` attempts a create and falls back
    /// to a conditional update on conflict — exactly the merge a second topic
    /// retiring onto the same prefix needs.
    pub(super) fn retired_prefix(&self, prefix: &str) -> OptiCon<RetiredPrefix> {
        OptiCon::<RetiredPrefix>::path(self.retired_prefix_path(prefix))
    }

    /// The cluster's sealed coalescing prefix shape (see [`PrefixShape`]).
    ///
    /// A cluster-level sibling of `meta.json` rather than anything under
    /// `prefixes/`, so the prefix listing that drives maintenance and `tansu
    /// audit` never returns it.
    pub(super) fn prefix_shape_path(&self) -> Path {
        Path::from(format!(
            "clusters/{}/prefix-shape.json",
            self.identity.cluster
        ))
    }

    /// The connector prefix a topition's records coalesce under (#57): the
    /// first [`Self::prefix_depth`] components of the topic name, split on
    /// [`Self::prefix_separator`] — the tenant/retention/isolation boundary the
    /// epic coalesces on. Popsink topics are `org.env.conn.<schema>.<table>` and
    /// the defaults (`3` / `.`) give the connector unit `org.env.conn`, which is
    /// what every deployment before #464 derived and what an unconfigured
    /// storage URL still derives byte for byte.
    ///
    /// A topic with fewer components than the depth is its own prefix, and so is
    /// every topic at depth `0` — the setting for a deployment whose topic names
    /// carry no grouping to coalesce on, at the PUT bill #56 set out to kill.
    ///
    /// The shape is fixed for the life of a cluster ([`Self::sealed_prefix_shape`]),
    /// which is what lets the maintenance paths keep deriving it rather than
    /// resolving each topic's pin (#236) at a GET apiece.
    ///
    /// A `prefix_map` override (custom topic-glob → prefix) is still not
    /// implemented; depth plus separator is deliberately the whole of #464.
    pub(super) fn prefix_of(&self, topition: &Topition) -> String {
        let topic = topition.topic();

        if self.tuning.prefix_depth == 0 {
            return topic.to_owned();
        }

        let mut parts = topic.split(self.tuning.prefix_separator.as_str());
        let mut prefix = String::new();

        for i in 0..self.tuning.prefix_depth {
            match parts.next() {
                Some(part) => {
                    if i > 0 {
                        prefix.push_str(&self.tuning.prefix_separator);
                    }
                    prefix.push_str(part);
                }
                None => return topic.to_owned(),
            }
        }

        prefix
    }

    /// The prefix a topition's records are segment-routed under, given a
    /// `compacted` verdict the caller already holds (#175): a compacted topic's
    /// dedicated prefix is its **full topic name**, so its segments never share
    /// an object with a sibling topic whose whole-segment retention (#61) would
    /// delete the compacted topic's old-but-latest keys. Everything else — and
    /// everything, when the flag is off — is [`Self::prefix_of`], byte-identical
    /// to today.
    pub(super) fn routed_prefix(&self, topition: &Topition, compacted: bool) -> String {
        if compacted {
            topition.topic().to_owned()
        } else {
            self.prefix_of(topition)
        }
    }

    /// The prefix `topition`'s records are segment-routed under: the **pinned**
    /// value (#236), read once per process and then served from memory forever.
    pub(super) async fn routed_prefix_of(&self, topition: &Topition) -> Result<String> {
        self.routing_of(topition)
            .await
            .map(|routing| routing.prefix)
    }

    /// Where `topition`'s records live and what identifies them there: the
    /// pinned prefix (#236) and the pinned sub-stream identity (#442), read once
    /// per process and then served from memory forever.
    ///
    /// Three steps, in cost order:
    ///
    /// 1. The permanent memo. Sound because the pin is immutable, which is the
    ///    property that makes this whole path free; see [`TopicRouting`].
    /// 2. Read the pin. A topic created before pinning existed has none, so the
    ///    fallback **reproduces exactly today's derivation** —
    ///    [`Self::prefix_of`] plus the [`Self::topic_is_compacted`] verdict — and
    ///    pins that answer, create-only. Reproducing it is not a nicety: a
    ///    different answer would route the topic's new records to a prefix its
    ///    existing segments are not under, which is not a cost regression but data
    ///    becoming unreachable.
    /// 3. A topic whose connector prefix already equals its own name (fewer
    ///    components than the configured depth, #464) and which has no pin needs
    ///    no *write*: both routings agree, so there is nothing to pin down and a
    ///    create would be an object per topic bought for nothing.
    ///
    /// Step 3 used to short-circuit the whole function — no memo, no read, no
    /// request at all. It cannot any more: whether a topic is id-keyed is not
    /// derivable from its name, and answering `Name` without looking would serve
    /// an id-keyed topic's reads against a key its records were never written
    /// under, which reads as an empty topic. So the read happens for every
    /// topic — once, and then never again, on the same permanent memo the prefix
    /// has always used.
    ///
    /// The lazy pin is create-only so peers converge: whoever writes first wins,
    /// and a loser adopts the winner's value rather than keeping its own. Without
    /// that, two pods that derived different answers — possible for exactly as long
    /// as the old 5s window was open, if an `AlterConfigs` lands between their
    /// reads — would each cache their own permanently, turning a bounded window
    /// into a permanent split. The pin is the tie-breaker.
    pub(super) async fn routing_of(&self, topition: &Topition) -> Result<TopicRouting> {
        let topic = topition.topic();

        if let Some(pinned) = self.topics.routing(topic)? {
            return Ok(pinned);
        }

        let pinned = match self.read_routing_pin(topic).await? {
            Some(pinned) => pinned,

            // Pre-#236 topic: derive as before, then pin it so this is the last
            // time anyone derives it. Name-keyed by construction — its records
            // are already in segments under its name.
            None => {
                let derived = TopicRouting {
                    prefix: self.routed_prefix(topition, self.topic_is_compacted(topic).await?),
                    substream_id: None,
                };

                // A topic whose connector prefix already equals its own name has
                // nothing to pin down — both routings agree and it is name-keyed
                // — so it is memoized without buying an object for it.
                if self.prefix_of(topition) == topic {
                    derived
                } else {
                    let won = self
                        .put_create(
                            &self.topic_routing_path(topic),
                            serde_json::to_vec(&derived)
                                .map(Bytes::from)
                                .map(PutPayload::from)?,
                        )
                        .await?;

                    if won {
                        derived
                    } else {
                        // A peer pinned it first: adopt its value, whatever we
                        // derived.
                        self.read_routing_pin(topic).await?.unwrap_or(derived)
                    }
                }
            }
        };

        self.topics
            .remember_routing(topic.to_owned(), pinned.clone());

        Ok(pinned)
    }

    /// Seal this cluster's coalescing prefix shape (#464) and hand the store
    /// back, or refuse to build one that disagrees with the seal already there.
    ///
    /// One GET at startup, and — once per cluster, ever — one create-only PUT.
    ///
    /// - Sealed already, and it matches: nothing to do.
    /// - Sealed already, and it does not: [`Error::PrefixShapeSealed`], naming
    ///   both shapes. Nothing is written. This is the whole point of the object
    ///   — see [`PrefixShape`] for what silently rots otherwise.
    /// - Not sealed: adopt the configured shape and write it. A bucket that has
    ///   been running on the pre-#464 derivation seals `depth 3 separator "."`,
    ///   which is what its routing pins (#236) already say, so an existing
    ///   cluster keeps its object layout untouched.
    ///
    /// The create is what makes a fleet converge rather than split: replicas
    /// starting together race for it, the winner's value is the cluster's, and a
    /// loser reads it back — adopting it if they agree, failing if they do not.
    /// Whichever way that race goes, every replica ends up on one shape or not
    /// running at all.
    pub async fn sealed_prefix_shape(self) -> Result<Self> {
        let configured = PrefixShape {
            depth: self.tuning.prefix_depth,
            separator: self.tuning.prefix_separator.clone(),
        };

        let sealed = match self.read_prefix_shape().await? {
            Some(sealed) => sealed,

            None => {
                let won = self
                    .put_create(
                        &self.prefix_shape_path(),
                        serde_json::to_vec(&configured)
                            .map(Bytes::from)
                            .map(PutPayload::from)?,
                    )
                    .await?;

                if won {
                    info!(shape = %configured, cluster = self.identity.cluster, "sealed the coalescing prefix shape");
                    configured.clone()
                } else {
                    // A peer sealed it between the read and the create: its
                    // value is the cluster's, whatever this replica configured.
                    self.read_prefix_shape()
                        .await?
                        .unwrap_or_else(|| configured.clone())
                }
            }
        };

        if sealed == configured {
            Ok(self)
        } else {
            Err(Error::PrefixShapeSealed {
                sealed: sealed.to_string(),
                configured: configured.to_string(),
            })
        }
    }

    /// The cluster's sealed prefix shape, or `None` on a bucket nothing has
    /// sealed yet. One GET, uncached — [`Self::sealed_prefix_shape`] is the only
    /// caller and it runs once per store.
    pub(super) async fn read_prefix_shape(&self) -> Result<Option<PrefixShape>> {
        match self.object_store.get(&self.prefix_shape_path()).await {
            Ok(get_result) => get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(|encoded| {
                    serde_json::from_slice::<PrefixShape>(&encoded).map_err(Into::into)
                })
                .map(Some),

            Err(object_store::Error::NotFound { .. }) => Ok(None),

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// What identifies `topition`'s sub-stream in a segment footer (#442), and
    /// the prefix its segments are under — the pair every read path needs, since
    /// neither answers on its own.
    pub(super) async fn routed_substream_of(
        &self,
        topition: &Topition,
    ) -> Result<(String, Substream)> {
        self.routing_of(topition)
            .await
            .map(|routing| (routing.prefix.clone(), routing.substream(topition.topic())))
    }

    /// The identity a topic of this name is written under **right now** (#442),
    /// read without pinning anything.
    ///
    /// The maintenance paths reach sub-streams by walking footers, and a footer
    /// can name an incarnation that no longer exists: a deleted topic's slices
    /// survive in shared segments until every co-tenant in them is past
    /// retention. Resolving those through [`Self::routing_of`] would create a
    /// routing pin for a topic that is gone, so this reads and never writes —
    /// and answers `Name` for a name that has no pin at all, which is what a
    /// deleted topic's name looks like.
    pub(super) async fn current_substream_of(&self, topic: &str) -> Result<Substream> {
        if let Some(pinned) = self.topics.routing(topic)? {
            return Ok(pinned.substream(topic));
        }

        Ok(self.read_routing_pin(topic).await?.map_or_else(
            || Substream::Name(topic.to_owned()),
            |routing| routing.substream(topic),
        ))
    }

    /// The pinned routing for `topic`, or `None` when the object does not exist (a
    /// topic created before #236). One GET, uncached — the caller memoizes.
    pub(super) async fn read_routing_pin(&self, topic: &str) -> Result<Option<TopicRouting>> {
        match self.object_store.get(&self.topic_routing_path(topic)).await {
            Ok(get_result) => get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(|encoded| {
                    serde_json::from_slice::<TopicRouting>(&encoded).map_err(Into::into)
                })
                .map(Some),

            Err(object_store::Error::NotFound { .. }) => Ok(None),

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Drop the process-local caches of every topic that no longer exists,
    /// returning how many topics were evicted (#283).
    ///
    /// [`TopicCaches::forget`] fixes only the replica that served the
    /// `DeleteTopics`. Eviction is process-local and a stateless fleet puts every
    /// topic through every replica, so on a ten-pod deployment nine pods keep
    /// their entries for a deleted topic — the growth is still monotonic, just at
    /// nine tenths of the rate. This is the half that converges the peers.
    ///
    /// Reconciled against the topic index rather than against a clock, because
    /// "this topic is gone" is a fact about the bucket and an idle window is only
    /// a guess at one. The index is exactly the right authority: it is rebuilt
    /// from a single LIST of `topic-metadata/`, drops deleted topics by
    /// construction, and is already maintained for the list-all metadata path —
    /// so the sweep costs one listing per maintenance tick and no per-topic
    /// requests.
    ///
    /// A refresh that **fails** propagates rather than evicting: a listing that
    /// did not happen says nothing about what exists, and treating it as "no
    /// topics" would drop the whole fleet's caches at once. An empty listing that
    /// succeeded is a cluster with no topics, and evicting is then correct.
    ///
    /// This is the one trigger that drops an offset hint without a local delete,
    /// so it is worth being explicit that it is not the size-triggered eviction
    /// that map must never have: the criterion is the topic's *absence from the
    /// bucket*, never memory pressure or age, so a live topic cannot be selected
    /// however hot or cold its partitions are. The only way to reach a live topic
    /// here is a listing that omits an object that exists, which is not a state
    /// either object store produces.
    pub(super) async fn evict_deleted_topic_caches(&self) -> Result<usize> {
        // Force the listing: a snapshot up to `TOPIC_INDEX_TTL` old is fine for
        // answering Metadata and is not fine for deciding what to forget.
        self.invalidate_topic_index();

        let live = self
            .topics_index()
            .await?
            .iter()
            .map(|metadata| metadata.topic.name.clone())
            .collect::<BTreeSet<_>>();

        let evicted = self.topics.retain_live(&live);
        let (topics, partitions) = self.topics.levels();

        if !evicted.is_empty() {
            debug!(
                evicted = evicted.len(),
                live = live.len(),
                topics,
                partitions,
                cluster = self.identity.cluster,
                "evicted per-topic caches of deleted topics"
            );
        }

        Ok(evicted.len())
    }
}
