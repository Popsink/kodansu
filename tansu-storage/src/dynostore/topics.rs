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

//! Topic metadata: the per-topic objects, the process-wide topic index that
//! serves `Metadata` (#387), and the one-shot backfill from the legacy
//! monolithic `meta.json`.

use super::*;

impl IndexedTopic {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::Hit(_) => "hit",
            Self::FreshMiss => "fresh_miss",
            Self::UnknownId => "unknown_id",
            Self::Stale => "stale",
        }
    }
}

impl TopicMetadata {
    /// Apply an incremental config change in place (used under the per-topic
    /// `OptiCon::with_mut` CAS). Mirrors Kafka `IncrementalAlterConfigs`
    /// Set/Delete semantics on `topic.configs`.
    pub(super) fn alter_configs(&mut self, changes: &[AlterableConfig]) -> Result<()> {
        let mut configuration = self
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .fold(BTreeMap::new(), |mut acc, item| {
                _ = acc.insert(item.name.as_str(), item.value.as_deref());
                acc
            });

        for change in changes {
            match OpType::try_from(change.config_operation)? {
                OpType::Set => {
                    _ = configuration.insert(change.name.as_str(), change.value.as_deref());
                }
                OpType::Delete => {
                    _ = configuration.remove(change.name.as_str());
                }
                // Kafka's list-valued APPEND/SUBTRACT are not implemented.
                // `config_operation` is a wire field, so a client picks it —
                // panicking the request task on one was remote input deciding
                // broker liveness (#276). Refuse the operation instead.
                OpType::Append | OpType::Subtract => {
                    error!(
                        config = change.name,
                        operation = change.config_operation,
                        "IncrementalAlterConfigs APPEND/SUBTRACT are not supported"
                    );

                    return Err(Error::Api(ErrorCode::InvalidConfig));
                }
            }
        }

        _ = self.topic.configs.replace(configuration.into_iter().fold(
            Vec::new(),
            |mut acc, (key, value)| {
                acc.push(
                    CreatableTopicConfig::default()
                        .name(key.to_owned())
                        .value(value.map(|value| value.to_owned())),
                );
                acc
            },
        ));

        Ok(())
    }
}

impl OptiCon<TopicMetadata> {
    pub(super) fn new(cluster: &str, name: &str) -> Self {
        Self::path(format!("clusters/{cluster}/topic-metadata/{name}.json"))
    }
}

impl DynoStore {
    /// Optimistic-concurrency handle on a topic's `topic-metadata/{name}.json`.
    pub(super) fn topic_meta(&self, name: &str) -> Result<OptiCon<TopicMetadata>> {
        self.topics.meta(self.identity.cluster.as_str(), name)
    }

    pub(super) fn topic_id_path(&self, id: &Uuid) -> Path {
        Path::from(format!(
            "clusters/{}/topic-ids/{}.json",
            self.identity.cluster, id
        ))
    }

    pub(super) fn topic_metadata_path(&self, name: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-metadata/{}.json",
            self.identity.cluster, name
        ))
    }

    /// Marker object recording that the one-shot legacy-metadata backfill has
    /// run. Kept outside the `topic-metadata/` prefix so it is never returned by
    /// [`Self::all_topics`]'s listing.
    pub(super) fn topic_metadata_migration_marker(&self) -> Path {
        Path::from(format!(
            "clusters/{}/.migrations/topic-metadata",
            self.identity.cluster
        ))
    }

    /// Resolve a topic-id to its name: in-memory cache first, then the
    /// `topic-ids/{uuid}.json` pointer (result cached). The mapping is immutable
    /// for a topic's lifetime, so the cache is safe until delete.
    pub(super) async fn topic_name_by_id(&self, id: &Uuid) -> Result<Option<Topic>> {
        if let Some(name) = self.topics.topic_id(id) {
            return Ok(Some(name));
        }

        match self.object_store.get(&self.topic_id_path(id)).await {
            Ok(get_result) => {
                let encoded = get_result.bytes().await?;
                let name = serde_json::from_slice::<TopicIdRef>(&encoded)?.name;
                self.topics.remember_topic_id(*id, name.clone());
                Ok(Some(name))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// All topics, served from the in-memory [`TopicIndex`]. Returns the shared
    /// snapshot if fresh; otherwise refreshes it (single-flight) by LISTing the
    /// `topic-metadata/` prefix and GETting only the objects whose etag changed.
    /// Used by the list-all metadata path, the by-name metadata path (#387) and
    /// the cleanup policies — never on the produce/fetch hot path.
    pub(super) async fn topics_index(&self) -> Result<Arc<Vec<TopicMetadata>>> {
        if let Some(snapshot) = self.fresh_topic_index()? {
            return Ok(snapshot);
        }

        // Stale or empty: one task refreshes, the rest await and reuse it.
        let _guard = self.topics.index_refresh().lock().await;
        if let Some(snapshot) = self.fresh_topic_index()? {
            return Ok(snapshot);
        }
        self.refresh_topic_index().await
    }

    /// The cached snapshot iff it was refreshed within [`Self::TOPIC_INDEX_TTL`].
    pub(super) fn fresh_topic_index(&self) -> Result<Option<Arc<Vec<TopicMetadata>>>> {
        self.topics.fresh_index(Self::TOPIC_INDEX_TTL)
    }

    /// Rebuild the index: LIST the prefix once, reuse cached entries whose etag
    /// is unchanged, GET only the new/changed objects, and drop deleted ones.
    pub(super) async fn refresh_topic_index(&self) -> Result<Arc<Vec<TopicMetadata>>> {
        let prefix = Path::from(format!(
            "clusters/{}/topic-metadata/",
            self.identity.cluster
        ));
        let listed = self.scan_delimited(Scan::TopicMetadata, &prefix).await?;

        let mut entries: BTreeMap<Topic, (Option<String>, TopicMetadata)> = BTreeMap::new();
        let mut stale = Vec::new();

        self.topics.with_index(|index| {
            for object in &listed.objects {
                let Some(name) = object
                    .location
                    .filename()
                    .and_then(|file| file.strip_suffix(".json"))
                else {
                    continue;
                };

                match index.entries.get(name) {
                    Some(cached @ (etag, _)) if etag.is_some() && *etag == object.e_tag => {
                        _ = entries.insert(name.to_owned(), cached.clone());
                    }
                    _ => stale.push((
                        name.to_owned(),
                        object.location.clone(),
                        object.e_tag.clone(),
                    )),
                }
            }
        })?;

        // GET only the new/changed objects (no lock held), with a bounded
        // fan-out. The cold build — every topic stale on the first refresh —
        // is otherwise O(topics) *sequential* round-trips: ~6s for 5k objects
        // on local minio and tens of seconds against real S3, which (now that
        // the warm-up runs this before the listener opens) would stretch boot
        // unacceptably at 15k topics. A small concurrency keeps it to a few
        // seconds without re-creating a request burst.
        const FETCH_EACH_CONCURRENCY: usize = 32;

        let object_store = &self.object_store;

        let fetched = futures::stream::iter(stale)
            .map(|(name, location, etag)| async move {
                let encoded = object_store.get(&location).await?.bytes().await?;
                let metadata = serde_json::from_slice::<TopicMetadata>(&encoded)?;
                Ok::<_, Error>((name, (etag, metadata)))
            })
            .buffer_unordered(FETCH_EACH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        TOPIC_INDEX_REFRESH_OBJECTS
            .add(fetched.len() as u64, &[KeyValue::new("outcome", "fetched")]);
        TOPIC_INDEX_REFRESH_OBJECTS
            .add(entries.len() as u64, &[KeyValue::new("outcome", "reused")]);

        for (name, value) in fetched {
            _ = entries.insert(name, value);
        }

        let snapshot = Arc::new(
            entries
                .values()
                .map(|(_, metadata)| metadata.clone())
                .collect::<Vec<_>>(),
        );

        self.topics.replace_index(entries, snapshot.clone())?;

        Ok(snapshot)
    }

    /// Force the next [`Self::topics_index`] to refresh (after a local create or
    /// delete), so the change is reflected without waiting out the TTL.
    pub(super) fn invalidate_topic_index(&self) {
        self.topics.invalidate_index();
    }

    /// Best-effort warm-up of the topic index at boot, run from
    /// [`Storage::register_broker`] which completes *before* the broker binds
    /// its listener. Without it, the first list-all metadata request pays the
    /// full cold build — LIST the `topic-metadata/` prefix and GET every object
    /// — which at 15k topics is several seconds and can exceed a Kafka client's
    /// first metadata timeout. Paying it here keeps the port closed (so the pod
    /// is not yet ready for traffic) until the index is hot.
    ///
    /// Best-effort by design: a transient object-store error at boot must not
    /// stop the broker starting. On failure the index stays empty and the lazy
    /// [`Self::topics_index`] path rebuilds it on the first request.
    pub(super) async fn warm_topic_index(&self) {
        let started = SystemTime::now();
        match self.refresh_topic_index().await {
            Ok(snapshot) => info!(
                topics = snapshot.len(),
                elapsed_ms = started.elapsed().ok().map(|elapsed| elapsed.as_millis()),
                "topic index warmed"
            ),
            Err(err) => warn!(
                ?err,
                "topic index warm-up failed; building lazily on first request"
            ),
        }
    }

    /// One-shot, idempotent backfill from the legacy monolithic `meta.json`
    /// (which embedded a `topics` map) to per-topic `topic-metadata/{name}.json`
    /// objects plus their `topic-ids/{uuid}.json` pointers.
    ///
    /// Safe to run on every boot and from every replica concurrently: each
    /// object is written create-only, so an already migrated (or freshly
    /// created) topic is skipped. A cluster with no legacy `meta.json`, or whose
    /// topics are already decomposed, is a no-op. The legacy `topics` bytes are
    /// left in `meta.json` untouched — they are dead data the current `Meta`
    /// deserialiser ignores.
    pub(super) async fn migrate_legacy_topic_metadata(&self) -> Result<()> {
        let marker = self.topic_metadata_migration_marker();

        // Fast path: a prior boot already backfilled. Without this, every
        // restart re-loads `meta.json` and re-attempts a create per topic — a
        // ~O(topics) startup cost (and memory spike) that, on a large cluster,
        // is what tipped the broker over its memory limit and crash-looped it.
        match self.object_store.head(&marker).await {
            Ok(_) => return Ok(()),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(otherwise) => return Err(otherwise.into()),
        }

        #[derive(Deserialize)]
        struct LegacyMeta {
            #[serde(default)]
            topics: BTreeMap<Topic, TopicMetadata>,
        }

        let path = Path::from(format!("clusters/{}/meta.json", self.identity.cluster));

        let legacy = match self.object_store.get(&path).await {
            Ok(get_result) => {
                let encoded = get_result.bytes().await?;
                serde_json::from_slice::<LegacyMeta>(&encoded)?
            }
            Err(object_store::Error::NotFound { .. }) => LegacyMeta {
                topics: BTreeMap::new(),
            },
            Err(otherwise) => return Err(otherwise.into()),
        };

        let mut migrated = 0u64;

        // Write per-topic objects directly (create-only), NOT through the cached
        // `topic_meta` handle: the per-topic `OptiCon` cache would otherwise
        // retain every migrated topic, making migration memory scale with topic
        // count. Consume by value so the legacy map shrinks as we go.
        for (name, metadata) in legacy.topics {
            let id = metadata.id;

            let payload = serde_json::to_vec(&metadata)
                .map(Bytes::from)
                .map(PutPayload::from)?;
            if self
                .put_create(&self.topic_metadata_path(&name), payload)
                .await?
            {
                migrated += 1;
            }

            let pointer = serde_json::to_vec(&TopicIdRef { name })
                .map(Bytes::from)
                .map(PutPayload::from)?;
            _ = self.put_create(&self.topic_id_path(&id), pointer).await?;
        }

        if migrated > 0 {
            info!(
                cluster = %self.identity.cluster,
                migrated,
                "backfilled legacy topic metadata into per-topic objects"
            );
        }

        // Record completion so every subsequent boot takes the fast path above.
        _ = self
            .put_create(&marker, PutPayload::from(Bytes::new()))
            .await?;

        Ok(())
    }

    /// A topic's metadata read from its own `topic-metadata/{name}.json`: one
    /// conditional GET, always fresh.
    ///
    /// The authority, and the only thing that may decide a routing derivation or
    /// a lifecycle operation. Read paths that only *describe* a topic should go
    /// through [`Self::described_topic_metadata`] instead — this costs a request
    /// per topic per etag-memo window, which is the ~1,040/s plane #387 removed.
    pub(super) async fn topic_metadata(&self, topic: &TopicId) -> Result<Option<TopicMetadata>> {
        debug!(?topic);

        match topic {
            TopicId::Name(name) => self.topic_meta(name)?.get_opt(&self.object_store).await,
            TopicId::Id(id) => match self.topic_name_by_id(id).await? {
                Some(name) => self.topic_meta(&name)?.get_opt(&self.object_store).await,
                None => Ok(None),
            },
        }
    }

    /// A topic's metadata from the in-memory [`TopicIndex`], at **zero**
    /// object-store requests, or `None` when the index cannot answer.
    ///
    /// `None` is not a claim of absence. It means the index does not hold the name
    /// — because the topic does not exist, or because it was created after the
    /// snapshot was taken — or that the snapshot has aged past
    /// [`Self::TOPIC_INDEX_TTL`]. Every caller therefore falls back to the topic's
    /// own object, which is what keeps a freshly created topic immediately
    /// resolvable (#28).
    ///
    /// Deliberately does **not** refresh: the caller refreshes once for a whole
    /// request (see [`Self::refresh_index_for_described_reads`]). A per-topic
    /// refresh here would serialise behind the index's single-flight lock — so a
    /// `Metadata` naming 1,500 topics against a failing object store would attempt
    /// 1,500 sequential LISTs rather than one.
    ///
    /// A topic-id is resolved to a name through the permanently-cached
    /// `topic-ids/{uuid}.json` pointer rather than by scanning the snapshot: the
    /// mapping is immutable for a topic's lifetime, so it costs one GET per id per
    /// process, where a scan would be O(topics) per lookup — 15k comparisons per
    /// topic on a by-id `Metadata`.
    pub(super) async fn indexed_topic_metadata(&self, topic: &TopicId) -> Result<IndexedTopic> {
        if self.fresh_topic_index()?.is_none() {
            return Ok(IndexedTopic::Stale);
        }

        let name = match topic {
            TopicId::Name(name) => name.clone(),
            TopicId::Id(id) => match self.topic_name_by_id(id).await? {
                Some(name) => name,
                None => return Ok(IndexedTopic::UnknownId),
            },
        };

        self.topics.with_index(|index| {
            index
                .entries
                .get(name.as_str())
                .map(|(_, metadata)| IndexedTopic::Hit(metadata.clone()))
                .unwrap_or(IndexedTopic::FreshMiss)
        })
    }

    /// A topic's metadata for a path that only *describes* it: served from the
    /// [`TopicIndex`] when the index holds it, falling back to the topic's own
    /// object otherwise (#387).
    ///
    /// This is the whole of #387. `Metadata` maps every requested topic to a
    /// per-topic read 32-way concurrently, and consumers here subscribe to
    /// hundreds of topics each, so one client refreshing metadata was ~100
    /// conditional GETs — 2,081 lookups/s against 21.5 `Metadata` requests/s on
    /// the production fleet, of which ~1,040/s reached S3 as a `304`, ~$38/day and
    /// 63% of the remaining request bill. The index answers all of them from one
    /// LIST per window per replica: a cost that does not scale with the topic
    /// count, the same reasoning as #112's per-prefix manifest.
    ///
    /// Not for anything that must be fresh. In particular `describe_config` stays
    /// on [`Self::topic_metadata`], because [`Self::topic_is_compacted`] reads
    /// through it to derive a routing prefix for a pre-#236 topic, and that
    /// derivation is pinned permanently — a stale verdict there is unreachable
    /// data, not a stale answer. Lifecycle operations (`create_topic`,
    /// `delete_topic`, `AlterConfigs`) likewise read the object.
    ///
    /// `caller` labels [`TOPIC_METADATA_READS`], so the residual `source="object"`
    /// population is attributable per call site rather than inferred from the API
    /// mix.
    pub(super) async fn described_topic_metadata(
        &self,
        topic: &TopicId,
        caller: &'static str,
    ) -> Result<Option<TopicMetadata>> {
        let indexed = self
            .indexed_topic_metadata(topic)
            .await
            .inspect_err(|error| warn!(?error, caller, ?topic, "indexed topic metadata"))
            // A failed index read is not a usable index, which is exactly the
            // case the object fallback exists for.
            .unwrap_or(IndexedTopic::Stale);

        let source = if matches!(indexed, IndexedTopic::Hit(_)) {
            "index"
        } else {
            "object"
        };

        // `index` alongside `source` (#407): a `fresh_miss` is a fallback that can
        // only confirm the topic does not exist, and a `stale` is the fallback
        // doing the job it was added for. `source="object"` alone cannot tell
        // them apart, so it could not say whether skipping the fallback on a
        // fresh index would cost any visibility at all.
        TOPIC_METADATA_READS.add(
            1,
            &[
                KeyValue::new("caller", caller),
                KeyValue::new("source", source),
                KeyValue::new("index", indexed.as_str()),
            ],
        );

        match indexed {
            IndexedTopic::Hit(metadata) => Ok(Some(metadata)),

            // The pointer has already been read and did not resolve.
            // `topic_metadata` would read the same key again, through the same
            // positives-only cache, and return `Ok(None)` — so this is the one
            // arm where the fallback is provably a no-op rather than a
            // visibility guarantee (#407).
            IndexedTopic::UnknownId => Ok(None),

            // Both keep the fallback, and for different reasons: `Stale` because
            // there is no usable index at all, `FreshMiss` because a fresh index
            // is *not* authoritative for absence — see [`IndexedTopic`].
            IndexedTopic::FreshMiss | IndexedTopic::Stale => self.topic_metadata(topic).await,
        }
    }

    /// Refresh the [`TopicIndex`] once for a request that is about to resolve
    /// topics through [`Self::described_topic_metadata`].
    ///
    /// Best-effort: a failed refresh is not a failed request. Every topic then
    /// misses the index and falls back to its own object, which is exactly the
    /// pre-#387 behaviour — so a LIST that cannot be served degrades the cost, not
    /// the answers.
    pub(super) async fn refresh_index_for_described_reads(&self, caller: &'static str) {
        if let Err(error) = self.topics_index().await {
            warn!(
                ?error,
                caller, "topics index unavailable; falling back to per-topic reads"
            );
        }
    }

    /// Whether `cleanup.policy` names `compact`, read straight off the stored
    /// config. Shared so the carry-over's selection and its prefix resolution
    /// cannot disagree about what "compacted" means (#211).
    pub(super) fn topic_configs_are_compacted(topic: &CreatableTopic) -> bool {
        topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|config| {
                config.name == "cleanup.policy"
                    && config
                        .value
                        .as_deref()
                        .is_some_and(|value| value.contains("compact"))
            })
    }

    /// Whether `topic`'s `cleanup.policy` is `compact`, memoized for
    /// [`Self::HIGH_WATERMARK_HINT_TTL`] (#113). `produce` needs this on every
    /// batch to choose the coalesce route; the value changes only on a rare
    /// `AlterConfigs`, so serving it from memory keeps the produce hot path off a
    /// per-batch conditional GET of the `topic-metadata/<name>.json` object. A
    /// policy change is observed within the TTL.
    pub(super) async fn topic_is_compacted(&self, topic: &str) -> Result<bool> {
        if let Some(compacted) = self
            .topics
            .compacted(topic, Self::HIGH_WATERMARK_HINT_TTL)?
        {
            return Ok(compacted);
        }

        let compacted = self
            .describe_config(topic, ConfigResource::Topic, None)
            .await
            .inspect_err(|err| debug!(?err))?
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|config| {
                config.name == "cleanup.policy"
                    && config
                        .value
                        .as_deref()
                        .is_some_and(|value| value.contains("compact"))
            });

        self.topics.remember_compacted(topic.to_owned(), compacted);

        Ok(compacted)
    }
    /// The body of [`Storage::metadata`]: the brokers, and one
    /// [`MetadataResponseTopic`] per topic the request names or, when it names
    /// none, per topic the index holds.
    pub(super) async fn metadata_response(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.identity.node)
                .host(
                    self.identity
                        .advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(
                    self.identity
                        .advertised_listener
                        .port()
                        .unwrap_or(9092)
                        .into(),
                )
                .rack(None),
        ];

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                // One refresh for the whole request (#387). Every topic below is
                // then answered from the in-memory index at zero requests, and
                // only a name the index does not hold — a topic that does not
                // exist, or one created since the snapshot — costs a read of its
                // own object.
                //
                // This is the fan-out that made `topic_metadata` the fleet's
                // largest remaining S3 cost: 2,081 lookups/s against 21.5
                // `Metadata` requests/s, ~97 per request, because consumers here
                // subscribe to hundreds of topics each. Per-topic reads spread
                // thin across ~15k topics missed their window about half the time,
                // so ~1,040/s reached S3 as a conditional GET answered `304`.
                self.refresh_index_for_described_reads("metadata").await;

                // Resolve each topic concurrently. The index lookup does not
                // await, but the fallback read does, and a serial loop over it is
                // O(topics × RTT) — on a high-latency store that blew past the
                // client's metadata/request timeout at scale, so a consumer group
                // leader resolving the union of its members' subscriptions
                // (hundreds–thousands of topics) never completed the fetch, never
                // sent SyncGroup, and the group stayed `Forming` with zero
                // partitions assigned. Bounded concurrency returns the same
                // answers in O(topics / concurrency) wall time. `buffered`
                // preserves response order; collecting `Result`s (not
                // `try_collect`) keeps the per-topic error handling below intact.
                // Same scaling fix as ListOffsets (#147), same concurrency bound.
                const METADATA_FETCH_CONCURRENCY: usize = 32;

                // Eagerly collected to pin every future to this call's lifetime
                // under `async_trait` (see the identical note on ListOffsets);
                // the futures are inert until polled by `buffered`, so this
                // allocates, it does not serialize.
                let fetches = topics
                    .iter()
                    .map(|topic| self.described_topic_metadata(topic, "metadata"))
                    .collect::<Vec<_>>();

                let fetched = futures::stream::iter(fetches)
                    .buffered(METADATA_FETCH_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await;

                // A per-topic read that resolved to nothing is NOT proof the
                // topic is gone (#214).
                //
                // `topic_metadata` reads one object through the `OptiCon` cache,
                // and `OptiCon::refresh` *clears* its cached value on any
                // `NotFound` — real, transient or spurious alike. So a single
                // 404 against a live topic's object turns into `Ok(None)`, and
                // reporting that as `UnknownTopicOrPartition` is the worst
                // available answer: the client cannot tell it from a deleted
                // topic, so it refreshes metadata until `max.block.ms` expires
                // and fails the batch. Production saw eight topics answered
                // absent minutes after being written, taking six of twenty-four
                // source connectors into restart loops.
                //
                // #387 moved most of that population out of reach: a topic the
                // index holds is answered from the index, so no object read
                // happens and no spurious 404 can be mistaken for absence. What
                // remains is the fallback — a name the index did not hold — and
                // there this witness still decides, because the index can be
                // refreshed between the lookup that missed and the read that came
                // back empty (a peer's create, a maintenance sweep, another
                // request's refresh after an invalidation).
                //
                // The topics index is an independent witness: it is built from a
                // LIST of the `topic-metadata/` prefix, not from the per-object
                // reads that just came back empty. If it knows the topic, the
                // read failed to resolve rather than the topic being absent, and
                // the honest answer is a retriable one.
                let unresolved: BTreeSet<&str> = topics
                    .iter()
                    .zip(&fetched)
                    .filter(|(_, result)| matches!(result, Ok(None)))
                    .filter_map(|(topic, _)| match topic {
                        TopicId::Name(name) => Some(name.as_str()),
                        TopicId::Id(_) => None,
                    })
                    .collect();

                let existing_but_unresolved: BTreeSet<String> = if unresolved.is_empty() {
                    BTreeSet::new()
                } else {
                    let known = self
                        .topics_index()
                        .await
                        .inspect_err(|error| warn!(?error, "topics index for absent-topic check"))
                        .unwrap_or_default();

                    let existing: BTreeSet<String> = known
                        .iter()
                        .map(|metadata| metadata.topic.name.clone())
                        .filter(|name| unresolved.contains(name.as_str()))
                        .collect();

                    for name in &existing {
                        METADATA_UNRESOLVED_EXISTING.add(1, &[]);
                        warn!(
                            topic = name.as_str(),
                            "metadata could not resolve a topic the index knows; \
                             answering retriably rather than reporting it absent"
                        );
                    }

                    existing
                };

                topics
                    .iter()
                    .zip(fetched)
                    .map(
                        |(topic, result)| match result.inspect_err(|error| error!(?error)) {
                            Ok(Some(topic_metadata)) => {
                                let name = Some(topic_metadata.topic.name.to_owned());
                                let error_code = ErrorCode::None.into();
                                let topic_id = Some(topic_metadata.id.into_bytes());
                                let is_internal = Some(false);
                                let partitions = topic_metadata.topic.num_partitions;
                                let replication_factor = topic_metadata.topic.replication_factor;

                                debug!(
                                    ?error_code,
                                    ?topic_id,
                                    ?name,
                                    ?is_internal,
                                    ?partitions,
                                    ?replication_factor
                                );

                                let mut rng = rng();
                                let mut broker_ids: Vec<_> =
                                    brokers.iter().map(|broker| broker.node_id).collect();
                                broker_ids.shuffle(&mut rng);

                                let mut brokers = broker_ids.into_iter().cycle();

                                let partitions = Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            let leader_id = brokers.next().expect("cycling");

                                            let replica_nodes = Some(
                                                (0..replication_factor)
                                                    .map(|_replica| {
                                                        brokers.next().expect("cycling")
                                                    })
                                                    .collect(),
                                            );
                                            let isr_nodes = replica_nodes.clone();

                                            MetadataResponsePartition::default()
                                                .error_code(error_code)
                                                .partition_index(partition_index)
                                                .leader_id(leader_id)
                                                .leader_epoch(Some(0))
                                                .replica_nodes(replica_nodes)
                                                .isr_nodes(isr_nodes)
                                                .offline_replicas(Some([].into()))
                                        })
                                        .collect(),
                                );

                                MetadataResponseTopic::default()
                                    .error_code(error_code)
                                    .name(name)
                                    .topic_id(topic_id)
                                    .is_internal(is_internal)
                                    .partitions(partitions)
                                    .topic_authorized_operations(Some(-2147483648))
                            }

                            Ok(None) => MetadataResponseTopic::default()
                                .error_code(
                                    // Retriable when the index says the topic is
                                    // there: the client backs off and retries
                                    // instead of spinning on an assertion of
                                    // absence it cannot question (#214).
                                    if matches!(topic, TopicId::Name(name) if existing_but_unresolved.contains(name))
                                    {
                                        ErrorCode::LeaderNotAvailable.into()
                                    } else {
                                        ErrorCode::UnknownTopicOrPartition.into()
                                    },
                                )
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                })
                                .topic_id(Some(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                }))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),

                            Err(_) => MetadataResponseTopic::default()
                                .error_code(ErrorCode::UnknownServerError.into())
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => Some("".into()),
                                })
                                .topic_id(Some(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                }))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),
                        },
                    )
                    .collect()
            }

            _ => {
                let mut responses = vec![];

                for topic_metadata in self.topics_index().await?.iter() {
                    debug!(?topic_metadata);

                    let name = Some(topic_metadata.topic.name.clone());
                    let error_code = ErrorCode::None.into();
                    let topic_id = Some(topic_metadata.id.into_bytes());
                    let is_internal = Some(false);
                    let partitions = topic_metadata.topic.num_partitions;
                    let replication_factor = topic_metadata.topic.replication_factor;

                    debug!(
                        ?error_code,
                        ?topic_id,
                        ?name,
                        ?is_internal,
                        ?partitions,
                        ?replication_factor
                    );

                    let mut rng = rng();
                    let mut broker_ids: Vec<_> =
                        brokers.iter().map(|broker| broker.node_id).collect();
                    broker_ids.shuffle(&mut rng);

                    let mut brokers = broker_ids.into_iter().cycle();

                    let partitions = Some(
                        (0..partitions)
                            .map(|partition_index| {
                                let leader_id = brokers.next().expect("cycling");

                                let replica_nodes = Some(
                                    (0..replication_factor)
                                        .map(|_replica| brokers.next().expect("cycling"))
                                        .collect(),
                                );
                                let isr_nodes = replica_nodes.clone();

                                MetadataResponsePartition::default()
                                    .error_code(error_code)
                                    .partition_index(partition_index)
                                    .leader_id(leader_id)
                                    .leader_epoch(Some(0))
                                    .replica_nodes(replica_nodes)
                                    .isr_nodes(isr_nodes)
                                    .offline_replicas(Some([].into()))
                            })
                            .collect(),
                    );

                    responses.push(
                        MetadataResponseTopic::default()
                            .error_code(error_code)
                            .name(name)
                            .topic_id(topic_id)
                            .is_internal(is_internal)
                            .partitions(partitions)
                            .topic_authorized_operations(Some(-2147483648)),
                    );
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.identity.cluster.clone()),
            controller: Some(self.identity.node),
            brokers,
            topics: responses,
        })
    }

    /// The body of [`Storage::delete_topic`]: the topic's data before its
    /// metadata (#251), because the metadata object is the only handle on the
    /// data and losing it first strands every segment behind it.
    pub(super) async fn delete_topic_objects(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some(metadata) = self.topic_metadata(topic).await? {
            // Data BEFORE metadata (#251). The metadata object is the only handle
            // on this topic's data: maintenance discovers work by listing
            // `topic-metadata/`, so a topic that no longer has one is never
            // revisited by anything. Removing it first meant that any failure
            // in the deletions below — an error, a throttle, a pod restart, a
            // client timeout — stranded whatever had not been reached, for good.
            // A production audit found 878,065 such objects under two deleted
            // topics, paid for indefinitely and skewing every audit of the
            // layout.
            //
            // Ordered this way, a partial delete instead leaves a topic that
            // still exists with some of its data gone: visible, and recoverable
            // by re-issuing `DeleteTopics`. The cost is a wider window in which
            // a topic being deleted is still served, which for an admin
            // operation is the better trade.
            //
            // This does not reopen the offset-reuse hazard of #241: nothing
            // below removes an authority on the log end. The watermark object is
            // rewritten as a truncation tombstone rather than deleted (#246,
            // below), and `coalesced_high_from_index` folds the segment tail
            // over it regardless — so a produce landing inside the widened
            // window still computes the same high watermark from the segments
            // that are still there.
            // Truncate every partition to its log end (#246) rather than
            // deleting its `watermark.json`.
            //
            // The slices this topic left inside SHARED segments cannot be
            // removed — a segment multiplexes many topics, is immutable, and is
            // reclaimed whole only once every sub-stream in it is past retention
            // (#61) — and they are located by `(topic, partition)` NAME, so a
            // same-named successor would find them and serve them as its own.
            // The floor the truncation machinery already maintains
            // (`watermark.truncate`, #176) hides them at read time without
            // rewriting a shared segment, and is as durable as the data it
            // hides. So the watermark object is not deleted: it BECOMES the
            // tombstone, and `create_topic` preserves it.
            //
            // Only `truncate` is written, never `high`: #179 restored
            // `expire_prefix_segments` as that field's single writer, which is
            // what keeps the floor-certified watermark cache's certification
            // argument unconditional (#237).
            //
            // The cost is one small object per partition of a deleted topic,
            // for as long as the slices it hides survive. Dropping it early
            // resurrects those records, so it is not dropped from here — the
            // expiry that takes a sub-stream's last segment drops it, being the
            // one operation that has just proved there is nothing left to hide
            // (#532). Until then it stays, and a same-named successor starts
            // past it.
            for partition in 0..metadata.topic.num_partitions {
                let topition = Topition::new(metadata.topic.name.as_str(), partition);

                _ = self.delete_records_before(&topition, -1).await?;
            }

            let prefix = Path::from(format!(
                "clusters/{}/groups/consumers/",
                self.identity.cluster
            ));

            let topic_name = metadata.topic.name.clone();
            let prefix_clone = prefix.clone();
            let locations = self
                .scan(Scan::AdminDelete, &prefix)
                .filter_map(move |m| {
                    let prefix = prefix_clone.clone();
                    let topic_name = topic_name.clone();
                    async move {
                        m.map_or(None, |m| {
                            debug!(?m.location);

                            m.location.prefix_match(&prefix).and_then(|mut i| {
                                // skip over the consumer group name
                                _ = i.next();

                                let sub = Path::from_iter(i);
                                debug!(?sub);

                                if sub.prefix_matches(&Path::from(format!(
                                    "offsets/{}/partitions/",
                                    topic_name
                                ))) {
                                    Some(Ok(m.location.clone()))
                                } else {
                                    None
                                }
                            })
                        })
                    }
                })
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            // And the same topic out of every group's one offsets object (#406).
            // A committed offset that outlives its topic is served against the
            // recreated one, which is #241's shape — 70 topics reporting a
            // committed offset above a high watermark of 0 — so the two layouts
            // have to be swept together or deleting a topic only half works.
            //
            // One delimited listing for the group ids, then a conditional GET
            // each and a write only where the topic was actually held. On an
            // admin path, against a group count, not a partition count.
            let groups = self
                .scan_delimited(
                    Scan::AdminDelete,
                    &Path::from(format!(
                        "clusters/{}/groups/consumers/",
                        self.identity.cluster
                    )),
                )
                .await?;

            for group in groups.common_prefixes {
                // A common prefix has no filename, so the group id is its last
                // path component.
                let Some(group_id) = group
                    .parts()
                    .next_back()
                    .map(|part| part.as_ref().to_owned())
                else {
                    continue;
                };

                let Some(offsets) = self.group_offsets(&group_id)? else {
                    continue;
                };

                let held = offsets
                    .get_opt(&self.object_store)
                    .await
                    .inspect_err(|error| warn!(?error, %group_id, "group offsets"))
                    .unwrap_or_default();

                if !held.is_some_and(|group| group.committed.contains_key(&metadata.topic.name)) {
                    continue;
                }

                _ = offsets
                    .with_mut(&self.object_store, |group| {
                        Ok(group.remove_topic(&metadata.topic.name))
                    })
                    .await
                    .inspect_err(|error| {
                        warn!(?error, %group_id, topic = %metadata.topic.name, "dropping a deleted topic's committed offsets")
                    });
            }

            // Hand the prefix a retention threshold that outlives the topic
            // (#532), before the metadata object that has been carrying it goes.
            //
            // The tombstones above are the whole of what the delete can do to a
            // shared segment, and their cost was accepted on the understanding
            // that the segments themselves are reclaimed later, as the other
            // topics on the prefix expire. For a topic that is the last occupant
            // of its prefix — every topic, when the names carry fewer components
            // than the prefix depth — there is no later: every maintenance
            // universe is derived from `topic-metadata/`, so this delete would
            // remove the only thing giving the prefix a threshold and its
            // segments would survive every retention setting, forever. A real
            // account kept 27 899 of 27 899 `.seg` objects after all 1 000 of
            // its topics were deleted.
            //
            // Written for every deleted topic, not only for a last occupant:
            // whether a sibling survives is not knowable here without a race,
            // and a marker on a prefix that still has live topics costs one
            // small object and changes nothing — `segment_retention_thresholds`
            // lets the live topic's threshold win.
            //
            // Under the *pinned* prefix (#236), which is where the topic's
            // records actually are, rather than a re-derivation: the marker has
            // to name the prefix holding the segments, and the pin is the only
            // authority on that. Read before the pin is deleted below.
            //
            // Fatal on failure, like the deletions above and for the same reason
            // (#251): the topic still exists, the failure is visible, and
            // re-issuing `DeleteTopics` finishes the job. Swallowing it would
            // strand the segments silently, which is the bug.
            let retired = Topition::new(metadata.topic.name.as_str(), 0);
            let retired_prefix = self.routed_prefix_of(&retired).await?;

            self.retire_prefix(
                &retired_prefix,
                metadata.topic.name.as_str(),
                Self::effective_retention_ms(&metadata.topic),
            )
            .await?;

            // Only now that the data is gone: the metadata object, its id ->
            // name pointer, and the routing pin. Past this point the topic no
            // longer exists for the API or for maintenance, so nothing above may
            // still need doing.
            self.topic_meta(metadata.topic.name.as_str())?
                .remove(&self.object_store)
                .await?;

            // Every process-local cache keyed by this topic, in one call (#554).
            // After the tombstone write above, so the watermark handle this drops
            // is not one a later step still needs.
            self.topics.forget(metadata.topic.name.as_str());
            self.invalidate_topic_index();

            for path in [
                self.topic_id_path(&metadata.id),
                // The routing pin goes with the topic (#236): it is immutable for a
                // topic's lifetime, so leaving it would let a same-named successor
                // inherit a dead incarnation's routing.
                self.topic_routing_path(metadata.topic.name.as_str()),
            ] {
                match self.object_store.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(otherwise) => return Err(otherwise.into()),
                }
            }

            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

    /// The body of [`Storage::create_topic`]: validation, the routing pin the
    /// topic's prefix is resolved through ever after (#236/#242), and the
    /// create-only PUTs that make it visible to the other replicas.
    pub(super) async fn create_topic_objects(
        &self,
        mut topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        // Before anything is written, and here rather than in the
        // `CreateTopics` service, for the same reason the config defaults are
        // (#225): this is the single creation choke point, and metadata
        // auto-create takes its topic name straight off the wire (#443).
        validation::creatable_topic(&topic)?;

        // `validate_only` is a dry run, and it used to create the topic — which
        // makes a plan/apply provider or a CI validation job provision for real
        // while reporting that it would have. Existence is still checked,
        // because Kafka's dry run reports `TOPIC_ALREADY_EXISTS` and that is the
        // answer a plan is asking for; nothing else is touched.
        //
        // The nil uuid, not a fresh one: no topic was created, so there is no id
        // to name — and `NULL_TOPIC_ID` is what a client reads it back as.
        if validate_only {
            return if self
                .topic_metadata(&TopicId::Name(topic.name.clone()))
                .await?
                .is_some()
            {
                Err(Error::Api(ErrorCode::TopicAlreadyExists))
            } else {
                Ok(Uuid::nil())
            };
        }

        let id = Uuid::now_v7();
        debug!(%id);

        // The single choke point for the broker-level config defaults (#225).
        // Every creation path lands here — `CreateTopics`, auto-create, and
        // anything added later — so a topic's stored config cannot depend on which
        // API materialised it. It used to be applied in the `CreateTopics` service
        // only, and auto-create, which builds its own `CreatableTopic`, stored no
        // config at all: invisible in `DescribeConfigs`, and expiring on Kafka's
        // absent-policy fallback instead of the configured default. Injection is
        // idempotent and never overwrites a value the caller supplied.
        self.tuning
            .topic_defaults
            .apply(topic.configs.get_or_insert_with(Vec::new));

        // Create-only PUT of the per-topic object. A losing creator (another
        // replica racing the same name) gets `false` here and returns
        // `TopicAlreadyExists` without overwriting the winner's object.
        let created = self
            .topic_meta(topic.name.as_str())?
            .create(
                &self.object_store,
                TopicMetadata {
                    id,
                    topic: topic.clone(),
                },
            )
            .await?;

        if !created {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        // id -> name pointer so a lookup by topic-id can resolve to the named
        // object. Written after the topic object so only the winning creator
        // (whose id is the topic's id) ever writes it.
        _ = self
            .object_store
            .put_opts(
                &self.topic_id_path(&id),
                serde_json::to_vec(&TopicIdRef {
                    name: topic.name.clone(),
                })
                .map(Bytes::from)
                .map(PutPayload::from)?,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        // Pin the routing prefix from the config this topic is created with
        // (#236), so it is never derived from `cleanup.policy` again. Reached only
        // by the creator that won the metadata CAS above, so an overwriting PUT is
        // uncontended here — and it is deliberately an overwrite rather than a
        // create: it clears any pin left behind by a torn delete of a same-named
        // predecessor, which a create-only write would silently adopt.
        //
        // The sub-stream identity is pinned by the same write (#442). Under the
        // v4 writer regime a topic is keyed by its own id, which is what makes
        // this creation a **new log** rather than a continuation of whatever a
        // same-named predecessor left in the shared segments — see
        // [`Substream`]. Under v3 it is `None`, byte-identical to every pin
        // written before this existed.
        let pinned = TopicRouting {
            prefix: self.routed_prefix(
                &Topition::new(topic.name.as_str(), 0),
                Self::topic_configs_are_compacted(&topic),
            ),
            substream_id: (self.tuning.segment_format_version >= SEGMENT_FORMAT_VERSION_V4)
                .then_some(id),
        };
        _ = self
            .object_store
            .put_opts(
                &self.topic_routing_path(topic.name.as_str()),
                serde_json::to_vec(&pinned)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                PutOptions::default(),
            )
            .await?;

        self.topics
            .remember_routing(topic.name.clone(), pinned.clone());

        for partition in 0..topic.num_partitions {
            let topition = Topition::new(topic.name.as_str(), partition);

            let watermark = self.watermark(&topition)?;

            // Drop any stale next-offset hint (e.g. a topic of the same
            // name was previously deleted) so the fresh, empty partition
            // re-derives its offsets from listing. The cached watermark floor
            // must go with it: the prefix's seq floor is unrelated to topic
            // lifecycle, so it alone would never invalidate a floor cached
            // for the deleted incarnation.
            //
            // The truncation-floor memo (#176) goes too, but for the opposite
            // reason since #246: not to forget the predecessor's floor but to
            // re-read it from the watermark object, which now carries the
            // deleted log end rather than a dead incarnation's stale value.
            self.topics.forget_partition(&topition)?;

            // Preserve `truncate` (#246).
            //
            // `delete_topic` removes a topic's own objects but cannot remove its
            // slices inside SHARED segments: a segment multiplexes many topics,
            // is immutable, and is reclaimed whole only once every sub-stream in
            // it is past retention (#61). Slices are located by `(topic,
            // partition)` NAME in the footer, so a topic created afterwards with
            // the same name — by an operator, or by auto-create on the next
            // metadata request — found its predecessor's slices, folded its
            // offsets from them, and served those records as its own. A
            // `DeleteTopics` that reads as "the data is gone" left it readable
            // through a same-named successor, silently, for as long as a segment
            // holding a slice survived.
            //
            // This used to clear the floor outright, on the assumption that a
            // fresh partition re-derives offset 0 from listing. That holds only
            // when nothing survives, which is exactly what a shared segment
            // breaks. `delete_topic` now leaves the floor at the deleted log end,
            // so keeping it is what makes the successor start past whatever it
            // would otherwise inherit — through the machinery the read paths
            // already honour (#176), without rewriting a shared segment.
            //
            // Deliberately NOT computed here from `high_watermark`: that answers
            // the same question, but it costs a segment LIST per partition on
            // every create — including auto-create, on the metadata path — which
            // is the cost #40 and #167 exist to remove. A name that never had a
            // predecessor has no watermark object at all, so preserving the field
            // costs nothing and does nothing.
            //
            // `high` is still cleared: it re-derives from the segment fold, and
            // `expire_prefix_segments` stays its single writer (#179, #237).
            //
            // A name whose predecessor's segments have since been reclaimed has
            // no watermark object at all — retention drops the tombstone with
            // the last slice it was hiding (#532) — so this preserves nothing
            // and the successor starts at 0, which is correct: there is no
            // longer anything for it to inherit.
            //
            // An **id-keyed** topic clears the floor instead of preserving it
            // (#442). The floor's whole job was to hide a predecessor's slices
            // from a successor that would otherwise find them by name; keyed by
            // id there is nothing to hide, because there is nothing this
            // incarnation can reach. Keeping it would be worse than pointless —
            // it would clamp a genuinely empty log to a dead incarnation's end,
            // which is the very "does not restart at 0" this exists to fix.
            //
            // The `served` certification goes with it for the same reason: it
            // certifies what a *previous* incarnation's expiry left servable, and
            // read paths honour it against the floor.
            let id_keyed = pinned.substream_id.is_some();

            watermark
                .with_mut(&self.object_store, |watermark| {
                    _ = watermark.high.take();

                    if id_keyed {
                        _ = watermark.truncate.take();
                        _ = watermark.served.take();
                    }

                    Ok(())
                })
                .await?;
        }

        // Reflect the new topic in this replica's list-all view at once.
        self.invalidate_topic_index();

        Ok(id)
    }
}
