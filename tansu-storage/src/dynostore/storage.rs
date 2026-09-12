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

//! The [`Storage`] trait implementation: one method per Kafka API, each of
//! them a thin arrangement of the helpers the sibling modules define.

use super::*;

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        // Idempotent, concurrency-safe backfill of any legacy monolithic topic
        // metadata into per-topic objects on first boot of this version.
        self.migrate_legacy_topic_metadata().await?;

        // Warm the in-memory topic index now, while the listener is still
        // closed, so the first client's list-all metadata request hits a hot
        // cache instead of paying the cold build (LIST + GET-per-topic).
        self.warm_topic_index().await;

        Ok(())
    }

    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        self.tuning.auto_create
    }

    fn fetch_max_bytes(&self) -> u32 {
        self.tuning.fetch_max_bytes
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _ = resource;

        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let handle = self.topic_meta(resource.resource_name.as_str())?;

                // Only mutate an existing topic: `with_mut` would otherwise
                // create one from `Default` (Kafka alter on an unknown topic is
                // a no-op here, matching the previous behaviour).
                if handle.get_opt(&self.object_store).await?.is_some() {
                    handle
                        .with_mut(&self.object_store, |topic_metadata| {
                            topic_metadata
                                .alter_configs(resource.configs.as_deref().unwrap_or_default())
                        })
                        .await?;
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.create_topic_objects(topic, validate_only).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.identity.cluster, ?topics);

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let topition = Topition::new(topic.name.clone(), partition.partition_index);

                    let partition_result = match self
                        .delete_records_before(&topition, partition.offset)
                        .await
                    {
                        Ok(low_watermark) => DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(ErrorCode::None.into()),

                        Err(err) => {
                            error!(?err, ?topition);

                            // Same reasoning as the offset-commit path (#275),
                            // lower stakes: an admin call rather than a client
                            // hot loop, but a transient storage error is still
                            // worth a retry rather than a fatal answer.
                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(storage_error_code(&err).into())
                        }
                    };

                    partition_responses.push(partition_result);
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.delete_topic_objects(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.identity.node;
        let host = self
            .identity
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self
            .identity
            .advertised_listener
            .port()
            .unwrap_or(9092)
            .into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        {
            // Kafka's `message.max.bytes`, refused before anything is buffered
            // (#443). Without a cap an unbounded payload reaches the write path,
            // and an application built against a broker with no limit breaks the
            // day it is pointed at a stock Kafka that has one — which is the
            // failure this exists to bring forward.
            //
            // Measured on the wire batch, which is what Kafka measures: the
            // batch length plus the `base_offset (i64) + batch_length (i32)`
            // prefix that precedes it in the log, so the number an operator sets
            // is the number of bytes a record set costs.
            let batch_bytes =
                size_of::<i64>() + size_of::<i32>() + deflated.batch_length.max(0) as usize;

            if batch_bytes > self.tuning.message_max_bytes {
                warn!(
                    ?topition,
                    batch_bytes,
                    message_max_bytes = self.tuning.message_max_bytes,
                    "refusing a batch larger than message_max_bytes"
                );

                return Err(Error::Api(ErrorCode::MessageTooLarge));
            }

            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            // Captured before `deflated` is moved into the prefix buffer; the
            // transaction registration below needs both.
            let last_offset_delta = deflated.last_offset_delta;
            let producer_epoch = deflated.producer_epoch;

            // Every batch is buffered into a prefix-coalesced segment (#57/#174):
            // a compacted topic under its own dedicated prefix (#175), everything
            // else under its connector prefix. There is no second write path, and
            // with #178 there is no longer any code that can form a legacy
            // `records/` key — so the #78 dual-offset-authority class is
            // impossible by construction rather than prevented by a routing
            // invariant. That covers transactional and control batches (#174
            // release B; footer v3 indexes them, `meta.json` stays the commit
            // authority) and bulk backfill (#90), which trips the raised byte
            // threshold and flushes as ~its own segment, keeping the 1-PUT parity
            // the old #62 bypass gave.
            //
            // Idempotent dedup belongs to the segment flush, which folds the log's
            // producer coordinates into a `ProducerTable` (#88) — a
            // cross-pod-convergent authority that cannot advance before the batch
            // is durable. The per-pod `producers/{id}.json` gate it replaced
            // diverged across a connection migration (#79) and mishandled i32
            // sequence wraparound (#80); with the legacy path gone, nothing
            // reaches it and it goes too.
            //
            // Deliberately NOT an early return: a transactional produce that
            // skipped the registration below would leave its range out of
            // `meta.transactions`, `txn_end` would find nothing to mark, and a
            // read-committed consumer would read aborted data as committed (the
            // #81 bug class).
            let offset = self.enqueue_prefix_coalesced(topition, deflated).await?;

            // Register the produced range on the open transaction. Covers the
            // end-transaction marker too (`txn_end` produces it with the
            // transaction id and the transactional attribute), extending
            // `offset_end` over the marker's offset. Idempotent under retries:
            // a leaseless `Duplicate` ack returns the *original* offset, and
            // the `and_modify` below only ever widens `offset_end`.
            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) = transaction.epochs.get_mut(&producer_epoch) {
                                debug!(?txn_detail);

                                let offset_end = offset + last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            Ok(offset)
        }
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let started_at = SystemTime::now();

        // Read-committed needs the last-stable offset, which `offset_stage`
        // derives from the cluster `meta.json` transactions. Read-uncommitted
        // (the common case) only needs the high watermark, derived purely from
        // the immutable batch objects — so go straight to `high_watermark` and
        // keep the hot fetch path off the meta object entirely.
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            self.offset_stage(topition).await?.last_stable
        } else {
            self.high_watermark(topition).await?
        };

        debug!(high_watermark);

        let mut batches = vec![];

        if offset < high_watermark {
            // Records live in shared segments, located by footer index and read
            // with a ranged GET of exactly the topition's byte span — no
            // cross-topic download. That is the only representation a read path
            // serves (#179): the legacy `records/` seam this used to stitch across
            // is gone, so there is no hybrid branch and no `[0, C)` region to
            // serve first.
            //
            // Nothing below the truncation floor is served (#176): truncated
            // records survive physically in shared segments, so the floor is
            // enforced by clamping the requested offset — skip, not error, and
            // batch-granular by construction via the whole-batch skip in
            // `fetch_prefix_coalesced`. Served from in-process caches on a warm
            // poll (`truncate_floor` memoizes absence, so floor-less partitions
            // add no request, #161).
            let offset = offset.max(self.truncate_floor(topition).await?);

            batches = self
                .fetch_prefix_coalesced(
                    topition,
                    offset,
                    max_bytes,
                    high_watermark,
                    started_at,
                    max_wait,
                )
                .await?;

            // An empty log cannot serve an offset below its end, and saying so is
            // what lets the consumer recover on its own (#337).
            //
            // The state: retention removed every segment, so `log_start` is
            // `high_watermark` (#299) and a group whose committed offset predates
            // that start asks for offsets no segment will ever hold. Answering
            // empty reads to a consumer as "caught up, nothing new", so it polls
            // again, forever — and because `poll()` covers the whole assignment, one
            // such partition stops delivery on every healthy partition sharing it.
            // Production: 77 stranded partitions, 15 of 16 members holding at least
            // one, zero records delivered for days.
            //
            // `OFFSET_OUT_OF_RANGE` is Kafka's defined answer here, and it is
            // load-bearing rather than cosmetic: `auto.offset.reset` then moves the
            // consumer to a live position with no operator action, and `none` fails
            // loudly, which is also correct.
            //
            // Why "no segment at all" and not "below `log_start`":
            //
            // - with segments present, an offset below their base is already served
            //   the records *above* it, so the consumer advances and nothing wedges;
            // - the truncation floor is deliberately a skip and not an error (#176),
            //   and a production fix is not the place to reverse that.
            //
            // Why this is not #292's detector, which reset live consumers on 61
            // topics and was removed in #314: that condition was *an index entry
            // claiming the offset over a read that produced none of it* — a
            // heuristic a stale index could forge. This one is the absence of any
            // segment, which is the definition of an empty log and is already what
            // the broker advertises through `log_start == log_end`. Answering
            // consistently with what ListOffsets already claims adds no new way to
            // be wrong.
            //
            // Cost: nothing when records are served. The index read happens only on
            // a fetch that already came back empty, and it is an index read — no
            // LIST, no confirming re-read.
            if batches.is_empty() && self.segment_region_start(topition).await?.is_none() {
                debug!(
                    ?topition,
                    offset,
                    high_watermark,
                    "no segment holds this offset and the log is empty; \
                     answering OFFSET_OUT_OF_RANGE (#337)"
                );

                return Err(Error::Api(ErrorCode::OffsetOutOfRange));
            }

            // The mid-log sibling of the check above (#290): the log is not
            // empty — segments below the offset are still served — but the
            // offsets from the surviving tail up to the floor were destroyed by
            // a segment expiry, and that expiry certified so in the watermark.
            // Without the certification this state is indistinguishable from a
            // peer having acked offsets this process never listed (where empty
            // is the right answer and an error would reset live consumers — the
            // #292/#314 lesson), so only the certified case errors. A consumer
            // parked here polls empty forever otherwise: `auto.offset.reset`
            // then moves it to a live position, and `none` fails loudly, which
            // is also correct.
            if batches.is_empty()
                && let Some(served) = self.certified_dead_gap(topition).await?
                && served.gap_contains(offset)
            {
                info!(
                    ?topition,
                    offset,
                    end = served.end,
                    at_high = served.at_high,
                    "offset is in a gap certified dead by segment expiry; \
                     answering OFFSET_OUT_OF_RANGE (#290)"
                );

                return Err(Error::Api(ErrorCode::OffsetOutOfRange));
            }
        }

        Ok(batches)
    }

    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        // Read-committed needs the last-stable offset and the aborted-transaction
        // list, both derived from the cluster `meta.json` object — take the full,
        // transaction-aware path (a fresh meta read, unchanged semantics).
        if isolation == IsolationLevel::ReadCommitted {
            return self.offset_stage(topition).await;
        }

        // Read-uncommitted (the common consumer case): no transaction state is
        // needed. The last-stable offset is the high watermark and there are no
        // aborted transactions to surface, so `meta.json` — the single, hot,
        // cluster-wide key — is never read. The high watermark comes from the
        // in-memory hint (#40), and the log start from the oldest surviving
        // segment (#179), clamped to the cached truncation floor (#176). The log
        // start used to come from the cached `watermark.low`, which only the legacy
        // retention paths ever advanced — authoritatively silent for a
        // pure-segment sub-stream, and usually absent (#161). The segment index is
        // the authority instead, and it is more accurate: nothing advances that
        // field after a segment expiry.
        //
        // Still request-free on a warm poll: the index refresh is per prefix and
        // TTL-bounded, so a caught-up consumer resolves its fetch-response offsets
        // with zero per-partition requests, off the meta-object throttle ceiling
        // entirely.
        let high_watermark = self.high_watermark(topition).await?;
        // No segment means an empty log, whose start is its end (#290) — see
        // [`Self::log_start`], which this mirrors on the read-uncommitted path.
        let log_start = self
            .segment_region_start(topition)
            .await?
            .unwrap_or(high_watermark)
            .max(self.cached_truncate(topition)?.unwrap_or(0));

        Ok(OffsetStage {
            last_stable: high_watermark,
            high_watermark,
            log_start,
            aborted: Vec::new(),
        })
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let (stable, aborted_raw) = self
            .meta
            .with(&self.object_store, |meta| {
                let stable = meta.open_transaction_floors();

                // Aborted transactions that produced to `topition` (#81), as
                // `(producer_id, first_offset, last_offset)` — read-committed
                // consumers use these to drop aborted records below the LSO. The
                // abort state is retained in `meta.transactions` (`txn_end` sets
                // `Aborted`, never prunes), so this is a pure meta read.
                let mut aborted_raw: Vec<(i64, i64, i64)> = Vec::new();
                for txn in meta.transactions.values() {
                    for detail in txn.epochs.values() {
                        if detail.state != Some(TxnState::Aborted) {
                            continue;
                        }
                        if let Some(partitions) = detail.produces.get(&topition.topic)
                            && let Some(Some(range)) = partitions.get(&topition.partition)
                        {
                            aborted_raw.push((txn.producer, range.offset_start, range.offset_end));
                        }
                    }
                }

                Ok((stable, aborted_raw))
            })
            .await?;

        debug!(?stable, ?aborted_raw);

        // The high watermark is derived from the immutable batch objects (the
        // authority), not from the mutable `watermark` object — so it is correct
        // across replicas even though offset assignment no longer CASes the
        // watermark on every produce (#13). The `watermark` object is consulted
        // only for the log start offset (`low`), advanced on the cold
        // maintain/expire path (and for lake-sink topics' high, folded into
        // `high_watermark`).
        let high_watermark = self.high_watermark(topition).await?;

        let log_start = self.log_start(topition, high_watermark).await?;
        let last_stable = stable.get(topition).copied().unwrap_or(high_watermark);

        // Keep aborted transactions whose records are still in the log (last
        // offset at/after the log start), as `(producer_id, first_offset)` sorted
        // by first offset (#81).
        let mut aborted: Vec<(i64, i64)> = aborted_raw
            .iter()
            .filter(|(_, _, offset_end)| *offset_end >= log_start)
            .map(|(producer_id, offset_start, _)| (*producer_id, *offset_start))
            .collect();
        aborted.sort_by_key(|(_, first_offset)| *first_offset);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
            aborted,
        })
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let stable = if isolation_level == IsolationLevel::ReadCommitted {
            self.meta
                .with(
                    &self.object_store,
                    |meta| Ok(meta.open_transaction_floors()),
                )
                .await?
        } else {
            BTreeMap::new()
        };

        // Resolve the partitions CONCURRENTLY (bounded) instead of awaiting
        // each in turn. A single ListOffsets can carry a consumer's whole
        // assignment — `endOffsets` over ~1500 partitions. On the warm
        // prefix-coalesced path a partition now costs ZERO per-partition
        // object-store round-trips (LATEST and EARLIEST are served from the
        // segment index, `coalesced_high_from_index` /
        // `coalesced_earliest_offset`; only per-prefix amortized requests
        // remain), but the cold, hybrid and non-coalesced paths still pay at
        // least one round-trip each (the `watermark.json` GET in
        // `persisted_high`, or a `records/` LIST). The sequential loop was
        // O(partitions × RTT) there and blew past the Kafka client's request
        // timeout at scale, so the consumer could never resolve its
        // positions. Bounded concurrency issues exactly the same
        // per-partition reads (and returns the same answers) while making
        // wall-time O(partitions / concurrency); `buffered` — not
        // `buffer_unordered` — preserves request order in the response, as
        // the loop did. These per-partition paths already run concurrently
        // across independent client requests, so no new interleaving is
        // introduced. The bound matches `FOOTER_FETCH_CONCURRENCY`, keeping a
        // wide request within the object store's throttling envelope.
        const LIST_OFFSETS_CONCURRENCY: usize = 32;

        let stable = &stable;

        // Eagerly collected: a lazily-mapped iterator of async blocks inside
        // `stream::iter` trips a higher-ranked lifetime inference failure
        // ("implementation of `FnOnce` is not general enough") under
        // `async_trait`; the Vec pins every future to the one concrete
        // lifetime of this call. The futures are inert until polled by
        // `buffered`, so this allocates, it does not serialize.
        let resolutions = offsets
            .iter()
            .map(|(topition, offset_request)| async move {
                self.list_offset_response(topition, offset_request, stable)
                    .await
                    .map(|response| response.map(|response| (topition.to_owned(), response)))
            })
            .collect::<Vec<_>>();

        futures::stream::iter(resolutions)
            .buffered(LIST_OFFSETS_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .map(|responses| responses.into_iter().flatten().collect())
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        // One index refresh for the whole commit (#387). The existence test below
        // is per *partition*, purely to decide whether to write the offset, so a
        // group committing 1,500 topics × 16 partitions asked the object store
        // 24,000 times for answers the index already holds — the same topic over
        // and over. Served from the index it costs nothing, and only a topic the
        // index does not hold reads its own object.
        self.refresh_index_for_described_reads("offset_commit")
            .await;

        // Resolve which topitions exist, concurrently and off the topics index
        // (#387/#154): a group committing 1,500 topics × 16 partitions asks about
        // the same topic 16 times, and the index answers all of it for free.
        //
        // `try_collect` keeps the fail-fast semantics of the `?` (a metadata read
        // error aborts the commit) and `buffered` preserves response order, which
        // the response the client gets is keyed on.
        const OFFSET_COMMIT_CONCURRENCY: usize = 32;

        // Eagerly collected to pin lifetimes under `async_trait` (see #147).
        let resolutions = offsets
            .iter()
            .map(|(topition, offset_commit)| async move {
                // The topic AND the partition (#445). Committing partition 9 of
                // a two-partition topic used to succeed, because the test was
                // whether the *topic* existed — so a configuration typo landed
                // as a stored offset and surfaced much later as "the consumer
                // restarted from the wrong place", pointing at the consumer
                // rather than at the number that was wrong.
                //
                // `UNKNOWN_TOPIC_OR_PARTITION` is the code the unknown-topic
                // case already answers, and it is Kafka's answer here too: the
                // partition does not exist, and the response path for that is
                // already in place below.
                let known = self
                    .described_topic_metadata(&TopicId::from(topition), "offset_commit")
                    .await?
                    .is_some_and(|metadata| {
                        (0..metadata.topic.num_partitions).contains(&topition.partition())
                    });

                Ok::<_, Error>((topition.to_owned(), offset_commit.clone(), known))
            })
            .collect::<Vec<_>>();

        let resolved = futures::stream::iter(resolutions)
            .buffered(OFFSET_COMMIT_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        // One conditional write for the whole request (#406, #111): the layout
        // was one unconditional overwrite per partition, so a commit over `t`
        // partitions cost `t` billed PUTs. Consumer-group writes are 67% of the
        // fleet's PUT plane, and #111's acceptance — "a commit over `t` topitions
        // issues O(1) PUTs, not `1 + t`" — was never met.
        //
        // The CAS matters beyond the count: two replicas committing for the same
        // group used to race per object with last-write-wins per partition, so a
        // commit could interleave with another and leave a group's offsets from
        // two different requests. `with_mut` re-applies the fold on conflict, so
        // the losing writer folds onto the winner's value instead of over it.
        let committable: Vec<(Topition, OffsetCommitRequest)> = resolved
            .iter()
            .filter(|(_, _, known)| *known)
            .map(|(topition, commit, _)| (topition.clone(), commit.clone()))
            .collect();

        let stored = if committable.is_empty() {
            Ok(())
        } else {
            match self.group_offsets(group_id)? {
                Some(offsets) => {
                    offsets
                        .with_mut(&self.object_store, |group| {
                            for (topition, commit) in &committable {
                                group.insert(topition, commit.clone());
                            }

                            Ok(())
                        })
                        .await
                }

                // The widening group id `group_prefix` refuses (#277): there is no
                // object to write, and writing the root would be the bug that
                // issue exists for.
                None => Err(Error::Api(ErrorCode::GroupIdNotFound)),
            }
        };

        // #275: a failure here is an object-store failure, and they are
        // overwhelmingly transient — a 503 SlowDown, a timeout, a 5xx.
        // `UnknownServerError` is non-retriable in Kafka clients, so `commitSync`
        // threw rather than retrying and a connector treating that as engine death
        // restarted: a throttle burst turned into connector restarts. Answer the
        // way produce already does.
        //
        // One write now covers every partition in the request, so the code it
        // fails with covers them all too — which is what a client committing a
        // batch already assumes when it retries the batch.
        let commit_code = match stored {
            Ok(()) => ErrorCode::None,
            Err(error) => {
                error!(?error, group_id, partitions = committable.len());
                storage_error_code(&error)
            }
        };

        Ok(resolved
            .into_iter()
            .map(|(topition, _, known)| {
                let error_code = if known {
                    commit_code
                } else {
                    ErrorCode::UnknownTopicOrPartition
                };

                (topition, error_code)
            })
            .collect())
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        // The union of what the group's one offsets object holds and what the
        // per-partition layout left behind (#406). The set has to be a union, not
        // a preference: a group whose commits only ever landed in the new object
        // has nothing under the `offsets/` prefix to list, and a group that has
        // not committed since the upgrade has nothing in the new object — an
        // `OffsetFetch(all)` that took either alone would answer empty for one of
        // them, which is a consumer resuming from nothing.
        let mut topitions: Vec<Topition> = match self.group_offsets(group_id)? {
            Some(offsets) => offsets
                .get_opt(&self.object_store)
                .await
                .inspect_err(|error| warn!(?error, group_id, "group offsets"))
                .unwrap_or_default()
                .map(|group| group.topitions().collect())
                .unwrap_or_default(),

            None => Vec::new(),
        };

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.identity.cluster, group_id,
            ));

            let mut list_stream = self.scan(Scan::Group, &location);

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(Error::from)?
            {
                debug!(?meta);
                let Some(topic): Option<String> = meta
                    .location
                    .parts()
                    .nth(6)
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                // The broker writes 10-digit zero-padded partition names, so a
                // shorter component means a foreign or truncated object in the
                // bucket rather than anything this cluster produced. Skip it:
                // slicing it panicked the request task (#276).
                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .and_then(|partition| {
                        partition
                            .as_ref()
                            .get(0..10)
                            .map(i32::from_str)
                            .or_else(|| {
                                warn!(
                                    location = %meta.location,
                                    "skipping an offset object whose partition component is too short"
                                );
                                None
                            })
                    })
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        topitions.sort();
        topitions.dedup();

        self.offset_fetch(Some(group_id), topitions.as_ref(), Some(false))
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            // Fetch each partition's committed offset concurrently (bounded):
            // the same O(N) -> O(N / concurrency) scaling fix as ListOffsets
            // (#147) and Metadata (#154). A large formed group (and the
            // rebalance-callback `committed()` lookups) reads offsets for
            // hundreds-to-thousands of partitions at once; a serial GET per
            // partition blew past the client timeout at scale. `try_collect`
            // preserves the fail-fast semantics of the `?` below (a transient
            // store error stays retriable, not a fatal `-1`, #6/#129).
            const OFFSET_FETCH_CONCURRENCY: usize = 32;

            // The group's one offsets object first (#406): one conditional GET,
            // served from the etag memo, answers every partition it holds. What it
            // does not hold falls through to that partition's own object below —
            // which is how the layout migrates without a fold-everything pass,
            // and why a rollback still finds the pre-upgrade offsets where it
            // expects them.
            let group = match self.group_offsets(group_id)? {
                Some(offsets) => offsets
                    .get_opt(&self.object_store)
                    .await
                    .inspect_err(|error| warn!(?error, group_id, "group offsets"))
                    .unwrap_or_default()
                    .unwrap_or_default(),

                None => GroupOffsets::default(),
            };

            // Eagerly collected to pin lifetimes under `async_trait` (see #147).
            let fetches = topics
                .iter()
                .map(|topition| {
                    // The whole commit, not just its offset: the metadata a
                    // client stored beside the offset used to be projected away
                    // right here, so it was accepted, written, and never read
                    // back (#445).
                    let held = group.get(topition).map(CommittedOffset::from);

                    async move {
                        if let Some(committed) = held {
                            return Ok::<_, Error>((topition.to_owned(), committed));
                        }

                        let location = Path::from(format!(
                            "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                            self.identity.cluster, group_id, topition.topic, topition.partition,
                        ));

                        let committed = match self.object_store.get(&location).await {
                            Ok(get_result) => get_result
                                .bytes()
                                .await
                                .map_err(Error::from)
                                .and_then(|encoded| {
                                    serde_json::from_slice::<OffsetCommitRequest>(&encoded[..])
                                        .map_err(Error::from)
                                })
                                .map(|commit| CommittedOffset::from(&commit))
                                .inspect_err(|error| error!(?error, ?group_id, ?topition)),

                            Err(object_store::Error::NotFound { .. }) => Ok(CommittedOffset::NONE),

                            Err(error) => {
                                error!(?error, ?group_id, ?topition);
                                // Preserve the storage error so a transient S3
                                // failure is retriable, not fatal `-1` (#6/#129).
                                Err(Error::from(error))
                            }
                        }?;

                        Ok::<_, Error>((topition.to_owned(), committed))
                    }
                })
                .collect::<Vec<_>>();

            responses = futures::stream::iter(fetches)
                .buffered(OFFSET_FETCH_CONCURRENCY)
                .try_collect()
                .await?;
        }

        Ok(responses)
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.metadata_response(topics).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        match resource {
            // Deliberately the authoritative per-topic read, not the
            // index-served `described_topic_metadata` (#387): `topic_is_compacted`
            // reads through here to derive a routing prefix for a pre-#236 topic,
            // and that derivation is pinned create-only and permanently. A stale
            // `cleanup.policy` would route a topic's new records to a prefix its
            // existing segments are not under — unreachable data, not a stale
            // answer. Admin-rate plus a memoized derivation, so the request it
            // costs is not part of the plane #387 removed.
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigType::String.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })))
                }

                Ok(None) => Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some(vec![]))),

                // Not an admin-only path: `topic_is_compacted` calls this, and
                // it runs on produce and fetch via `routed_prefix_of` whenever
                // the memo misses (first use of a topic in a process, or TTL
                // expiry). So any transient object-store error while reading
                // topic metadata reached this arm, and transient storage errors
                // are routine.
                //
                // Propagate rather than panic, and rather than guessing a
                // routing verdict: `topic_is_compacted` already has a `?`, and
                // the storage error then classifies retriable (#275) instead of
                // taking the request task down (#276). Answering "not
                // compacted" on a failed read would be the other option, but
                // routing is pinned create-only (#236) and a wrong pin is
                // permanent — not a guess worth making on a blip.
                Err(err) => Err(err),
            },

            _ => Ok(DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(vec![]))),
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (partition_limit, cursor);

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        // One refresh, then every topic below is described from the index (#387).
        self.refresh_index_for_described_reads("describe_topic_partitions")
            .await;

        for topic in topics.unwrap_or_default() {
            match self
                .described_topic_metadata(topic, "describe_topic_partitions")
                .await
                .inspect_err(|error| error!(?error))
            {
                Ok(Some(topic_metadata)) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(topic_metadata.topic.name))
                        .topic_id(topic_metadata.id.into_bytes())
                        .is_internal(false)
                        .partitions(Some(
                            (0..topic_metadata.topic.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition_index(partition_index)
                                        .leader_id(self.identity.node)
                                        .leader_epoch(0)
                                        .replica_nodes(Some(vec![
                                            self.identity.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .isr_nodes(Some(vec![
                                            self.identity.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .eligible_leader_replicas(Some(vec![]))
                                        .last_known_elr(Some(vec![]))
                                        .offline_replicas(Some(vec![]))
                                })
                                .collect(),
                        ))
                        .topic_authorized_operations(-2147483648),
                ),

                Ok(None) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),

                Err(_) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownServerError.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),
            }
        }

        Ok(responses)
    }

    /// Every group in the cluster, optionally filtered by state.
    ///
    /// A group is whatever owns something under the consumer root: a `{group}/`
    /// common prefix — its committed offsets, its member documents and its
    /// generation — or a legacy `{group}.json` state object, which after the
    /// #359 cutover is an inert leftover that expiry reaps on its own. Both are
    /// collected, which fixes a group that has state but has never committed an
    /// offset being omitted from its own cluster's listing.
    ///
    /// Every listed group reports its real state, filtered or not (#475). That
    /// costs a read per group — the state is derived per group, and deriving it
    /// is reading the group — which is what [`Self::group_state`] keeps to one
    /// GET instead of one per member. Reporting `Unknown` unless the caller
    /// filtered was not the saving it looked like: the response field exists
    /// from `ListGroups` v4 on, every admin client surfaces it, and the tooling
    /// this request is for — find the idle groups, lag-dashboard the committed
    /// ones — filters on it *client side*, so `Unknown` for all of them just
    /// moved the failure. `Unknown` now means only what it says: this replica
    /// could not read that group.
    ///
    /// An **empty** `states_filter` is not a filter, it is the absence of one,
    /// which is Kafka's reading of the same field. Taking it as "match nothing"
    /// answered a plain `listConsumerGroups()` with nothing at all, because that
    /// is the request librdkafka and the Java admin client both send when no
    /// state was asked for (#475).
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        /// As the describe fan-out.
        const LIST_STATE_CONCURRENCY: usize = 32;

        let root = self.groups_root();
        let list_result = self
            .scan_delimited(Scan::Group, &root)
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.identity.cluster))?;

        let mut group_ids = BTreeSet::new();

        for prefix in list_result.common_prefixes {
            if let Some(group_id) = prefix.parts().next_back() {
                _ = group_ids.insert(group_id.as_ref().to_owned());
            }
        }

        for meta in list_result.objects {
            if let Some(group_id) = Self::group_of(&root, &meta.location) {
                _ = group_ids.insert(group_id);
            }
        }

        let listed = |group_id: String, state: String| {
            ListedGroup::default()
                .group_id(group_id)
                .protocol_type("consumer".into())
                .group_state(Some(state))
                .group_type(Some("classic".into()))
        };

        let wanted = states_filter
            .filter(|states| !states.is_empty())
            .map(|states| states.iter().cloned().collect::<BTreeSet<_>>());

        Ok(
            futures::stream::iter(group_ids.into_iter().map(|group_id| async move {
                // `Unknown` is a read that did not answer — a throttle, a
                // 5xx. Not knowing is not a state the group is in, and a
                // filter that names it is asking for exactly the groups this
                // replica could not read.
                let state = self
                    .group_state(&group_id)
                    .await
                    .inspect_err(|err| debug!(?err, group_id))
                    .unwrap_or(ConsumerGroupState::Unknown)
                    .to_string();

                (group_id, state)
            }))
            .buffered(LIST_STATE_CONCURRENCY)
            .filter_map(|(group_id, state)| {
                let keep = wanted
                    .as_ref()
                    .is_none_or(|wanted| wanted.contains(state.as_str()));

                async move { keep.then(|| listed(group_id, state)) }
            })
            .collect::<Vec<_>>()
            .await,
        )
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                // #277: an id contributing no path component of its own widens
                // the deletion prefix to the root of the consumer tree, and the
                // `delete_stream` below would then take every group and every
                // committed offset in the cluster with it. Refuse before any
                // path is built, and report it the way Kafka does.
                //
                // Not only reachable from a client: `expire_groups` derives its
                // ids by stripping `.json` off a listing, so a stray object
                // named exactly `.json` under the root yields an empty id from
                // the maintenance loop.
                let Some(prefix) = self.group_prefix(group_id) else {
                    warn!(
                        ?group_id,
                        cluster = self.identity.cluster,
                        "refusing to delete a group id that resolves to the consumer tree root"
                    );

                    results.push(
                        DeletableGroupResult::default()
                            .group_id(group_id.into())
                            .error_code(ErrorCode::InvalidGroupId.into()),
                    );

                    continue;
                };

                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.identity.cluster, group_id,
                ));

                // A group with live members is not deleted (#445). A cleanup
                // script or an operator command that removes the group of a
                // live consumer takes its coordinator out from under it
                // mid-poll; Kafka simply refuses, and refusing is the whole
                // safety property — an admin API that cannot be run by mistake.
                //
                // Membership comes from the same view `DescribeGroups` reports,
                // so the two APIs cannot disagree about a group an operator is
                // looking at while deciding to delete it. It is the generation's
                // member set, which `SyncGroup` maintains and a session-timeout
                // sweep retires, less whatever the clock has since condemned
                // (#523) — so a group whose consumers have all gone away becomes
                // deletable on its own, rather than only once some *other*
                // member of it comes back to trigger the sweep that retires
                // them. Nothing else was going to: with every member silent
                // there is by construction nobody left to make the request the
                // sweep hangs off.
                let view = self
                    .group_view(group_id)
                    .await
                    .inspect_err(|error| error!(?error, group_id, "composing the group view"))?;

                debug!(group_id, ?view);

                if let Some(detail) = view.as_ref()
                    && !detail.members.is_empty()
                {
                    debug!(group_id, members = detail.members.len(), "non-empty group");

                    results.push(
                        DeletableGroupResult::default()
                            .group_id(group_id.into())
                            .error_code(ErrorCode::NonEmptyGroup.into()),
                    );

                    continue;
                }

                // The legacy state object. Deleted for as long as one can
                // exist; after the cutover this is a 404 per deleted group and
                // the prefix below is what carries the group.
                _ = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| debug!(group_id, ?err));

                let locations = self
                    .scan(Scan::AdminDelete, &prefix)
                    .map_ok(|m| m.location)
                    .boxed();

                // Everything the group owns under its prefix: its committed
                // offsets, and since #359 its member documents, its generation
                // and every generation's assignment. One sweep covers all of
                // them because they share the prefix — which is also what makes
                // `expire_groups` layout-agnostic.
                // Drop the memoized `offsets.json` handle with the group (#406);
                // see `ClientCaches::forget_group` for why it is growth and not
                // correctness.
                self.clients.forget_group(group_id);

                let deleted = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted);

                // A group existed if ANYTHING of it did: a generation, or
                // something the sweep above removed — its committed offsets,
                // its member documents, its assignments. A client may commit
                // for a group id without ever joining, so neither signal alone
                // is sufficient.
                //
                // The legacy object used to be the third signal, taken from the
                // outcome of the delete just above — and a delete of an absent
                // key **succeeds**, on S3 and on `InMemory` alike. So that term
                // was always true, this branch was unreachable, and deleting a
                // group that never existed answered `NONE` (#445). Tooling that
                // distinguishes "idle group" from "nonexistent group" had
                // nothing to read.
                let existed = view.is_some() || !deleted.is_empty();

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if existed {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    /// Describe each group in `group_ids` (#240).
    ///
    /// Concurrent, because this is a fan-out over per-group objects and a client
    /// asks about every group it owns in one call. Sequentially this cost one
    /// round-trip per group, serialized: at a few hundred groups that is tens of
    /// seconds, past any admin client's deadline — observed in production as
    /// `context deadline exceeded` on `group describe`, and as the
    /// `listConsumerGroupOffsets` timeouts that made a consumer's rebalance
    /// callback throw. `buffered` keeps the response in request order while
    /// letting the object store answer in parallel.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        /// Matches the other per-object fan-outs in this file
        /// (`FETCH_EACH_CONCURRENCY`, `FOOTER_FETCH_CONCURRENCY`).
        const DESCRIBE_CONCURRENCY: usize = 32;

        let Some(group_ids) = group_ids else {
            return Ok(vec![]);
        };

        Ok(
            futures::stream::iter(group_ids.iter().cloned().map(|group_id| async move {
                match self.group_view(&group_id).await {
                    Ok(Some(group_detail)) => NamedGroupDetail::found(group_id, group_detail),

                    // No `generation.json` means the group does not exist, and
                    // Kafka's word for that is `Dead` — not `Empty`, which is a
                    // group that exists with nobody in it (#445). Reporting the
                    // two alike left cleanup-by-inactivity tooling unable to
                    // tell a group it should reap from one it has already
                    // reaped.
                    //
                    // A legacy `{group}.json` left behind by the cutover is
                    // still not consulted: it describes a membership that a
                    // quiesce made vacuous, and reading it would put a 404 on
                    // the describe path of every group in the cluster, forever.
                    Ok(None) => match self.group_remnants_exist(&group_id).await {
                        // It is there, with nobody in it.
                        Ok(true) => NamedGroupDetail::found(group_id, GroupDetail::default()),

                        Ok(false) => NamedGroupDetail::dead(group_id),

                        // Not knowing is not the same as not existing, for the
                        // same reason it is not the same as empty below.
                        Err(error) => {
                            error!(?error, group_id, "probing for group remnants");

                            NamedGroupDetail::error_code(
                                group_id,
                                if matches!(error, Error::ObjectStore(_)) {
                                    ErrorCode::CoordinatorLoadInProgress
                                } else {
                                    ErrorCode::UnknownServerError
                                },
                            )
                        }
                    },

                    // Not knowing is not the same as empty, and it is
                    // retriable. Answering `GroupDetail::default()` here made a
                    // throttle or a 5xx report a live group as empty — the same
                    // shape as #214, where an unresolvable topic was reported
                    // absent and clients could not tell it from a deleted one.
                    Err(error) => {
                        error!(?error, group_id, "could not compose the group view");

                        NamedGroupDetail::error_code(
                            group_id,
                            if matches!(error, Error::ObjectStore(_)) {
                                ErrorCode::CoordinatorLoadInProgress
                            } else {
                                ErrorCode::UnknownServerError
                            },
                        )
                    }
                }
            }))
            .buffered(DESCRIBE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await,
        )
    }

    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>> {
        let location = self
            .group_member_location(group_id, member_id)
            .ok_or_else(|| UpdateError::Error(Error::Api(ErrorCode::InvalidGroupId)))?;

        self.put(
            &location,
            member,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>> {
        let Some(location) = self.group_member_location(group_id, member_id) else {
            return Ok(None);
        };

        Self::absent_is_none(self.get::<MemberDoc>(&location).await)
    }

    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        let Some(location) = self.group_member_location(group_id, member_id) else {
            return Ok(());
        };

        match self.object_store.delete(&location).await {
            Ok(()) => Ok(()),
            // Already gone is the outcome asked for. The caller's contract is
            // best-effort anyway, and a member document is deleted from more
            // than one path (its own leave, and the sweep that evicted it).
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>> {
        let Some(prefix) = self.group_members_prefix(group_id) else {
            return Ok(BTreeMap::new());
        };

        let mut members = BTreeMap::new();
        let mut listing = self.scan(Scan::Group, &prefix);

        while let Some(meta) = listing.next().await.transpose()? {
            let Some(member_id) = Self::listed_member_id(&meta.location) else {
                continue;
            };

            // A document deleted between the listing and the read is a member
            // that left, not a failure of the listing.
            if let Some(pair) = Self::absent_is_none(self.get::<MemberDoc>(&meta.location).await)? {
                _ = members.insert(member_id, pair);
            }
        }

        Ok(members)
    }

    async fn list_group_member_stamps(&self, group_id: &str) -> Result<BTreeMap<String, i64>> {
        let Some(prefix) = self.group_members_prefix(group_id) else {
            return Ok(BTreeMap::new());
        };

        let mut stamps = BTreeMap::new();
        let mut listing = self.scan(Scan::Group, &prefix);

        // The listing and nothing else. `last_modified` is what the document
        // would have said: `MemberDoc::last_contact_ms` is only ever written
        // *as* `now`, so the object's write time and the stamp inside it are
        // the same reading — one taken by the store's clock rather than the
        // broker's, which for a window measured in seconds is a distinction
        // without a difference (#427).
        while let Some(meta) = listing.next().await.transpose()? {
            let Some(member_id) = Self::listed_member_id(&meta.location) else {
                continue;
            };

            _ = stamps.insert(member_id, meta.last_modified.timestamp_millis());
        }

        Ok(stamps)
    }

    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        let Some(location) = self.group_generation_location(group_id) else {
            return Ok(None);
        };

        Self::absent_is_none(self.get::<GenerationDoc>(&location).await)
    }

    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>> {
        let location = self
            .group_generation_location(group_id)
            .ok_or_else(|| UpdateError::Error(Error::Api(ErrorCode::InvalidGroupId)))?;

        self.put(
            &location,
            generation,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome> {
        let location = self
            .group_assignment_location(group_id, generation_id)
            .ok_or(Error::Api(ErrorCode::InvalidGroupId))?;

        // `None` is `PutMode::Create`, so this races on the key rather than on
        // an etag: exactly one writer creates it, and the loser is handed what
        // is stored.
        match self
            .put(&location, assignment, json_content_type(), None)
            .await
        {
            Ok(put_result) => Ok(AssignmentOutcome::Created(put_result.into())),

            Err(UpdateError::Outdated { current, .. }) => {
                Ok(AssignmentOutcome::AlreadyExists(current))
            }

            // The winner's assignment was deleted before it could be read back,
            // and the only thing that deletes one is
            // `delete_group_assignments_before` — so the group has already moved
            // past this generation and there is nothing here to adopt (#431).
            //
            // That is the same situation the caller's own post-create fence
            // answers, so give it the same retriable code rather than a value it
            // would have to invent: `RebalanceInProgress` is `Severity::Expected`
            // at the boundary, so the client is *told* to re-join instead of
            // having its connection dropped.
            Err(UpdateError::Vanished) => {
                debug!(
                    group_id,
                    generation_id,
                    sync_outcome = ?ErrorCode::RebalanceInProgress,
                    "the assignment this create lost to was swept",
                );

                Err(Error::Api(ErrorCode::RebalanceInProgress))
            }

            Err(UpdateError::Error(error)) => Err(error),
            Err(UpdateError::SerdeJson(error)) => Err(Error::SerdeJson(error)),
            Err(UpdateError::Uuid(error)) => Err(Error::Uuid(error)),
            // `put` never raises it — nothing here reads an etag back — but the
            // variant is part of the type, and a silent `unreachable!()` in a
            // storage path is worse than a named error.
            Err(UpdateError::MissingEtag) => Err(Error::Message(String::from(
                "assignment create reported a missing etag",
            ))),
        }
    }

    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>> {
        let Some(location) = self.group_assignment_location(group_id, generation_id) else {
            return Ok(None);
        };

        Ok(Self::absent_is_none(self.get::<AssignmentDoc>(&location).await)?.map(|(doc, _)| doc))
    }

    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64> {
        let Some(prefix) = self.group_assignments_prefix(group_id) else {
            return Ok(0);
        };

        // The names are zero-padded, so the listing is in generation order and
        // `start_after` would do — but a generation that overflowed into a
        // wider name, or an object written by a future layout, must not stop
        // the sweep. Decoding the name and comparing is cheap at one object per
        // rebalance.
        let mut condemned = vec![];
        let mut listing = self.scan(Scan::Group, &prefix);

        while let Some(meta) = listing.next().await.transpose()? {
            let below = meta
                .location
                .parts()
                .next_back()
                .and_then(|name| {
                    name.as_ref()
                        .strip_suffix(".json")
                        .and_then(|stem| stem.parse::<i32>().ok())
                })
                .is_some_and(|generation| generation < generation_id);

            if below {
                condemned.push(meta.location);
            }
        }

        let deleted = self
            .object_store
            .delete_stream(futures::stream::iter(condemned.into_iter().map(Ok)).boxed())
            .try_collect::<Vec<Path>>()
            .await?;

        debug!(group_id, generation_id, ?deleted);

        Ok(deleted.len() as u64)
    }

    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        // Every creation reports `None`: a rule that is already there is not a
        // failure. `kafka-acls.sh` is run from configuration management, so
        // re-applying the same file must not start reporting errors on the
        // second run.
        let outcome = vec![ErrorCode::None; bindings.len()];

        if bindings.is_empty() {
            return Ok(outcome);
        }

        self.update_acls(|acls| {
            for binding in bindings {
                _ = acls.bindings.insert(binding.clone());
            }
        })
        .await
        .map(|()| outcome)
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.read_acls()
            .await
            .map(|(acls, _)| acls.matching(filter).cloned().collect())
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        // Decided inside the CAS, not before it: a filter evaluated against a
        // snapshot that then lost the race would report deleting rules another
        // writer had already removed, or miss ones it had just added.
        let mut deleted = vec![];

        self.update_acls(|acls| {
            deleted = filters
                .iter()
                .map(|filter| {
                    let selected = acls.matching(filter).cloned().collect::<Vec<_>>();

                    for binding in &selected {
                        _ = acls.bindings.remove(binding);
                    }

                    selected
                })
                .collect();
        })
        .await
        .map(|()| deleted)
    }

    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>> {
        // Validated against the current document before anything is written,
        // so that a request naming a key this broker does not enforce is
        // refused whole rather than half-applied — and so `validate_only` is
        // the same check without the write, rather than a second
        // implementation of it that can drift from the first.
        let mut proposed = self.read_quotas().await.map(|(quotas, _)| quotas)?;

        let outcomes = alterations
            .iter()
            .map(|alteration| match proposed.alter(alteration) {
                Ok(()) => ErrorCode::None,

                Err(error) => {
                    warn!(?alteration, %error, "refusing a quota alteration");
                    ErrorCode::InvalidConfig
                }
            })
            .collect::<Vec<_>>();

        if !validate_only && outcomes.contains(&ErrorCode::None) {
            self.update_quotas(|quotas| {
                for alteration in alterations {
                    // Re-applied against whatever document won the CAS, and the
                    // refusals above stay refused: a key this broker cannot
                    // enforce does not become enforceable by another writer
                    // having landed first.
                    _ = quotas.alter(alteration);
                }
            })
            .await?;
        }

        Ok(outcomes)
    }

    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>> {
        self.read_quotas()
            .await
            .map(|(quotas, _)| quotas.matching(components, strict))
    }

    async fn client_quotas(&self) -> Result<Quotas> {
        self.read_quotas().await.map(|(quotas, _)| quotas)
    }

    async fn assert_group_schema(&self) -> Result<()> {
        let location = Path::from(format!(
            "clusters/{}/schema/groups.json",
            self.identity.cluster
        ));

        let refuse = |found: u32| {
            error!(
                cluster = self.identity.cluster,
                found,
                expected = GROUP_SCHEMA_VERSION,
                "refusing to start: this cluster's consumer groups are in a layout \
                 this binary does not write (#359)"
            );

            Err(Error::Message(format!(
                "cluster {} holds consumer groups in layout version {found}, \
                 but this binary writes version {GROUP_SCHEMA_VERSION}",
                self.identity.cluster,
            )))
        };

        match self.get::<GroupSchema>(&location).await {
            Ok((schema, _)) if schema.version == GROUP_SCHEMA_VERSION => Ok(()),

            Ok((schema, _)) => refuse(schema.version),

            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                // Create-only, so two replicas starting together cannot both
                // claim it: the loser is handed what the winner wrote and
                // checks that instead.
                match self
                    .put(
                        &location,
                        GroupSchema {
                            version: GROUP_SCHEMA_VERSION,
                        },
                        json_content_type(),
                        None,
                    )
                    .await
                {
                    Ok(_) => {
                        info!(
                            cluster = self.identity.cluster,
                            version = GROUP_SCHEMA_VERSION,
                            "claimed the consumer group layout for this cluster (#359)"
                        );

                        Ok(())
                    }

                    Err(UpdateError::Outdated { current, .. })
                        if current.version == GROUP_SCHEMA_VERSION =>
                    {
                        Ok(())
                    }

                    Err(UpdateError::Outdated { current, .. }) => refuse(current.version),

                    // Nothing in this codebase deletes the layout claim, so this
                    // is the arm that should never run — but it is the arm where
                    // folding `Vanished` into `Outdated` with a defaulted
                    // document would be actively harmful: `GroupSchema::default()`
                    // is version 0, `refuse` would fire, and the broker would
                    // reject the cluster's group layout over a document nobody
                    // wrote (#431). Named, not defaulted, and not `unreachable!()`.
                    Err(UpdateError::Vanished) => Err(Error::Message(String::from(
                        "the consumer group layout claim was deleted while being claimed",
                    ))),

                    Err(UpdateError::Error(error)) => Err(error),
                    Err(UpdateError::SerdeJson(error)) => Err(Error::SerdeJson(error)),
                    Err(UpdateError::Uuid(error)) => Err(Error::Uuid(error)),
                    Err(UpdateError::MissingEtag) => Err(Error::Message(String::from(
                        "group schema claim reported a missing etag",
                    ))),
                }
            }

            Err(error) => Err(error),
        }
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.init_producer_id(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.add_txn_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                }

                let txn_detail = current_epoch.get_mut();

                let mut responses = vec![];

                for topic in &offsets.topics {
                    let mut partition_responses = vec![];

                    if let Some(partitions) = topic.partitions.as_deref() {
                        for partition in partitions {
                            _ = txn_detail
                                .offsets
                                .entry(offsets.group_id.clone())
                                .or_default()
                                .entry(topic.name.clone())
                                .or_default()
                                .insert(
                                    partition.partition_index,
                                    TxnCommitOffset {
                                        committed_offset: partition.committed_offset,
                                        leader_epoch: partition.committed_leader_epoch,
                                        metadata: partition.committed_metadata.clone(),
                                    },
                                );

                            partition_responses.push(
                                TxnOffsetCommitResponsePartition::default()
                                    .partition_index(partition.partition_index)
                                    .error_code(ErrorCode::None.into()),
                            );
                        }
                    }

                    responses.push(
                        TxnOffsetCommitResponseTopic::default()
                            .name(topic.name.to_string())
                            .partitions(Some(partition_responses)),
                    );
                }

                Ok(responses)
            })
            .await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.end_txn(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);

        // Claim this tick's prefix-segment maintenance work-set once (#126),
        // stateless and coordinator-free: N maintainer replicas partition the
        // prefixes by first-arrival on the per-prefix lease + a recency stamp,
        // so retention and compaction of a prefix run under one claim (one
        // discovery LIST, not one per pass per replica). `None` when prefix
        // coalescing is off = today's every-prefix behaviour.
        let owned = self.claim_maintenance_prefixes(now_ms).await?;
        let owned = Some(&owned);

        // Retention and segment compaction per prefix, interleaved and
        // concurrent (#140) — not two whole sequential passes, where a delete
        // backlog consumed the bounded run (#131) before compaction ever ran.
        // The per-partition legacy `records/` pass that used to run first went
        // with that layout (#179).
        let (deleted_segments, compacted_segments) =
            self.maintain_prefix_segments(now_ms, owned).await?;
        let (expired_groups, reclaimed_members) = self.expire_groups(now).await?;

        // Converge the per-topic caches of topics another replica deleted (#283).
        // Not `?`: this is memory hygiene, and a failed topic listing must not
        // cost the pass its retention and compaction — the next tick retries.
        let evicted_topics = self
            .evict_deleted_topic_caches()
            .await
            .inspect_err(|err| warn!(?err, "could not evict deleted-topic caches"))
            .unwrap_or_default();

        // Per-cache occupancy (#554), on the same tick and for the same reason as
        // the topic-cache gauges above: the resident-memory work (#476, #543)
        // keeps landing on these maps and could not name which one.
        //
        // Kept here as well as on the index walk that records it everywhere
        // (#573): a tick whose topic listing failed still has to report what this
        // process holds, and that listing is the one thing above that is
        // deliberately not `?`.
        self.record_cache_occupancy();

        // Measurement only (#283): a failure here must not cost this replica its
        // retention and compaction, which is why it is not `?`.
        let meta = self
            .measure_meta()
            .await
            .inspect_err(|err| debug!(?err, "could not measure meta.json"))
            .ok();

        debug!(
            deleted_segments,
            compacted_segments,
            expired_groups,
            reclaimed_members,
            evicted_topics,
            ?meta
        );

        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.identity.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.identity.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.identity.advertised_listener.clone())
    }

    /// Deleting a credential nobody has is not a failure.
    ///
    /// The same reasoning as `create_acls`: credentials are applied from
    /// configuration management, and a delete that has already taken effect
    /// must not start reporting an error on the second run.
    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        match self
            .object_store
            .delete(&self.user_scram_credential_location(user, mechanism))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Last writer wins, as it does in Kafka.
    ///
    /// No CAS: two administrators setting one user's password at once is not a
    /// state worth preserving half of, and a read-modify-write would only make
    /// the loser's password win at a different moment.
    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&credential)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        self.object_store
            .put_opts(
                &self.user_scram_credential_location(user, mechanism),
                payload,
                PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: json_content_type(),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    /// A principal nobody has written a credential for is `None`, not an error.
    ///
    /// That is what the handshake turns into `unknown-user`, and it must be
    /// distinguishable from a store that could not answer — which stays an
    /// error, so a throttled bucket fails the handshake loudly rather than
    /// quietly telling every client their password is wrong.
    ///
    /// One GET per handshake, uncached on purpose: a cache here would keep a
    /// deleted principal working for its lifetime, and revoking access is the
    /// one operation that must not be eventually consistent. Handshakes are per
    /// connection, not per request, and connections are long-lived.
    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Self::absent_is_none(
            self.get::<ScramCredential>(&self.user_scram_credential_location(user, mechanism))
                .await,
        )
        .map(|found| found.map(|(credential, _version)| credential))
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        // Verify connectivity by listing objects at the root
        let _ = self.scan(Scan::Ping, &Path::from("/")).next().await;
        Ok(())
    }
}
