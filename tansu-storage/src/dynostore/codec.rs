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

//! The segment wire format (#58/#64/#87/#174/#442): encoding sub-streams into
//! one shared object with a self-describing footer, and decoding one
//! sub-stream back out of it.

use super::*;

impl Substream {
    /// The name a sub-stream's records were produced under.
    ///
    /// An id-keyed sub-stream still carries its topic name in the footer, so
    /// this is only ever `None` for a caller holding an identity with no entry
    /// beside it — which is why every caller that needs the name takes it from
    /// the entry, not from here.
    pub(super) fn id(&self) -> Option<Uuid> {
        match self {
            Self::Id(id) => Some(*id),
            Self::Name(_) => None,
        }
    }
}

impl SubstreamEntry {
    /// What this entry's records are identified by (#442): the topic id when the
    /// footer carries one, else the topic name.
    pub(crate) fn substream(&self) -> Substream {
        self.topic_id
            .map_or_else(|| Substream::Name(self.topic.to_string()), Substream::Id)
    }

    /// Whether this entry holds `substream`'s records.
    ///
    /// Deliberately **not** "the ids match, or else the names match": a
    /// name-keyed read must not pick up an id-keyed entry that happens to carry
    /// the same name, or a recreated topic's records would be served as its
    /// predecessor's — which is the mixing the id keying exists to make
    /// impossible.
    pub(super) fn is(&self, substream: &Substream) -> bool {
        match substream {
            Substream::Id(id) => self.topic_id == Some(*id),
            Substream::Name(name) => self.topic_id.is_none() && *self.topic == **name,
        }
    }

    /// Drop the producer coordinates that cannot change the [`ProducerTail`]
    /// this entry contributes to (#543), keeping the resident index's price per
    /// entry off the length of the log it indexes.
    ///
    /// The index kept every coordinate ever written and the only thing that read
    /// them — [`DynoStore::producer_tail_folded`] — folded them into a window of
    /// [`IDEMPOTENT_WINDOW`] batches per producer id. On the production fleet
    /// that was ~847 MiB per replica, 37 % of `allocated`, and it grew with
    /// *compaction*: merging concatenates many batches into one sub-stream
    /// region, so `> 10 MiB` segments carried 14.4 coordinates per entry against
    /// 2.2 for `< 0.25 MiB` ones, and the index's per-entry price moved with the
    /// bucket's size distribution instead of standing still.
    ///
    /// Three reductions, all of them exact — the fold's output is identical
    /// coordinate-for-coordinate, for any prior tail and any following entries:
    ///
    /// 1. A coordinate that [`ProducerCoord::folds`] rejects is skipped by the
    ///    fold, so it can be dropped.
    /// 2. Only a producer's coordinates at its **highest epoch in this entry**
    ///    survive. The fold's epoch is `max(prior epoch, epochs seen so far)`, so
    ///    a coordinate below the running max is already ignored, and one at an
    ///    epoch the entry later exceeds is folded and then cleared by the epoch
    ///    bump — it can affect nothing downstream either way.
    /// 3. Only the **last** [`IDEMPOTENT_WINDOW`] of those survive: the ones
    ///    before them are pushed out of the window by their own successors, and
    ///    `epoch` / `seen` / `next_sequence` are all set by the last coordinate.
    ///    Hence the backwards walk below — "the last five" is the first five met
    ///    — and the reversal after it, because the fold reads log order.
    ///
    /// A pure function of the entry, deliberately: the fold stays a pure function
    /// of the observed footer set, so two replicas that have seen the same
    /// segments still derive an identical tail — the property a connection
    /// migration rests on (#88). Anything that instead folded *across* entries
    /// and retained the result would be path-dependent — a replica that folded a
    /// segment before a peer retired it could not withdraw it — and would diverge
    /// exactly there.
    ///
    /// The residue is bounded by `5 × distinct producer ids` per entry however
    /// long the log gets, which is what stops the compaction term.
    pub(super) fn retain_foldable_producers(&mut self) {
        if self.producers.is_empty() {
            return;
        }

        // Association lists rather than maps (#543): the mean coordinate count
        // per entry is single digit and a sub-stream is normally written by one
        // producer, so hashing costs more than the scan it replaces.
        let mut top: Vec<(i64, i16)> = Vec::new();
        for pc in self.producers.iter().filter(|pc| pc.folds()) {
            match top.iter_mut().find(|(id, _)| *id == pc.producer_id) {
                Some((_, epoch)) => *epoch = (*epoch).max(pc.producer_epoch),
                None => top.push((pc.producer_id, pc.producer_epoch)),
            }
        }

        let mut kept: Vec<ProducerCoord> = Vec::new();
        let mut taken: Vec<(i64, usize)> = Vec::new();
        for pc in self.producers.iter().rev() {
            if !pc.folds()
                || top
                    .iter()
                    .find(|(id, _)| *id == pc.producer_id)
                    .is_none_or(|(_, epoch)| *epoch != pc.producer_epoch)
            {
                continue;
            }

            let at = taken
                .iter()
                .position(|(id, _)| *id == pc.producer_id)
                .unwrap_or_else(|| {
                    taken.push((pc.producer_id, 0));
                    taken.len() - 1
                });

            if taken[at].1 == IDEMPOTENT_WINDOW {
                continue;
            }

            taken[at].1 += 1;
            kept.push(*pc);
        }

        kept.reverse();
        self.producers = kept.into_boxed_slice();
    }
}

impl FencedSegment {
    /// One past the last offset this segment serves.
    pub(super) fn end(&self) -> i64 {
        self.entry.base_offset + self.entry.record_count
    }

    /// Whether this entry's head is served by an earlier segment (#461).
    pub(super) fn is_clipped(&self) -> bool {
        self.served_from > self.entry.base_offset
    }
}

impl FrameTail {
    /// Byte offset within the region where the scan stopped.
    pub(super) fn at(&self) -> usize {
        match self {
            Self::Exhausted => 0,
            Self::Short { at, .. } | Self::Malformed { at, .. } => *at,
        }
    }

    /// The `batch_length` read where the scan stopped, if one was read.
    pub(super) fn declared(&self) -> Option<i32> {
        match self {
            Self::Malformed { declared, .. } => Some(*declared),
            _ => None,
        }
    }
}

impl RegionRead<'_> {
    /// Frame header bytes reported in a diagnostic.
    pub(super) const HEAD: usize = size_of::<i64>() + size_of::<i32>();

    /// Whether the read came back short of the extent the footer claims — a torn
    /// or partially-visible object rather than a footer that disagrees with its
    /// payload. The whole discrimination #386 asked for rests on this, so it is
    /// one named predicate and not an inline comparison.
    pub(super) fn truncated(&self) -> bool {
        (self.encoded.len() as u64) < self.entry.byte_len
    }

    /// The diagnostic for this read, with the scan's stopping point folded in.
    pub(super) fn region(&self, at: usize, declared: Option<i32>, detail: String) -> CorruptRegion {
        let head = self
            .encoded
            .get(at..)
            .unwrap_or_default()
            .iter()
            .take(Self::HEAD)
            .fold(String::new(), |mut head, byte| {
                let _ = write!(head, "{byte:02x}");
                head
            });

        CorruptRegion {
            prefix: self.prefix.to_owned(),
            seq: self.seq,
            topic: self.entry.topic.to_string(),
            partition: self.entry.partition,
            base_offset: self.entry.base_offset,
            byte_start: self.entry.byte_start,
            byte_len: self.entry.byte_len,
            read_len: self.encoded.len(),
            at,
            declared,
            head,
            detail,
        }
    }

    /// Report the region as damaged, counted and logged with everything needed to
    /// tell the two causes apart on the next occurrence.
    pub(super) fn corrupt(&self, at: usize, declared: Option<i32>, detail: String) -> Error {
        let region = self.region(at, declared, detail);

        SEGMENT_REGION_CORRUPT.add(1, &[]);
        error!(?region, "segment region does not begin at a batch frame");

        Error::CorruptSegment(Box::new(region))
    }

    /// Report the read as short of the extent its footer entry claims (#397).
    ///
    /// The same `CorruptRegion` payload as [`Self::corrupt`], under its own
    /// counter, because the two say different things about *what* is wrong: a
    /// full-length read that holds no frame means the entry does not describe
    /// the region, while a short read means the entry claims bytes the object
    /// does not have at all.
    pub(super) fn short_of_extent(&self, detail: String) -> Error {
        let region = self.region(self.encoded.len(), None, detail);

        SEGMENT_REGION_TRUNCATED.add(1, &[]);
        error!(?region, "segment region read short of its footer extent");

        Error::CorruptSegment(Box::new(region))
    }
}

impl ProducerCoord {
    /// Whether folding this coordinate into a [`ProducerTail`] is meaningful —
    /// i.e. whether it carries an idempotent sequence at all.
    ///
    /// A transaction marker does not (#174): it carries
    /// `base_sequence = last_sequence = -1`, so folding it would set
    /// `next_sequence` to `seq_increment(-1) = 0` and mark the tail seen,
    /// misclassifying the producer's genuine next in-order data batch as
    /// `OutOfOrder`. The `base_sequence == -1` half is belt and braces: it also
    /// catches any future non-sequenced coordinate that lacks the flag (a v2
    /// footer decodes with `flags == 0`).
    ///
    /// One definition, two callers: [`DynoStore::producer_tail_folded`] skips
    /// what it says, and [`SubstreamEntry::retain_foldable_producers`] drops it
    /// from the resident index (#543). Those two must agree — a coordinate the
    /// index discards but the fold would have used is silent dedup corruption —
    /// so they read the same predicate rather than repeat the condition.
    pub(super) fn folds(&self) -> bool {
        self.flags & FLAG_CONTROL == 0 && self.base_sequence != -1
    }
}

impl ProducerTail {
    /// The sequence a next in-order batch must carry (0 for an unseen producer).
    pub(super) fn expected(&self) -> i32 {
        if self.seen { self.next_sequence } else { 0 }
    }

    /// Fold one batch's coordinate (in log order) into the tail.
    pub(super) fn fold(
        &mut self,
        epoch: i16,
        base_sequence: i32,
        last_sequence: i32,
        base_offset: i64,
    ) {
        if epoch < self.epoch {
            return; // a stale, fenced writer's coordinate — ignore it
        }
        if epoch > self.epoch {
            self.window.clear(); // the new epoch resets the stream
        }
        self.epoch = epoch;
        self.seen = true;
        self.next_sequence = Self::seq_increment(last_sequence);
        if self.window.len() == IDEMPOTENT_WINDOW {
            _ = self.window.remove(0);
        }
        self.window.push((base_sequence, base_offset));
    }

    /// Classify a batch's `(epoch, base_sequence)` against the folded tail.
    pub(super) fn classify(&self, epoch: i16, base_sequence: i32) -> IdempotentClass {
        if epoch < self.epoch {
            return IdempotentClass::Fenced;
        }
        // A higher epoch resets the stream: only sequence 0 is in order, and the
        // prior epoch's duplicate window no longer applies.
        let (expected, fresh_epoch) = if epoch > self.epoch {
            (0, true)
        } else {
            (self.expected(), false)
        };
        if base_sequence == expected {
            IdempotentClass::Admit
        } else if !fresh_epoch
            && let Some((_, offset)) = self.window.iter().find(|(seq, _)| *seq == base_sequence)
        {
            IdempotentClass::Duplicate(*offset)
        } else {
            IdempotentClass::OutOfOrder
        }
    }

    /// Kafka's `DefaultRecordBatch` sequence increment: wraps at `i32::MAX` back
    /// to 0 (sequences stay non-negative), keeping the dedup arithmetic
    /// wraparound-safe (#80).
    pub(super) fn seq_increment(sequence: i32) -> i32 {
        if sequence == i32::MAX {
            0
        } else {
            sequence + 1
        }
    }
}

impl SegmentFooter {
    /// The entry for a `(topic, partition)` sub-stream, if it is present in this
    /// segment. `None` means the segment holds no records for that topition.
    pub(super) fn get(&self, substream: &Substream, partition: i32) -> Option<&SubstreamEntry> {
        self.entries
            .iter()
            .find(|entry| entry.partition == partition && entry.is(substream))
    }
}

impl DynoStore {
    /// Decide which of the footer index and the object is wrong when a region
    /// read comes back short of the extent the index claims (#397), and repair
    /// the index if it is the one at fault.
    ///
    /// Segments are immutable and created atomically, so a ranged GET cannot
    /// return fewer bytes than the object holds over that range. A short read
    /// therefore says the entry the read was issued from claims bytes past the
    /// end of the object — and `encode_segment_indexed` measures `byte_len` from the
    /// bytes it just appended, so it cannot over-claim for the payload it built.
    /// Two things can produce the pairing, and they are distinguishable by one
    /// suffix GET:
    ///
    /// - **the index is wrong.** The reader locates a region from the in-memory
    ///   prefix index, not from the object's trailer. An entry that describes a
    ///   different payload — a rewrite's, an adopted create's — is a *cache*
    ///   fault, and the trailer is the authority. Re-index the segment from the
    ///   trailer and let the caller retry: `Ok(())`.
    /// - **the object is wrong.** The trailer says exactly what the index said,
    ///   and the object is still short of it. Nothing in the bucket can serve
    ///   these offsets, so the read is answered `CORRUPT_MESSAGE` rather than
    ///   returning part of a region and calling it the whole thing.
    ///
    /// This is why the short read is no longer served as a silently truncated
    /// region. #395 said it did not repair the regions already on the fleet; this
    /// makes the ones caused by a stale entry read whole, and makes the rest say
    /// so.
    pub(super) async fn resolve_short_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        location: &Path,
        encoded: &Bytes,
    ) -> Result<()> {
        let read = RegionRead {
            prefix,
            seq,
            entry,
            encoded,
        };

        let Some(footer) = self.read_segment_footer(location).await? else {
            return Err(read.short_of_extent(
                "object carries no segment trailer to resolve the region against".to_owned(),
            ));
        };

        let Some(own) = footer.get(&entry.substream(), entry.partition) else {
            return Err(read.short_of_extent(format!(
                "object's own footer holds no {}-{} region",
                entry.topic, entry.partition
            )));
        };

        if own.byte_start == entry.byte_start && own.byte_len == entry.byte_len {
            return Err(read.short_of_extent(format!(
                "object's own footer claims the same {} bytes at {} and the object is short of it",
                entry.byte_len, entry.byte_start
            )));
        }

        // The index served an entry that does not belong to this object.
        self.adopt_segment_trailer(prefix, seq, entry, footer)
    }

    /// Resolve a **full-length** region that holds no whole batch against the
    /// object's own trailer (#432), the way [`Self::resolve_short_region`]
    /// already resolves a short one.
    ///
    /// #403 built that mechanism on the short-read arm alone, because the
    /// population it was aimed at *over*-stated the region: the ranged GET came
    /// back short, and `read_len < byte_len` was the tell. The discriminator run
    /// on #397 then found the other half of the same fault — an entry that
    /// **under**-states a healthy frame. The GET returns those bytes in full, so
    /// `read_len == byte_len`, the short arm never fires, and the frame decoder
    /// correctly reports that the truncated span holds no whole batch. Same
    /// index/object disagreement, opposite sign, and the arm that could ask the
    /// authority was the one that never saw it.
    ///
    /// The verdict is the same discrimination against the same authority:
    ///
    /// - **the index is wrong.** The trailer describes this object differently —
    ///   a different extent for this sub-stream, or no region for it at all.
    ///   Re-index the segment from the trailer and let the caller retry:
    ///   `Ok(())`.
    /// - **the object is wrong.** The trailer claims exactly what the index
    ///   claimed and the bytes still hold no frame — #395's husk population. The
    ///   original verdict stands and the read is answered `CORRUPT_MESSAGE`.
    ///
    /// Why this arm matters more than the short one: a `CORRUPT_MESSAGE` is
    /// retried by a Kafka client **at the same offset**, so a partition served
    /// through a wrong entry never advances. Its only exits were compaction
    /// merging the object away or retention expiring it, and the second skips
    /// every record in between. Measured on `1.0.0-alpha.4`: five partitions
    /// across four replicas re-reading one segment each at ~100/minute, against
    /// 41 285 corrupt reads on the brokers in 25.5 h.
    pub(super) async fn resolve_corrupt_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        location: &Path,
        corrupt: Box<CorruptRegion>,
    ) -> Result<()> {
        // No trailer is no authority: the original verdict is the only one
        // available, and it is the honest one.
        let Some(footer) = self.read_segment_footer(location).await? else {
            return Err(Error::CorruptSegment(corrupt));
        };

        // A trailer holding no region for this sub-stream at all is the loudest
        // form of the fault — the entry belongs to another object outright, which
        // is what the #397 discriminator found (40 sub-streams in the object, and
        // the index named a 41st). An entry present but at a different extent is
        // the same fault seen through a partial overlap.
        let disagrees = footer
            .get(&entry.substream(), entry.partition)
            .is_none_or(|own| own.byte_start != entry.byte_start || own.byte_len != entry.byte_len);

        if !disagrees {
            return Err(Error::CorruptSegment(corrupt));
        }

        self.adopt_segment_trailer(prefix, seq, entry, footer)
    }

    /// Replace this segment's cached footer with the object's own trailer, after
    /// one of the resolvers above has established that the index entry the read
    /// was issued from does not belong to this object (#397, #432).
    ///
    /// Replaces the **whole** cached footer, not just this sub-stream's entry: if
    /// one entry came from another payload they all did, and a footer half from
    /// each would be a third thing that describes nothing.
    pub(super) fn adopt_segment_trailer(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        footer: SegmentFooter,
    ) -> Result<()> {
        // Read out before the footer is moved into the index.
        let trailer = footer.get(&entry.substream(), entry.partition).map(|own| {
            (
                own.byte_start,
                own.byte_len,
                own.base_offset,
                own.record_count,
            )
        });

        // The append time is preserved from the cached segment where there is one,
        // because whole-segment retention (#61) decides expiry on it and a `0`
        // here would read as "ancient" and delete a live segment.
        let last_modified_ms = self
            .prefixes
            .index()?
            .get(prefix)
            .and_then(|index| index.segments.get(&seq))
            .map(|cached| cached.last_modified_ms)
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|since| since.as_millis() as i64)
                    .unwrap_or_default()
            });

        SEGMENT_INDEX_ENTRIES_CORRECTED.add(1, &[]);
        warn!(
            prefix,
            seq,
            topic = %entry.topic,
            partition = entry.partition,
            indexed = ?(entry.byte_start, entry.byte_len, entry.base_offset, entry.record_count),
            ?trailer,
            "footer index entry did not belong to this object; taking the object's own trailer"
        );

        self.index_insert(prefix, seq, footer, last_modified_ms)
    }

    pub(super) fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        debug!(encoded = ?&encoded[..]);
        deflated::Batch::try_from(encoded)
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    /// Decode a stored `records/` object into its constituent batches.
    ///
    /// An object written by the coalescing produce path (#50) holds several
    /// Kafka record batches concatenated; a legacy or non-coalesced object holds
    /// exactly one. The wire layout is identical either way — each batch is
    /// `base_offset (i64) + batch_length (i32) + batch_length bytes` — so this
    /// handles both, and a single-batch object decodes to a one-element vec that
    /// is byte-for-byte what [`Self::decode`] would return. Trailing bytes that
    /// do not form a whole batch are ignored (mirroring `Batch::try_from`) — the
    /// returned [`FrameTail`] says why the scan stopped, which is what lets a
    /// caller holding the footer entry tell an ignorable tail from a region that
    /// never began at a frame (#386).
    ///
    /// Both malformed-length cases now stop the scan and report, where a negative
    /// length used to `?` out as a bare `TryFromIntError`: the guard was
    /// asymmetric, and the arm that raised was the one carrying no diagnostic.
    /// Reading a length is not the place to decide severity — a length can be
    /// unusable because the region is a truncated read (benign) or because it
    /// starts in the wrong place (damage), and only [`Self::decode_region`] can
    /// see which.
    pub(super) fn decode_frame(&self, encoded: Bytes) -> Result<(Vec<deflated::Batch>, FrameTail)> {
        // base_offset (i64) + batch_length (i32) precede the `batch_length` body.
        const PREFIX: usize = size_of::<i64>() + size_of::<i32>();

        let mut batches = Vec::new();
        let mut remaining = encoded;
        let mut at = 0usize;

        let tail = loop {
            if remaining.len() < PREFIX {
                break if remaining.is_empty() {
                    FrameTail::Exhausted
                } else {
                    FrameTail::Short {
                        at,
                        remaining: remaining.len(),
                    }
                };
            }

            let mut length = [0u8; size_of::<i32>()];
            length.copy_from_slice(&remaining[size_of::<i64>()..PREFIX]);
            let declared = i32::from_be_bytes(length);

            // A `batch_length` is a byte count: negative is not a short frame, it
            // is not a frame. Same outcome as one that overruns what is left.
            let Ok(batch_length) = usize::try_from(declared) else {
                break FrameTail::Malformed { at, declared };
            };

            let total = PREFIX + batch_length;
            if total > remaining.len() {
                break FrameTail::Malformed { at, declared };
            }

            batches.push(self.decode(remaining.slice(0..total))?);
            remaining = remaining.slice(total..);
            at += total;
        };

        Ok((batches, tail))
    }

    /// Decode the bytes read for footer `entry` into that sub-stream's batches,
    /// attributing damage to the segment it came from (#386).
    ///
    /// [`Self::decode_frame`] knows only bytes, so on its own it cannot tell the
    /// documented ignorable tail from a region that is not where the footer says
    /// it is. Here the entry is in hand, and the discriminator is whether the read
    /// returned the whole extent the footer claims:
    ///
    /// - **short read** — the entry claims bytes the object does not hold.
    ///   Answered as damage, counted ([`SEGMENT_REGION_TRUNCATED`]).
    /// - **full-length read that yields no batch** — the region arrived whole and
    ///   still holds no frame, so `byte_start` does not point at one: the footer
    ///   and the payload disagree. That is damage, and it is answered as such.
    ///
    /// The short read used to return the batches it managed to decode and drop the
    /// rest of the region, on the reasoning that it was a torn or partially
    /// visible object and should stay the bounded empty read #290 settled on. The
    /// fleet then produced 313 of them in 29 minutes on
    /// `*.connect.ibmi-offsets` — `KafkaOffsetBackingStore`'s durable state —
    /// with `read_len` pinned at exactly 199 across 216 distinct sequences (#397).
    /// A constant over-claim across hundreds of objects is not tearing, and the
    /// consumer of a partial offsets region is a connector resuming from an offset
    /// map with holes in it, with nothing in its own logs to say so. Nor is
    /// tearing something an immutable, atomically created object can do: a short
    /// read means the entry and the object disagree, full stop. The reader
    /// resolves *which* of them is wrong against the object's own trailer before
    /// this is reached — see [`Self::resolve_short_region`].
    ///
    /// Beyond that, only a region that decodes to *nothing* is treated as corrupt.
    /// A malformed tail after whole batches keeps the frame contract's behaviour —
    /// it cannot be a divergent start, and erroring there would fail reads that
    /// serve data today.
    pub(super) fn decode_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        encoded: Bytes,
    ) -> Result<Vec<deflated::Batch>> {
        let read = RegionRead {
            prefix,
            seq,
            entry,
            encoded: &encoded,
        };

        // A frame header that parsed over a body that will not decode is the same
        // damage seen one layer down, and it reached the client as a bare protocol
        // error naming no segment. Attribute it here too.
        let (batches, tail) = self
            .decode_frame(encoded.clone())
            .map_err(|error| read.corrupt(0, None, format!("undecodable batch: {error:?}")))?;

        if read.truncated() {
            return Err(read.short_of_extent(format!(
                "region short of its footer extent by {} bytes, {} whole batches: {tail:?}",
                entry.byte_len - encoded.len() as u64,
                batches.len(),
            )));
        }

        match tail {
            FrameTail::Exhausted => Ok(batches),

            _ if batches.is_empty() => Err(read.corrupt(
                tail.at(),
                tail.declared(),
                format!("region holds no whole batch: {tail:?}"),
            )),

            // The documented ignore: bytes past the last whole batch.
            _ => {
                debug!(
                    prefix,
                    seq,
                    topic = %entry.topic,
                    partition = entry.partition,
                    batches = batches.len(),
                    ?tail,
                    "ignoring trailing bytes of a segment region"
                );

                Ok(batches)
            }
        }
    }

    /// Serialize a run of contiguous batches into one `records/` object payload
    /// (the coalescing produce write, #50). The batches are concatenated in wire
    /// order; a single-batch slice is byte-identical to [`Self::encode`], so a
    /// coalesced object and a legacy object are read back the same way by
    /// [`Self::decode_frame`].
    #[cfg(test)]
    pub(super) fn encode_frame(&self, batches: &[deflated::Batch]) -> Result<PutPayload> {
        let mut buf = Vec::new();
        for batch in batches {
            buf.extend_from_slice(&Bytes::from(batch.clone()));
        }
        Ok(PutPayload::from(Bytes::from(buf)))
    }

    /// Serialize many `(topic, partition)` sub-streams into one shared,
    /// prefix-coalesced segment object (#64) — the write produced by #57. Each
    /// sub-stream's batches are concatenated contiguously (byte-compatible with
    /// [`Self::encode_frame`], so a region decodes with [`Self::decode_frame`]);
    /// the regions are laid end to end; then a self-describing [`SegmentFooter`]
    /// and a fixed [`SEGMENT_TRAILER_LEN`] trailer are appended. A reader
    /// locates any sub-stream by footer lookup + a ranged GET of its byte span
    /// (#60) rather than deriving offsets from the filename. Each element is
    /// `(topition, base_offset, batches)`, where `base_offset` is the absolute
    /// offset already assigned to the sub-stream's first record (#58).
    /// `writer_epoch` is the producing writer's lease epoch (#59), stamped into
    /// the footer so a fenced writer's segment is identifiable. Empty
    /// sub-streams are skipped. Returns the payload and the footer, which the
    /// writer keeps as the segment's in-memory index.
    #[cfg(test)]
    pub(super) fn encode_segment(
        &self,
        substreams: &[(Topition, i64, Vec<deflated::Batch>)],
        writer_epoch: i64,
    ) -> Result<(PutPayload, SegmentFooter)> {
        let mut body = Vec::new();
        let mut entries = Vec::with_capacity(substreams.len());

        for (topition, base_offset, batches) in substreams {
            if batches.is_empty() {
                continue;
            }

            let byte_start = body.len() as u64;
            let mut record_count = 0i64;
            let mut max_timestamp = i64::MIN;

            for batch in batches {
                body.extend_from_slice(&Bytes::from(batch.clone()));
                record_count += batch.last_offset_delta as i64 + 1;
                max_timestamp = max_timestamp.max(batch.max_timestamp);
            }

            entries.push(SubstreamEntry {
                topic: Arc::from(topition.topic()),
                // v1 has no place to put one, so a v1 sub-stream is keyed by
                // name — which is what every topic written by that path was
                // (#442).
                topic_id: None,
                partition: topition.partition(),
                base_offset: *base_offset,
                record_count,
                byte_start,
                byte_len: body.len() as u64 - byte_start,
                max_timestamp,
                // Populated when the writer emits v2 (#88); empty on the current
                // v1 write path.
                producers: Box::default(),
            });
        }

        let footer = SegmentFooter {
            writer_epoch,
            nonce: 0,
            entries,
        };
        let footer_bytes = Self::encode_footer(&footer, SEGMENT_FORMAT_VERSION);

        body.extend_from_slice(&footer_bytes);
        body.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
        body.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
        body.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_be_bytes());
        body.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

        Ok((PutPayload::from(Bytes::from(body)), footer))
    }

    /// Like [`Self::encode_segment`] but emits a **v3** footer (#87/#174): a
    /// per-flush `nonce` plus, per sub-stream, the producer coordinates — with
    /// an attribute-derived `flags` byte — of its idempotent, transactional
    /// and control batches (in region/offset order). Used by every leaseless
    /// write path (#86): the flush, merge compaction (#66) and the per-key
    /// compaction rewrite (#175). The coordinates back log-based idempotent
    /// dedup (#88), the nonce backs ambiguous-PUT adoption (#89), and the
    /// flags make transaction markers and transactional data locatable from
    /// the footer alone (#174) — placement metadata, not commit authority:
    /// LSO/aborted derivation stays a pure `meta.json` function. `offset_delta`
    /// is the batch's offset within its sub-stream so it survives the
    /// conflict-correction re-encode.
    ///
    /// `version` is stamped unconditionally and comes from the writer regime
    /// ([`Tuning::segment_format_version`]), never from the segment's
    /// content — a v4 segment whose sub-streams are all name-keyed is still v4,
    /// and its entries carry the nil uuid. What *is* content is the identity of
    /// each sub-stream (#442), which the caller decides and passes in: a v3
    /// writer handed an id-keyed sub-stream would drop the id on the floor and
    /// write records under a key nothing reads, so that combination is refused
    /// rather than silently downgraded.
    pub(super) fn encode_segment_indexed(
        &self,
        substreams: &[SubstreamWrite],
        writer_epoch: i64,
        nonce: u64,
        version: u16,
    ) -> Result<(PutPayload, SegmentFooter)> {
        let mut body = Vec::new();
        let mut entries = Vec::with_capacity(substreams.len());

        for SubstreamWrite {
            topition,
            substream,
            base_offset,
            batches,
        } in substreams
        {
            if batches.is_empty() {
                continue;
            }

            // An id-keyed sub-stream cannot be expressed below v4, and writing
            // it name-keyed would put its records where no reader of that topic
            // looks — durable, acked, and invisible. The only way to reach here
            // is a deployment that stamped `substream_id` on a topic and then
            // went back to a v3 writer regime, which the flag's one-way
            // discipline exists to prevent.
            if version < SEGMENT_FORMAT_VERSION_V4
                && let Some(id) = substream.id()
            {
                error!(
                    ?topition,
                    %id,
                    version,
                    "refusing to write an id-keyed sub-stream into a pre-v4 segment"
                );

                return Err(Error::Api(ErrorCode::KafkaStorageError));
            }

            let byte_start = body.len() as u64;
            let mut record_count = 0i64;
            let mut max_timestamp = i64::MIN;
            let mut producers = Vec::new();

            for (index, batch) in batches.iter().enumerate() {
                // A footer entry must not be able to claim bytes the payload does
                // not hold (#393).
                //
                // `byte_len` below is measured from `body`, so it always covers
                // exactly what was written — which means the only way the two can
                // disagree is a batch whose own `batch_length` header lies about
                // its bytes. `From<Batch> for Bytes` writes that field verbatim,
                // so such a batch serialises a frame declaring a length no
                // reader will find, and the region becomes permanently
                // undecodable: the exact damage #386 had to answer for on the
                // read side.
                //
                // The one shape known to produce it is the pre-v2 husk the
                // decoder returns for `magic != 2` — the wire's `batch_length`
                // over an empty `record_data` (see `declares_its_own_length`).
                // The produce path already refuses that with
                // `UNSUPPORTED_FOR_MESSAGE_FORMAT` (#320), so reaching here is a
                // defect rather than a client error, and this is the invariant
                // asserted where the footer is built rather than where it is
                // read.
                //
                // Refusing costs the caller its tick or its flush, having
                // written nothing — the same trade #388 made for compaction.
                if !batch.declares_its_own_length() {
                    let divergent = DivergentBatch {
                        topic: topition.topic().to_owned(),
                        partition: topition.partition(),
                        base_offset: *base_offset,
                        index,
                        declared: batch.batch_length,
                        encoded: batch.encoded_batch_length().unwrap_or(-1),
                        magic: batch.magic,
                        record_data_len: batch.record_data.len(),
                    };

                    error!(
                        ?divergent,
                        "refusing to encode a batch that misdeclares its length"
                    );

                    return Err(Error::DivergentBatch(Box::new(divergent)));
                }

                // Offset of this batch within the sub-stream, before it is added.
                let offset_delta = record_count as u32;
                body.extend_from_slice(&Bytes::from(batch.clone()));
                // v3 emission rule (#174): a coordinate per idempotent,
                // transactional or control batch. A transaction marker is not
                // idempotent (`base_sequence == -1`) yet must be indexed: its
                // coordinate carries its real producer_id/epoch, the -1
                // sequences, and `flags = 0b11` — and is never folded into
                // the producer tail (see `producer_tail_folded`).
                if batch.is_idempotent() || batch.is_transactional() || batch.is_control() {
                    let mut flags = 0u8;
                    if batch.is_transactional() {
                        flags |= FLAG_TRANSACTIONAL;
                    }
                    if batch.is_control() {
                        flags |= FLAG_CONTROL;
                    }
                    producers.push(ProducerCoord {
                        producer_id: batch.producer_id,
                        producer_epoch: batch.producer_epoch,
                        base_sequence: batch.base_sequence,
                        last_sequence: batch.base_sequence.wrapping_add(batch.last_offset_delta),
                        offset_delta,
                        flags,
                    });
                }
                record_count += batch.last_offset_delta as i64 + 1;
                max_timestamp = max_timestamp.max(batch.max_timestamp);
            }

            entries.push(SubstreamEntry {
                topic: Arc::from(topition.topic()),
                topic_id: substream.id(),
                partition: topition.partition(),
                base_offset: *base_offset,
                record_count,
                byte_start,
                byte_len: body.len() as u64 - byte_start,
                max_timestamp,
                producers: producers.into_boxed_slice(),
            });
        }

        let footer = SegmentFooter {
            writer_epoch,
            nonce,
            entries,
        };
        let footer_bytes = Self::encode_footer(&footer, version);

        body.extend_from_slice(&footer_bytes);
        body.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
        body.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
        body.extend_from_slice(&version.to_be_bytes());
        body.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

        Ok((PutPayload::from(Bytes::from(body)), footer))
    }

    /// [`Self::encode_segment_indexed`] at v3 over name-keyed sub-streams: the
    /// shape every write had before #442, kept for the tests that pin the v3
    /// byte layout and the read paths built on it.
    #[cfg(test)]
    pub(super) fn encode_segment_v3(
        &self,
        substreams: &[(Topition, i64, Vec<deflated::Batch>)],
        writer_epoch: i64,
        nonce: u64,
    ) -> Result<(PutPayload, SegmentFooter)> {
        let substreams = substreams
            .iter()
            .map(|(topition, base_offset, batches)| SubstreamWrite {
                substream: Substream::Name(topition.topic().to_owned()),
                topition: topition.clone(),
                base_offset: *base_offset,
                batches: batches.clone(),
            })
            .collect::<Vec<_>>();

        self.encode_segment_indexed(&substreams, writer_epoch, nonce, SEGMENT_FORMAT_VERSION_V3)
    }

    /// Serialize a [`SegmentFooter`] index (#64/#59). Header: `writer_epoch
    /// (i64)`, plus `nonce (u64)` at v2. Then each entry: `topic_len (u16) +
    /// topic (utf8)`, plus at v4 `topic_id ([16], #442)`, then `partition
    /// (i32) + base_offset (i64) + record_count (i64) + byte_start (u64) +
    /// byte_len (u64) + max_timestamp (i64)`, plus at v2 `pcoord_count (u16)`
    /// and that many `producer_id (i64) + producer_epoch (i16) + base_sequence
    /// (i32) + last_sequence (i32) + offset_delta (u32)`, plus at v3 a per-coordinate
    /// `flags (u8)` (#174) — all big-endian. Asked for v2, this MUST keep
    /// emitting the exact pre-v3 bytes (`flags` is dropped, not zero-filled),
    /// and asked for v3 the exact pre-v4 bytes (`topic_id` likewise): deployed
    /// readers, internal and S3-direct external, decode those versions by those
    /// byte layouts. Paired with [`Self::decode_footer`]; the external contract
    /// is `docs/virtual-topics-format.md`.
    ///
    /// A v4 entry for a name-keyed topic writes the **nil** uuid, which decodes
    /// back to `None` — so "the segment is v4" and "this sub-stream is keyed by
    /// id" are independent, exactly as they have to be while both kinds of topic
    /// coexist in one prefix.
    pub(super) fn encode_footer(footer: &SegmentFooter, version: u16) -> Vec<u8> {
        let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
        let v3 = version >= SEGMENT_FORMAT_VERSION_V3;
        let v4 = version >= SEGMENT_FORMAT_VERSION_V4;
        let mut buf = Vec::new();
        buf.extend_from_slice(&footer.writer_epoch.to_be_bytes());
        if v2 {
            buf.extend_from_slice(&footer.nonce.to_be_bytes());
        }
        for entry in &footer.entries {
            let topic = entry.topic.as_bytes();
            buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            buf.extend_from_slice(topic);
            if v4 {
                buf.extend_from_slice(entry.topic_id.unwrap_or(Uuid::nil()).as_bytes());
            }
            buf.extend_from_slice(&entry.partition.to_be_bytes());
            buf.extend_from_slice(&entry.base_offset.to_be_bytes());
            buf.extend_from_slice(&entry.record_count.to_be_bytes());
            buf.extend_from_slice(&entry.byte_start.to_be_bytes());
            buf.extend_from_slice(&entry.byte_len.to_be_bytes());
            buf.extend_from_slice(&entry.max_timestamp.to_be_bytes());
            if v2 {
                buf.extend_from_slice(&(entry.producers.len() as u16).to_be_bytes());
                for pc in &entry.producers {
                    buf.extend_from_slice(&pc.producer_id.to_be_bytes());
                    buf.extend_from_slice(&pc.producer_epoch.to_be_bytes());
                    buf.extend_from_slice(&pc.base_sequence.to_be_bytes());
                    buf.extend_from_slice(&pc.last_sequence.to_be_bytes());
                    buf.extend_from_slice(&pc.offset_delta.to_be_bytes());
                    if v3 {
                        buf.push(pc.flags);
                    }
                }
            }
        }
        buf
    }

    /// Parse a [`SegmentFooter`] from `footer_bytes`, the `footer_len` bytes that
    /// precede the trailer (#64). Inverse of [`Self::encode_footer`]; a
    /// truncated or malformed footer is a corrupt segment, not a legacy object.
    pub(super) fn decode_footer(
        footer_bytes: &[u8],
        entry_count: usize,
        version: u16,
    ) -> Result<SegmentFooter> {
        let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
        let v3 = version >= SEGMENT_FORMAT_VERSION_V3;
        let v4 = version >= SEGMENT_FORMAT_VERSION_V4;
        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = footer_bytes;

        fn take<'a>(cursor: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            if cursor.len() < n {
                return Err(Error::Message(String::from("truncated segment footer")));
            }
            let (head, tail) = cursor.split_at(n);
            *cursor = tail;
            Ok(head)
        }

        let writer_epoch = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
        let nonce = if v2 {
            u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?)
        } else {
            0
        };

        for _ in 0..entry_count {
            let topic_len = u16::from_be_bytes(take(&mut cursor, 2)?.try_into()?) as usize;
            let topic: Arc<str> = str::from_utf8(take(&mut cursor, topic_len)?)
                .map_err(|e| Error::Message(e.to_string()))?
                .into();
            // v4 carries the topic id the records were produced under (#442). A
            // nil uuid means this sub-stream is keyed by name — the same state a
            // v1/v2/v3 entry is in, and the state every topic created before the
            // v4 writer regime stays in for its whole life.
            let topic_id = if v4 {
                Some(Uuid::from_bytes(take(&mut cursor, 16)?.try_into()?)).filter(|id| !id.is_nil())
            } else {
                None
            };
            let partition = i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?);
            let base_offset = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let record_count = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let byte_start = u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let byte_len = u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let max_timestamp = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);

            let producers = if v2 {
                let pcoord_count = u16::from_be_bytes(take(&mut cursor, 2)?.try_into()?) as usize;
                let mut producers = Vec::with_capacity(pcoord_count);
                for _ in 0..pcoord_count {
                    producers.push(ProducerCoord {
                        producer_id: i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?),
                        producer_epoch: i16::from_be_bytes(take(&mut cursor, 2)?.try_into()?),
                        base_sequence: i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        last_sequence: i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        offset_delta: u32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        // v3 appends one flags byte per coordinate (#174); a
                        // v1/v2 footer has no such byte and decodes to 0, the
                        // exact value pre-v3 code observed.
                        flags: if v3 { take(&mut cursor, 1)?[0] } else { 0 },
                    });
                }
                producers.into_boxed_slice()
            } else {
                Box::default()
            };

            entries.push(SubstreamEntry {
                topic,
                topic_id,
                partition,
                base_offset,
                record_count,
                byte_start,
                byte_len,
                max_timestamp,
                producers,
            });
        }

        Ok(SegmentFooter {
            writer_epoch,
            nonce,
            entries,
        })
    }

    /// Recover the [`SegmentFooter`] of a segment given its tail bytes (#64):
    /// `tail` must include at least the footer and the [`SEGMENT_TRAILER_LEN`]
    /// trailer (in practice the last N bytes fetched by a ranged GET, or the
    /// whole object). Returns `Ok(None)` when the trailer magic is absent — the
    /// object is a legacy single-topic coalesced object (#50, the v0 case) and
    /// must be read as a bare batch concatenation via [`Self::decode_frame`].
    pub(crate) fn decode_segment_footer(tail: &[u8]) -> Result<Option<SegmentFooter>> {
        if tail.len() < SEGMENT_TRAILER_LEN {
            return Ok(None);
        }

        let trailer = &tail[tail.len() - SEGMENT_TRAILER_LEN..];
        let magic = u32::from_be_bytes(trailer[14..18].try_into()?);
        if magic != SEGMENT_MAGIC {
            return Ok(None);
        }

        let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;
        let entry_count = u32::from_be_bytes(trailer[8..12].try_into()?) as usize;
        let version = u16::from_be_bytes(trailer[12..14].try_into()?);
        // v3 is accepted one release before anything writes it (#174), and v4
        // one *flag* before anything writes it (#442): this rejection is a hard
        // error that propagates through the index refresh into fetch, so a
        // reader that lacks a version suffers a partition-wide read outage the
        // moment a writer emits it — see [`SEGMENT_FORMAT_VERSION_V3`] and
        // [`SEGMENT_FORMAT_VERSION_V4`]. Rejecting every *other* version stays:
        // it is the external contract's MUST (`docs/virtual-topics-format.md`),
        // and guessing at an unknown layout would mis-decode, not degrade.
        if version != SEGMENT_FORMAT_VERSION
            && version != SEGMENT_FORMAT_VERSION_V2
            && version != SEGMENT_FORMAT_VERSION_V3
            && version != SEGMENT_FORMAT_VERSION_V4
        {
            return Err(Error::Message(format!(
                "unsupported segment format version {version}"
            )));
        }

        let footer_end = tail.len() - SEGMENT_TRAILER_LEN;
        let footer_start = footer_end
            .checked_sub(footer_len)
            .ok_or_else(|| Error::Message(String::from("segment footer length exceeds tail")))?;

        Self::decode_footer(&tail[footer_start..footer_end], entry_count, version).map(Some)
    }
}

#[cfg(test)]
mod foldable_producers_tests {
    use super::{
        FLAG_CONTROL, FLAG_TRANSACTIONAL, IDEMPOTENT_WINDOW, ProducerCoord, ProducerTail,
        SubstreamEntry,
    };
    use std::collections::BTreeSet;

    fn entry(base_offset: i64, producers: Vec<ProducerCoord>) -> SubstreamEntry {
        SubstreamEntry {
            topic: "org.env.conn.tab_a".into(),
            topic_id: None,
            partition: 0,
            base_offset,
            record_count: producers.len() as i64,
            byte_start: 0,
            byte_len: 64,
            max_timestamp: 0,
            producers: producers.into_boxed_slice(),
        }
    }

    fn coord(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        offset_delta: u32,
    ) -> ProducerCoord {
        ProducerCoord {
            producer_id,
            producer_epoch,
            base_sequence,
            last_sequence: base_sequence,
            offset_delta,
            flags: 0,
        }
    }

    /// A transaction marker as the v3 writer emits it (#174): a real
    /// producer/epoch, the -1 sequences, `flags = 0b11`.
    fn marker(producer_id: i64, producer_epoch: i16, offset_delta: u32) -> ProducerCoord {
        ProducerCoord {
            producer_id,
            producer_epoch,
            base_sequence: -1,
            last_sequence: -1,
            offset_delta,
            flags: FLAG_CONTROL | FLAG_TRANSACTIONAL,
        }
    }

    /// `DynoStore::producer_tail_folded`'s inner loop, over entries already in
    /// log order — the fold whose output pruning must not change.
    fn fold(entries: &[SubstreamEntry], producer_id: i64) -> ProducerTail {
        let mut tail = ProducerTail::default();
        for entry in entries {
            for pc in entry.producers.iter().filter(|pc| pc.folds()) {
                if pc.producer_id == producer_id {
                    tail.fold(
                        pc.producer_epoch,
                        pc.base_sequence,
                        pc.last_sequence,
                        entry.base_offset + pc.offset_delta as i64,
                    );
                }
            }
        }
        tail
    }

    fn producer_ids(entries: &[SubstreamEntry]) -> BTreeSet<i64> {
        entries
            .iter()
            .flat_map(|entry| entry.producers.iter())
            .map(|pc| pc.producer_id)
            .collect()
    }

    fn coord_count(entries: &[SubstreamEntry]) -> usize {
        entries.iter().map(|entry| entry.producers.len()).sum()
    }

    fn pruned(entries: &[SubstreamEntry]) -> Vec<SubstreamEntry> {
        entries
            .iter()
            .cloned()
            .map(|mut entry| {
                entry.retain_foldable_producers();
                entry
            })
            .collect()
    }

    /// Every shape the pruner has to be exact on, as a whole sub-stream in log
    /// order. Pruning is per entry but its correctness is a claim about the
    /// *fold*, so each case is folded end to end: the prior tail an entry is met
    /// with, and the entries that follow it, are both part of what could go
    /// wrong.
    fn cases() -> Vec<(&'static str, Vec<SubstreamEntry>)> {
        vec![
            (
                "a compacted region: one producer, far more batches than the window",
                vec![entry(
                    0,
                    (0..40).map(|n| coord(7, 3, n, n as u32)).collect(),
                )],
            ),
            (
                "the window spans entries: the tail is not wholly in the newest one",
                (0..8)
                    .map(|n| {
                        entry(
                            n * 3,
                            vec![coord(7, 3, n as i32, 0), coord(7, 3, n as i32 + 100, 1)],
                        )
                    })
                    .collect(),
            ),
            (
                "markers interleaved with transactional data",
                vec![entry(
                    0,
                    vec![
                        coord(7, 1, 0, 0),
                        marker(7, 1, 1),
                        coord(7, 1, 1, 2),
                        marker(7, 1, 3),
                    ],
                )],
            ),
            (
                "a marker is the only coordinate: the tail must stay unseen",
                vec![entry(0, vec![marker(7, 1, 0)])],
            ),
            (
                "markers must not eat window slots: more than five data batches, \
                 each followed by one",
                vec![entry(
                    0,
                    (0..8)
                        .flat_map(|n| {
                            [coord(7, 1, n, n as u32 * 2), marker(7, 1, n as u32 * 2 + 1)]
                        })
                        .collect(),
                )],
            ),
            (
                "a marker at a higher epoch than the data does not fence the data",
                vec![entry(
                    0,
                    vec![coord(7, 1, 0, 0), coord(7, 1, 1, 1), marker(7, 2, 2)],
                )],
            ),
            (
                "an epoch bump inside one entry clears what preceded it",
                vec![entry(
                    0,
                    vec![
                        coord(7, 1, 0, 0),
                        coord(7, 1, 1, 1),
                        coord(7, 2, 0, 2),
                        coord(7, 2, 1, 3),
                    ],
                )],
            ),
            (
                "a fenced writer's coordinate lands after the higher epoch",
                vec![entry(
                    0,
                    vec![coord(7, 4, 0, 0), coord(7, 2, 9, 1), coord(7, 4, 1, 2)],
                )],
            ),
            (
                "the epoch bumps in a later entry, after the earlier one filled the window",
                vec![
                    entry(0, (0..7).map(|n| coord(7, 1, n, n as u32)).collect()),
                    entry(7, vec![coord(7, 5, 0, 0)]),
                ],
            ),
            (
                "an entry wholly below the tail's epoch changes nothing",
                vec![
                    entry(0, vec![coord(7, 6, 0, 0)]),
                    entry(1, (0..9).map(|n| coord(7, 2, n, n as u32)).collect()),
                    entry(10, vec![coord(7, 6, 1, 0)]),
                ],
            ),
            (
                "three producers sharing a region, one of them fenced mid-entry",
                vec![
                    entry(
                        0,
                        vec![
                            coord(1, 0, 0, 0),
                            coord(2, 0, 0, 1),
                            coord(1, 0, 1, 2),
                            coord(3, 7, 40, 3),
                            coord(2, 1, 0, 4),
                            coord(2, 0, 1, 5),
                        ],
                    ),
                    entry(
                        6,
                        (0..12)
                            .map(|n| coord(1 + (n % 3), 1, n as i32, n as u32))
                            .collect(),
                    ),
                ],
            ),
            ("nothing idempotent at all", vec![entry(0, vec![])]),
        ]
    }

    /// The claim pruning rests on (#543): for every producer, the tail folded
    /// from the pruned entries is the tail folded from the complete ones — not
    /// merely equivalent to classify against, *equal*, window and all. Anything
    /// weaker and two replicas holding different amounts of history would derive
    /// different tails, which is the convergence #88 needs across a connection
    /// migration.
    #[test]
    fn pruning_the_resident_coordinates_cannot_change_the_fold() {
        for (what, entries) in cases() {
            let pruned = pruned(&entries);

            for producer_id in producer_ids(&entries) {
                assert_eq!(
                    fold(&entries, producer_id),
                    fold(&pruned, producer_id),
                    "{what}: producer {producer_id}"
                );
            }
        }
    }

    /// Idempotence: the index prunes on insert and a *replace* of the same
    /// sequence re-prunes what it already pruned, so a second pass must be a
    /// no-op rather than eat further into the window.
    #[test]
    fn pruning_twice_prunes_no_further() {
        for (what, entries) in cases() {
            let once = pruned(&entries);
            assert_eq!(once, pruned(&once), "{what}");
        }
    }

    /// The point of the exercise: what an entry retains stops depending on how
    /// much log was merged into it. A compacted region carrying 40 coordinates
    /// for one producer holds five afterwards, and the bound is
    /// `IDEMPOTENT_WINDOW × distinct producer ids` whatever compaction does
    /// next.
    #[test]
    fn what_is_retained_is_bounded_by_the_window_not_by_the_history() {
        for (what, entries) in cases() {
            let pruned = pruned(&entries);
            assert!(coord_count(&pruned) <= coord_count(&entries), "{what}");

            for entry in &pruned {
                let ids = entry
                    .producers
                    .iter()
                    .map(|pc| pc.producer_id)
                    .collect::<BTreeSet<_>>();
                assert!(
                    entry.producers.len() <= IDEMPOTENT_WINDOW * ids.len(),
                    "{what}: {} coordinates for {} producers",
                    entry.producers.len(),
                    ids.len()
                );
            }
        }

        let compacted = vec![entry(
            0,
            (0..40).map(|n| coord(7, 3, n, n as u32)).collect(),
        )];
        assert_eq!(
            IDEMPOTENT_WINDOW,
            coord_count(&pruned(&compacted)),
            "a pruner that quietly became a no-op passes every equivalence above"
        );
    }
}
