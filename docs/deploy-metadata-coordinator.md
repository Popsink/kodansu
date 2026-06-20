# Deploying the metadata coordinator (preprod)

Topology for the Milestone-2 data-plane split (#18): stateless proxy fronts
write record bytes straight to object storage and ask a single-writer
coordinator only for offsets; everything else (metadata, fetch, consumer groups)
is forwarded to the coordinator's Kafka port.

```
                    Kafka :9092           ┌──────────────────────────────┐
 producers/        ┌───────────┐  produce │ tansu proxy (stateless, ×N)  │
 consumers ───────▶│  proxy    │─────────▶│  • PUT batch bytes → S3      │
                   └───────────┘          │  • reserve/confirm → :9093   │
                         │ metadata/fetch/ │  • forward rest    → :9092   │
                         │ groups (Kafka)  └──────────────┬───────────────┘
                         ▼                                │
                   ┌──────────────────────────────────────▼──────────┐
                   │ tansu broker — COORDINATOR (single SlateDB writer)│
                   │  Kafka :9092  +  coordinator RPC :9093            │
                   │  STORAGE_ENGINE=hybrid://<bucket>                 │
                   └──────────────────────┬───────────────────────────┘
                                          ▼
                         object storage (S3/GCS): records + SlateDB metadata
```

Reads (fetch) and metadata still go through the coordinator's Kafka port — only
the produce *write* path is offloaded to the proxies + object store. Batch
objects keep the offset-derived key layout (`…/records/{offset:020}.batch`), so
Kotatsu reads are unaffected.

## Build

```shell
# release binary (host arch)
just release                       # target/release/tansu
# or directly:
cargo build --release --bin tansu --no-default-features \
  --features delta,dynostore,iceberg,libsql,parquet,postgres,slatedb

# container image (static musl, scratch base, multi-arch)
just docker-build                  # ghcr.io/tansu-io/tansu  (--all-features)
```

Branch: `integration/metadata-coordinator` (merges #20 + #21).

## Coordinator broker (exactly one)

The single SlateDB writer. Serves Kafka on 9092 and the reserve/confirm RPC on
9093.

```shell
AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…  AWS_DEFAULT_REGION=…
tansu broker \
  --storage-engine        hybrid://tansu \          # S3 bucket "tansu"
  --listener-url          tcp://0.0.0.0:9092 \
  --advertised-listener-url tcp://coordinator:9092 \
  --rpc-listener-url      tcp://0.0.0.0:9093
```

`hybrid://<bucket>`: records → object store, metadata → SlateDB, both in
`<bucket>` (S3 credentials from the environment, as for `s3://`). Omitting
`--rpc-listener-url` makes it a plain broker (no coordinator RPC).

## Proxy fronts (scale horizontally)

Stateless; each one writes batch bytes to S3 itself and calls the coordinator
for offsets. Point produce at the RPC port and everything else at the Kafka
port of the *same* coordinator.

```shell
AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…  AWS_DEFAULT_REGION=…
tansu proxy \
  --listener-url            tcp://0.0.0.0:9092 \
  --advertised-listener-url tcp://proxy-$POD:9092 \   # what clients reconnect to
  --origin-url              tcp://coordinator:9092 \  # metadata/fetch/groups
  --coordinator-url         tcp://coordinator:9093 \  # produce reserve/confirm
  --object-store-url        s3://tansu                # batch bytes
```

If `--coordinator-url`/`--object-store-url` are omitted the proxy is a plain
forwarding proxy (all traffic to `--origin-url`) — useful as a control baseline
for the perf comparison.

Per-topic, the coordinator-front produce path is gated by the `tansu.batch=true`
topic config (as today); set it on the topics under test.

## Perf test

Create the topic with batching enabled, then drive load. Either tool works
(tansu speaks the Kafka protocol); point them at a **proxy**'s advertised
address.

Built-in (`topic` and the topic name are positional; the perf subcommand is
`produce`):

```shell
tansu topic create --broker tcp://proxy:9092 perf \
  --partitions 12 --config tansu.batch=true

tansu perf --broker tcp://proxy:9092 perf produce \
  --producers 8 --batch-size 100 --record-size 1k --duration 2m
```

Standard Kafka tools (baseline numbers teams recognise):

```shell
kafka-topics.sh --bootstrap-server proxy:9092 --create \
  --topic perf --partitions 12 --config tansu.batch=true
kafka-producer-perf-test.sh --topic perf \
  --num-records 5000000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=proxy:9092 acks=1 linger.ms=10 batch.size=1048576
```

## What to measure

- **Produce throughput / p99** at the proxy vs a plain forwarding proxy (omit
  `--coordinator-url`) — the split should lift produce by keeping bytes off the
  coordinator.
- **Coordinator CPU / network**: with the split it should see only the small
  reserve/confirm RPC and metadata/fetch, not the record bytes.
- **Scaling**: add proxy replicas; produce should scale with fronts while the
  single coordinator stays flat on the write path.
- **Object count / PUT rate** on the bucket (one object per batch today).

## Known characteristics (preprod scope)

- One coordinator (single writer). No standby/failover yet (Phase 2: K8s Lease +
  SlateDB epoch fencing — mechanism validated, orchestration pending).
- Fetch/metadata still served by the coordinator's Kafka port (read path not yet
  offloaded).
- One S3 object per batch (no multi-batch coalescing — Phase 4 candidate).
- `await_durable`/group-commit behaviour under real S3 latency is the main thing
  this perf run should validate against the Phase-0 estimates.
```
