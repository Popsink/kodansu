<div align="center">

# Tansu 🗃️
stateless Kafka-compatible broker backed by an object store (S3, GCS, memory)

<br>

[![License](https://img.shields.io/badge/License-Apache-165dfc.svg)](https://github.com/tansu-io/tansu/blob/main/LICENSE)
&nbsp;
[![Built with Rust](https://img.shields.io/badge/built_with-Rust-165dfc.svg?logo=rust)](https://www.rust-lang.org/)
&nbsp;
<br>
[![Docs](https://img.shields.io/badge/📖%20docs-docs.tansu.io-165dfc.svg)](https://docs.tansu.io/)
&nbsp;
[![Blog](https://img.shields.io/badge/%F0%9F%93%98%20blog-blog.tansu.io-165dfc.svg)](https://blog.tansu.io/articles)
&nbsp;
<br>
[![GitHub stars](https://img.shields.io/github/stars/tansu-io/tansu?style=social)](https://github.com/tansu-io/tansu)
&nbsp;
[![Bluesky](https://img.shields.io/bluesky/followers/tansu.io)](https://bsky.app/profile/tansu.io)

<br>

</div>

# What is Tansu?

[Tansu][github-com-tansu-io] is a drop-in replacement for
Apache Kafka backed by an object store: S3, Google Cloud Storage or memory.

Features:

- Apache Kafka API compatible
- Backed by [S3](https://en.wikipedia.org/wiki/Amazon_S3), Google Cloud Storage or an in-memory store
- A single, statically linked binary

For data durability:

- S3 is designed to exceed [99.999999999% (11 nines)][aws-s3-storage-classes]
- The memory storage engine is designed for ephemeral non-production environments

Tansu is a single statically linked binary containing the following:

- **broker** an Apache Kafka API compatible broker
- **topic** a CLI to create/delete Topics
- **perf** a CLI to benchmark broker throughput
- **proxy** an Apache Kafka compatible proxy

## broker

The broker subcommand is default if no other command is supplied.

```shell
Usage: tansu [OPTIONS]
       tansu <COMMAND>

Commands:
  broker  Apache Kafka compatible broker [default if no command supplied]
  topic   Create or delete topics managed by the broker
  perf    Benchmark broker throughput
  proxy   Apache Kafka compatible proxy
  help    Print this message or the help of the given subcommand(s)

Options:
      --kafka-cluster-id <KAFKA_CLUSTER_ID>
          All members of the same cluster should use the same id [env: CLUSTER_ID=RvQwrYegSUCkIPkaiAZQlQ] [default: tansu_cluster]
      --kafka-listener-url <KAFKA_LISTENER_URL>
          The broker will listen on this address [env: LISTENER_URL=] [default: tcp://[::]:9092]
      --kafka-advertised-listener-url <KAFKA_ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=tcp://localhost:9092] [default: tcp://localhost:9092]
      --storage-engine <STORAGE_ENGINE>
          Storage engine examples are: s3://tansu/, gs://tansu/ or memory://tansu/ [env: STORAGE_ENGINE=s3://tansu/] [default: memory://tansu/]
      --prometheus-listener-url <PROMETHEUS_LISTENER_URL>
          Broker metrics can be scraped by Prometheus from this URL [env: PROMETHEUS_LISTENER_URL=tcp://0.0.0.0:9100] [default: tcp://[::]:9100]
  -h, --help
          Print help
  -V, --version
          Print version
```

A broker can be started by simply running `tansu`, all options have defaults. Tansu pickup any existing environment,
loading any found in `.env`. An [example.env](example.env) is provided as part of the distribution
and can be copied into `.env` for local modification.

## topic

The `tansu topic` command has the following subcommands:

```shell
Create or delete topics managed by the broker

Usage: tansu topic <COMMAND>

Commands:
  create  Create a topic
  delete  Delete an existing topic
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

To create a topic use:

```shell
tansu topic create taxi
```

## storage

### s3

The following will configure a S3 storage engine
using the "tansu" bucket (full context is in
[compose.yaml](compose.yaml) and [example.env](example.env)):

Copy `example.env` into `.env` so that you have a local working copy:

```shell
cp example.env .env
```

Edit `.env` so that `STORAGE_ENGINE` is defined as:

```shell
STORAGE_ENGINE="s3://tansu/"
```

First time startup, you'll need to create a bucket, an access key
and a secret in minio.

Just bring minio up, without tansu:

```shell
docker compose up -d minio
```

Create a minio `local` alias representing `http://localhost:9000` with the default credentials of `minioadmin`:

```shell
docker compose exec minio \
   /usr/bin/mc \
   alias \
   set \
   local \
   http://localhost:9000 \
   minioadmin \
   minioadmin
```

Create a `tansu` bucket in minio using the `local` alias:

```shell
docker compose exec minio \
   /usr/bin/mc mb local/tansu
```

Once this is done, you can start tansu with:

```shell
docker compose up -d tansu
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Tansu:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Describe the `test` topic:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test
```

Note that node 111 is the leader and ISR for each topic partition.
This node represents the broker handling your request. All brokers are node 111.

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Group consumer using `test-consumer-group`:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Describe the consumer `test-consumer-group` group:

```shell
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --describe
```

### memory

The memory storage engine keeps everything in process and is intended for
ephemeral, non-production environments (tests, demos, local experiments). Set
the storage engine in [.env](.env):

```shell
STORAGE_ENGINE="memory://tansu/"
```

Then start the broker:

```shell
tansu
```

## Feedback

Please [raise an issue][tansu-issues] if you encounter a problem.

## License

Tansu is licensed under [Apache 2.0][apache-license].

[apache-license]: https://www.apache.org/licenses/LICENSE-2.0
[apache-zookeeper]: https://en.wikipedia.org/wiki/Apache_ZooKeeper
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[aws-s3-storage-classes]: https://aws.amazon.com/s3/storage-classes/
[cloudflare-r2]: https://developers.cloudflare.com/r2/
[crates-io-object-store]: https://crates.io/crates/object_store
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[json-schema-org]: https://json-schema.org/
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[minio-create-access-key]: https://min.io/docs/minio/container/administration/console/security-and-access.html#id1
[minio-create-bucket]: https://min.io/docs/minio/container/administration/console/managing-objects.html#creating-buckets
[object-store-dynamo-conditional-put]: https://docs.rs/object_store/0.11.0/object_store/aws/struct.DynamoCommit.html
[protocol-buffers]: https://protobuf.dev
[raft-consensus]: https://raft.github.io
[rust-lang-org]: https://www.rust-lang.org
[tansu-issues]: https://github.com/tansu-io/tansu/issues
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
