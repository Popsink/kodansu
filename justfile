set dotenv-load := true

default: fmt build test clippy

about:
    cargo about generate about.hbs > license.html

cargo-build +args:
    cargo build {{ args }}

clean-workspace:
    cargo clean --workspace

license:
    cargo about generate about.hbs > license.html

build profile="dev" features="dynostore" bin="tansu": (cargo-build "--profile" profile "--timings" "--bin" bin "--no-default-features" "--features" features)

build-storage: clean-workspace (build "dev" "dynostore")

build-examples: (cargo-build "--examples")

release: (cargo-build "--release" "--bin" "tansu" "--no-default-features" "--features" "dynostore")

test: test-workspace test-doc

test-workspace *args: (nextest "run" "--workspace" "--all-targets" "--all-features" "--no-fail-fast" "--exclude" "fuzz" args)

nextest *args:
    cargo nextest {{ args }}

test-doc:
    cargo test --workspace --doc --all-features

doc:
    cargo doc --all-features --open

cargo-fuzz +args:
    cargo +nightly fuzz {{ args }}

fuzz-request-decode: (cargo-fuzz "run" "fuzz_request_decode" "--" "-max_total_time=60")

fuzz-member-metadata: (cargo-fuzz "run" "fuzz_member_metadata" "--" "-max_total_time=60")

fuzz-generate-seed: (cargo-fuzz "run" "--package" "fuzz" "--bin" "generate_seeds")

check:
    cargo check --workspace --all-features --all-targets

clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all --check

miri:
    cargo +nightly miri test --no-fail-fast --all-features

docker-build:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain --debug .

docker-build-cross:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain --platform linux/amd64,linux/arm64 --debug .

minio-up: (docker-compose-up "minio")

minio-down: (docker-compose-down "minio")

minio-mc +args:
    docker compose exec minio /usr/bin/mc {{ args }}

minio-local-alias: (minio-mc "alias" "set" "local" "http://localhost:9000" "minioadmin" "minioadmin")

minio-tansu-bucket: (minio-mc "mb" "local/tansu")

# Opt-in latency-injecting proxy in front of minio (see compose.yaml).
toxiproxy-up:
    docker compose --profile latency --ansi never --progress plain up --no-color --quiet-pull --wait --detach toxiproxy

toxiproxy-down:
    docker compose --profile latency down --remove-orphans

# Inject a round-trip latency (ms), split evenly across the proxy's
# upstream/downstream legs, onto every request the broker makes to minio
# through the toxiproxy proxy. Idempotent: safe to rerun to change the value
# (each POST creates-or-replaces the named toxic).
toxiproxy-latency round_trip_ms="20":
    #!/usr/bin/env bash
    set -euo pipefail
    # The image has no shell/healthcheck tooling (distroless), so
    # toxiproxy-up only waits for "running", not "serving" -- retry until
    # the control API answers.
    for _ in $(seq 1 20); do
      curl -sf http://localhost:18474/version >/dev/null 2>&1 && break
      sleep 0.25
    done
    half=$(( {{round_trip_ms}} / 2 ))
    curl -sf -X POST http://localhost:18474/proxies/minio/toxics \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"upstream_latency\",\"type\":\"latency\",\"stream\":\"upstream\",\"attributes\":{\"latency\":${half},\"jitter\":2}}" \
      >/dev/null 2>&1 || curl -sf -X POST "http://localhost:18474/proxies/minio/toxics/upstream_latency" \
      -X PATCH -H 'Content-Type: application/json' \
      -d "{\"attributes\":{\"latency\":${half},\"jitter\":2}}" >/dev/null
    curl -sf -X POST http://localhost:18474/proxies/minio/toxics \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"downstream_latency\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":${half},\"jitter\":2}}" \
      >/dev/null 2>&1 || curl -sf -X POST "http://localhost:18474/proxies/minio/toxics/downstream_latency" \
      -X PATCH -H 'Content-Type: application/json' \
      -d "{\"attributes\":{\"latency\":${half},\"jitter\":2}}" >/dev/null

# minio fronted by toxiproxy at a realistic round-trip latency (default
# 20ms, same-region S3 order of magnitude). Point AWS_ENDPOINT at
# http://localhost:19000 (not minio's own 9000) to route through the proxy —
# see `broker-s3-latency` and docs/storage-tuning.md.
s3-latency-up round_trip_ms="20": docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket toxiproxy-up (toxiproxy-latency round_trip_ms)

# a debug broker against minio-via-toxiproxy at a realistic round-trip
# latency (default 20ms) -- for A/B tuning sweeps (recent_cache_bytes,
# coalesce_linger/coalesce_bytes) against a request cost that approximates
# real S3, unlike bare localhost minio's near-zero RTT.
broker-s3-latency round_trip_ms="20" profile="profiling" *args: (build profile "dynostore") (s3-latency-up round_trip_ms)
    AWS_ENDPOINT=http://localhost:19000 target/{{ replace(profile, "dev", "debug") }}/tansu broker --storage-engine=s3://tansu/ {{ args }}

minio-ready-local: (minio-mc "ready" "local")

tansu-up: (docker-compose-up "tansu")

tansu-down: (docker-compose-down "tansu")

jaeger-up: (docker-compose-up "jaeger")

jaeger-down: (docker-compose-down "jaeger")

prometheus-up: (docker-compose-up "prometheus")

prometheus-down: (docker-compose-down "prometheus")

grafana-up: (docker-compose-up "grafana")

grafana-down: (docker-compose-down "grafana")

grafana-ui:
    open http://localhost:3000

docker-compose-up *args:
    docker compose --ansi never --progress plain up --no-color --quiet-pull --wait --detach {{ args }}

docker-compose-down *args:
    docker compose down --remove-orphans --volumes {{ args }}

ps:
    docker compose ps

docker-compose-logs *args:
    docker compose logs {{ args }}

docker-prune:
    docker system prune --force

docker-run:
    docker run --detach --name tansu --publish 9092:9092 tansu

docker-rm-f:
    docker rm --force tansu

list-topics:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command.properties --list

list-topics-plain:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-plain.properties --list

list-topics-scram-256:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-scram-256.properties --list

list-topics-scram-512:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-scram-512.properties --list

user-create user password profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/tansu user create {{ user }} {{ password }} --mechanism {{ mechanism }}

add-alice-user profile="dev": (user-create "alice" "secret" profile "scram256") (user-create "alice" "secret" profile "scram512")

user-delete user profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/tansu user delete {{ user }} --mechanism {{ mechanism }}

delete-alice-user profile="dev": (user-delete "alice" profile "scram256") (user-delete "alice" profile "scram512")

# add-alice-user:
#    kafka-configs --alter --add-config "SCRAM-SHA-256=[iterations=8192,password=secret],SCRAM-SHA-512=[iterations=8192,password=secret]" --entity-type users --entity-name alice --bootstrap-server localhost:9092

test-topic-describe:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-create-1m-retention:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=delete --config retention.ms=60000 --partitions=3 --replication-factor=1 --create --topic test

test-topic-alter:
    kafka-configs --bootstrap-server ${ADVERTISED_LISTENER} --alter --entity-type topics --entity-name test --add-config retention.ms=3600000,retention.bytes=524288000

test-topic-delete:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --delete --topic test

test-topic-get-offsets-earliest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time earliest

test-topic-get-offsets-latest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time latest

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --bootstrap-server ${ADVERTISED_LISTENER} --consumer-property fetch.max.wait.ms=15000 --group test-consumer-group --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

test-consumer-group-describe:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --describe

consumer-group-list:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --list

test-reset-offsets-to-earliest:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --topic test:0 --reset-offsets --to-earliest --execute

topic-create topic *args:
    target/debug/tansu topic create {{ topic }} {{ args }}

topic-delete topic:
    target/debug/tansu topic delete {{ topic }}

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.9.0

kafka39:
    docker run --rm -p 9092:9092 apache/kafka:3.9.0

kafka41:
    docker run --rm -p 9092:9092 apache/kafka:4.1.0

codespace-create:
    gh codespace create \
        --repo $(gh repo view --json nameWithOwner --jq .nameWithOwner) \
        --branch $(git branch --show-current) \
        --machine basicLinux32gb

codespace-delete:
    gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner) \
        --json name \
        --jq '.[].name' | xargs --no-run-if-empty -n1 gh codespace delete --codespace

codespace-logs:
    gh codespace logs \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

codespace-ls:
    gh codespace list \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)

codespace-ssh:
    gh codespace ssh \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

all: test miri

flamegraph *args:
    cargo flamegraph {{ args }}

benchmark-flamegraph: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
    flamegraph -- target/debug/tansu broker 2>&1  | tee broker.log

benchmark: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
    target/debug/tansu broker 2>&1  | tee broker.log

otel profile="dev" *args: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
    OTEL_METRIC_EXPORT_INTERVAL=5000 OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:9090/api/v1/otlp/" target/{{ replace(profile, "dev", "debug") }}/tansu broker {{ args }}  | tee broker.log

otel-up: docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up tansu-up

tansu-broker profile *args:
    target/{{ replace(profile, "dev", "debug") }}/tansu broker {{ args }} 2>&1 >broker.log

flamegraph-tansu-broker profile *args:
    #!/usr/bin/env zsh
    export RUST_LOG=warn
    flamegraph --verbose -- ./target/{{ replace(profile, "dev", "debug") }}/tansu broker {{ args }}

# run a debug broker with configuration from .env
broker *args: build docker-compose-down prometheus-up grafana-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket (tansu-broker "debug" args)

# run a release broker with configuration from .env
broker-release *args: release docker-compose-down prometheus-up grafana-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket (tansu-broker "release" args)

# run a proxy with configuration from .env
proxy *args:
    target/debug/tansu proxy {{ args }} 2>&1 | tee proxy.log

# teardown compose, rebuild: minio and tansu bucket
server: (cargo-build "--bin" "tansu") docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket
    target/debug/tansu broker 2>&1  | tee broker.log

gdb: (cargo-build "--bin" "tansu") docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket
    rust-gdb --args target/debug/tansu broker

lldb: (cargo-build "--bin" "tansu") docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket
    rust-lldb target/debug/tansu broker

ci: docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket

broker-memory profile="profiling": (build profile "dynostore") (tansu-broker profile "--storage-engine=memory://")

broker-null profile="profiling": (build profile "default") (tansu-broker profile "--storage-engine=null://")

s3-up: docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket

broker-s3 profile="profiling": (build profile "dynostore") s3-up (tansu-broker profile "--storage-engine=s3://tansu/")

samply-null profile="profiling":
    cargo build --profile {{ profile }} --bin tansu
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/tansu --storage-engine=null://sink

flamegraph-null profile="profiling": (build profile "default") (flamegraph-tansu-broker profile "--storage-engine=null://sink")

flamegraph-memory profile="profiling": (build profile "dynostore") (flamegraph-tansu-broker profile "--storage-engine=memory://tansu/")

flamegraph-s3 profile="profiling": (build profile "dynostore") docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket (flamegraph-tansu-broker profile "--storage-engine=s3://tansu/")

samply-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

flamegraph-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

consumer-perf num_records="1000" topic="test":
    kafka-consumer-perf-test --topic {{ topic }} --num-records {{ num_records }} --bootstrap-server ${ADVERTISED_LISTENER}

soak-producer-perf seconds throughput="1000" record_size="1024":
    kafka-producer-perf-test --topic test --warmup-records {{ throughput }} --num-records $(({{ seconds }} * {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

soak-producer-perf-500: (soak-producer-perf "600" "500" "1024")

soak-producer-perf-1000: (soak-producer-perf "3600" "1000" "1024")

producer-perf throughput="1000" record_size="1024" num_records="100000" topic="test":
    kafka-producer-perf-test --topic {{ topic }} --warmup-records {{ throughput }} --num-records $(({{ num_records }} + {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

producer-perf-10: (producer-perf "10")

producer-perf-1000: (producer-perf "1000" "1024" "25000")

producer-perf-2000: (producer-perf "2000" "1024" "50000")

producer-perf-3000: (producer-perf "3000" "1024" "75000")

producer-perf-4000: (producer-perf "4000" "1024" "100000")

producer-perf-5000: (producer-perf "5000" "1024" "125000")

producer-perf-6000: (producer-perf "6000" "1024" "150000")

producer-perf-7000: (producer-perf "7000" "1024" "175000")

producer-perf-8000: (producer-perf "8000" "1024" "200000")

producer-perf-9000: (producer-perf "9000" "1024" "225000")

producer-perf-10000: (producer-perf "10000" "1024" "250000")

producer-perf-15000: (producer-perf "15000" "1024" "375000")

producer-perf-20000: (producer-perf "20000" "1024" "500000")

producer-perf-30000: (producer-perf "30000" "1024" "750000")

producer-perf-40000: (producer-perf "40000" "1024" "1000000")

producer-perf-45000: (producer-perf "45000" "1024" "1100000")

producer-perf-50000: (producer-perf "50000" "1024" "1250000")

producer-perf-60000: (producer-perf "60000" "1024" "1500000")

producer-perf-70000: (producer-perf "70000" "1024" "1750000")

producer-perf-80000: (producer-perf "80000" "1024" "2000000")

producer-perf-90000: (producer-perf "90000" "1024" "2250000")

producer-perf-100000: (producer-perf "100000" "1024" "2500000")

producer-perf-200000: (producer-perf "200000" "1024" "5000000")

producer-perf-300000: (producer-perf "300000" "1024" "7500000")

producer-perf-400000: (producer-perf "400000" "1024" "10000000")

producer-perf-500000: (producer-perf "500000" "1024" "12500000")

producer-perf-600000: (producer-perf "600000" "1024" "15000000")

producer-perf-1000000: (producer-perf "1000000" "1024" "25000000")

ps-tansu-rss:
    ps -p $(pgrep tansu) -o rss= | awk '{print $1/1024 " MB"}'

telemetry-topic-create: (topic-create "telemetry" "--config" "tansu.virtual=true")

telemetry-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-consumer-group \
        --topic telemetry \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

telemetry-vrm-consume vrm="SK06 YPM":
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-sk06-consumer-group \
        --topic 'telemetry/"{{ vrm }}"' \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

group-consumer-example topics="test":
    cargo run --package tansu-client --example group_consumer -- --topics {{ topics }}
