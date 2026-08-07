set dotenv-load := true

default: fmt build test clippy

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

# The crates and features coverage is measured over. Deliberately the same set
# `test-workspace` runs, minus `--all-targets`: measuring a different set to the
# one CI tests would report a percentage that no run can reproduce.
#
# `--all-targets` is dropped because it makes cargo build the benchmark and
# example targets too, and llvm-cov then counts their lines as uncovered source.
# `--exclude fuzz` is here for the same reason as in `test-workspace`: the crate
# needs a C++ libfuzzer toolchain that is not always present.
coverage-scope := "--workspace --all-features --no-fail-fast --exclude fuzz"

# Line coverage in the terminal.
coverage *args:
    cargo llvm-cov nextest {{ coverage-scope }} {{ args }}

# Coverage as a browsable report: the one that answers "what is not covered".
coverage-html:
    cargo llvm-cov nextest {{ coverage-scope }} --html --open

# The floor is a ratchet, not a target. It exists so a change cannot quietly
# delete the tests covering the code it touches; raise it when the real number
# moves up, never lower it to make a red build green.
#
# Instrument once, report three ways off the same profraw set: lcov for Codecov,
# a summary for the job summary, and HTML for the artifact. See docs/testing.md.

# CI entry point: coverage as lcov + HTML + a summary, failing under floor%.
coverage-ci floor="0":
    cargo llvm-cov clean --workspace
    cargo llvm-cov nextest --no-report {{ coverage-scope }}
    cargo llvm-cov report --lcov --output-path lcov.info
    cargo llvm-cov report --html --output-dir coverage-html
    cargo llvm-cov report --json --summary-only --output-path coverage-summary.json
    cargo llvm-cov report --summary-only --fail-under-lines {{ floor }}

doc:
    cargo doc --all-features --open

cargo-fuzz +args:
    cargo +nightly fuzz {{ args }}

fuzz-request-decode: (cargo-fuzz "run" "fuzz_request_decode" "--" "-max_total_time=60")

fuzz-member-metadata: (cargo-fuzz "run" "fuzz_member_metadata" "--" "-max_total_time=60")

fuzz-generate-seed: (cargo-fuzz "run" "--package" "fuzz" "--bin" "generate_seeds")

check:
    cargo check --workspace --all-features --all-targets

# Every crate must still build with its own optional features OFF.
#
# Nothing checked this, and a re-export that had lost its `#[cfg]` sat broken for
# as long as it took someone to build a single crate on its own — the failure
# surfaces in a crate you did not touch, pointing at a line you did not write.
# Every other invocation here passes `--all-features`, so the gap was invisible.
#
# `fuzz` needs a C++ libfuzzer toolchain that is not always present. `tansu` is
# the binary, and its only feature gates the dependencies it is made of
# (`dep:tansu-broker`, `dep:tansu-cli`, `dep:tansu-storage`), so building it
# without them is not a thing to ask for.
check-no-default-features:
    cargo check --workspace --exclude fuzz --exclude tansu --no-default-features --all-targets

clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all --check

miri:
    cargo +nightly miri test --no-fail-fast --all-features

docker-build:
    docker build --tag ghcr.io/popsink/tansu --no-cache --progress plain --debug .

docker-build-cross:
    docker build --tag ghcr.io/popsink/tansu --no-cache --progress plain --platform linux/amd64,linux/arm64 --debug .

minio-up: (docker-compose-up "minio")

minio-down: (docker-compose-down "minio")

minio-mc +args:
    docker compose exec minio /usr/bin/mc {{ args }}

minio-local-alias: (minio-mc "alias" "set" "local" "http://localhost:9000" "minioadmin" "minioadmin")

minio-tansu-bucket: (minio-mc "mb" "local/tansu")

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
