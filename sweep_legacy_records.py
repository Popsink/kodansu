#!/usr/bin/env python3
"""Read-only sweep: which topics still hold legacy `records/` objects, and are
any of them compacted?

Step 4 of the Popsink/tansu#171 rollout. #178, #179 and #180 are all gated on
one answer: **zero compacted topics holding legacy objects**. A compacted
topic's surviving latest-value-per-key *is* its state, so anything left in
`records/` there becomes unreadable when #179 deletes the legacy read paths —
for a connector offsets topic that means losing its journal position and
re-snapshotting the source.

Non-compacted topics holding legacy objects are **expected and accepted**: the
epic's decision (2) abandons pre-cutover CDC history in place, and the 2026-07-26
sweep measured 3,168 such hybrid seams. They are counted, not flagged.

Read-only: `ListObjectsV2` and `GetObject` only. Nothing is written or deleted.

Key layout (from `tansu-storage/src/dynostore.rs`):
    clusters/{cluster}/topic-metadata/{name}.json
    clusters/{cluster}/topics/{name}/partitions/{partition:010}/records/

Usage:
    pip install boto3
    aws sso login --profile <profile>
    AWS_PROFILE=<profile> AWS_DEFAULT_REGION=eu-west-3 \
        python3 sweep_legacy_records.py --bucket eks-mb-production-tansu-733281893834
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
from dataclasses import dataclass, field

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


@dataclass
class Findings:
    topics: int = 0
    with_legacy: list[str] = field(default_factory=list)
    compacted_with_legacy: list[tuple[str, int]] = field(default_factory=list)
    metadata_missing: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)


def client(bucket: str):
    # Generous pool: the sweep is one LIST per topic across ~15k topics, so the
    # default pool of 10 is the bottleneck rather than S3.
    return boto3.client("s3", config=Config(max_pool_connections=64, retries={"max_attempts": 5}))


def topic_names(s3, bucket: str, cluster: str) -> list[str]:
    """Every topic that has ever had an object, from one paginated delimiter LIST."""
    prefix = f"clusters/{cluster}/topics/"
    names: list[str] = []

    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            name = common["Prefix"][len(prefix) :].rstrip("/")
            if name:
                names.append(name)

    return names


def legacy_object_count(s3, bucket: str, cluster: str, topic: str, cap: int) -> int:
    """How many legacy `records/` objects this topic holds, capped.

    Capped because the answer that matters is "any at all"; the count is only
    reported so a leftover can be sized. Scans every partition — a topic can
    hold legacy objects on one partition and not another.
    """
    prefix = f"clusters/{cluster}/topics/{topic}/partitions/"
    found = 0

    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}
    ):
        for obj in page.get("Contents", []):
            if "/records/" in obj["Key"]:
                found += 1
                if found >= cap:
                    return found

    return found


def is_compacted(s3, bucket: str, cluster: str, topic: str) -> bool | None:
    """Whether `cleanup.policy` contains `compact`. `None` if metadata is absent."""
    key = f"clusters/{cluster}/topic-metadata/{topic}.json"

    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise

    configs = json.loads(body).get("topic", {}).get("configs") or []

    for config in configs:
        if config.get("name") == "cleanup.policy":
            return "compact" in (config.get("value") or "")

    # Absent policy reads as Kafka's default, `delete` — not compacted. See the
    # retention alignment in Popsink/tansu#199.
    return False


def sweep(bucket: str, cluster: str, workers: int, cap: int) -> Findings:
    s3 = client(bucket)
    found = Findings()

    names = topic_names(s3, bucket, cluster)
    found.topics = len(names)
    print(f"{len(names)} topics discovered", file=sys.stderr)

    # Phase 1: which topics still hold legacy objects at all. This is the
    # expensive phase (one paginated LIST per topic) and it narrows ~15k topics
    # to the few thousand that matter.
    def probe(topic: str):
        try:
            return topic, legacy_object_count(s3, bucket, cluster, topic, cap), None
        except Exception as error:  # noqa: BLE001 - reported, never fatal
            return topic, 0, str(error)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for done, (topic, count, error) in enumerate(pool.map(probe, names), start=1):
            if error:
                found.errors.append((topic, error))
            elif count:
                found.with_legacy.append(topic)

            if done % 1000 == 0:
                print(
                    f"  probed {done}/{len(names)}; {len(found.with_legacy)} hold legacy",
                    file=sys.stderr,
                )

    print(f"{len(found.with_legacy)} topics hold legacy objects", file=sys.stderr)

    # Phase 2: of those, which are compacted. Only this set gets a metadata GET.
    def classify(topic: str):
        try:
            return topic, is_compacted(s3, bucket, cluster, topic), None
        except Exception as error:  # noqa: BLE001
            return topic, None, str(error)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for topic, compacted, error in pool.map(classify, found.with_legacy):
            if error:
                found.errors.append((topic, error))
            elif compacted is None:
                found.metadata_missing.append(topic)
            elif compacted:
                count = legacy_object_count(s3, bucket, cluster, topic, cap)
                found.compacted_with_legacy.append((topic, count))

    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--cluster", default="tansu")
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument(
        "--count-cap",
        type=int,
        default=1000,
        help="stop counting a topic's legacy objects here; only 'any' matters",
    )
    args = parser.parse_args()

    found = sweep(args.bucket, args.cluster, args.workers, args.count_cap)

    print()
    print(f"topics discovered                   {found.topics}")
    print(f"holding legacy records/ objects      {len(found.with_legacy)}")
    print(f"  of which COMPACTED                 {len(found.compacted_with_legacy)}")
    print(f"  of which non-compacted (accepted)  "
          f"{len(found.with_legacy) - len(found.compacted_with_legacy) - len(found.metadata_missing)}")

    if found.metadata_missing:
        print()
        print(f"!! {len(found.metadata_missing)} topics hold legacy objects but have no "
              f"topic-metadata — cannot classify:")
        for topic in sorted(found.metadata_missing)[:20]:
            print(f"     {topic}")

    if found.errors:
        print()
        print(f"!! {len(found.errors)} errors (sweep is incomplete, do not treat as clean):")
        for topic, error in found.errors[:10]:
            print(f"     {topic}: {error}")

    print()

    if found.compacted_with_legacy:
        print("GATE NOT MET — compacted topics still hold legacy objects.")
        print("#179 would make this data unreadable. Do not proceed.")
        for topic, count in sorted(found.compacted_with_legacy, key=lambda t: -t[1]):
            print(f"  {count:>6}  {topic}")
        return 1

    if found.errors or found.metadata_missing:
        print("INCONCLUSIVE — no compacted topic was found holding legacy objects, but the")
        print("sweep did not complete cleanly. Resolve the above before calling the gate met.")
        return 2

    print("GATE MET — zero compacted topics hold legacy objects.")
    print("#178, #179 and #180 are unblocked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
