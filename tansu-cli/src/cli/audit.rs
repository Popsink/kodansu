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

//! `tansu audit` — the offline segment audit of #447.
//!
//! Points [`tansu_storage::Audit`] at a bucket, or at a **copy** of one on
//! local disk, and prints the offsets its segments cannot serve.

use std::{
    fmt::Write as _,
    time::{Duration, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use owo_colors::{OwoColorize as _, Stream, Style};
use tansu_sans_io::ErrorCode;
use tansu_storage::{Audit, AuditReport, TopicAudit};
use url::Url;

use crate::{EnvVarExp, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub(super) enum Format {
    #[default]
    Text,
    /// The whole report, every range included. This is the form to keep: the
    /// bracketing timestamps of each range bound when the lost records were
    /// written, which is what a re-snapshot or a source-journal replay targets.
    Json,
}

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    /// All members of the same cluster should use the same id
    #[arg(
        long,
        env = "CLUSTER_ID",
        default_value = "tansu_cluster",
        visible_alias = "kafka-cluster-id"
    )]
    cluster_id: String,

    /// What to audit: s3://tansu/, gs://tansu/, abfss://tansu@acct.dfs.core.windows.net/, or file:///path/to/a/copy for an offline copy of the bucket
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://tansu/")]
    storage_engine: EnvVarExp<Url>,

    /// Only report these topics (the whole store is still walked; every topic shares its prefix's segments)
    #[arg(long)]
    topic: Vec<String>,

    /// Print every lost offset range, with the segments bracketing it
    #[arg(long)]
    ranges: bool,

    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,

    /// Segments read concurrently. One ranged GET each.
    #[arg(long, default_value = "32")]
    concurrency: usize,

    /// Exit non-zero when records are lost, for a scripted fleet sweep. The exit is carried as CORRUPT_MESSAGE, the code the read path answers for a damaged segment: a mechanism for the shell, not a claim that a surviving object is corrupt.
    #[arg(long)]
    fail_on_loss: bool,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let storage_engine = self.storage_engine.into_inner();

        let report = Audit::try_from_url(&storage_engine, self.cluster_id.as_str())
            .map_err(|error| crate::Error::Box(Box::new(error)))?
            .concurrency(self.concurrency)
            .run()
            .await
            .map_err(|error| crate::Error::Box(Box::new(error)))?;

        match self.format {
            Format::Json => println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .map_err(|error| crate::Error::Box(Box::new(error)))?
            ),

            Format::Text => print!(
                "{}",
                text(&report, &storage_engine, &self.topic, self.ranges)
            ),
        }

        Ok(exit_code(&report, self.fail_on_loss))
    }
}

/// The verdict the shell sees.
///
/// Opt-in, because any non-`None` code makes the binary log that code's generic
/// description — and `CORRUPT_MESSAGE`'s is "this message has failed its CRC
/// checksum", which is the opposite of what an audit finds. Every surviving byte
/// validates; the records are simply not there. So the default is a clean report
/// and exit 0, and a sweep that wants a shell-visible verdict asks for one.
fn exit_code(report: &AuditReport, fail_on_loss: bool) -> ErrorCode {
    if report.lost_records() > 0 && fail_on_loss {
        ErrorCode::CorruptMessage
    } else {
        ErrorCode::None
    }
}

struct Sheet {
    headline: Style,
    label: Style,
    loss: Style,
    quiet: Style,
}

impl Default for Sheet {
    fn default() -> Self {
        Self {
            headline: Style::new().bold(),
            label: Style::new().cyan(),
            loss: Style::new().red().bold(),
            quiet: Style::new().dimmed(),
        }
    }
}

/// The whole report, as an operator reads it.
///
/// Returned rather than printed. Every line below is a decision about what a
/// data-loss report says and how it says it — the headline that must not read
/// as a failure when nothing is lost, compaction's gaps kept out of the count,
/// faults named separately — and none of it was assertable while it went
/// straight to stdout (#556).
fn text(report: &AuditReport, storage_engine: &Url, topics: &[String], ranges: bool) -> String {
    let mut out = String::new();

    render(&mut out, report, storage_engine, topics, ranges);

    out
}

/// Every line, into a `String`.
///
/// The writes are not propagated: a `String` sink cannot fail, and threading a
/// `fmt::Result` through would put eighteen error paths no test can reach into
/// a file whose whole point is being covered (#556).
fn render(
    out: &mut String,
    report: &AuditReport,
    storage_engine: &Url,
    topics: &[String],
    ranges: bool,
) {
    let sheet = Sheet::default();

    _ = writeln!(
        out,
        "tansu {} {}",
        "audit".if_supports_color(Stream::Stdout, |text| text.style(sheet.headline)),
        env!("CARGO_PKG_VERSION")
    );

    _ = writeln!(
        out,
        "cluster {} · {}",
        report
            .cluster
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.label)),
        storage_engine.if_supports_color(Stream::Stdout, |text| text.style(sheet.label))
    );

    let versions = report
        .versions
        .iter()
        .map(|(version, count)| format!("v{version} {count}"))
        .collect::<Vec<_>>()
        .join(", ");

    _ = writeln!(out);
    _ = writeln!(
        out,
        "segments      {} in {} prefixes{}",
        thousands(report.segments as i64),
        report.prefixes,
        if versions.is_empty() {
            String::new()
        } else {
            format!(" ({versions})")
        }
    );

    _ = writeln!(
        out,
        "unreadable    {}",
        if report.segments_unreadable == 0 {
            String::from("0")
        } else {
            format!("{} — see faults below", report.segments_unreadable)
        }
    );

    if report.legacy_batches > 0 {
        _ = writeln!(
            out,
            "legacy        {} abandoned records/ objects — the broker has served none since #179",
            thousands(report.legacy_batches as i64)
        );
    }

    if report.retired_substreams > 0 {
        _ = writeln!(
            out,
            "retired       {} sub-streams of deleted topic incarnations — unreachable, \
             held until every co-tenant of their segments is past retention",
            thousands(report.retired_substreams as i64)
        );
    }

    _ = writeln!(out);

    let lost = report.lost_records();

    if lost == 0 {
        _ = writeln!(
            out,
            "{}",
            format!(
                "no records lost over {} offsets of cleanup.policy=delete",
                thousands(report.spanned_records())
            )
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.headline))
        );
    } else {
        _ = writeln!(
            out,
            "{}",
            format!(
                "records lost  {} of {} offsets — {:.2} %",
                thousands(lost),
                thousands(report.spanned_records()),
                report.lost_percentage()
            )
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.loss))
        );

        _ = writeln!(
            out,
            "{}",
            "              cleanup.policy=delete only, and a floor: a hole is visible \
             only between two surviving segments, never at the head or tail of a log."
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.quiet))
        );
    }

    let wanted = |topic: &&TopicAudit| topics.is_empty() || topics.contains(&topic.topic);

    let damaged: Vec<&TopicAudit> = report
        .topics
        .iter()
        .filter(|topic| !topic.gaps_expected && topic.records_lost() > 0)
        .filter(wanted)
        .collect();

    if !damaged.is_empty() {
        _ = writeln!(out);
        table(out, &damaged, &sheet);

        if ranges {
            for topic in &damaged {
                detail(out, topic, &sheet);
            }
        }
    }

    let compacted: Vec<&TopicAudit> = report
        .topics
        .iter()
        .filter(|topic| topic.gaps_expected && topic.records_lost() > 0)
        .filter(wanted)
        .collect();

    if !compacted.is_empty() {
        _ = writeln!(out);
        _ = writeln!(
            out,
            "{}",
            "compacted topics — per-key compaction removes superseded keys, and a removed key \
             IS an offset gap. Not loss, not counted above."
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.quiet))
        );
        _ = writeln!(out);
        table(out, &compacted, &sheet);
    }

    if !report.faults.is_empty() {
        _ = writeln!(out);
        _ = writeln!(
            out,
            "{}",
            "faults — segments whose footer does not describe the object it is in"
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.loss))
        );

        for fault in &report.faults {
            _ = writeln!(
                out,
                "  {}/{:0>20}  {}",
                fault.prefix, fault.seq, fault.detail
            );
        }
    }
}

fn table(out: &mut String, topics: &[&TopicAudit], sheet: &Sheet) {
    let width = topics
        .iter()
        .map(|topic| topic.topic.len())
        .max()
        .unwrap_or(5)
        .max(5);

    _ = writeln!(
        out,
        "{}",
        format!(
            "{:width$}  {:>3}  {:>14}  {:>14}  {:>7}  {:>6}",
            "TOPIC", "P", "SPAN", "LOST", "%", "RANGES"
        )
        .if_supports_color(Stream::Stdout, |text| text.style(sheet.label))
    );

    for topic in topics {
        let span = topic.span();
        let lost = topic.records_lost();
        let gaps: usize = topic.partitions.iter().map(|p| p.gaps.len()).sum();

        _ = writeln!(
            out,
            "{:width$}  {:>3}  {:>14}  {:>14}  {:>6.2}%  {:>6}",
            topic.topic,
            topic.partitions.len(),
            thousands(span),
            thousands(lost),
            if span > 0 {
                (lost as f64) * 100.0 / (span as f64)
            } else {
                0.0
            },
            gaps,
        );
    }
}

fn detail(out: &mut String, topic: &TopicAudit, sheet: &Sheet) {
    _ = writeln!(out);
    _ = writeln!(
        out,
        "{}",
        topic
            .topic
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.headline))
    );

    for partition in &topic.partitions {
        for gap in &partition.gaps {
            _ = writeln!(
                out,
                "  p{:<3} {} .. {}  {} records",
                partition.partition,
                thousands(gap.lost_from),
                thousands(gap.lost_to),
                thousands(gap.records),
            );

            // The bracketing timestamps bound when the lost records were
            // written — what a re-snapshot or a source-journal replay has to
            // target — and the bracketing sizes are the merge-path signal: a
            // segment at the ~16 MiB roll target before a hole is a merge, not
            // a flush.
            _ = writeln!(
                out,
                "       written between {} and {}",
                timestamp(gap.before.max_timestamp),
                timestamp(gap.after.max_timestamp),
            );

            _ = writeln!(
                out,
                "       between {}/{} ({}) and {}/{} ({})",
                gap.before.prefix,
                gap.before.seq,
                bytes(gap.before.size),
                gap.after.prefix,
                gap.after.seq,
                bytes(gap.after.size),
            );
        }
    }
}

/// A footer `max_timestamp` as RFC 3339. Kafka record timestamps are
/// milliseconds since the epoch; a negative one is Kafka's "no timestamp"
/// sentinel and has no useful rendering.
fn timestamp(millis: i64) -> String {
    u64::try_from(millis)
        .map(|millis| {
            humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(millis)).to_string()
        })
        .unwrap_or_else(|_| String::from("no timestamp"))
}

/// A segment size, rounded, for reading the merge-path signal at a glance.
fn bytes(size: u64) -> String {
    if size >= 1024 * 1024 {
        format!("{:.1} MiB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{:.1} KiB", size as f64 / 1024.0)
    } else {
        format!("{size} B")
    }
}

/// `32435697` as `32 435 697`. A twelve-digit offset span is unreadable without
/// it, and this report's whole job is to be read.
fn thousands(value: i64) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 + 1);

    if value < 0 {
        grouped.push('-');
    }

    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(' ');
        }

        grouped.push(digit);
    }

    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use tansu_storage::{Bracket, Gap, PartitionAudit, SegmentFault};

    /// The renderer asks `owo-colors` whether stdout takes colour, and under a
    /// terminal it would answer yes — so every assertion below would be
    /// comparing against escape sequences. Pinned off rather than left to how
    /// the suite was launched.
    fn plain() {
        owo_colors::set_override(false);
    }

    fn bracket(prefix: &str, seq: u64, size: u64, max_timestamp: i64) -> Bracket {
        Bracket {
            prefix: String::from(prefix),
            seq,
            size,
            max_timestamp,
        }
    }

    fn partition(partition: i32, span: i64, gaps: Vec<Gap>) -> PartitionAudit {
        let records_lost = gaps.iter().map(|gap| gap.records).sum();

        PartitionAudit {
            partition,
            first_offset: 0,
            next_offset: span,
            span,
            records_present: span - records_lost,
            records_lost,
            gaps,
            overlaps_dropped: 0,
            overlaps_clipped: 0,
            legacy_batches: 0,
        }
    }

    fn topic(name: &str, cleanup_policy: &str, partitions: Vec<PartitionAudit>) -> TopicAudit {
        TopicAudit {
            topic: String::from(name),
            cleanup_policy: String::from(cleanup_policy),
            gaps_expected: cleanup_policy.contains("compact"),
            metadata_missing: false,
            partitions,
        }
    }

    fn gap(lost_from: i64, lost_to: i64) -> Gap {
        Gap {
            lost_from,
            lost_to,
            records: lost_to - lost_from + 1,
            before: bracket("abcd/0", 1, 16 * 1024 * 1024, 1_756_000_000_000),
            after: bracket("abcd/0", 2, 512, 1_756_000_060_000),
        }
    }

    fn storage_engine() -> Url {
        Url::parse("s3://tansu/").expect("s3://tansu/")
    }

    #[test]
    fn digits_group_in_threes() {
        assert_eq!("0", thousands(0));
        assert_eq!("999", thousands(999));
        assert_eq!("1 000", thousands(1_000));
        assert_eq!("32 435 697", thousands(32_435_697));
        assert_eq!("-1 234", thousands(-1_234));
    }

    /// Kafka's "no timestamp" sentinel is negative, and there is no instant to
    /// render for it. The alternative — an epoch-relative date in 1969 — would
    /// be read as a real bracketing time by whoever is targeting a replay.
    #[test]
    fn a_negative_timestamp_has_no_rendering() {
        assert_eq!("1970-01-01T00:00:00.000Z", timestamp(0));
        assert_eq!("2025-08-24T01:53:20.000Z", timestamp(1_756_000_400_000));
        assert_eq!("no timestamp", timestamp(-1));
    }

    /// Sizes carry the merge-path signal, so the unit has to change where the
    /// eye does: a segment at the ~16 MiB roll target before a hole is a
    /// merge, a half-kilobyte one is a flush.
    #[test]
    fn a_size_is_rendered_in_the_unit_that_reads() {
        assert_eq!("0 B", bytes(0));
        assert_eq!("1023 B", bytes(1023));
        assert_eq!("1.0 KiB", bytes(1024));
        assert_eq!("16.0 MiB", bytes(16 * 1024 * 1024));
    }

    /// A clean bucket says so, in the affirmative, and never prints a
    /// "records lost" line with a zero in it — an audit is run when loss is
    /// suspected and the headline is the answer.
    #[test]
    fn a_clean_report_states_that_nothing_is_lost() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            segments: 2_048,
            prefixes: 32,
            topics: vec![topic("orders", "delete", vec![partition(0, 1_000, vec![])])],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[], false);

        assert!(
            rendered.contains("no records lost over 1 000 offsets"),
            "{rendered}"
        );
        assert!(!rendered.contains("records lost  "), "{rendered}");
        assert!(
            rendered.contains("segments      2 048 in 32 prefixes"),
            "{rendered}"
        );
        assert!(rendered.contains("unreadable    0"), "{rendered}");
        assert!(
            rendered.contains("cluster tansu_cluster · s3://tansu/"),
            "{rendered}"
        );
    }

    /// The headline is a count and a percentage over the `delete`-policy span,
    /// and the damaged topics arrive in a table under it.
    #[test]
    fn loss_is_counted_and_the_damaged_topic_is_tabulated() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            topics: vec![topic(
                "orders",
                "delete",
                vec![partition(0, 1_000, vec![gap(100, 199)])],
            )],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[], false);

        assert!(
            rendered.contains("records lost  100 of 1 000 offsets — 10.00 %"),
            "{rendered}"
        );
        assert!(rendered.contains("TOPIC"), "{rendered}");
        assert!(
            rendered.contains("orders    1           1 000             100   10.00%       1"),
            "{rendered}"
        );
    }

    /// #471's distinction, in the output: a compacted topic's gaps are what
    /// compaction is, so they are reported apart and counted in neither the
    /// headline nor the damaged table. Losing this is how an operator is told
    /// a healthy cluster is losing records.
    #[test]
    fn compaction_gaps_are_reported_apart_and_counted_in_neither_total() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            topics: vec![topic(
                "sessions",
                "compact",
                vec![partition(0, 1_000, vec![gap(100, 199)])],
            )],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[], false);

        assert!(
            rendered.contains("no records lost over 0 offsets"),
            "the compacted topic's span is not part of the headline: {rendered}"
        );
        assert!(rendered.contains("compacted topics"), "{rendered}");
        assert!(rendered.contains("sessions"), "{rendered}");
    }

    /// `--topic` narrows the tables and nothing else: the headline still
    /// counts the whole cluster, because the store is walked whole and a
    /// filtered percentage would be a different measurement wearing the same
    /// label.
    #[test]
    fn the_topic_filter_narrows_the_tables_and_not_the_headline() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            topics: vec![
                topic(
                    "orders",
                    "delete",
                    vec![partition(0, 1_000, vec![gap(100, 199)])],
                ),
                topic(
                    "shipments",
                    "delete",
                    vec![partition(0, 1_000, vec![gap(300, 399)])],
                ),
            ],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[String::from("orders")], false);

        assert!(
            rendered.contains("records lost  200 of 2 000 offsets — 10.00 %"),
            "{rendered}"
        );
        assert!(rendered.contains("orders"), "{rendered}");
        assert!(!rendered.contains("shipments"), "{rendered}");
    }

    /// `--ranges` is the form #447 wants kept: every hole with the timestamps
    /// and sizes of the segments bracketing it, which is what a re-snapshot or
    /// a source-journal replay targets. Off by default, and off means absent.
    #[test]
    fn ranges_print_the_brackets_that_a_replay_targets() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            topics: vec![topic(
                "orders",
                "delete",
                vec![partition(7, 1_000, vec![gap(100, 199)])],
            )],
            ..Default::default()
        };

        let without = text(&report, &storage_engine(), &[], false);
        assert!(!without.contains("written between"), "{without}");

        let with = text(&report, &storage_engine(), &[], true);

        assert!(with.contains("p7   100 .. 199  100 records"), "{with}");
        assert!(
            with.contains("written between 2025-08-24T01:46:40.000Z and 2025-08-24T01:47:40.000Z"),
            "{with}"
        );
        assert!(
            with.contains("between abcd/0/1 (16.0 MiB) and abcd/0/2 (512 B)"),
            "{with}"
        );
    }

    /// The counters that are informational rather than loss — legacy `records/`
    /// objects (#179) and the retired sub-streams of #442 — appear only when
    /// there are any. A line of zeroes in a report read under pressure is
    /// noise.
    #[test]
    fn the_informational_counters_appear_only_when_they_are_not_zero() {
        plain();

        let quiet = AuditReport {
            cluster: String::from("tansu_cluster"),
            ..Default::default()
        };

        let rendered = text(&quiet, &storage_engine(), &[], false);
        assert!(!rendered.contains("legacy"), "{rendered}");
        assert!(!rendered.contains("retired"), "{rendered}");

        let loud = AuditReport {
            legacy_batches: 12,
            retired_substreams: 3,
            versions: [(3, 100), (4, 20)].into_iter().collect(),
            segments_unreadable: 2,
            ..quiet
        };

        let rendered = text(&loud, &storage_engine(), &[], false);
        assert!(
            rendered.contains("legacy        12 abandoned"),
            "{rendered}"
        );
        assert!(
            rendered.contains("retired       3 sub-streams"),
            "{rendered}"
        );
        assert!(rendered.contains("(v3 100, v4 20)"), "{rendered}");
        assert!(
            rendered.contains("unreadable    2 — see faults below"),
            "{rendered}"
        );
    }

    /// A topic whose span is zero renders `0.00%` rather than `NaN%`. The
    /// span is a denominator, and a sub-stream whose slices were all dropped
    /// as overlaps reaches the table with a loss and nothing to divide by.
    #[test]
    fn a_zero_span_is_a_percentage_and_not_a_nan() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            topics: vec![topic(
                "orders",
                "delete",
                vec![PartitionAudit {
                    partition: 0,
                    first_offset: 0,
                    next_offset: 0,
                    span: 0,
                    records_present: 0,
                    records_lost: 100,
                    gaps: vec![gap(100, 199)],
                    overlaps_dropped: 1,
                    overlaps_clipped: 0,
                    legacy_batches: 0,
                }],
            )],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[], false);

        assert!(rendered.contains("   0.00%"), "{rendered}");
        assert!(!rendered.contains("NaN"), "{rendered}");
    }

    /// A fault is damage that left a trace, and it is named with the object it
    /// is in — the audit's whole output is a list of things to go and look at.
    #[test]
    fn a_fault_names_the_object_it_is_in() {
        plain();

        let report = AuditReport {
            cluster: String::from("tansu_cluster"),
            faults: vec![SegmentFault {
                prefix: String::from("abcd/0"),
                seq: 42,
                detail: String::from("footer describes 3 sub-streams, object holds 2"),
            }],
            ..Default::default()
        };

        let rendered = text(&report, &storage_engine(), &[], false);

        assert!(rendered.contains("faults —"), "{rendered}");
        assert!(
            rendered.contains("abcd/0/00000000000000000042  footer describes 3 sub-streams"),
            "{rendered}"
        );
    }

    /// Loss is exit 0 unless the sweep asked otherwise: any non-`None` code
    /// makes the binary log that code's generic description, and
    /// `CORRUPT_MESSAGE` reads "failed its CRC checksum" — the opposite of
    /// what an audit finds. A clean report is exit 0 with the flag on too, so
    /// a fleet sweep's non-zero means loss and nothing else.
    #[test]
    fn only_a_sweep_that_asked_for_a_verdict_gets_one() {
        let lost = AuditReport {
            topics: vec![topic(
                "orders",
                "delete",
                vec![partition(0, 1_000, vec![gap(100, 199)])],
            )],
            ..Default::default()
        };

        let clean = AuditReport {
            topics: vec![topic("orders", "delete", vec![partition(0, 1_000, vec![])])],
            ..Default::default()
        };

        assert_eq!(ErrorCode::None, exit_code(&lost, false));
        assert_eq!(ErrorCode::CorruptMessage, exit_code(&lost, true));
        assert_eq!(ErrorCode::None, exit_code(&clean, true));
        assert_eq!(ErrorCode::None, exit_code(&clean, false));
    }

    /// A compacted topic's gaps must not become a fleet sweep's non-zero
    /// exit, for the reason they are kept out of the headline: they are what
    /// compaction is.
    #[test]
    fn compaction_gaps_are_not_a_verdict() {
        let compacted = AuditReport {
            topics: vec![topic(
                "sessions",
                "compact",
                vec![partition(0, 1_000, vec![gap(100, 199)])],
            )],
            ..Default::default()
        };

        assert_eq!(ErrorCode::None, exit_code(&compacted, true));
    }

    /// Both output arms run end to end over a store the audit can actually
    /// walk. Text is the default; `--format json` is the form to keep.
    #[tokio::test]
    async fn each_format_runs_over_a_store() -> Result<()> {
        for format in ["text", "json"] {
            let arg = Arg::try_parse_from([
                "audit",
                "--storage-engine",
                "memory://tansu/",
                "--format",
                format,
            ])
            .unwrap_or_else(|error| panic!("--format {format}: {error}"));

            assert_eq!(ErrorCode::None, arg.main().await?);
        }

        assert_eq!(
            Format::Text,
            Arg::try_parse_from(["audit"]).expect("no arguments").format
        );

        Ok(())
    }

    /// The engine that stores nothing has no segments to walk, and the audit
    /// says so at the URL rather than reporting an empty cluster as clean.
    #[tokio::test]
    async fn the_null_engine_has_nothing_to_audit() {
        let arg = Arg::try_parse_from(["audit", "--storage-engine", "null://tansu/"])
            .expect("null://tansu/");

        assert!(arg.main().await.is_err());
    }

    /// Zero is read as one rather than as a division by zero, and the flag is
    /// a segment count and not a size.
    #[test]
    fn the_concurrency_is_a_count_with_a_default() {
        assert_eq!(
            32,
            Arg::try_parse_from(["audit"])
                .expect("no arguments")
                .concurrency
        );

        assert_eq!(
            0,
            Arg::try_parse_from(["audit", "--concurrency", "0"])
                .expect("--concurrency 0")
                .concurrency
        );

        assert!(Arg::try_parse_from(["audit", "--concurrency", "lots"]).is_err());
    }
}
