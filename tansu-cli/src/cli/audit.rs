// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::time::{Duration, UNIX_EPOCH};

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

    /// What to audit: s3://tansu/, gs://tansu/, or file:///path/to/a/copy for an offline copy of the bucket
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

            Format::Text => text(&report, &storage_engine, &self.topic, self.ranges),
        }

        // Opt-in, because any non-`None` code makes the binary log that code's
        // generic description — and `CORRUPT_MESSAGE`'s is "this message has
        // failed its CRC checksum", which is the opposite of what an audit
        // finds. Every surviving byte validates; the records are simply not
        // there. So the default is a clean report and exit 0, and a sweep that
        // wants a shell-visible verdict asks for one.
        Ok(if report.lost_records() > 0 && self.fail_on_loss {
            ErrorCode::CorruptMessage
        } else {
            ErrorCode::None
        })
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

fn text(report: &AuditReport, storage_engine: &Url, topics: &[String], ranges: bool) {
    let sheet = Sheet::default();

    println!(
        "tansu {} {}",
        "audit".if_supports_color(Stream::Stdout, |text| text.style(sheet.headline)),
        env!("CARGO_PKG_VERSION")
    );

    println!(
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

    println!();
    println!(
        "segments      {} in {} prefixes{}",
        thousands(report.segments as i64),
        report.prefixes,
        if versions.is_empty() {
            String::new()
        } else {
            format!(" ({versions})")
        }
    );

    println!(
        "unreadable    {}",
        if report.segments_unreadable == 0 {
            String::from("0")
        } else {
            format!("{} — see faults below", report.segments_unreadable)
        }
    );

    if report.legacy_batches > 0 {
        println!(
            "legacy        {} abandoned records/ objects — the broker has served none since #179",
            thousands(report.legacy_batches as i64)
        );
    }

    println!();

    let lost = report.lost_records();

    if lost == 0 {
        println!(
            "{}",
            format!(
                "no records lost over {} offsets of cleanup.policy=delete",
                thousands(report.spanned_records())
            )
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.headline))
        );
    } else {
        println!(
            "{}",
            format!(
                "records lost  {} of {} offsets — {:.2} %",
                thousands(lost),
                thousands(report.spanned_records()),
                report.lost_percentage()
            )
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.loss))
        );

        println!(
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
        println!();
        table(&damaged, &sheet);

        if ranges {
            for topic in &damaged {
                detail(topic, &sheet);
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
        println!();
        println!(
            "{}",
            "compacted topics — per-key compaction removes superseded keys, and a removed key \
             IS an offset gap. Not loss, not counted above."
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.quiet))
        );
        println!();
        table(&compacted, &sheet);
    }

    if !report.faults.is_empty() {
        println!();
        println!(
            "{}",
            "faults — segments whose footer does not describe the object it is in"
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.loss))
        );

        for fault in &report.faults {
            println!("  {}/{:0>20}  {}", fault.prefix, fault.seq, fault.detail);
        }
    }
}

fn table(topics: &[&TopicAudit], sheet: &Sheet) {
    let width = topics
        .iter()
        .map(|topic| topic.topic.len())
        .max()
        .unwrap_or(5)
        .max(5);

    println!(
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

        println!(
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

fn detail(topic: &TopicAudit, sheet: &Sheet) {
    println!();
    println!(
        "{}",
        topic
            .topic
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.headline))
    );

    for partition in &topic.partitions {
        for gap in &partition.gaps {
            println!(
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
            println!(
                "       written between {} and {}",
                timestamp(gap.before.max_timestamp),
                timestamp(gap.after.max_timestamp),
            );

            println!(
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
    use super::thousands;

    #[test]
    fn digits_group_in_threes() {
        assert_eq!("0", thousands(0));
        assert_eq!("999", thousands(999));
        assert_eq!("1 000", thousands(1_000));
        assert_eq!("32 435 697", thousands(32_435_697));
        assert_eq!("-1 234", thousands(-1_234));
    }
}
