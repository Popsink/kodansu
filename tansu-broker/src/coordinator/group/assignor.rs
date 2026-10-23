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

//! The KIP-848 server-side assignors (#612): `uniform`, Kafka's default, and
//! `range`.
//!
//! Both are pure functions of the members' subscriptions, the subscribed
//! topics' partition counts and — for `uniform` — the previous target
//! assignment, so every replica computing the target for the same group
//! document computes the same one.

use std::collections::{BTreeMap, BTreeSet};

use tansu_storage::Partitions;
use uuid::Uuid;

/// The assignor Kafka uses when no member asks for one.
pub(crate) const UNIFORM: &str = "uniform";

/// Contiguous ranges of each topic's partitions, per topic.
pub(crate) const RANGE: &str = "range";

/// Whether `name` is an assignor this broker runs.
pub(crate) fn is_supported(name: &str) -> bool {
    name == UNIFORM || name == RANGE
}

/// What the assignor knows of one member.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemberSpec {
    /// The subscribed topics that exist, by id.
    pub(crate) subscribed: BTreeSet<Uuid>,

    /// The member's previous target, which `uniform` keeps where it can.
    pub(crate) previous: Partitions,
}

/// The target assignment of every member, computed by `assignor`.
///
/// `topics` is the partition count of every subscribed topic. Every member is
/// in the result, with an empty assignment if nothing is assigned to it.
pub(crate) fn assign(
    assignor: &str,
    members: &BTreeMap<String, MemberSpec>,
    topics: &BTreeMap<Uuid, i32>,
) -> BTreeMap<String, Partitions> {
    if assignor == RANGE {
        range(members, topics)
    } else {
        uniform(members, topics)
    }
}

fn range(
    members: &BTreeMap<String, MemberSpec>,
    topics: &BTreeMap<Uuid, i32>,
) -> BTreeMap<String, Partitions> {
    let mut assigned = members
        .keys()
        .map(|member_id| (member_id.clone(), Partitions::new()))
        .collect::<BTreeMap<_, _>>();

    for (topic_id, partitions) in topics {
        let subscribers = members
            .iter()
            .filter(|(_, spec)| spec.subscribed.contains(topic_id))
            .map(|(member_id, _)| member_id)
            .collect::<Vec<_>>();

        let Ok(count) = i32::try_from(subscribers.len()) else {
            continue;
        };

        if count == 0 {
            continue;
        }

        let quota = partitions / count;
        let extra = partitions % count;

        let mut next = 0;

        for (index, member_id) in (0..).zip(subscribers) {
            let width = quota + i32::from(index < extra);

            if width > 0
                && let Some(member) = assigned.get_mut(member_id)
            {
                member
                    .entry(*topic_id)
                    .or_default()
                    .extend(next..next + width);
            }

            next += width;
        }
    }

    assigned
}

/// A balanced, sticky assignment.
///
/// Every partition a member held in its previous target and is still entitled
/// to is kept; the rest go to the least loaded subscriber; then a partition
/// moves from the most to the least loaded of its subscribers while that
/// narrows the gap by more than one. Every move strictly lowers the sum of the
/// squared loads, so the last step terminates.
fn uniform(
    members: &BTreeMap<String, MemberSpec>,
    topics: &BTreeMap<Uuid, i32>,
) -> BTreeMap<String, Partitions> {
    let mut owner: BTreeMap<(Uuid, i32), String> = BTreeMap::new();

    for (member_id, spec) in members {
        for (topic_id, partitions) in &spec.previous {
            let Some(count) = topics.get(topic_id) else {
                continue;
            };

            if !spec.subscribed.contains(topic_id) {
                continue;
            }

            for partition in partitions.iter().filter(|partition| **partition < *count) {
                _ = owner
                    .entry((*topic_id, *partition))
                    .or_insert_with(|| member_id.clone());
            }
        }
    }

    let mut load = members
        .keys()
        .map(|member_id| (member_id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();

    for member_id in owner.values() {
        if let Some(count) = load.get_mut(member_id) {
            *count += 1;
        }
    }

    for (topic_id, count) in topics {
        for partition in 0..*count {
            if owner.contains_key(&(*topic_id, partition)) {
                continue;
            }

            if let Some(member_id) = least_loaded(members, &load, topic_id) {
                *load.entry(member_id.clone()).or_default() += 1;
                _ = owner.insert((*topic_id, partition), member_id);
            }
        }
    }

    while let Some((partition, from, to)) = improving_move(members, &owner, &load) {
        _ = owner.insert(partition, to.clone());
        *load.entry(from).or_default() -= 1;
        *load.entry(to).or_default() += 1;
    }

    owner.into_iter().fold(
        members
            .keys()
            .map(|member_id| (member_id.clone(), Partitions::new()))
            .collect::<BTreeMap<_, _>>(),
        |mut assigned, ((topic_id, partition), member_id)| {
            _ = assigned
                .entry(member_id)
                .or_default()
                .entry(topic_id)
                .or_default()
                .insert(partition);
            assigned
        },
    )
}

fn least_loaded(
    members: &BTreeMap<String, MemberSpec>,
    load: &BTreeMap<String, usize>,
    topic_id: &Uuid,
) -> Option<String> {
    members
        .iter()
        .filter(|(_, spec)| spec.subscribed.contains(topic_id))
        .min_by_key(|(member_id, _)| {
            (
                load.get(*member_id).copied().unwrap_or_default(),
                *member_id,
            )
        })
        .map(|(member_id, _)| member_id.clone())
}

/// One move of a partition from a member to a subscriber of its topic carrying
/// at least two fewer, or `None` when the assignment is balanced.
fn improving_move(
    members: &BTreeMap<String, MemberSpec>,
    owner: &BTreeMap<(Uuid, i32), String>,
    load: &BTreeMap<String, usize>,
) -> Option<((Uuid, i32), String, String)> {
    let load_of = |member_id: &String| load.get(member_id).copied().unwrap_or_default();

    let mut best: Option<(usize, (Uuid, i32), String, String)> = None;

    for (partition, from) in owner {
        let Some(to) = least_loaded(members, load, &partition.0) else {
            continue;
        };

        let gap = load_of(from).saturating_sub(load_of(&to));

        if gap >= 2 && best.as_ref().is_none_or(|(held, ..)| gap > *held) {
            best = Some((gap, *partition, from.clone(), to));
        }
    }

    best.map(|(_, partition, from, to)| (partition, from, to))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscribed(topics: &[Uuid]) -> MemberSpec {
        MemberSpec {
            subscribed: topics.iter().copied().collect(),
            previous: Partitions::new(),
        }
    }

    fn counts(assigned: &BTreeMap<String, Partitions>) -> Vec<usize> {
        assigned
            .values()
            .map(|partitions| partitions.values().map(BTreeSet::len).sum())
            .collect()
    }

    fn every_partition_once(assigned: &BTreeMap<String, Partitions>, topics: &BTreeMap<Uuid, i32>) {
        let mut seen = BTreeSet::new();

        for partitions in assigned.values() {
            for (topic_id, partitions) in partitions {
                for partition in partitions {
                    assert!(seen.insert((*topic_id, *partition)), "assigned twice");
                }
            }
        }

        let expected = topics.values().map(|count| *count as usize).sum::<usize>();
        assert_eq!(expected, seen.len());
    }

    #[test]
    fn range_splits_each_topic_into_contiguous_ranges() {
        let t = Uuid::from_u128(1);
        let topics = BTreeMap::from([(t, 5)]);

        let members = BTreeMap::from([
            ("a".to_owned(), subscribed(&[t])),
            ("b".to_owned(), subscribed(&[t])),
        ]);

        let assigned = assign(RANGE, &members, &topics);

        assert_eq!(Some(&BTreeSet::from([0, 1, 2])), assigned["a"].get(&t));
        assert_eq!(Some(&BTreeSet::from([3, 4])), assigned["b"].get(&t));
    }

    #[test]
    fn uniform_balances_and_assigns_every_partition_once() {
        let t = Uuid::from_u128(1);
        let u = Uuid::from_u128(2);
        let topics = BTreeMap::from([(t, 7), (u, 3)]);

        let members = BTreeMap::from([
            ("a".to_owned(), subscribed(&[t, u])),
            ("b".to_owned(), subscribed(&[t, u])),
            ("c".to_owned(), subscribed(&[t, u])),
        ]);

        let assigned = assign(UNIFORM, &members, &topics);

        every_partition_once(&assigned, &topics);

        let counts = counts(&assigned);
        let (min, max) = (counts.iter().min(), counts.iter().max());
        assert!(
            max.zip(min).is_some_and(|(max, min)| max - min <= 1),
            "{counts:?}"
        );
    }

    #[test]
    fn uniform_keeps_what_a_member_already_had() {
        let t = Uuid::from_u128(1);
        let topics = BTreeMap::from([(t, 4)]);

        let first = assign(
            UNIFORM,
            &BTreeMap::from([
                ("a".to_owned(), subscribed(&[t])),
                ("b".to_owned(), subscribed(&[t])),
            ]),
            &topics,
        );

        let with_previous = |member_id: &str| MemberSpec {
            subscribed: BTreeSet::from([t]),
            previous: first[member_id].clone(),
        };

        let second = assign(
            UNIFORM,
            &BTreeMap::from([
                ("a".to_owned(), with_previous("a")),
                ("b".to_owned(), with_previous("b")),
                ("c".to_owned(), subscribed(&[t])),
            ]),
            &topics,
        );

        every_partition_once(&second, &topics);

        for member_id in ["a", "b"] {
            let kept = second[member_id]
                .get(&t)
                .map(|partitions| partitions.is_subset(&first[member_id][&t]))
                .unwrap_or(true);
            assert!(kept, "{member_id} was given a partition it did not have");
        }

        assert_eq!(
            1,
            counts(&BTreeMap::from([("c".to_owned(), second["c"].clone())]))[0]
        );
    }

    #[test]
    fn uniform_only_hands_a_topic_to_its_subscribers() {
        let t = Uuid::from_u128(1);
        let u = Uuid::from_u128(2);
        let topics = BTreeMap::from([(t, 2), (u, 4)]);

        let members = BTreeMap::from([
            ("a".to_owned(), subscribed(&[t])),
            ("b".to_owned(), subscribed(&[u])),
        ]);

        let assigned = assign(UNIFORM, &members, &topics);

        every_partition_once(&assigned, &topics);
        assert_eq!(None, assigned["a"].get(&u));
        assert_eq!(None, assigned["b"].get(&t));
    }
}
