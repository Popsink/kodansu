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

//! The transaction document's own reads: what a transaction produced, and
//! which other transactions it overlaps.

use std::collections::BTreeMap;

use tansu_sans_io::ErrorCode;

use super::super::{Meta, Offset, Txn, TxnDetail, TxnId, TxnProduceOffset};
use crate::{Error, Result, Topition, TxnState};

/// An epoch in `state` that produced `ranges` to partitions of topic `t`;
/// a `None` range is a partition added to the transaction but not written.
fn epoch(state: Option<TxnState>, ranges: &[(i32, Option<(i64, i64)>)]) -> TxnDetail {
    let to_range = |(offset_start, offset_end)| TxnProduceOffset {
        offset_start,
        offset_end,
    };
    let partitions = ranges
        .iter()
        .map(|&(partition, range)| (partition, range.map(to_range)))
        .collect();
    TxnDetail {
        state,
        produces: BTreeMap::from([(String::from("t"), partitions)]),
        ..TxnDetail::default()
    }
}

/// A transaction of `producer` with these `epochs`.
fn txn(producer: i64, epochs: Vec<(i16, TxnDetail)>) -> Txn {
    Txn {
        producer,
        epochs: epochs.into_iter().collect(),
    }
}

/// `a` is ours: producer 1, epoch 0, wrote `t-0` over `[10, 20)`.
fn meta() -> Meta {
    let begun = Some(TxnState::Begin);
    let transactions = BTreeMap::from([
        (
            String::from("a"),
            txn(
                1,
                vec![
                    (0, epoch(begun, &[(0, Some((10, 20))), (1, None)])),
                    (1, epoch(begun, &[(0, Some((15, 16)))])),
                ],
            ),
        ),
        (
            String::from("b"),
            txn(
                2,
                vec![
                    (0, epoch(None, &[(0, Some((0, 30)))])),
                    (
                        1,
                        epoch(Some(TxnState::PrepareCommit), &[(0, Some((5, 6)))]),
                    ),
                    (2, epoch(begun, &[(0, Some((7, 8)))])),
                ],
            ),
        ),
        (
            String::from("c"),
            txn(
                3,
                vec![(0, epoch(begun, &[(0, Some((25, 30))), (1, Some((0, 1)))]))],
            ),
        ),
    ]);
    Meta {
        transactions,
        ..Meta::default()
    }
}

#[test]
fn a_transaction_produced_its_written_partitions_only() -> Result<()> {
    let produced = meta().produced("a", 1, 0)?;
    let range = TxnProduceOffset {
        offset_start: 10,
        offset_end: 20,
    };
    assert_eq!(BTreeMap::from([(Topition::new("t", 0), range)]), produced);
    Ok(())
}

#[test]
fn an_unknown_transaction_producer_or_epoch_is_refused() {
    let meta = meta();
    for ((transaction, producer, epoch), expected) in [
        (("z", 1, 0), ErrorCode::TransactionalIdNotFound),
        (("a", 2, 0), ErrorCode::UnknownProducerId),
        (("a", 1, 9), ErrorCode::ProducerFenced),
    ] {
        let refused = meta.produced(transaction, producer, epoch);
        assert!(
            matches!(refused, Err(Error::Api(code)) if code == expected),
            "{transaction} {producer} {epoch}: {refused:?}"
        );
    }
}

/// Each other transaction is reported once, by its first epoch that has a
/// state and starts below where ours ends on a partition we wrote: `b`'s
/// stateless epoch 0 and its later epoch 2 are not, `c` starts past us, and
/// our own other epoch counts as another transaction.
#[test]
fn each_overlapping_transaction_is_reported_by_its_first_clashing_epoch() -> Result<()> {
    let expected = vec![
        TxnId {
            transaction: String::from("a"),
            producer_id: 1,
            producer_epoch: 1,
            state: TxnState::Begin,
        },
        TxnId {
            transaction: String::from("b"),
            producer_id: 2,
            producer_epoch: 1,
            state: TxnState::PrepareCommit,
        },
    ];
    assert_eq!(expected, meta().overlapping_transactions("a", 1, 0)?);
    Ok(())
}

#[test]
fn an_epoch_starts_at_each_written_partition() {
    let detail = epoch(None, &[(0, Some((10, 20))), (1, None), (2, Some((3, 4)))]);
    let expected: BTreeMap<Topition, Offset> =
        BTreeMap::from([(Topition::new("t", 0), 10), (Topition::new("t", 2), 3)]);
    assert_eq!(expected, BTreeMap::from(&detail));
}
