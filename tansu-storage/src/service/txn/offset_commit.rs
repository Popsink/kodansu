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

//! `TxnOffsetCommit`: stages a group's offsets inside a transaction.

use tansu_sans_io::{TxnOffsetCommitRequest, TxnOffsetCommitResponse};

storage_service! {
    /// Passes the commit to [`Storage::txn_offset_commit`](crate::Storage::txn_offset_commit),
    /// which holds the offsets until the transaction ends, and returns its
    /// per-partition results.
    OffsetCommitService: TxnOffsetCommitRequest => TxnOffsetCommitResponse,
    |ctx, req| {
        let TxnOffsetCommitRequest {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics,
            ..
        } = req;
        let commit = crate::TxnOffsetCommitRequest {
            transaction_id: transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics: topics.unwrap_or_default(),
        };
        let staged = ctx.state().txn_offset_commit(commit).await?;
        Ok(TxnOffsetCommitResponse::default().topics(Some(staged)))
    }
}
