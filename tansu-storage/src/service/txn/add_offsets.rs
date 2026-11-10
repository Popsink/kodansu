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

//! `AddOffsetsToTxn`: brings a consumer group's offsets into a transaction.

use tansu_sans_io::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse};

storage_service! {
    /// Registers the group with the producer's open transaction through
    /// [`Storage::txn_add_offsets`](crate::Storage::txn_add_offsets), and
    /// reports its error code.
    AddOffsetsService: AddOffsetsToTxnRequest => AddOffsetsToTxnResponse,
    |ctx, req| {
        let AddOffsetsToTxnRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            ..
        } = req;
        let outcome = ctx
            .state()
            .txn_add_offsets(&transactional_id, producer_id, producer_epoch, &group_id)
            .await?;
        Ok(AddOffsetsToTxnResponse::default().error_code(outcome.into()))
    }
}
