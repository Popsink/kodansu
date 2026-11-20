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

//! `AddPartitionsToTxn`: enlists partitions in a producer's transaction.

use tansu_sans_io::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse};

storage_service! {
    /// Enlists the partitions through [`Storage::txn_add_partitions`](crate::Storage::txn_add_partitions).
    ///
    /// Version 4 reshaped the API: a client up to v3 sends one transaction and
    /// reads results by topic, a later one may batch several transactions and
    /// reads results by transaction. The engine answers in the shape it was
    /// asked in, with the field for the other shape empty.
    AddPartitionService: AddPartitionsToTxnRequest => AddPartitionsToTxnResponse,
    |ctx, req| {
        ctx.state().txn_add_partitions(req).await
    }
}
