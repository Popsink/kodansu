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

use tansu_sans_io::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ErrorCode};

use crate::{TxnAddPartitionsRequest, TxnAddPartitionsResponse};

storage_service! {
    /// Enlists the partitions through [`Storage::txn_add_partitions`](crate::Storage::txn_add_partitions).
    ///
    /// Version 4 reshaped the API: a client up to v3 sends one transaction and
    /// reads results by topic, a later one may batch several transactions and
    /// reads results by transaction. The storage answer says which it was, and
    /// the field for the other shape goes back empty.
    AddPartitionService: AddPartitionsToTxnRequest => AddPartitionsToTxnResponse,
    |ctx, req| {
        let enlisted = ctx
            .state()
            .txn_add_partitions(TxnAddPartitionsRequest::try_from(req)?)
            .await?;
        let (by_transaction, by_topic) = match enlisted {
            TxnAddPartitionsResponse::VersionZeroToThree(by_topic) => (Vec::new(), by_topic),
            TxnAddPartitionsResponse::VersionFourPlus(by_transaction) => {
                (by_transaction, Vec::new())
            }
        };
        let response = AddPartitionsToTxnResponse::default()
            .error_code(Some(i16::from(ErrorCode::None)))
            .results_by_transaction(Some(by_transaction))
            .results_by_topic_v_3_and_below(Some(by_topic));
        Ok(response)
    }
}
