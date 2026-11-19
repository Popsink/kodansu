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

//! `DeleteRecords`: moves each partition's log start offset forward.

use tansu_sans_io::{DeleteRecordsRequest, DeleteRecordsResponse};

storage_service! {
    /// Hands the requested topics to [`Storage::delete_records`](crate::Storage::delete_records)
    /// and returns its per-partition results as they are.
    DeleteRecordsService: DeleteRecordsRequest => DeleteRecordsResponse,
    |ctx, req| {
        let requested = req.topics.unwrap_or_default();
        let deleted = ctx.state().delete_records(&requested).await?;
        Ok(DeleteRecordsResponse::default().topics(Some(deleted)))
    }
}
