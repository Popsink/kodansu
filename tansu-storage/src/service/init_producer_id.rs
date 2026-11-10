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

//! `InitProducerId`: a producer id and epoch for an idempotent or
//! transactional producer.

use tansu_sans_io::{InitProducerIdRequest, InitProducerIdResponse};

storage_service! {
    /// Allocates or bumps the producer through
    /// [`Storage::init_producer`](crate::Storage::init_producer) and returns
    /// what it decided, error code included.
    ///
    /// `tests/init_producer_id.rs` allocates two producers without a
    /// transaction and checks they get consecutive ids.
    InitProducerIdService: InitProducerIdRequest => InitProducerIdResponse,
    |ctx, req| {
        let InitProducerIdRequest {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            ..
        } = req;
        let transactional_id = transactional_id.as_deref();
        let producer = ctx
            .state()
            .init_producer(transactional_id, transaction_timeout_ms, producer_id, producer_epoch)
            .await?;
        Ok(InitProducerIdResponse::default()
            .error_code(producer.error.into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch))
    }
}
