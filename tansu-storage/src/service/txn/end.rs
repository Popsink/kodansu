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

use rama::{Context, Service};
use tansu_sans_io::{ApiKey, EndTxnRequest, EndTxnResponse};
use tracing::{debug, instrument, warn};

use crate::{Error, Result, Storage, storage_error_code};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`EndTxnRequest`]
/// returning [`EndTxnResponse`] — the commit and the abort of a transaction
/// (#441).
///
/// [`Storage::txn_end`] has been implemented and tested since transactions
/// landed; what was missing was the service and the route, so `ApiVersions`
/// advertised `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn` and
/// `TxnOffsetCommit` but not api key 26. A transactional producer therefore
/// engaged fully — init, begin and produce all succeeding — and met the dead
/// end only at `commit_transaction`, with its records already written,
/// permanently invisible to a read-committed consumer, and neither committable
/// nor abortable. That is worse than not supporting transactions at all,
/// because the entry APIs let the client past the point of no return before
/// there was anything to fail on.
///
/// Unlike its siblings this service **folds `Error::Api` into the response**
/// rather than propagating it. [`Storage::txn_add_offsets`] and friends answer
/// a rejection as `Ok(ErrorCode)`; `txn_end` answers it as
/// `Err(Error::Api(code))` — and an `Err` out of a service ends the connection
/// with **no response written**. `PRODUCER_FENCED` is a code a transactional
/// client is required to handle by aborting and re-initialising; handing it a
/// dropped socket instead turns a recoverable fence into a reconnect-and-replay
/// loop (the #219 shape).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EndService;

impl ApiKey for EndService {
    const KEY: i16 = EndTxnRequest::KEY;
}

impl<G> Service<G, EndTxnRequest> for EndService
where
    G: Storage,
{
    type Response = EndTxnResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: EndTxnRequest,
    ) -> Result<Self::Response, Self::Error> {
        let error_code = match ctx
            .state()
            .txn_end(
                req.transactional_id.as_str(),
                req.producer_id,
                req.producer_epoch,
                req.committed,
            )
            .await
        {
            Ok(error_code) => error_code,

            // A rejection the client is entitled to act on: fenced by a newer
            // epoch, an unknown transactional id, a producer id that is not
            // this transaction's. Every one of them is an answer.
            Err(Error::Api(error_code)) => {
                debug!(?req, ?error_code);
                error_code
            }

            // Anything else is the broker failing, not the client being
            // refused. `storage_error_code` is what decides whether the client
            // retries: an object-store fault becomes the retriable
            // `KAFKA_STORAGE_ERROR` rather than the fatal `UNKNOWN_SERVER_ERROR`
            // that would make a producer drop the transaction outright.
            Err(otherwise) => {
                let error_code = storage_error_code(&otherwise);
                warn!(?req, ?otherwise, ?error_code);
                error_code
            }
        };

        Ok(EndTxnResponse::default()
            .throttle_time_ms(0)
            .error_code(error_code.into()))
    }
}
