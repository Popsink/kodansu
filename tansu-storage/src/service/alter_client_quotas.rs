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

//! `AlterClientQuotas`, whose codec existed and whose service did not (#384).
//!
//! The same gap ACLs had before #363 and SCRAM had before #381: the message was
//! generated from the Kafka descriptors and routed by nobody, so
//! `kafka-configs.sh --alter --add-config producer_byte_rate=…` reached a
//! broker that did not implement the API at all. Closing it the same way means
//! every operator tool configures a quota with no bespoke tooling.

use rama::{Context, Service};
use tansu_sans_io::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, ApiKey, ErrorCode,
    alter_client_quotas_request::{EntityData, EntryData},
    alter_client_quotas_response::{EntityData as EntityResult, EntryData as EntryResult},
};

use tansu_sans_io::acl::Operation;

use crate::{
    Error, QuotaAlteration, QuotaEntity, QuotaOp, Storage, USER_ENTITY, authorized_cluster,
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlterClientQuotasService;

impl ApiKey for AlterClientQuotasService {
    const KEY: i16 = AlterClientQuotasRequest::KEY;
}

/// One request entry, either understood or refused with the reason.
///
/// The refusal has to carry the entity the client sent rather than a parsed
/// one, because the entity is exactly what could not be parsed: a client
/// matches results to entries by their entity, and answering an `ip` filter
/// with an echo of something else tells it nothing.
enum Parsed {
    Understood(QuotaAlteration),
    Refused(ErrorCode, String),
}

/// The entity a request entry names, as something storable.
///
/// KIP-546 entities are a list of `(type, name)` pairs so that a quota can be
/// written against a *combination* — `user` and `client-id` together. This
/// broker has one dimension of isolation, the principal (#363), so a
/// combination is refused rather than being silently narrowed to its `user`
/// half: an operator who wrote a quota for one client id of one user and got a
/// quota for *every* client id of that user has been given something stricter
/// than they asked for, and told it succeeded.
fn entity_of(entity: &[EntityData]) -> Result<QuotaEntity, String> {
    match entity {
        [single] if single.entity_type == USER_ENTITY => Ok(single
            .entity_name
            .clone()
            .map_or(QuotaEntity::Default, QuotaEntity::User)),

        [single] => Err(format!(
            "this broker writes quotas against the {USER_ENTITY} entity, not {:?}",
            single.entity_type
        )),

        [] => Err(String::from("a quota entry names no entity")),

        _ => Err(String::from(
            "this broker writes quotas against one entity type at a time",
        )),
    }
}

fn parse(entry: &EntryData) -> Parsed {
    let entity = match entity_of(entry.entity.as_deref().unwrap_or_default()) {
        Ok(entity) => entity,
        Err(message) => return Parsed::Refused(ErrorCode::InvalidRequest, message),
    };

    Parsed::Understood(QuotaAlteration {
        entity,
        ops: entry
            .ops
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|op| QuotaOp {
                key: op.key.clone(),
                value: op.value,
                remove: op.remove,
            })
            .collect(),
    })
}

/// Echo an entry's outcome, carrying back the entity the client sent.
///
/// The request and response spell an entity with two different generated
/// types, so it is copied across rather than moved. Echoing it at all is what
/// lets a client match a result to the entry it sent, which matters most for
/// the entries this broker refused.
fn result(entry: &EntryData, error_code: ErrorCode, error_message: Option<String>) -> EntryResult {
    EntryResult::default()
        .error_code(error_code.into())
        .error_message(error_message)
        .entity(entry.entity.as_ref().map(|entity| {
            entity
                .iter()
                .map(|entity| {
                    EntityResult::default()
                        .entity_type(entity.entity_type.clone())
                        .entity_name(entity.entity_name.clone())
                })
                .collect()
        }))
}

impl<G> Service<G, AlterClientQuotasRequest> for AlterClientQuotasService
where
    G: Storage,
{
    type Response = AlterClientQuotasResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: AlterClientQuotasRequest,
    ) -> Result<Self::Response, Self::Error> {
        let entries = req.entries.unwrap_or_default();

        // `ALTER_CONFIGS` on the cluster, as Kafka requires of this API. A
        // principal that can raise its own quota has no quota, so this is the
        // same hole `CreateAcls` would have been: enforcing everything else
        // while leaving the configuration open enforces nothing.
        if !authorized_cluster(&ctx, Operation::AlterConfigs).await {
            return Ok(AlterClientQuotasResponse::default()
                .throttle_time_ms(0)
                .entries(Some(
                    entries
                        .iter()
                        .map(|entry| result(entry, ErrorCode::ClusterAuthorizationFailed, None))
                        .collect(),
                )));
        }

        let parsed = entries.iter().map(parse).collect::<Vec<_>>();

        // Only the entries this broker understood reach the storage, and their
        // outcomes are woven back into the request's order — a client matches
        // results to entries positionally as well as by entity, so a short list
        // attributes a failure to the wrong entry.
        let understood = parsed
            .iter()
            .filter_map(|parsed| match parsed {
                Parsed::Understood(alteration) => Some(alteration.clone()),
                Parsed::Refused(..) => None,
            })
            .collect::<Vec<_>>();

        let outcomes = if understood.is_empty() {
            vec![]
        } else {
            match ctx
                .state()
                .alter_client_quotas(&understood[..], req.validate_only)
                .await
            {
                Ok(outcomes) => outcomes,

                Err(error) => {
                    tracing::error!(?error, "could not alter client quotas");
                    vec![ErrorCode::UnknownServerError; understood.len()]
                }
            }
        };

        let mut outcomes = outcomes.into_iter();

        Ok(AlterClientQuotasResponse::default()
            .throttle_time_ms(0)
            .entries(Some(
                entries
                    .iter()
                    .zip(parsed.iter())
                    .map(|(entry, parsed)| match parsed {
                        Parsed::Refused(error_code, message) => {
                            result(entry, *error_code, Some(message.clone()))
                        }

                        Parsed::Understood(alteration) => {
                            let error_code =
                                outcomes.next().unwrap_or(ErrorCode::UnknownServerError);

                            result(
                                entry,
                                error_code,
                                (error_code != ErrorCode::None).then(|| {
                                    format!(
                                        "could not alter the quotas of {:?}",
                                        alteration.entity.name().unwrap_or("<default>")
                                    )
                                }),
                            )
                        }
                    })
                    .collect(),
            )))
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use object_store::memory::InMemory;
    use tansu_sans_io::alter_client_quotas_request::OpData;

    use super::*;
    use crate::{PRODUCER_BYTE_RATE, Quotas, Result, dynostore::DynoStore};

    fn storage() -> DynoStore {
        DynoStore::new("tansu", 111, InMemory::new())
    }

    fn entry(entity_type: &str, entity_name: Option<&str>, key: &str, value: f64) -> EntryData {
        EntryData::default()
            .entity(Some(vec![
                EntityData::default()
                    .entity_type(entity_type.into())
                    .entity_name(entity_name.map(Into::into)),
            ]))
            .ops(Some(vec![
                OpData::default().key(key.into()).value(value).remove(false),
            ]))
    }

    async fn alter(storage: &DynoStore, entries: Vec<EntryData>) -> Result<Vec<i16>> {
        AlterClientQuotasService
            .serve(
                Context::with_state(storage.clone()),
                AlterClientQuotasRequest::default()
                    .entries(Some(entries))
                    .validate_only(false),
            )
            .await
            .map(|response| {
                response
                    .entries
                    .unwrap_or_default()
                    .into_iter()
                    .map(|entry| entry.error_code)
                    .collect()
            })
    }

    /// What `kafka-configs.sh --entity-type users --entity-name alice --alter
    /// --add-config producer_byte_rate=1024` sends, end to end.
    #[tokio::test]
    async fn a_quota_written_by_kafka_configs_is_stored_and_read_back() -> Result<()> {
        let storage = storage();

        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            alter(
                &storage,
                vec![entry("user", Some("alice"), PRODUCER_BYTE_RATE, 1024.0)],
            )
            .await?,
        );

        assert_eq!(
            Some(1024.0),
            storage
                .client_quotas()
                .await?
                .for_principal("User:alice")
                .producer_byte_rate,
            "the quota must be found under the principal the request path carries",
        );

        Ok(())
    }

    /// An entity type this broker does not write is refused, rather than
    /// accepted and dropped. `kafka-configs.sh --entity-type clients` is a
    /// thing an operator will try, and being told it worked is the failure
    /// mode #363 and #381 both shipped once.
    #[tokio::test]
    async fn an_entity_this_broker_does_not_write_is_refused() -> Result<()> {
        let storage = storage();

        assert_eq!(
            vec![i16::from(ErrorCode::InvalidRequest)],
            alter(
                &storage,
                vec![entry(
                    "client-id",
                    Some("consumer-1"),
                    PRODUCER_BYTE_RATE,
                    1.0
                )],
            )
            .await?,
        );

        assert_eq!(Quotas::default(), storage.client_quotas().await?);

        Ok(())
    }

    #[tokio::test]
    async fn a_key_this_broker_does_not_enforce_is_refused() -> Result<()> {
        let storage = storage();

        assert_eq!(
            vec![i16::from(ErrorCode::InvalidConfig)],
            alter(
                &storage,
                vec![entry("user", Some("alice"), "request_percentage", 200.0)],
            )
            .await?,
        );

        Ok(())
    }

    /// A refused entry must not shift the outcomes of the entries around it: a
    /// client reads results positionally, and a short list attributes one
    /// entry's failure to another.
    #[tokio::test]
    async fn a_refused_entry_keeps_its_place_in_the_results() -> Result<()> {
        let storage = storage();

        assert_eq!(
            vec![
                i16::from(ErrorCode::None),
                i16::from(ErrorCode::InvalidRequest),
                i16::from(ErrorCode::None),
            ],
            alter(
                &storage,
                vec![
                    entry("user", Some("alice"), PRODUCER_BYTE_RATE, 1.0),
                    entry("ip", Some("10.0.0.1"), PRODUCER_BYTE_RATE, 2.0),
                    entry("user", None, PRODUCER_BYTE_RATE, 3.0),
                ],
            )
            .await?,
        );

        let quotas = storage.client_quotas().await?;
        assert_eq!(Some(1.0), quotas.for_user("alice").producer_byte_rate);
        assert_eq!(Some(3.0), quotas.default.producer_byte_rate);

        Ok(())
    }

    /// `--validate-only` says whether it would work and writes nothing.
    #[tokio::test]
    async fn validate_only_writes_nothing() -> Result<()> {
        let storage = storage();

        let response = AlterClientQuotasService
            .serve(
                Context::with_state(storage.clone()),
                AlterClientQuotasRequest::default()
                    .entries(Some(vec![entry(
                        "user",
                        Some("alice"),
                        PRODUCER_BYTE_RATE,
                        1024.0,
                    )]))
                    .validate_only(true),
            )
            .await?;

        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            response
                .entries
                .unwrap_or_default()
                .into_iter()
                .map(|entry| entry.error_code)
                .collect::<Vec<_>>(),
        );

        assert_eq!(Quotas::default(), storage.client_quotas().await?);

        Ok(())
    }
}
