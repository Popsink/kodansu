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

//! `DescribeClientQuotas`, the other half of the API #384 routed.
//!
//! What `kafka-configs.sh --describe` reads back, and the only way an operator
//! confirms that the limit they applied is the limit the fleet is enforcing.

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, DescribeClientQuotasRequest, DescribeClientQuotasResponse, ErrorCode,
    describe_client_quotas_response::{EntityData, EntryData, ValueData},
};

use tansu_sans_io::acl::Operation;

use crate::{
    Error, QuotaEntity, QuotaFilterComponent, QuotaMatch, Storage, USER_ENTITY, authorized_cluster,
};

/// KIP-546's match types, as the wire spells them.
const MATCH_EXACT: i8 = 0;
const MATCH_DEFAULT: i8 = 1;
const MATCH_ANY: i8 = 2;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeClientQuotasService;

impl ApiKey for DescribeClientQuotasService {
    const KEY: i16 = DescribeClientQuotasRequest::KEY;
}

/// The entity a described quota is answered with.
fn entity_of(entity: &QuotaEntity) -> Vec<EntityData> {
    vec![
        EntityData::default()
            .entity_type(USER_ENTITY.into())
            .entity_name(entity.name().map(Into::into)),
    ]
}

impl<G> Service<G, DescribeClientQuotasRequest> for DescribeClientQuotasService
where
    G: Storage,
{
    type Response = DescribeClientQuotasResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeClientQuotasRequest,
    ) -> Result<Self::Response, Self::Error> {
        // `DESCRIBE_CONFIGS` on the cluster, as Kafka requires: on a mutualised
        // fleet one tenant's limits are not another tenant's business.
        if !authorized_cluster(&ctx, Operation::DescribeConfigs).await {
            return Ok(DescribeClientQuotasResponse::default()
                .throttle_time_ms(0)
                .error_code(ErrorCode::ClusterAuthorizationFailed.into())
                .error_message(None)
                .entries(None));
        }

        let components = req
            .components
            .unwrap_or_default()
            .into_iter()
            .map(|component| QuotaFilterComponent {
                entity_type: component.entity_type,
                matches: match component.match_type {
                    // Absent when the match type says the name is unused, and
                    // an exact match on nothing selects nothing rather than
                    // everything.
                    MATCH_EXACT => QuotaMatch::Exact(component.r#match.unwrap_or_default()),
                    MATCH_DEFAULT => QuotaMatch::Default,
                    MATCH_ANY => QuotaMatch::Any,

                    // A match type from a later KIP than this broker reads.
                    // Selecting nothing is the answer that cannot report
                    // another tenant's quotas by accident.
                    unknown => {
                        tracing::warn!(
                            unknown,
                            "an unknown quota filter match type selects nothing"
                        );
                        QuotaMatch::Exact(String::new())
                    }
                },
            })
            .collect::<Vec<_>>();

        ctx.state()
            .describe_client_quotas(&components[..], req.strict)
            .await
            .map(|described| {
                DescribeClientQuotasResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::None.into())
                    .error_message(None)
                    .entries(Some(
                        described
                            .into_iter()
                            .map(|(entity, limits)| {
                                EntryData::default()
                                    .entity(Some(entity_of(&entity)))
                                    .values(Some(
                                        limits
                                            .configured()
                                            .into_iter()
                                            .map(|(key, value)| {
                                                ValueData::default().key(key.into()).value(value)
                                            })
                                            .collect(),
                                    ))
                            })
                            .collect(),
                    ))
            })
            .or_else(|error| {
                tracing::error!(?error, "could not describe client quotas");

                Ok(DescribeClientQuotasResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::UnknownServerError.into())
                    .error_message(Some("could not describe client quotas".into()))
                    .entries(None))
            })
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use object_store::memory::InMemory;
    use tansu_sans_io::describe_client_quotas_request::ComponentData;

    use super::*;
    use crate::{PRODUCER_BYTE_RATE, QuotaAlteration, QuotaOp, Result, dynostore::DynoStore};

    async fn storage() -> Result<DynoStore> {
        let storage = DynoStore::new("tansu", 111, InMemory::new());

        _ = storage
            .alter_client_quotas(
                &[
                    QuotaAlteration {
                        entity: QuotaEntity::User("alice".into()),
                        ops: vec![QuotaOp {
                            key: PRODUCER_BYTE_RATE.into(),
                            value: 1024.0,
                            remove: false,
                        }],
                    },
                    QuotaAlteration {
                        entity: QuotaEntity::Default,
                        ops: vec![QuotaOp {
                            key: PRODUCER_BYTE_RATE.into(),
                            value: 512.0,
                            remove: false,
                        }],
                    },
                ],
                false,
            )
            .await?;

        Ok(storage)
    }

    async fn describe(components: Vec<ComponentData>) -> Result<Vec<(Option<String>, Vec<f64>)>> {
        DescribeClientQuotasService
            .serve(
                Context::with_state(storage().await?),
                DescribeClientQuotasRequest::default()
                    .components(Some(components))
                    .strict(false),
            )
            .await
            .map(|response| {
                response
                    .entries
                    .unwrap_or_default()
                    .into_iter()
                    .map(|entry| {
                        (
                            entry
                                .entity
                                .unwrap_or_default()
                                .first()
                                .and_then(|entity| entity.entity_name.clone()),
                            entry
                                .values
                                .unwrap_or_default()
                                .into_iter()
                                .map(|value| value.value)
                                .collect(),
                        )
                    })
                    .collect()
            })
    }

    fn component(match_type: i8, name: Option<&str>) -> ComponentData {
        ComponentData::default()
            .entity_type(USER_ENTITY.into())
            .match_type(match_type)
            .r#match(name.map(Into::into))
    }

    /// `kafka-configs.sh --describe --entity-type users --entity-name alice`.
    #[tokio::test]
    async fn a_named_entity_is_described_with_what_was_written_against_it() -> Result<()> {
        assert_eq!(
            vec![(Some("alice".to_owned()), vec![1024.0])],
            describe(vec![component(MATCH_EXACT, Some("alice"))]).await?,
        );

        Ok(())
    }

    /// `--entity-default`. The default entity is answered with a null name,
    /// which is how the wire says "the default".
    #[tokio::test]
    async fn the_default_entity_is_described_with_no_name() -> Result<()> {
        assert_eq!(
            vec![(None, vec![512.0])],
            describe(vec![component(MATCH_DEFAULT, None)]).await?,
        );

        Ok(())
    }

    /// A describe with no components at all is `--entity-type users` with
    /// nothing narrowing it: everything, including the default entity.
    #[tokio::test]
    async fn an_empty_filter_describes_everything() -> Result<()> {
        assert_eq!(2, describe(vec![]).await?.len());

        Ok(())
    }

    /// What a principal *would* be limited to is not what is described: a
    /// describe of `bob`, who nothing names, is empty even though the cluster
    /// default applies to it. Reporting the default under `bob` would show a
    /// limit no `--alter` wrote and no `--delete-config` can remove.
    #[tokio::test]
    async fn a_principal_with_no_quota_of_its_own_describes_empty() -> Result<()> {
        assert_eq!(
            Vec::<(Option<String>, Vec<f64>)>::new(),
            describe(vec![component(MATCH_EXACT, Some("bob"))]).await?,
        );

        Ok(())
    }
}
