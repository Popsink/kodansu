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

//! `CreateAcls`, `DescribeAcls` and `DeleteAcls`, through the services a client
//! reaches (#363).
//!
//! All three used to be theatre: create and describe answered success without
//! touching anything, and delete was not routed at all. `kafka-acls.sh`
//! appeared to work — an operator applied ACLs and was told they took effect.
//!
//! This drives them the way `kafka-acls.sh` does, over the same request and
//! response types, because the property that broke is not "the storage layer
//! stores things" but "what an operator applies is what a later describe
//! reports".
//!
//! Every round trip here runs with an `Authorizer` in the context, because a
//! broker without one answers all three `SECURITY_DISABLED` and there is no
//! round trip to make (#578).

use std::sync::Arc;

use rama::{Context, Service as _};
use tansu_sans_io::{
    CreateAclsRequest, DeleteAclsRequest, DescribeAclsRequest, ErrorCode,
    acl::{Operation, Permission, Resource},
    create_acls_request::AclCreation,
    delete_acls_request::DeleteAclsFilter,
    resource::Pattern,
};
use tansu_storage::{
    AclBinding, AclFilter, Authorizer, CreateAclsService, DeleteAclsService, DescribeAclsService,
    Error, NO_AUTHORIZER, NO_AUTHORIZER_DESCRIBE, Requester, Storage, StorageContainer,
    WILDCARD_HOST,
};
use url::Url;

/// A super user, so `kafka-acls.sh` gets past the cluster `ALTER` every one of
/// these APIs requires — the escape that lets the first rule be written into a
/// cluster that has none (#363).
const ADMIN: &str = "User:admin";
const HOST: &str = "10.0.0.1";

async fn storage() -> Result<Arc<dyn Storage>, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
}

/// The context a broker with `--authentication` builds for an administrator.
fn asking(storage: &Arc<dyn Storage>) -> Context<Arc<dyn Storage>> {
    let mut ctx = Context::default();

    _ = ctx.insert(Requester {
        principal: Some(ADMIN.to_owned()),
        host: HOST.into(),
    });

    _ = ctx.insert(Authorizer::new(storage.clone(), [ADMIN.to_owned()]));

    ctx.map_state(|()| storage.clone())
}

/// And the one a broker without it builds: no authorizer anywhere.
fn unauthenticated(storage: &Arc<dyn Storage>) -> Context<Arc<dyn Storage>> {
    Context::default().map_state(|()| storage.clone())
}

fn creation(resource_name: &str, pattern: Pattern, principal: &str) -> AclCreation {
    AclCreation::default()
        .resource_type(Resource::Topic.into())
        .resource_name(resource_name.into())
        .resource_pattern_type(Some(pattern.into()))
        .principal(principal.into())
        .host(WILDCARD_HOST.into())
        .operation(Operation::Read.into())
        .permission_type(Permission::Allow.into())
}

/// Everything, in the spelling `kafka-acls.sh --list` uses with no narrowing
/// flags.
fn describe_everything() -> DescribeAclsRequest {
    DescribeAclsRequest::default()
        .resource_type_filter(Resource::Any.into())
        .resource_name_filter(None)
        .pattern_type_filter(Some(Pattern::Any.into()))
        .principal_filter(None)
        .host_filter(None)
        .operation(Operation::Any.into())
        .permission_type(Permission::Any.into())
}

async fn create(
    storage: &Arc<dyn Storage>,
    creations: Vec<AclCreation>,
) -> Result<Vec<i16>, Error> {
    CreateAclsService
        .serve(
            asking(storage),
            CreateAclsRequest::default().creations(Some(creations)),
        )
        .await
        .map(|response| {
            response
                .results
                .unwrap_or_default()
                .into_iter()
                .map(|result| result.error_code)
                .collect()
        })
}

/// Every rule a filter selects, flattened out of the per-resource grouping the
/// response uses, as `(resource_name, principal)`.
async fn describe(
    storage: &Arc<dyn Storage>,
    request: DescribeAclsRequest,
) -> Result<Vec<(String, String)>, Error> {
    let response = DescribeAclsService.serve(asking(storage), request).await?;

    assert_eq!(i16::from(ErrorCode::None), response.error_code);

    let mut found = response
        .resources
        .unwrap_or_default()
        .into_iter()
        .flat_map(|resource| {
            resource
                .acls
                .unwrap_or_default()
                .into_iter()
                .map(move |acl| (resource.resource_name.clone(), acl.principal))
        })
        .collect::<Vec<_>>();

    found.sort();

    Ok(found)
}

/// What an operator applies is what a later describe reports, and it survives
/// the process that applied it.
#[tokio::test]
async fn what_is_applied_is_what_is_described() -> Result<(), Error> {
    let storage = storage().await?;

    assert!(
        describe(&storage, describe_everything()).await?.is_empty(),
        "a cluster with no ACLs has none, rather than an error",
    );

    assert_eq!(
        vec![i16::from(ErrorCode::None); 2],
        create(
            &storage,
            vec![
                creation("tenant-a.", Pattern::Prefixed, "User:alice"),
                creation("shared", Pattern::Literal, "User:bob"),
            ],
        )
        .await?,
    );

    assert_eq!(
        vec![
            ("shared".to_owned(), "User:bob".to_owned()),
            ("tenant-a.".to_owned(), "User:alice".to_owned()),
        ],
        describe(&storage, describe_everything()).await?,
    );

    Ok(())
}

/// Re-applying the same rules is success, not a duplicate and not an error.
///
/// `kafka-acls.sh` is run from configuration management, so the second run of
/// the same file must not start reporting failures — and must not leave two
/// copies of every rule behind.
#[tokio::test]
async fn re_applying_the_same_acls_is_idempotent() -> Result<(), Error> {
    let storage = storage().await?;

    let rules = || {
        vec![
            creation("tenant-a.", Pattern::Prefixed, "User:alice"),
            creation("shared", Pattern::Literal, "User:bob"),
        ]
    };

    _ = create(&storage, rules()).await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None); 2],
        create(&storage, rules()).await?,
        "re-applying must not report failure",
    );

    assert_eq!(
        2,
        describe(&storage, describe_everything()).await?.len(),
        "re-applying must not duplicate",
    );

    Ok(())
}

/// A narrowed describe answers only what it asked about.
#[tokio::test]
async fn a_filter_narrows_what_is_described() -> Result<(), Error> {
    let storage = storage().await?;

    _ = create(
        &storage,
        vec![
            creation("tenant-a.", Pattern::Prefixed, "User:alice"),
            creation("tenant-b.", Pattern::Prefixed, "User:bob"),
        ],
    )
    .await?;

    assert_eq!(
        vec![("tenant-a.".to_owned(), "User:alice".to_owned())],
        describe(
            &storage,
            describe_everything().principal_filter(Some("User:alice".into()))
        )
        .await?,
    );

    Ok(())
}

/// Delete removes exactly what its filter selects, and reports what it removed.
///
/// The reporting is not decoration: an operator reads it to confirm they
/// deleted what they meant to, and a filter that selects more than intended is
/// how a cluster loses its authorization in one command.
#[tokio::test]
async fn delete_removes_what_it_selects_and_says_what_it_removed() -> Result<(), Error> {
    let storage = storage().await?;

    _ = create(
        &storage,
        vec![
            creation("tenant-a.", Pattern::Prefixed, "User:alice"),
            creation("tenant-b.", Pattern::Prefixed, "User:bob"),
        ],
    )
    .await?;

    let response = DeleteAclsService
        .serve(
            asking(&storage),
            DeleteAclsRequest::default().filters(Some(vec![
                DeleteAclsFilter::default()
                    .resource_type_filter(Resource::Any.into())
                    .resource_name_filter(None)
                    .pattern_type_filter(Some(Pattern::Any.into()))
                    .principal_filter(Some("User:alice".into()))
                    .host_filter(None)
                    .operation(Operation::Any.into())
                    .permission_type(Permission::Any.into()),
            ])),
        )
        .await?;

    let results = response.filter_results.unwrap_or_default();
    assert_eq!(1, results.len(), "one result per filter, in request order");
    assert_eq!(i16::from(ErrorCode::None), results[0].error_code);

    let removed = results[0].matching_acls.clone().unwrap_or_default();
    assert_eq!(1, removed.len());
    assert_eq!("tenant-a.", removed[0].resource_name);
    assert_eq!("User:alice", removed[0].principal);

    assert_eq!(
        vec![("tenant-b.".to_owned(), "User:bob".to_owned())],
        describe(&storage, describe_everything()).await?,
        "the rule the filter did not select must survive",
    );

    Ok(())
}

/// A broker with no authorizer refuses to store a rule, rather than storing one
/// nothing will consult.
///
/// This is the shape of the report in #578: an operator ran `create_acls`, was
/// told it succeeded, and read the binding back weeks and several restarts
/// later off a cluster that had never enforced a thing.
#[tokio::test]
async fn create_is_refused_without_an_authorizer() -> Result<(), Error> {
    let storage = storage().await?;

    let response = CreateAclsService
        .serve(
            unauthenticated(&storage),
            CreateAclsRequest::default().creations(Some(vec![
                creation("tenant-a.", Pattern::Prefixed, "User:alice"),
                creation("shared", Pattern::Literal, "User:bob"),
            ])),
        )
        .await?;

    let results = response.results.unwrap_or_default();

    assert_eq!(
        vec![i16::from(ErrorCode::SecurityDisabled); 2],
        results
            .iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
        "one result per creation, in request order",
    );

    assert_eq!(
        vec![Some(NO_AUTHORIZER.to_owned()); 2],
        results
            .iter()
            .map(|result| result.error_message.clone())
            .collect::<Vec<_>>(),
    );

    assert!(
        storage.describe_acls(&AclFilter::any()).await?.is_empty(),
        "a refused creation must not be stored",
    );

    Ok(())
}

/// And refuses to delete one, which matters because the answer would otherwise
/// report rules removed from a cluster they were never protecting.
#[tokio::test]
async fn delete_is_refused_without_an_authorizer() -> Result<(), Error> {
    let storage = storage().await?;

    // Stored underneath the API, the way a broker upgraded from a release that
    // accepted the creation arrives carrying them (#578).
    _ = storage
        .create_acls(&[AclBinding {
            resource_type: Resource::Topic,
            resource_name: "tenant-a.".into(),
            pattern: Pattern::Prefixed,
            principal: "User:alice".into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        }])
        .await?;

    let response = DeleteAclsService
        .serve(
            unauthenticated(&storage),
            DeleteAclsRequest::default().filters(Some(vec![
                DeleteAclsFilter::default()
                    .resource_type_filter(Resource::Any.into())
                    .resource_name_filter(None)
                    .pattern_type_filter(Some(Pattern::Any.into()))
                    .principal_filter(None)
                    .host_filter(None)
                    .operation(Operation::Any.into())
                    .permission_type(Permission::Any.into()),
            ])),
        )
        .await?;

    let results = response.filter_results.unwrap_or_default();
    assert_eq!(1, results.len(), "one result per filter, in request order");
    assert_eq!(
        i16::from(ErrorCode::SecurityDisabled),
        results[0].error_code
    );
    assert_eq!(Some(NO_AUTHORIZER.to_owned()), results[0].error_message);
    assert_eq!(
        Some([].into()),
        results[0].matching_acls,
        "a refusal removed nothing, and must not claim to have",
    );

    assert_eq!(
        1,
        storage.describe_acls(&AclFilter::any()).await?.len(),
        "a refused deletion must not delete",
    );

    Ok(())
}

/// And refuses to read them back, which is the half that makes an audit honest.
///
/// A broker that answers an empty list here tells an auditor "no rules", which
/// on a cluster that cannot have any is the wrong sentence; one carrying
/// bindings written before #578 would answer worse, listing rules nothing
/// enforces.
#[tokio::test]
async fn describe_is_refused_without_an_authorizer() -> Result<(), Error> {
    let storage = storage().await?;

    _ = storage
        .create_acls(&[AclBinding {
            resource_type: Resource::Topic,
            resource_name: "tenant-a.".into(),
            pattern: Pattern::Prefixed,
            principal: "User:alice".into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        }])
        .await?;

    let response = DescribeAclsService
        .serve(unauthenticated(&storage), describe_everything())
        .await?;

    assert_eq!(i16::from(ErrorCode::SecurityDisabled), response.error_code);
    assert_eq!(
        Some(NO_AUTHORIZER_DESCRIBE.to_owned()),
        response.error_message
    );
    assert_eq!(
        Some([].into()),
        response.resources,
        "a stored binding must not be reported by a broker that cannot enforce it",
    );

    Ok(())
}
