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
//! reports". Nothing is *enforced* yet — that is the next slice — so what these
//! pin is the half that has to exist first and be right.

use std::sync::Arc;

use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    CreateAclsRequest, DeleteAclsRequest, DescribeAclsRequest, ErrorCode,
    acl::{Operation, Permission, Resource},
    create_acls_request::AclCreation,
    delete_acls_request::DeleteAclsFilter,
    resource::Pattern,
};
use tansu_storage::{
    CreateAclsService, DeleteAclsService, DescribeAclsService, Error, Storage, StorageContainer,
    WILDCARD_HOST,
};
use url::Url;

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
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
    storage: &Arc<Box<dyn Storage>>,
    creations: Vec<AclCreation>,
) -> Result<Vec<i16>, Error> {
    let service = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage.clone()
    })
    .into_layer(CreateAclsService);

    service
        .serve(
            Context::default(),
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
    storage: &Arc<Box<dyn Storage>>,
    request: DescribeAclsRequest,
) -> Result<Vec<(String, String)>, Error> {
    let service = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage.clone()
    })
    .into_layer(DescribeAclsService);

    let response = service.serve(Context::default(), request).await?;

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

    let service = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage.clone()
    })
    .into_layer(DeleteAclsService);

    let response = service
        .serve(
            Context::default(),
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
