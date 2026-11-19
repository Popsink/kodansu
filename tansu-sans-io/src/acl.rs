// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use serde::Deserialize;
use serde::Serialize;

/// What an ACL does with the operation it names: grant it or refuse it.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum Permission {
    /// A wire value this enum has no variant for.
    #[default]
    Unknown = 0,

    /// Only in a filter: either permission.
    Any = 1,

    /// The operation is refused.
    Deny = 2,

    /// The operation is granted.
    Allow = 3,
}

impl From<i8> for Permission {
    fn from(value: i8) -> Self {
        let known = [Self::Any, Self::Deny, Self::Allow];
        known
            .into_iter()
            .find(|each| *each as i8 == value)
            .unwrap_or_default()
    }
}

/// The operation an ACL grants or refuses.
///
/// Granting some operations grants others with them:
///
/// - [`Operation::All`] covers every operation, granted or refused;
/// - [`Operation::Read`], [`Operation::Write`], [`Operation::Delete`] and
///   [`Operation::Alter`] each also grant [`Operation::Describe`];
/// - [`Operation::AlterConfigs`] also grants [`Operation::DescribeConfigs`].
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum Operation {
    #[default]
    Unknown = 0,

    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = 14,
    TwoPhaseCommit = 15,
}

impl From<i8> for Operation {
    fn from(value: i8) -> Self {
        let known = [
            Self::Any,
            Self::All,
            Self::Read,
            Self::Write,
            Self::Create,
            Self::Delete,
            Self::Alter,
            Self::Describe,
            Self::ClusterAction,
            Self::DescribeConfigs,
            Self::AlterConfigs,
            Self::IdempotentWrite,
            Self::CreateTokens,
            Self::DescribeTokens,
            Self::TwoPhaseCommit,
        ];
        known
            .into_iter()
            .find(|each| *each as i8 == value)
            .unwrap_or_default()
    }
}

/// The kind of thing an ACL is written on.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum Resource {
    /// A wire value this enum has no variant for.
    #[default]
    Unknown = 0,

    /// Only in a filter: any kind of resource.
    Any = 1,

    /// A topic, by name.
    Topic = 2,

    /// A consumer group, by id.
    Group = 3,

    /// The cluster itself.
    Cluster = 4,

    /// A producer's transactional id.
    TransactionalId = 5,

    /// A delegation token, by id.
    DelegationToken = 6,

    /// A user principal.
    User = 7,
}

impl From<i8> for Resource {
    fn from(value: i8) -> Self {
        let known = [
            Self::Any,
            Self::Topic,
            Self::Group,
            Self::Cluster,
            Self::TransactionalId,
            Self::DelegationToken,
            Self::User,
        ];
        known
            .into_iter()
            .find(|each| *each as i8 == value)
            .unwrap_or_default()
    }
}

/// The wire representation. `as i8` on the discriminant rather than a match,
/// because `#[repr(i8)]` is what makes that the definition rather than a
/// parallel table to keep in step with it.
impl From<Permission> for i8 {
    fn from(value: Permission) -> Self {
        value as i8
    }
}

impl From<Operation> for i8 {
    fn from(value: Operation) -> Self {
        value as i8
    }
}

impl From<Resource> for i8 {
    fn from(value: Resource) -> Self {
        value as i8
    }
}
