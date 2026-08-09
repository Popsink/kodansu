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

//! SASL/SCRAM credentials through the storage trait.
//!
//! These exist because all three of these methods were stubs returning
//! `Ok(())` and `Ok(None)` on the only backend that ships. Every test passed:
//! the handshake is covered against a hand-written mock storage in
//! `tansu-broker/tests/auth.rs`, and the `Null` engine correctly refuses. What
//! nothing asserted was that a credential written to the object store could be
//! read back from it — so `tansu user create` reported success, persisted
//! nothing, and `--authentication` produced a broker no client could ever
//! authenticate to.

use bytes::Bytes;
use object_store::memory::InMemory;

use tansu_sans_io::ScramMechanism;

use crate::{
    Result, ScramCredential, Storage,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

fn credential(salt: &'static [u8]) -> ScramCredential {
    ScramCredential {
        salt: Bytes::from_static(salt),
        iterations: 8_192,
        stored_key: Bytes::from_static(b"stored"),
        server_key: Bytes::from_static(b"server"),
    }
}

/// The one that was missing: what is written is what comes back. Without it the
/// handshake has nothing to check a client's proof against, and every principal
/// is `unknown-user`.
#[tokio::test]
async fn a_credential_round_trips() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let alice = credential(b"alice-salt");

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, alice.clone())
        .await?;

    assert_eq!(
        Some(alice),
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await?
    );

    Ok(())
}

/// A principal nobody has written a credential for is `None` — the answer the
/// handshake turns into `unknown-user`. It must not be an error, or a cluster
/// with no users would fail handshakes as though the store were down.
#[tokio::test]
async fn an_unknown_principal_is_none() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert_eq!(
        None,
        storage
            .user_scram_credential("nobody", ScramMechanism::Scram512)
            .await?
    );

    Ok(())
}

/// SCRAM-SHA-256 and SCRAM-SHA-512 derive different keys from the same
/// password, so they are two credentials for one user. Reading one must never
/// answer with the other — a client that presents 256 would be checked against
/// 512's keys and fail with the right password.
#[tokio::test]
async fn the_two_mechanisms_are_separate_credentials() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, credential(b"for-512"))
        .await?;

    assert_eq!(
        None,
        storage
            .user_scram_credential("alice", ScramMechanism::Scram256)
            .await?
    );

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram256, credential(b"for-256"))
        .await?;

    assert_eq!(
        Some(credential(b"for-512")),
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await?
    );
    assert_eq!(
        Some(credential(b"for-256")),
        storage
            .user_scram_credential("alice", ScramMechanism::Scram256)
            .await?
    );

    Ok(())
}

/// Changing a password overwrites. A create-only put here would leave the old
/// password working and report success for the new one.
#[tokio::test]
async fn a_password_change_overwrites() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, credential(b"first"))
        .await?;
    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, credential(b"second"))
        .await?;

    assert_eq!(
        Some(credential(b"second")),
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await?
    );

    Ok(())
}

/// Revocation is the operation that must actually take effect: after a delete
/// the principal is unknown again, on every replica, because there is no cache
/// in front of this.
#[tokio::test]
async fn a_deleted_credential_stops_authenticating() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, credential(b"alice-salt"))
        .await?;
    storage
        .delete_user_scram_credential("alice", ScramMechanism::Scram512)
        .await?;

    assert_eq!(
        None,
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await?
    );

    Ok(())
}

/// Credentials are applied from configuration management, so a delete that has
/// already taken effect must not start reporting an error on the second run —
/// the same reasoning that makes `create_acls` idempotent.
#[tokio::test]
async fn deleting_a_credential_nobody_has_is_not_an_error() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    storage
        .delete_user_scram_credential("nobody", ScramMechanism::Scram512)
        .await?;

    Ok(())
}

/// One user's credential is not another's. The user name is a path segment, so
/// this is also what catches a name that escapes its own key.
#[tokio::test]
async fn principals_do_not_share_a_credential() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    storage
        .upsert_user_scram_credential("alice", ScramMechanism::Scram512, credential(b"alices"))
        .await?;
    storage
        .upsert_user_scram_credential("bob", ScramMechanism::Scram512, credential(b"bobs"))
        .await?;

    assert_eq!(
        Some(credential(b"alices")),
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await?
    );
    assert_eq!(
        Some(credential(b"bobs")),
        storage
            .user_scram_credential("bob", ScramMechanism::Scram512)
            .await?
    );

    // A name with a slash in it must stay one principal, not become a path into
    // somebody else's prefix.
    storage
        .upsert_user_scram_credential(
            "alice/../bob",
            ScramMechanism::Scram512,
            credential(b"impostor"),
        )
        .await?;

    assert_eq!(
        Some(credential(b"bobs")),
        storage
            .user_scram_credential("bob", ScramMechanism::Scram512)
            .await?
    );

    Ok(())
}
