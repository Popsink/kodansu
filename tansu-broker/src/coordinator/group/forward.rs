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

//! Foundations for in-broker forward-to-owner group coordination.
//!
//! Consumer-group state is a single object per group mutated via etag CAS.
//! With N stateless replicas behind one load balancer, a group's Join/Sync
//! long-polls scatter across replicas and thrash that object's CAS, so
//! membership never quiesces. The structural fix is *soft ownership*: every
//! replica deterministically maps each `group_id` to exactly one owner
//! replica and forwards the six group APIs there, so the whole membership
//! state machine for a group runs on a single [`Controller`] — the regime
//! already proven to converge by the single-controller tests.
//!
//! Ownership is *soft* because it is an optimisation, never a correctness
//! requirement: every write remains conditional on the object's etag, so two
//! replicas transiently believing they own the same group (DNS skew during a
//! rolling restart) degrades at worst to today's CAS-retry behaviour, never
//! to corruption.
//!
//! This module provides the ownership decision only:
//!
//! * [`fnv1a_64`] — an inline FNV-1a 64 hash. [`std::hash::DefaultHasher`]
//!   is deliberately not used: SipHash keys are unstable across processes
//!   and Rust versions, while the owner decision must agree across pods and
//!   across a rolling upgrade. FNV-1a is fully specified, trivially inlined,
//!   and stable forever.
//! * [`owner`] — rendezvous (highest-random-weight) hashing over the peer
//!   set. Rendezvous beats mod-N: removing one peer reassigns only that
//!   peer's groups (~1/N of the total); every other group keeps its owner.
//! * [`PeerRegistry`] — the peer set, discovered by polling the DNS A/AAAA
//!   records of a headless-Service hostname, plus this replica's own
//!   identity (its pod IP).
//!
//! When the peer set is empty — DNS not yet resolved, resolution failing, or
//! discovery not configured — [`owner`] returns the local replica, i.e. the
//! broker processes the request itself. That fallback is bit-for-bit today's
//! local CAS behaviour, so every discovery failure mode degrades to the
//! status quo rather than to an outage.
//!
//! [`Controller`]: super::administrator::Controller

use crate::Result;
use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash of `bytes` folded into an existing `state`.
///
/// Exposed as a fold so a rendezvous score can hash `group_id ‖ peer`
/// without allocating a concatenated buffer.
fn fnv1a_64_fold(state: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(state, |hash, octet| {
        (hash ^ u64::from(*octet)).wrapping_mul(FNV_PRIME)
    })
}

/// FNV-1a 64-bit hash.
///
/// Chosen over [`std::hash::DefaultHasher`] because the result must be
/// identical on every pod and across rolling upgrades: SipHash is keyed per
/// process and unspecified across Rust versions, whereas FNV-1a is a fixed
/// public algorithm. Distribution quality is more than sufficient for
/// rendezvous hashing over tens of peers.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    fnv1a_64_fold(FNV_OFFSET_BASIS, bytes)
}

/// The splitmix64 finalizer (Steele, Lea & Flood; also murmur-style
/// `fmix64`): a fixed, fully-specified bijection on `u64` used to avalanche
/// the rendezvous score.
///
/// Needed because FNV-1a diffuses trailing bytes weakly: rendezvous peers
/// are pod IPs that typically differ only in their last octet — exactly the
/// bytes folded in last — and without a finalizer one peer can capture a
/// large multiple of its fair share of groups. Like FNV-1a itself the
/// finalizer uses only fixed public constants, so scores stay identical
/// across pods and rolling upgrades.
fn mix64(hash: u64) -> u64 {
    let z = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The rendezvous score of `peer` for `group_id`:
/// `mix64(fnv1a_64(group_id ‖ peer))` where the peer is folded in as its raw
/// address octets.
fn rendezvous_score(group_id: &str, peer: &IpAddr) -> u64 {
    let group = fnv1a_64_fold(FNV_OFFSET_BASIS, group_id.as_bytes());

    mix64(match peer {
        IpAddr::V4(v4) => fnv1a_64_fold(group, &v4.octets()),
        IpAddr::V6(v6) => fnv1a_64_fold(group, &v6.octets()),
    })
}

/// The owner of `group_id` under rendezvous hashing: the candidate maximising
/// `mix64(fnv1a_64(group_id ‖ candidate))`, with ties broken by [`IpAddr`]
/// ordering so every replica agrees regardless of the order peers were
/// discovered in.
///
/// `self_ip` is ALWAYS one of the candidates, even when it is absent from
/// `peers`. A replica can always coordinate a group itself, and forcing self
/// into the candidate set removes a startup/rollout asymmetry: in the window
/// where the headless Service has published this pod to its *peers'* DNS but
/// this pod's own resolver has not yet listed itself, a self-excluding view
/// would forward this replica's own groups elsewhere while peers route those
/// same groups back to it — transient double-ownership. Including self makes
/// this replica agree with its peers about the groups it owns. (An empty
/// `peers` slice therefore still yields `self_ip` = local, today's behaviour.)
pub fn owner(group_id: &str, self_ip: IpAddr, peers: &[IpAddr]) -> IpAddr {
    peers
        .iter()
        .copied()
        .chain(std::iter::once(self_ip))
        .max_by_key(|candidate| (rendezvous_score(group_id, candidate), *candidate))
        .unwrap_or(self_ip)
}

/// The set of replicas eligible to own groups, refreshed from DNS.
///
/// Peers are the A/AAAA records of a headless-Service hostname (Kubernetes
/// publishes one record per *ready* pod, so ownership only lands on ready
/// replicas). `self_ip` is this replica's pod IP; it is used both for the
/// [`PeerRegistry::is_local`] decision and as the fallback owner when the
/// peer set is empty.
///
/// The registry itself performs no forwarding — it only answers "who owns
/// this group?" and "is that me?".
#[derive(Debug)]
pub struct PeerRegistry {
    self_ip: IpAddr,
    peer_dns: String,
    interval: Duration,
    peers: Mutex<Arc<Vec<IpAddr>>>,
}

impl PeerRegistry {
    /// A registry that will resolve `peer_dns` (a headless-Service
    /// hostname) every `interval`, starting with an empty peer set (all
    /// groups local until the first successful refresh).
    pub fn new(self_ip: IpAddr, peer_dns: impl Into<String>, interval: Duration) -> Self {
        Self {
            self_ip,
            peer_dns: peer_dns.into(),
            interval,
            peers: Mutex::new(Arc::new(Vec::new())),
        }
    }

    /// This replica's own address.
    pub fn self_ip(&self) -> IpAddr {
        self.self_ip
    }

    /// A snapshot of the current peer set.
    pub fn peers(&self) -> Arc<Vec<IpAddr>> {
        self.peers
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Replace the peer set. Peers are sorted and deduplicated; order does
    /// not affect ownership (see [`owner`]), this just keeps snapshots
    /// canonical for logging and comparison.
    pub fn set_peers(&self, mut peers: Vec<IpAddr>) {
        peers.sort_unstable();
        peers.dedup();

        if let Ok(mut guard) = self.peers.lock() {
            *guard = Arc::new(peers);
        }
    }

    /// Resolve the configured DNS name and replace the peer set with the
    /// addresses found, returning how many there are.
    ///
    /// On resolution failure the peer set is cleared: an unresolvable
    /// headless service means the peer view cannot be trusted, and an empty
    /// set degrades every decision to "local", i.e. today's behaviour.
    pub async fn refresh(&self) -> Result<usize> {
        match tokio::net::lookup_host((self.peer_dns.as_str(), 0u16)).await {
            Ok(addresses) => {
                let peers = addresses.map(|address| address.ip()).collect::<Vec<_>>();

                debug!(peer_dns = %self.peer_dns, ?peers, "refreshed group forwarding peers");

                let n = peers.len();
                self.set_peers(peers);
                Ok(n)
            }

            Err(error) => {
                warn!(
                    peer_dns = %self.peer_dns,
                    %error,
                    "peer DNS resolution failed; falling back to local group coordination"
                );

                self.set_peers(Vec::new());
                Err(error.into())
            }
        }
    }

    /// The owner of `group_id` given the current peer snapshot.
    pub fn owner(&self, group_id: &str) -> IpAddr {
        owner(group_id, self.self_ip, &self.peers())
    }

    /// Whether this replica should coordinate `group_id` itself (it is the
    /// owner, or no peers are known).
    pub fn is_local(&self, group_id: &str) -> bool {
        self.owner(group_id) == self.self_ip
    }

    /// Spawn a background task refreshing the peer set every `interval`
    /// (the first refresh happens immediately). Resolution failures are
    /// logged by [`PeerRegistry::refresh`] and retried on the next tick;
    /// the task only ends when the registry is dropped by every other
    /// holder.
    pub fn spawn_refresh(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                _ = interval.tick().await;

                _ = self.refresh().await;

                if Arc::strong_count(&self) == 1 {
                    debug!(peer_dns = %self.peer_dns, "peer registry dropped; refresh ending");
                    break;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, prelude::*};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn peer(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    fn group_ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("group-{i}")).collect()
    }

    #[test]
    fn fnv1a_64_golden_vectors() {
        // Published FNV-1a 64 test vectors. These lock the hash across
        // refactors, Rust versions and pods: if any of these change, owner
        // placement changes fleet-wide during a rolling upgrade.
        assert_eq!(0xcbf2_9ce4_8422_2325, fnv1a_64(b""));
        assert_eq!(0xaf63_dc4c_8601_ec8c, fnv1a_64(b"a"));
        assert_eq!(0x8594_4171_f739_67e8, fnv1a_64(b"foobar"));
        assert_eq!(0x980f_eb29_c5a2_796c, fnv1a_64(b"tansu"));
    }

    #[test]
    fn rendezvous_score_golden_vectors() {
        // Locks the full score pipeline (FNV-1a fold + splitmix64
        // finalizer): a change here reshuffles group ownership fleet-wide.
        assert_eq!(0x8897_4c77_96b5_efe1, rendezvous_score("group-0", &peer(1)));
        assert_eq!(
            0xd76a_768b_aea3_2195,
            rendezvous_score("consumers", &IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)))
        );
    }

    #[test]
    fn owner_of_empty_peer_set_is_self() {
        let self_ip = peer(99);

        assert_eq!(self_ip, owner("some-group", self_ip, &[]));
        assert_eq!(self_ip, owner("", self_ip, &[]));
    }

    #[test]
    fn owner_is_deterministic_under_peer_order() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        let self_ip = peer(200);
        let mut rng = StdRng::seed_from_u64(0x0074_616e_7375);

        for group_id in group_ids(250) {
            let expected = owner(&group_id, self_ip, &peers);

            for _ in 0..5 {
                let mut shuffled = peers.clone();
                shuffled.shuffle(&mut rng);

                assert_eq!(expected, owner(&group_id, self_ip, &shuffled));
            }
        }
    }

    #[test]
    fn owner_handles_ipv6_peers() {
        let peers = (1..=4)
            .map(|i| IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, i)))
            .collect::<Vec<_>>();
        let self_ip = peer(200);

        for group_id in group_ids(50) {
            let elected = owner(&group_id, self_ip, &peers);
            // self_ip is always a candidate, so the winner may be self even
            // when it is absent from `peers`.
            assert!(elected == self_ip || peers.contains(&elected));
        }
    }

    #[test]
    fn rendezvous_removal_moves_only_the_removed_peers_groups() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        // self is one of the peers here: this test is about peer
        // redistribution, not self-candidacy (see
        // `owner_considers_self_even_when_absent_from_peers`).
        let self_ip = peer(1);
        let removed = peer(4);
        let survivors = peers
            .iter()
            .copied()
            .filter(|p| *p != removed)
            .collect::<Vec<_>>();

        let groups = group_ids(2_000);

        let before = groups
            .iter()
            .map(|g| (g.clone(), owner(g, self_ip, &peers)))
            .collect::<BTreeMap<_, _>>();

        let after = groups
            .iter()
            .map(|g| (g.clone(), owner(g, self_ip, &survivors)))
            .collect::<BTreeMap<_, _>>();

        let mut moved = 0;

        for group in &groups {
            if before[group] == removed {
                moved += 1;
                assert_ne!(removed, after[group]);
                assert!(survivors.contains(&after[group]));
            } else {
                // rendezvous invariant: every group not owned by the removed
                // peer keeps its owner.
                assert_eq!(before[group], after[group]);
            }
        }

        // the removed peer owned ~1/10 of the groups; loose bounds guard
        // against a degenerate hash distribution.
        assert!(
            (100..=350).contains(&moved),
            "expected ~200 of 2000 groups to move, got {moved}"
        );
    }

    #[test]
    fn owners_are_reasonably_distributed() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        // self is one of the peers here (this test measures peer distribution).
        let self_ip = peer(1);
        let mut per_owner = BTreeMap::<IpAddr, usize>::new();

        for group_id in group_ids(2_000) {
            *per_owner
                .entry(owner(&group_id, self_ip, &peers))
                .or_default() += 1;
        }

        assert_eq!(10, per_owner.len(), "every peer should own some groups");

        for (owner, n) in per_owner {
            assert!(
                (100..=350).contains(&n),
                "peer {owner} owns {n} of 2000 groups; expected ~200"
            );
        }
    }

    #[test]
    fn owner_considers_self_even_when_absent_from_peers() {
        // Startup/rollout window: the headless Service has published this pod
        // to its peers' DNS, but this pod's own resolver has not yet listed
        // itself, so `peers` excludes `self_ip`. Self must still win the groups
        // that rendezvous to it — otherwise this replica forwards its own
        // groups away while its peers route them back to it (double-ownership).
        let self_ip = peer(200); // deliberately NOT in `peers`
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        assert!(!peers.contains(&self_ip));

        let self_owned = group_ids(2_000)
            .into_iter()
            .filter(|g| owner(g, self_ip, &peers) == self_ip)
            .count();

        // self is a genuine 11th candidate: it must win a roughly fair share,
        // never zero (the pre-fix bug) and never all.
        assert!(
            (100..=320).contains(&self_owned),
            "self owns {self_owned} of 2000 groups; expected ~182 (1/11)"
        );
    }

    #[test]
    fn registry_owner_and_is_local_agree() {
        let self_ip = peer(3);
        let registry = PeerRegistry::new(self_ip, "unused.invalid", Duration::from_secs(5));

        // empty peer set: everything is local.
        assert!(registry.peers().is_empty());
        assert!(registry.is_local("some-group"));
        assert_eq!(self_ip, registry.owner("some-group"));

        registry.set_peers((1..=10).map(peer).collect());
        assert_eq!(10, registry.peers().len());

        let mut local = 0;

        for group_id in group_ids(500) {
            let elected = registry.owner(&group_id);

            assert!(registry.peers().contains(&elected));
            assert_eq!(elected == self_ip, registry.is_local(&group_id));

            if registry.is_local(&group_id) {
                local += 1;
            }
        }

        assert!(
            (10..=120).contains(&local),
            "self owns {local} of 500 groups; expected ~50"
        );
    }

    #[test]
    fn set_peers_sorts_and_deduplicates() {
        let registry = PeerRegistry::new(peer(1), "unused.invalid", Duration::from_secs(5));

        registry.set_peers(vec![peer(3), peer(1), peer(3), peer(2)]);

        assert_eq!(vec![peer(1), peer(2), peer(3)], *registry.peers());
    }

    #[tokio::test]
    async fn refresh_resolves_localhost() -> Result<()> {
        let registry = PeerRegistry::new(peer(1), "localhost", Duration::from_secs(5));

        let n = registry.refresh().await?;

        assert!(n >= 1);
        assert!(registry.peers().iter().all(|ip| ip.is_loopback()));

        Ok(())
    }

    #[tokio::test]
    async fn refresh_failure_clears_peers() {
        // RFC 2606 reserves .invalid: resolution must fail.
        let registry = PeerRegistry::new(
            peer(1),
            "peer-discovery.does-not-exist.invalid",
            Duration::from_secs(5),
        );

        registry.set_peers((1..=3).map(peer).collect());
        assert_eq!(3, registry.peers().len());

        assert!(registry.refresh().await.is_err());

        assert!(registry.peers().is_empty());
        assert!(registry.is_local("any-group"));
    }
}
