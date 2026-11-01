//! Source address policy, quota snapshot, and new-connection rate limiting.
//!
//! The relay asks this module two questions for every accepted connection:
//! may this peer connect, and may it connect now? Deny entries win over allow
//! entries, and an empty allow list denies everything, so an incomplete
//! configuration fails closed.
//!
//! [`RuntimePolicy`] is the whole reloadable policy in one value: address
//! rules, the per-source connection limit, and the new-connection token
//! bucket. A reload replaces this value atomically and assigns a stable
//! [`PolicyVersion`] so replicas can compare the policy they are running.

use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::time::Duration;

use tokio::time::Instant;

use crate::config;

/// Scale used to keep token-bucket arithmetic in integers.
const TOKEN_SCALE: u64 = 1_000_000;

/// FNV-1a offset basis used for policy version digests.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a prime used for policy version digests.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A stable identifier for one effective policy snapshot.
///
/// The value is a deterministic digest of the admission-relevant settings, so
/// two replicas that run the same policy report the same version even after an
/// independent restart. It is an operational comparison value, not a
/// cryptographic commitment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PolicyVersion(u64);

impl PolicyVersion {
    /// Returns the numeric form used in logs and metrics.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Computes the version of the admission-relevant settings in `config`.
    fn digest(config: &config::Config) -> Self {
        let mut hash = FNV_OFFSET;
        let mut allow: Vec<String> = config
            .policy
            .allow
            .iter()
            .map(ToString::to_string)
            .collect();
        allow.sort_unstable();
        let mut deny: Vec<String> = config.policy.deny.iter().map(ToString::to_string).collect();
        deny.sort_unstable();
        hash_part(&mut hash, &format!("allow={}", allow.join(",")));
        hash_part(&mut hash, &format!("deny={}", deny.join(",")));
        hash_part(
            &mut hash,
            &format!(
                "emergency_block={}",
                u8::from(config.policy.emergency_block)
            ),
        );

        let limits = &config.limits;
        hash_part(
            &mut hash,
            &format!(
                "limits={},{},{},{},{},{},{},{},{},{}",
                limits.max_connections,
                limits.max_connections_per_listener,
                limits.max_outbound_connections,
                limits.max_connections_per_ip,
                limits.max_new_connections_per_second,
                limits.new_connection_burst,
                limits.max_sessions,
                limits.max_ip_entries,
                limits.ip_entry_ttl_seconds,
                limits.bandwidth_limit_bytes_per_second,
            ),
        );

        let mut listeners: Vec<(&str, bool)> = config
            .listeners
            .iter()
            .map(|listener| (listener.name.as_str(), listener.enabled))
            .collect();
        listeners.sort_unstable();
        for (name, enabled) in listeners {
            hash_part(&mut hash, &format!("listener={name}:{}", u8::from(enabled)));
        }

        let mut l7_listeners: Vec<&config::Listener> = config
            .listeners
            .iter()
            .filter(|listener| listener.l7.is_some())
            .collect();
        l7_listeners.sort_unstable_by_key(|listener| listener.name.as_str());
        for listener in l7_listeners {
            hash_part(
                &mut hash,
                &format!("l7={}:{}", listener.name, l7_digest(listener.l7.as_ref())),
            );
        }
        Self(hash)
    }
}

/// Returns a stable description of the L7 settings that affect admission.
fn l7_digest(l7: Option<&config::L7>) -> String {
    let Some(l7) = l7 else {
        return "none".to_owned();
    };
    let mut allow = l7.allow_methods.clone();
    allow.sort_unstable();
    let mut deny = l7.deny_methods.clone();
    deny.sort_unstable();
    let expensive = l7
        .expensive_methods
        .iter()
        .map(|(method, limit)| format!("{method}={}/{}", limit.rate_per_second, limit.concurrent))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "h={},{},{};b={},{};j={},{},{};a={};d={};e={}",
        l7.max_header_bytes,
        l7.max_header_count,
        l7.header_timeout_ms,
        l7.max_body_bytes,
        l7.body_timeout_ms,
        l7.max_json_depth,
        l7.max_json_array,
        l7.max_batch,
        allow.join(","),
        deny.join(","),
        expensive,
    )
}

impl fmt::Display for PolicyVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Appends one length-delimited field to an FNV-1a digest.
fn hash_part(hash: &mut u64, value: &str) {
    for byte in value.as_bytes() {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
    // A separator keeps adjacent fields from colliding with one longer field.
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(FNV_PRIME);
}

/// A validated snapshot of the address rules from configuration.
#[derive(Debug, Clone, Default)]
pub struct AddressPolicy {
    allow: Vec<ipnet::IpNet>,
    deny: Vec<ipnet::IpNet>,
    emergency_block: bool,
}

impl AddressPolicy {
    /// Builds a policy snapshot from configuration.
    #[must_use]
    pub fn new(config: &config::Policy) -> Self {
        Self {
            allow: config.allow.clone(),
            deny: config.deny.clone(),
            emergency_block: config.emergency_block,
        }
    }

    /// Returns whether the emergency block is active.
    ///
    /// An active emergency block rejects every address, including addresses
    /// that an allow entry matches.
    #[must_use]
    pub const fn emergency_block(&self) -> bool {
        self.emergency_block
    }

    /// Returns whether `address` may open a session.
    ///
    /// Deny rules are checked first, so a broad allow entry cannot override a
    /// narrower deny entry.
    #[must_use]
    pub fn allows(&self, address: IpAddr) -> bool {
        if self.emergency_block {
            return false;
        }
        if self.deny.iter().any(|network| network.contains(&address)) {
            return false;
        }
        self.allow.iter().any(|network| network.contains(&address))
    }
}

/// The reloadable policy in one atomic snapshot.
#[derive(Debug, Clone)]
pub struct RuntimePolicy {
    version: PolicyVersion,
    address: AddressPolicy,
    max_connections_per_ip: usize,
    max_new_connections_per_second: u32,
    new_connection_burst: u32,
}

impl RuntimePolicy {
    /// Builds the runtime policy from a validated configuration.
    #[must_use]
    pub fn from_config(config: &config::Config) -> Self {
        Self {
            version: PolicyVersion::digest(config),
            address: AddressPolicy::new(&config.policy),
            max_connections_per_ip: config.limits.max_connections_per_ip,
            max_new_connections_per_second: config.limits.max_new_connections_per_second,
            new_connection_burst: config.limits.new_connection_burst,
        }
    }

    /// Returns the stable version of this snapshot.
    #[must_use]
    pub const fn version(&self) -> PolicyVersion {
        self.version
    }

    /// Returns whether `address` may open a session.
    #[must_use]
    pub fn allows(&self, address: IpAddr) -> bool {
        self.address.allows(address)
    }

    /// Returns whether the emergency block is active.
    #[must_use]
    pub const fn emergency_block(&self) -> bool {
        self.address.emergency_block()
    }

    /// Returns the concurrent session limit for one source address.
    #[must_use]
    pub const fn max_connections_per_ip(&self) -> usize {
        self.max_connections_per_ip
    }

    /// Returns the sustained new-connection rate for one listener.
    #[must_use]
    pub const fn max_new_connections_per_second(&self) -> u32 {
        self.max_new_connections_per_second
    }

    /// Returns the token bucket burst size for new connections.
    #[must_use]
    pub const fn new_connection_burst(&self) -> u32 {
        self.new_connection_burst
    }
}

/// The policy that new sessions are checked against.
///
/// A reload replaces the whole snapshot. Sessions re-check the current
/// snapshot while they register, and the reload path cancels sessions that the
/// new snapshot rejects, so a peer cannot slip in between those two steps.
#[derive(Debug)]
pub struct PolicyStore {
    current: RwLock<Arc<RuntimePolicy>>,
}

impl PolicyStore {
    /// Creates a store from the validated configuration.
    #[must_use]
    pub fn new(config: &config::Config) -> Self {
        Self {
            current: RwLock::new(Arc::new(RuntimePolicy::from_config(config))),
        }
    }

    /// Returns the current policy snapshot.
    #[must_use]
    pub fn current(&self) -> Arc<RuntimePolicy> {
        let current = self.current.read().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(&current)
    }

    /// Installs `policy` and returns the snapshot it replaced.
    ///
    /// The replacement is a single atomic swap, so a reader observes either
    /// the old or the new rules and never a mixture.
    pub fn replace(&self, policy: RuntimePolicy) -> Arc<RuntimePolicy> {
        let replacement = Arc::new(policy);
        let mut current = self.current.write().unwrap_or_else(PoisonError::into_inner);
        std::mem::replace(&mut current, replacement)
    }
}

/// Token bucket that bounds the rate of new connections for one listener.
///
/// Tokens refill continuously and are capped by the burst size. The bucket
/// starts full, so a fresh listener may admit one burst immediately. A reload
/// may change the rate or burst size; the next acquisition clamps the stored
/// tokens to the new capacity.
#[derive(Debug)]
pub struct TokenBucket {
    state: Mutex<BucketState>,
}

#[derive(Debug)]
struct BucketState {
    tokens: u64,
    capacity: u64,
    updated: Instant,
    initialized: bool,
}

impl TokenBucket {
    /// Creates an empty bucket. The first acquisition fills it to its burst.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BucketState {
                tokens: 0,
                capacity: 0,
                updated: Instant::now(),
                initialized: false,
            }),
        }
    }

    /// Tries to consume one new-connection token without waiting.
    ///
    /// Returns `false` when the bucket has no token or when the configured
    /// rate or burst is zero. The relay never waits for a token: a queued
    /// connection would keep its socket alive while the listener is already
    /// over budget.
    pub fn try_acquire(&self, rate_per_second: u32, burst: u32) -> bool {
        let capacity = u64::from(burst) * TOKEN_SCALE;
        let rate = u64::from(rate_per_second);
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        if state.initialized {
            let elapsed = now.saturating_duration_since(state.updated);
            if elapsed > Duration::ZERO {
                let refill =
                    elapsed.as_nanos() * u128::from(rate) * u128::from(TOKEN_SCALE) / 1_000_000_000;
                let refill = u64::try_from(refill).unwrap_or(u64::MAX);
                state.tokens = state.tokens.saturating_add(refill).min(capacity);
                state.updated = now;
            }
            if state.capacity != capacity {
                state.capacity = capacity;
                state.tokens = state.tokens.min(capacity);
            }
        } else {
            state.tokens = capacity;
            state.capacity = capacity;
            state.updated = now;
            state.initialized = true;
        }

        if rate == 0 || capacity == 0 || state.tokens < TOKEN_SCALE {
            return false;
        }
        state.tokens -= TOKEN_SCALE;
        true
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::str::FromStr;

    use crate::config::Limits;
    use crate::config::Observe;

    fn policy_config(allow: &[&str], deny: &[&str], emergency_block: bool) -> config::Policy {
        config::Policy {
            allow: allow
                .iter()
                .map(|entry| ipnet::IpNet::from_str(entry).expect("test CIDR"))
                .collect(),
            deny: deny
                .iter()
                .map(|entry| ipnet::IpNet::from_str(entry).expect("test CIDR"))
                .collect(),
            emergency_block,
        }
    }

    fn policy(allow: &[&str], deny: &[&str], emergency_block: bool) -> AddressPolicy {
        AddressPolicy::new(&policy_config(allow, deny, emergency_block))
    }

    fn config_with(
        policy: config::Policy,
        limits: Limits,
        listeners: Vec<config::Listener>,
    ) -> config::Config {
        config::Config {
            limits,
            policy,
            observe: Observe::default(),
            upstreams: Vec::new(),
            listeners,
        }
    }

    fn store(allow: &[&str], deny: &[&str], emergency_block: bool) -> PolicyStore {
        PolicyStore::new(&config_with(
            policy_config(allow, deny, emergency_block),
            Limits::default(),
            Vec::new(),
        ))
    }

    #[test]
    fn empty_allow_list_denies_every_address() {
        let policy = policy(&[], &[], false);

        assert!(!policy.allows(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!policy.allows(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn allows_addresses_inside_an_allow_entry() {
        let policy = policy(&["10.0.0.0/8", "2001:db8::/32"], &[], false);

        assert!(policy.allows(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
        assert!(policy.allows(IpAddr::V6("2001:db8::1".parse().expect("IPv6"))));
        assert!(!policy.allows(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    }

    #[test]
    fn deny_entries_win_over_allow_entries() {
        let policy = policy(&["0.0.0.0/0"], &["10.0.0.0/8"], false);

        assert!(!policy.allows(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
        assert!(policy.allows(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    }

    #[test]
    fn deny_entries_win_for_ipv6_too() {
        let policy = policy(&["::/0"], &["2001:db8::/32"], false);

        assert!(!policy.allows(IpAddr::V6("2001:db8::1".parse().expect("IPv6"))));
        assert!(policy.allows(IpAddr::V6("2001:db9::1".parse().expect("IPv6"))));
    }

    #[test]
    fn emergency_block_denies_allowed_addresses() {
        let policy = policy(&["0.0.0.0/0", "::/0"], &[], true);

        assert!(!policy.allows(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
        assert!(!policy.allows(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn store_swaps_the_whole_policy() {
        let store = store(&["10.0.0.0/8"], &[], false);
        let allowed = IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3));
        assert!(store.current().allows(allowed));

        let previous = store.replace(RuntimePolicy::from_config(&config_with(
            policy_config(&[], &[], true),
            Limits::default(),
            Vec::new(),
        )));

        assert!(
            previous.allows(allowed),
            "the old snapshot must stay intact"
        );
        assert!(!store.current().allows(allowed));
    }

    #[test]
    fn policy_version_is_stable_for_equivalent_configs() {
        let first = store(&["10.0.0.0/8", "192.168.0.0/16"], &[], false);
        let second = store(&["10.0.0.0/8", "192.168.0.0/16"], &[], false);

        assert_eq!(first.current().version(), second.current().version());
    }

    #[test]
    fn policy_version_ignores_allow_order() {
        let first = store(&["10.0.0.0/8", "192.168.0.0/16"], &[], false);
        let second = store(&["192.168.0.0/16", "10.0.0.0/8"], &[], false);

        assert_eq!(first.current().version(), second.current().version());
    }

    #[test]
    fn policy_version_changes_when_rules_change() {
        let first = store(&["10.0.0.0/8"], &[], false);
        let second = store(&["10.0.0.0/8"], &[], true);

        assert_ne!(first.current().version(), second.current().version());
    }

    #[test]
    fn policy_version_changes_when_listener_state_changes() {
        let listener = |enabled| config::Listener {
            name: "public".to_owned(),
            listen: "127.0.0.1:26657".parse().expect("socket address"),
            mode: config::ListenerMode::Inbound,
            upstream: "node".to_owned(),
            l7: None,
            enabled,
        };
        let first = PolicyStore::new(&config_with(
            policy_config(&["10.0.0.0/8"], &[], false),
            Limits::default(),
            vec![listener(true)],
        ));
        let second = PolicyStore::new(&config_with(
            policy_config(&["10.0.0.0/8"], &[], false),
            Limits::default(),
            vec![listener(false)],
        ));

        assert_ne!(first.current().version(), second.current().version());
    }

    #[test]
    fn runtime_policy_exposes_reloadable_quotas() {
        let limits = Limits {
            max_connections_per_ip: 3,
            max_new_connections_per_second: 7,
            new_connection_burst: 9,
            ..Limits::default()
        };
        let store = PolicyStore::new(&config_with(
            policy_config(&["10.0.0.0/8"], &[], false),
            limits,
            Vec::new(),
        ));
        let current = store.current();

        assert_eq!(current.max_connections_per_ip(), 3);
        assert_eq!(current.max_new_connections_per_second(), 7);
        assert_eq!(current.new_connection_burst(), 9);
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_allows_burst_then_rejects() {
        let bucket = TokenBucket::new();

        assert!(bucket.try_acquire(2, 2));
        assert!(bucket.try_acquire(2, 2));
        assert!(!bucket.try_acquire(2, 2));

        tokio::time::advance(Duration::from_millis(500)).await;

        assert!(bucket.try_acquire(2, 2));
        assert!(!bucket.try_acquire(2, 2));
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_refill_is_capped_at_burst() {
        let bucket = TokenBucket::new();

        assert!(bucket.try_acquire(1, 1));
        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(bucket.try_acquire(1, 1));
        assert!(!bucket.try_acquire(1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_clamps_tokens_when_burst_shrinks() {
        let bucket = TokenBucket::new();

        assert!(bucket.try_acquire(1, 10));
        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(bucket.try_acquire(1, 1));
        assert!(!bucket.try_acquire(1, 1));
    }

    #[test]
    fn zero_rate_denies_every_connection() {
        let bucket = TokenBucket::new();

        assert!(!bucket.try_acquire(0, 10));
    }
}
