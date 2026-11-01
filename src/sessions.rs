//! Active session tracking and per-source admission state.
//!
//! The registry holds one entry per session that passed every admission check.
//! An entry is removed by an RAII guard, so normal completion, copy errors,
//! cancellation, and task abortion all release the slot. Each entry carries a
//! cancellation token that stops the session immediately when the policy
//! changes or shutdown begins.
//!
//! The per-source map enforces a concurrent session limit per IP address and
//! keeps idle entries for a bounded time. It has both a time-to-live and a hard
//! entry cap: a TTL alone cannot bound memory, and an entry with active
//! sessions is never removed by the TTL.

use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::Endpoint;
use crate::policy::PolicyStore;

/// Identifier assigned to a registered session.
pub type SessionId = u64;

/// Everything the guard knows about one active session.
#[derive(Debug, Clone)]
pub struct SessionMeta {
    /// Listener that accepted the connection.
    pub listener: String,
    /// Peer address observed by the guard.
    pub peer: SocketAddr,
    /// Fixed upstream the session relays to.
    pub upstream: Endpoint,
    /// Token that stops the session when cancelled.
    pub cancel: CancellationToken,
    /// Time the session was registered.
    pub started_at: Instant,
}

/// Stop/resume state for one listener.
///
/// A stopped listener keeps its socket bound, closes newly accepted
/// connections immediately, and cancels its existing sessions. Registration
/// checks this gate while it holds the registry lock, so a session cannot slip
/// in between a stop request and the cancellation scan.
#[derive(Debug)]
pub struct ListenerGate {
    stopped: AtomicBool,
}

impl ListenerGate {
    /// Creates a gate for a listener that is enabled or stopped.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            stopped: AtomicBool::new(!enabled),
        }
    }

    /// Returns whether the listener is currently stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Applies `enabled` and returns whether the state changed.
    pub fn set_enabled(&self, enabled: bool) -> bool {
        let previous = self.stopped.swap(!enabled, Ordering::AcqRel);
        previous == enabled
    }
}

/// Why a session could not be registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    /// The policy current at registration time rejects the peer.
    Rejected,
    /// The listener was stopped before the session could register.
    Stopped,
    /// The registry already holds its maximum number of entries.
    Full,
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected => formatter.write_str("rejected by address policy"),
            Self::Stopped => formatter.write_str("listener is stopped"),
            Self::Full => formatter.write_str("session registry is full"),
        }
    }
}

impl std::error::Error for RegisterError {}

/// Bounded map of active sessions.
#[derive(Debug)]
pub struct Registry {
    max_sessions: usize,
    next_id: AtomicU64,
    inner: Mutex<Inner>,
    removed: Notify,
}

#[derive(Debug, Default)]
struct Inner {
    sessions: HashMap<SessionId, SessionMeta>,
}

impl Registry {
    /// Creates a registry that holds at most `max_sessions` entries.
    #[must_use]
    pub fn new(max_sessions: usize) -> Self {
        Self {
            max_sessions,
            next_id: AtomicU64::new(1),
            inner: Mutex::new(Inner::default()),
            removed: Notify::new(),
        }
    }

    /// Registers a session, re-checking the current policy and listener gate.
    ///
    /// Both checks run while the registry lock is held. A reload installs the
    /// new policy and listener states before scanning the registry, so a
    /// session that registers concurrently either sees the new rules here or
    /// is already visible to the reload scan and gets cancelled. Either way a
    /// blocked peer or a stopped listener cannot keep a session.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Rejected`] when the current policy denies the
    /// peer, [`RegisterError::Stopped`] when the listener is stopped, and
    /// [`RegisterError::Full`] when the registry is at its entry cap.
    pub fn register(
        self: &Arc<Self>,
        policy: &PolicyStore,
        gate: &ListenerGate,
        meta: SessionMeta,
    ) -> Result<SessionGuard, RegisterError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if gate.is_stopped() {
            return Err(RegisterError::Stopped);
        }
        if !policy.current().allows(meta.peer.ip()) {
            return Err(RegisterError::Rejected);
        }
        if inner.sessions.len() >= self.max_sessions {
            return Err(RegisterError::Full);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cancel = meta.cancel.clone();
        let started_at = meta.started_at;
        inner.sessions.insert(id, meta);
        Ok(SessionGuard {
            registry: Arc::clone(self),
            id,
            cancel,
            started_at,
        })
    }

    /// Returns the number of registered sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .sessions
            .len()
    }

    /// Returns whether no session is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cancels every registered session.
    pub fn cancel_all(&self) {
        self.cancel_if(|_| true);
    }

    /// Cancels every registered session that matches `predicate` and returns
    /// how many sessions it cancelled.
    pub fn cancel_if(&self, predicate: impl Fn(&SessionMeta) -> bool) -> usize {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut cancelled = 0;
        for meta in inner.sessions.values() {
            if predicate(meta) && !meta.cancel.is_cancelled() {
                meta.cancel.cancel();
                cancelled += 1;
            }
        }
        cancelled
    }

    /// Returns a copy of every registered session.
    ///
    /// Shutdown uses this to report the sessions it has to cancel. The copy is
    /// bounded by the registry cap.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(SessionId, SessionMeta)> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .sessions
            .iter()
            .map(|(id, meta)| (*id, meta.clone()))
            .collect()
    }

    /// Waits until no session is registered.
    ///
    /// Cancellation wakes the session task, which drops its guard and removes
    /// the entry, so this future completes only after those tasks unwind.
    pub async fn wait_empty(&self) {
        loop {
            let notified = self.removed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_empty() {
                return;
            }
            notified.await;
        }
    }
}

/// RAII registry entry that removes its session when dropped.
#[derive(Debug)]
pub struct SessionGuard {
    registry: Arc<Registry>,
    id: SessionId,
    cancel: CancellationToken,
    started_at: Instant,
}

impl SessionGuard {
    /// Returns the identifier assigned at registration.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// Returns the token that stops this session.
    #[must_use]
    pub const fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Returns the time the session was registered.
    #[must_use]
    pub const fn started_at(&self) -> Instant {
        self.started_at
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let removed = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .sessions
            .remove(&self.id);
        if removed.is_some() {
            self.registry.removed.notify_waiters();
        }
    }
}

/// Why a per-source session slot could not be acquired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpError {
    /// The source already holds the maximum number of sessions.
    LimitReached,
    /// The state map is full and no idle entry has expired.
    MapFull,
}

impl std::fmt::Display for IpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitReached => formatter.write_str("per-IP session limit reached"),
            Self::MapFull => formatter.write_str("per-IP state map is full"),
        }
    }
}

impl std::error::Error for IpError {}

/// Per-source connection state with both a time-to-live and an entry cap.
#[derive(Debug)]
pub struct IpStates {
    max_entries: usize,
    ttl: Duration,
    inner: Mutex<IpInner>,
}

#[derive(Debug)]
struct IpInner {
    entries: HashMap<IpAddr, IpState>,
    last_sweep: Instant,
}

#[derive(Debug, Clone, Copy)]
struct IpState {
    active: usize,
    last_seen: Instant,
}

impl IpStates {
    /// Creates a state map that tracks at most `max_entries` addresses and
    /// forgets an idle address after `ttl`.
    #[must_use]
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries,
            ttl,
            inner: Mutex::new(IpInner {
                entries: HashMap::new(),
                last_sweep: Instant::now(),
            }),
        }
    }

    /// Reserves one session slot for `ip` without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`IpError::LimitReached`] when `ip` already holds `max_per_ip`
    /// sessions and [`IpError::MapFull`] when the map cannot track another
    /// address without evicting an active one.
    pub fn try_acquire(
        self: &Arc<Self>,
        ip: IpAddr,
        max_per_ip: usize,
    ) -> Result<IpPermit, IpError> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if now.saturating_duration_since(inner.last_sweep) >= self.sweep_interval() {
            Self::sweep_locked(&mut inner, now, self.ttl);
            inner.last_sweep = now;
        }
        if let Some(state) = inner.entries.get_mut(&ip) {
            if state.active >= max_per_ip {
                return Err(IpError::LimitReached);
            }
            state.active += 1;
            state.last_seen = now;
        } else {
            if inner.entries.len() >= self.max_entries {
                Self::sweep_locked(&mut inner, now, self.ttl);
                if inner.entries.len() >= self.max_entries {
                    return Err(IpError::MapFull);
                }
            }
            inner.entries.insert(
                ip,
                IpState {
                    active: 1,
                    last_seen: now,
                },
            );
        }
        Ok(IpPermit {
            states: Arc::clone(self),
            ip,
        })
    }

    /// Returns the number of tracked addresses, including idle entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }

    /// Returns the number of active sessions for `ip`, if it is tracked.
    #[cfg(test)]
    #[must_use]
    pub fn active(&self, ip: IpAddr) -> Option<usize> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .get(&ip)
            .map(|state| state.active)
    }

    fn sweep_interval(&self) -> Duration {
        self.ttl / 2
    }

    /// Drops idle entries that outlived the TTL.
    ///
    /// Entries with active sessions are always kept: removing a live counter
    /// would let a source exceed its limit.
    fn sweep_locked(inner: &mut IpInner, now: Instant, ttl: Duration) {
        inner.entries.retain(|_, state| {
            state.active > 0 || now.saturating_duration_since(state.last_seen) < ttl
        });
    }
}

/// RAII per-source slot that is returned when the session ends.
#[derive(Debug)]
pub struct IpPermit {
    states: Arc<IpStates>,
    ip: IpAddr,
}

impl Drop for IpPermit {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut inner = self
            .states
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(state) = inner.entries.get_mut(&self.ip) {
            state.active = state.active.saturating_sub(1);
            state.last_seen = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    use crate::config;
    use crate::policy::RuntimePolicy;

    fn config_with(policy: config::Policy) -> config::Config {
        config::Config {
            limits: config::Limits::default(),
            policy,
            observe: config::Observe::default(),
            upstreams: Vec::new(),
            listeners: Vec::new(),
        }
    }

    fn policy_store(allow_all: bool) -> PolicyStore {
        let policy = config::Policy {
            allow: if allow_all {
                vec![ipnet::IpNet::from_str("0.0.0.0/0").expect("test CIDR")]
            } else {
                Vec::new()
            },
            deny: Vec::new(),
            emergency_block: false,
        };
        PolicyStore::new(&config_with(policy))
    }

    fn gate() -> ListenerGate {
        ListenerGate::new(true)
    }

    fn meta(peer: SocketAddr) -> SessionMeta {
        SessionMeta {
            listener: "test".to_owned(),
            peer,
            upstream: Endpoint::from_str("127.0.0.1:26656").expect("test endpoint"),
            cancel: CancellationToken::new(),
            started_at: Instant::now(),
        }
    }

    fn peer(last: u8) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::new(127, 0, 0, last), 40_000))
    }

    #[test]
    fn registry_rejects_entries_above_its_cap() {
        let registry = Arc::new(Registry::new(1));
        let policy = policy_store(true);

        let first = registry
            .register(&policy, &gate(), meta(peer(1)))
            .expect("first session");
        assert!(matches!(
            registry.register(&policy, &gate(), meta(peer(2))),
            Err(RegisterError::Full)
        ));
        assert_eq!(registry.len(), 1);

        drop(first);
        assert!(registry.is_empty());
        assert!(registry.register(&policy, &gate(), meta(peer(2))).is_ok());
    }

    #[test]
    fn registration_rechecks_the_current_policy() {
        let registry = Arc::new(Registry::new(4));
        let policy = policy_store(true);
        let _session = registry
            .register(&policy, &gate(), meta(peer(1)))
            .expect("first session");

        policy.replace(RuntimePolicy::from_config(&config_with(
            config::Policy::default(),
        )));

        assert!(matches!(
            registry.register(&policy, &gate(), meta(peer(1))),
            Err(RegisterError::Rejected)
        ));
    }

    #[test]
    fn dropping_a_guard_removes_the_entry() {
        let registry = Arc::new(Registry::new(4));
        let policy = policy_store(true);
        let session = registry
            .register(&policy, &gate(), meta(peer(1)))
            .expect("session");
        assert_eq!(session.id(), 1);

        drop(session);

        assert!(registry.is_empty());
        assert_eq!(
            registry
                .register(&policy, &gate(), meta(peer(1)))
                .expect("new session")
                .id(),
            2
        );
    }

    #[test]
    fn cancel_if_stops_matching_sessions_only() {
        let registry = Arc::new(Registry::new(4));
        let policy = policy_store(true);
        let first = registry
            .register(&policy, &gate(), meta(peer(1)))
            .expect("first session");
        let second = registry
            .register(&policy, &gate(), meta(peer(2)))
            .expect("second session");

        let cancelled = registry.cancel_if(|meta| meta.peer == peer(1));

        assert_eq!(cancelled, 1);
        assert!(first.cancel().is_cancelled());
        assert!(!second.cancel().is_cancelled());
    }

    #[test]
    fn registration_rejects_a_stopped_listener() {
        let registry = Arc::new(Registry::new(4));
        let policy = policy_store(true);
        let gate = ListenerGate::new(true);
        let session = registry
            .register(&policy, &gate, meta(peer(1)))
            .expect("first session");

        assert!(gate.set_enabled(false));

        assert!(matches!(
            registry.register(&policy, &gate, meta(peer(2))),
            Err(RegisterError::Stopped)
        ));
        assert!(!gate.set_enabled(false), "repeated stop is a no-op");
        assert!(gate.set_enabled(true));
        assert!(registry.register(&policy, &gate, meta(peer(2))).is_ok());
        drop(session);
    }

    #[tokio::test]
    async fn wait_empty_returns_after_the_last_guard_is_dropped() {
        let registry = Arc::new(Registry::new(4));
        let policy = policy_store(true);
        let session = registry
            .register(&policy, &gate(), meta(peer(1)))
            .expect("session");
        let waiter = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move { registry.wait_empty().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(session);
        waiter.await.expect("wait task");
    }

    #[test]
    fn per_ip_limit_is_enforced() {
        let states = Arc::new(IpStates::new(16, Duration::from_secs(60)));
        let ip = Ipv4Addr::LOCALHOST.into();

        let first = states.try_acquire(ip, 2).expect("first slot");
        let second = states.try_acquire(ip, 2).expect("second slot");
        assert!(matches!(
            states.try_acquire(ip, 2),
            Err(IpError::LimitReached)
        ));

        drop(first);
        assert_eq!(states.active(ip), Some(1));
        assert!(states.try_acquire(ip, 2).is_ok());
        drop(second);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_entries_expire_after_the_ttl() {
        let states = Arc::new(IpStates::new(1, Duration::from_secs(1)));
        let first_ip = Ipv4Addr::LOCALHOST.into();
        let second_ip = Ipv4Addr::new(127, 0, 0, 2).into();

        drop(states.try_acquire(first_ip, 1).expect("first slot"));
        assert_eq!(states.len(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(states.try_acquire(second_ip, 1).is_ok());
        assert_eq!(states.len(), 1);
        assert_eq!(states.active(first_ip), None);
    }

    #[tokio::test(start_paused = true)]
    async fn active_entries_are_never_expired_by_the_ttl() {
        let states = Arc::new(IpStates::new(1, Duration::from_secs(1)));
        let first_ip = Ipv4Addr::LOCALHOST.into();
        let second_ip = Ipv4Addr::new(127, 0, 0, 2).into();

        let _active = states.try_acquire(first_ip, 1).expect("first slot");
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(matches!(
            states.try_acquire(second_ip, 1),
            Err(IpError::MapFull)
        ));
        assert_eq!(states.active(first_ip), Some(1));
    }

    #[test]
    fn map_full_is_rejected_without_evicting_active_entries() {
        let states = Arc::new(IpStates::new(1, Duration::from_secs(60)));
        let first_ip = Ipv4Addr::LOCALHOST.into();
        let second_ip = Ipv4Addr::new(127, 0, 0, 2).into();

        let _active = states.try_acquire(first_ip, 8).expect("first slot");

        assert!(matches!(
            states.try_acquire(second_ip, 8),
            Err(IpError::MapFull)
        ));
        assert_eq!(states.active(first_ip), Some(1));
    }
}
