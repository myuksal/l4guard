//! TCP relay core.
//!
//! Every listener follows the same fixed order for each connection:
//! `accept`, address policy, per-source and connection budgets, permit,
//! registration, task creation, upstream connect, bidirectional copy, and
//! cleanup. A session becomes a task only after all checks pass, so overload
//! cannot accumulate queued tasks or accepted sockets.
//!
//! Permits, the per-source slot, and the registry guard are owned by the
//! session task. Normal completion, copy errors, cancellation, and task
//! abortion all drop the guards, so every budget returns to its pool.
//!
//! Each registered session carries a cancellation token. The policy reload
//! path cancels sessions that the new rules reject, and shutdown cancels every
//! remaining session after the drain window.
//!
//! No overall session timeout is applied: long-lived P2P connections are
//! expected to stay open. Only the upstream connect deadline bounds a session
//! before copying starts.
//!
//! This module enforces address policy, connection budgets, per-source
//! limits, per-listener new-connection rate limits, listener stop/resume, and
//! the session registry. The reload pipeline replaces the policy snapshot and
//! cancels sessions that the new policy or listener state rejects.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config;
use crate::config::Config;
use crate::config::Endpoint;
use crate::config::ListenerMode;
use crate::l7;
use crate::policy::PolicyStore;
use crate::policy::PolicyVersion;
use crate::policy::RuntimePolicy;
use crate::policy::TokenBucket;
use crate::sessions::IpError;
use crate::sessions::IpPermit;
use crate::sessions::IpStates;
use crate::sessions::ListenerGate;
use crate::sessions::RegisterError;
use crate::sessions::Registry;
use crate::sessions::SessionGuard;
use crate::sessions::SessionMeta;

/// Bytes read from one side before writing them to the other.
///
/// This matches Tokio's `copy_bidirectional` default of 8 KiB per direction.
const COPY_BUFFER_SIZE: usize = 8 * 1024;

/// First delay after an accept error, before retrying the listener.
const MIN_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);

/// Upper bound for accept retry delays.
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// Time allowed for cancelled sessions to drop their guards during shutdown.
const CANCEL_GRACE: Duration = Duration::from_secs(2);

/// A bound relay that is ready to accept connections.
pub struct Relay {
    shared: Arc<Shared>,
    listeners: Vec<(Arc<ListenerContext>, TcpListener)>,
}

impl Relay {
    /// Binds every configured listener without accepting connections yet.
    ///
    /// All sockets are bound before any listener task starts, so one unusable
    /// address stops startup instead of leaving a partially open proxy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Bind`] when a listen address cannot be bound and
    /// [`Error::UnknownUpstream`] when a listener names an upstream that does
    /// not exist. Configuration validation normally prevents the latter.
    pub async fn bind(config: Arc<Config>) -> Result<Self, Error> {
        let shared = Arc::new(Shared::new(&config));
        info!(
            policy_version = %shared.policy.current().version(),
            "policy snapshot loaded"
        );
        let mut listeners = Vec::with_capacity(config.listeners.len());
        for listener in &config.listeners {
            let upstream = config
                .upstreams
                .iter()
                .find(|upstream| upstream.name == listener.upstream)
                .ok_or_else(|| Error::UnknownUpstream {
                    listener: listener.name.clone(),
                    upstream: listener.upstream.clone(),
                })?;
            let socket =
                TcpListener::bind(listener.listen)
                    .await
                    .map_err(|source| Error::Bind {
                        listener: listener.name.clone(),
                        address: listener.listen,
                        source,
                    })?;
            info!(
                listener = %listener.name,
                mode = listener.mode.as_str(),
                listen = %listener.listen,
                upstream = %upstream.address,
                "listener ready"
            );
            let context = Arc::new(ListenerContext::new(
                Arc::clone(&shared),
                listener,
                upstream.address.clone(),
            ));
            listeners.push((context, socket));
        }
        Ok(Self { shared, listeners })
    }

    /// Returns a handle for reading relay counters.
    #[must_use]
    pub fn stats(&self) -> StatsHandle {
        StatsHandle::new(
            Arc::clone(&self.shared.stats),
            Arc::clone(&self.shared.sessions),
        )
    }

    /// Returns a handle that can reload policy while the relay runs.
    #[must_use]
    pub fn handle(&self) -> RelayHandle {
        RelayHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Serves every listener until `shutdown` completes, then drains sessions.
    ///
    /// Shutdown stops new accepts first, waits for listener tasks to finish,
    /// gives active sessions up to `limits.drain_timeout_ms` to close on their
    /// own, and finally cancels whatever remains. Dropping this future aborts
    /// the listener tasks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TaskFailed`] when a listener task panics and
    /// [`Error::ListenerExited`] when listener tasks end before `shutdown`.
    pub async fn run<F>(self, shutdown: F) -> Result<(), Error>
    where
        F: Future<Output = ()>,
    {
        let mut tasks = JoinSet::new();
        for (context, listener) in self.listeners {
            tasks.spawn(serve_listener(context, listener));
        }
        tokio::pin!(shutdown);
        tokio::select! {
            () = &mut shutdown => {}
            joined = tasks.join_next() => {
                return match joined {
                    Some(Ok(())) | None => Err(Error::ListenerExited),
                    Some(Err(source)) => Err(Error::TaskFailed { source }),
                };
            }
        }

        info!("shutdown started; no new connections accepted");
        self.shared.accept.cancel();
        while let Some(joined) = tasks.join_next().await {
            joined.map_err(|source| Error::TaskFailed { source })?;
        }

        if timeout(
            self.shared.config.limits.drain_timeout(),
            self.shared.sessions.wait_empty(),
        )
        .await
        .is_err()
        {
            let remaining = self.shared.sessions.len();
            warn!(
                remaining,
                "drain deadline exceeded; cancelling remaining sessions"
            );
            for (id, meta) in self.shared.sessions.snapshot() {
                warn!(
                    session = id,
                    listener = %meta.listener,
                    peer = %meta.peer,
                    upstream = %meta.upstream,
                    age_ms = meta.started_at.elapsed().as_millis(),
                    "cancelling session at drain deadline"
                );
            }
            self.shared.sessions.cancel_all();
            let _ = timeout(CANCEL_GRACE, self.shared.sessions.wait_empty()).await;
        }
        info!(
            active = self.shared.sessions.len(),
            ip_entries = self.shared.ip_states.len(),
            policy_version = %self.shared.policy.current().version(),
            "relay stopped; resources released"
        );
        Ok(())
    }
}

/// Handle for reloading policy while the relay owns the listener tasks.
#[derive(Clone)]
pub struct RelayHandle {
    shared: Arc<Shared>,
}

impl RelayHandle {
    /// Loads and validates `path`, then applies the reloadable settings.
    ///
    /// The whole document is parsed and validated first. A document that
    /// changes a setting that requires a restart, or that cannot be parsed, is
    /// rejected and the last valid policy stays in effect.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError::Config`] when the file is invalid,
    /// [`ReloadError::RequiresRestart`] when a fixed setting changed, and
    /// [`ReloadError::Task`] when the blocking load task failed.
    pub async fn reload(&self, path: impl AsRef<Path>) -> Result<ReloadOutcome, ReloadError> {
        let _guard = self.shared.reload.lock().await;
        let path = path.as_ref().to_path_buf();
        let next = tokio::task::spawn_blocking(move || Config::load(path)).await??;
        self.shared.apply_reload(&next)
    }
}

/// Summary of one successful policy reload.
#[derive(Debug, Clone)]
pub struct ReloadOutcome {
    /// Version that was active before the reload.
    pub previous_version: PolicyVersion,
    /// Version that is active after the reload.
    pub version: PolicyVersion,
    /// Sessions cancelled because the new policy or listener state rejects them.
    pub cancelled: usize,
    /// Listeners stopped by this reload.
    pub stopped: Vec<String>,
    /// Listeners resumed by this reload.
    pub resumed: Vec<String>,
}

/// Failure while reloading policy.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReloadError {
    /// The document could not be read, parsed, or validated.
    Config(config::Error),
    /// A setting that cannot change at runtime was modified.
    RequiresRestart {
        /// Dotted key of the first fixed setting that changed.
        key: String,
    },
    /// The blocking configuration load task failed.
    Task(tokio::task::JoinError),
}

impl From<config::Error> for ReloadError {
    fn from(error: config::Error) -> Self {
        Self::Config(error)
    }
}

impl From<tokio::task::JoinError> for ReloadError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Task(error)
    }
}

impl std::fmt::Display for ReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "cannot load configuration: {error}"),
            Self::RequiresRestart { key } => {
                write!(formatter, "{key} changed; restart the guard to apply it")
            }
            Self::Task(source) => write!(formatter, "configuration load task failed: {source}"),
        }
    }
}

impl std::error::Error for ReloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Task(source) => Some(source),
            Self::RequiresRestart { .. } => None,
        }
    }
}

/// Relay startup failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A listener address could not be bound.
    Bind {
        /// Operator-facing listener name.
        listener: String,
        /// Address that failed to bind.
        address: SocketAddr,
        /// Underlying bind failure.
        source: io::Error,
    },
    /// A listener references an upstream that does not exist.
    UnknownUpstream {
        /// Operator-facing listener name.
        listener: String,
        /// Upstream name that was not found.
        upstream: String,
    },
    /// A listener task ended unexpectedly.
    TaskFailed {
        /// Task failure reported by Tokio.
        source: tokio::task::JoinError,
    },
    /// Every listener task ended before a shutdown was requested.
    ListenerExited,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bind {
                listener,
                address,
                source,
            } => write!(
                formatter,
                "listener {listener}: cannot bind {address}: {source}"
            ),
            Self::UnknownUpstream { listener, upstream } => write!(
                formatter,
                "listener {listener}: unknown upstream \"{upstream}\""
            ),
            Self::TaskFailed { source } => write!(formatter, "listener task failed: {source}"),
            Self::ListenerExited => formatter.write_str("listener tasks ended unexpectedly"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind { source, .. } => Some(source),
            Self::TaskFailed { source } => Some(source),
            Self::UnknownUpstream { .. } | Self::ListenerExited => None,
        }
    }
}

/// State shared by every listener.
struct Shared {
    config: Arc<Config>,
    policy: Arc<PolicyStore>,
    listener_gates: HashMap<String, Arc<ListenerGate>>,
    reload: tokio::sync::Mutex<()>,
    inbound: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    sessions: Arc<Registry>,
    ip_states: Arc<IpStates>,
    accept: CancellationToken,
    stats: Arc<Stats>,
}

impl Shared {
    fn new(config: &Arc<Config>) -> Self {
        let policy = Arc::new(PolicyStore::new(config));
        let listener_gates = config
            .listeners
            .iter()
            .map(|listener| {
                (
                    listener.name.clone(),
                    Arc::new(ListenerGate::new(listener.enabled)),
                )
            })
            .collect();
        let stats = Arc::new(Stats::default());
        stats
            .policy_version
            .store(policy.current().version().get(), Ordering::Relaxed);
        Self {
            config: Arc::clone(config),
            policy,
            listener_gates,
            reload: tokio::sync::Mutex::new(()),
            inbound: Arc::new(Semaphore::new(config.limits.inbound_connections())),
            outbound: Arc::new(Semaphore::new(config.limits.max_outbound_connections)),
            sessions: Arc::new(Registry::new(config.limits.max_sessions)),
            ip_states: Arc::new(IpStates::new(
                config.limits.max_ip_entries,
                config.limits.ip_entry_ttl(),
            )),
            accept: CancellationToken::new(),
            stats,
        }
    }

    /// Returns the stop/resume gate for a configured listener.
    fn listener_gate(&self, name: &str) -> Arc<ListenerGate> {
        Arc::clone(
            self.listener_gates
                .get(name)
                .expect("every configured listener has a gate"),
        )
    }

    /// Installs the reloadable settings from `next` and stops sessions that the
    /// new policy or listener state rejects.
    ///
    /// Listener gates and the policy snapshot are updated before the registry
    /// is scanned. A session that registers concurrently either re-checks the
    /// new state under the registry lock or is already visible to the scan, so
    /// neither path can keep a blocked peer or a stopped listener connected.
    fn apply_reload(&self, next: &Config) -> Result<ReloadOutcome, ReloadError> {
        if let Some(key) = self.config.non_reloadable_change(next) {
            return Err(ReloadError::RequiresRestart { key });
        }
        let runtime = RuntimePolicy::from_config(next);
        let previous_version = self.policy.current().version();
        let mut gates = Vec::with_capacity(next.listeners.len());
        for listener in &next.listeners {
            let gate = self.listener_gates.get(&listener.name).ok_or_else(|| {
                ReloadError::RequiresRestart {
                    key: format!("listeners.{}", listener.name),
                }
            })?;
            gates.push((listener, gate));
        }
        let mut stopped = Vec::new();
        let mut resumed = Vec::new();
        for (listener, gate) in gates {
            if gate.set_enabled(listener.enabled) {
                if listener.enabled {
                    resumed.push(listener.name.clone());
                } else {
                    stopped.push(listener.name.clone());
                }
            }
        }

        self.policy.replace(runtime);
        let current = self.policy.current();
        let cancelled = self.sessions.cancel_if(|meta| {
            !current.allows(meta.peer.ip())
                || self
                    .listener_gates
                    .get(&meta.listener)
                    .is_none_or(|gate| gate.is_stopped())
        });
        self.stats
            .policy_version
            .store(current.version().get(), Ordering::Relaxed);
        Ok(ReloadOutcome {
            previous_version,
            version: current.version(),
            cancelled,
            stopped,
            resumed,
        })
    }

    /// Tries to reserve one session slot without waiting.
    ///
    /// Waiting is never allowed here: a queued accept would keep the socket
    /// and its task alive while the proxy is already at its limit.
    fn try_acquire(&self, mode: ListenerMode, listener: &Arc<Semaphore>) -> Option<Permits> {
        let mode_budget = match mode {
            ListenerMode::Inbound => &self.inbound,
            ListenerMode::Outbound => &self.outbound,
        };
        let mode_permit = Arc::clone(mode_budget).try_acquire_owned().ok()?;
        let listener_permit = Arc::clone(listener).try_acquire_owned().ok()?;
        Some(Permits {
            _mode: mode_permit,
            _listener: listener_permit,
        })
    }
}

/// Session slots held for as long as a session task runs.
///
/// The fields are only kept for their drop behavior.
struct Permits {
    _mode: OwnedSemaphorePermit,
    _listener: OwnedSemaphorePermit,
}

/// Per-listener runtime state.
struct ListenerContext {
    shared: Arc<Shared>,
    name: String,
    mode: ListenerMode,
    upstream: Endpoint,
    permits: Arc<Semaphore>,
    bandwidth: Option<BandwidthLimiter>,
    gate: Arc<ListenerGate>,
    rate: TokenBucket,
    l7: Option<l7::L7Context>,
}

impl ListenerContext {
    fn new(shared: Arc<Shared>, listener: &config::Listener, upstream: Endpoint) -> Self {
        let limits = &shared.config.limits;
        let gate = shared.listener_gate(&listener.name);
        Self {
            name: listener.name.clone(),
            mode: listener.mode,
            upstream,
            permits: Arc::new(Semaphore::new(limits.max_connections_per_listener)),
            bandwidth: BandwidthLimiter::new(limits.bandwidth_limit_bytes_per_second),
            gate,
            rate: TokenBucket::new(),
            l7: listener.l7.as_ref().map(l7::L7Context::new),
            shared,
        }
    }
}

/// Admission slots granted to one accepted connection.
struct Admission {
    permits: Permits,
    ip_permit: Option<IpPermit>,
    session: SessionGuard,
}

/// Why an accepted connection was not admitted.
enum Rejection {
    Stopped,
    Policy,
    PerIp(IpError),
    Rate,
    Budget,
    Sessions(RegisterError),
}

impl ListenerContext {
    /// Runs every admission check without waiting.
    ///
    /// A connection is admitted only when the listener is running, the policy
    /// allows it, the per-source budget and new-connection rate have room, the
    /// connection budgets have room, and the session registry accepts it. Any
    /// guard acquired before a later rejection is dropped here, so a rejected
    /// socket never keeps a slot.
    fn try_admit(&self, peer: SocketAddr) -> Result<Admission, Rejection> {
        if self.gate.is_stopped() {
            return Err(Rejection::Stopped);
        }
        let policy = self.shared.policy.current();
        if !policy.allows(peer.ip()) {
            return Err(Rejection::Policy);
        }
        // Check the per-source budget before the listener-wide rate bucket so
        // one source that is already at its limit cannot drain tokens that
        // other sources need.
        let ip_permit = if self.mode == ListenerMode::Inbound {
            Some(
                self.shared
                    .ip_states
                    .try_acquire(peer.ip(), policy.max_connections_per_ip())
                    .map_err(Rejection::PerIp)?,
            )
        } else {
            None
        };
        if !self.rate.try_acquire(
            policy.max_new_connections_per_second(),
            policy.new_connection_burst(),
        ) {
            return Err(Rejection::Rate);
        }
        let permits = self
            .shared
            .try_acquire(self.mode, &self.permits)
            .ok_or(Rejection::Budget)?;
        let meta = SessionMeta {
            listener: self.name.clone(),
            peer,
            upstream: self.upstream.clone(),
            cancel: CancellationToken::new(),
            started_at: Instant::now(),
        };
        let session = self
            .shared
            .sessions
            .register(&self.shared.policy, &self.gate, meta)
            .map_err(Rejection::Sessions)?;
        Ok(Admission {
            permits,
            ip_permit,
            session,
        })
    }
}

/// Accepts connections until shutdown is requested.
async fn serve_listener(context: Arc<ListenerContext>, listener: TcpListener) {
    let mut backoff = Backoff::new();
    loop {
        let accepted = tokio::select! {
            biased;
            () = context.shared.accept.cancelled() => break,
            result = listener.accept() => result,
        };
        let (stream, peer) = match accepted {
            Ok(accepted) => {
                backoff.reset();
                accepted
            }
            Err(error) => {
                context
                    .shared
                    .stats
                    .accept_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    listener = %context.name,
                    error = %error,
                    "accept failed; retrying after backoff"
                );
                sleep(backoff.next_delay()).await;
                continue;
            }
        };
        if context.shared.accept.is_cancelled() {
            break;
        }
        if context.gate.is_stopped() {
            record_rejection(&context, peer, &Rejection::Stopped);
            continue;
        }

        let admission = match context.try_admit(peer) {
            Ok(admission) => admission,
            Err(rejection) => {
                record_rejection(&context, peer, &rejection);
                continue;
            }
        };

        context
            .shared
            .stats
            .accepted
            .fetch_add(1, Ordering::Relaxed);
        let Admission {
            permits,
            ip_permit,
            session,
        } = admission;
        let context = Arc::clone(&context);
        tokio::spawn(async move {
            handle_session(context, stream, peer, permits, ip_permit, session).await;
        });
    }
}

/// Counts and logs one rejected connection.
///
/// Every rejection carries the same `reason` field so a log pipeline can group
/// them without parsing the message. The policy version is attached only when
/// debug logging is enabled.
fn record_rejection(context: &ListenerContext, peer: SocketAddr, rejection: &Rejection) {
    let stats = &context.shared.stats;
    match rejection {
        Rejection::Stopped => {
            stats.rejected_stopped.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "stopped",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Policy => {
            stats.rejected_policy.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "policy",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::PerIp(error) => {
            stats.rejected_per_ip.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "per_ip",
                detail = %error,
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Rate => {
            stats.rejected_rate.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "rate",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Budget => {
            stats.rejected_budget.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "budget",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Sessions(RegisterError::Rejected) => {
            stats.rejected_policy.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "policy",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Sessions(RegisterError::Stopped) => {
            stats.rejected_stopped.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "stopped",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
        Rejection::Sessions(RegisterError::Full) => {
            stats.rejected_sessions.fetch_add(1, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                reason = "sessions",
                version = %context.shared.policy.current().version(),
                "connection rejected"
            );
        }
    }
}

/// Runs one session and always returns its permits and registry entry.
async fn handle_session(
    context: Arc<ListenerContext>,
    stream: TcpStream,
    peer: SocketAddr,
    permits: Permits,
    ip_permit: Option<IpPermit>,
    session: SessionGuard,
) {
    let started = session.started_at();
    let outcome = tokio::select! {
        biased;
        () = session.cancel().cancelled() => Err(SessionError::Cancelled),
        result = relay(context.as_ref(), stream, peer) => result,
    };
    record_outcome(&context, peer, started, &session, outcome);
    // Return the admission slots before the registry entry, so a drain that
    // observes an empty registry also observes released budgets.
    drop(ip_permit);
    drop(permits);
    drop(session);
}

/// Counts and logs how one session ended.
fn record_outcome(
    context: &ListenerContext,
    peer: SocketAddr,
    started: Instant,
    session: &SessionGuard,
    outcome: Result<(u64, u64), SessionError>,
) {
    match outcome {
        Ok((to_upstream, to_client)) => {
            context
                .shared
                .stats
                .completed
                .fetch_add(1, Ordering::Relaxed);
            context
                .shared
                .stats
                .bytes_to_upstream
                .fetch_add(to_upstream, Ordering::Relaxed);
            context
                .shared
                .stats
                .bytes_to_client
                .fetch_add(to_client, Ordering::Relaxed);
            debug!(
                listener = %context.name,
                %peer,
                to_upstream,
                to_client,
                elapsed_ms = started.elapsed().as_millis(),
                version = %context.shared.policy.current().version(),
                "session closed"
            );
        }
        Err(error) => {
            let elapsed_ms = started.elapsed().as_millis();
            match &error {
                SessionError::Connect(ConnectError::Timeout) => {
                    context
                        .shared
                        .stats
                        .connect_timeout
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        listener = %context.name,
                        %peer,
                        reason = "connect_timeout",
                        error = %error,
                        elapsed_ms,
                        version = %context.shared.policy.current().version(),
                        "session failed"
                    );
                }
                SessionError::Connect(ConnectError::Failed(_)) => {
                    context
                        .shared
                        .stats
                        .connect_failed
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        listener = %context.name,
                        %peer,
                        reason = "connect_failed",
                        error = %error,
                        elapsed_ms,
                        version = %context.shared.policy.current().version(),
                        "session failed"
                    );
                }
                SessionError::Copy(_) => {
                    context
                        .shared
                        .stats
                        .copy_failed
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        listener = %context.name,
                        %peer,
                        reason = "copy",
                        error = %error,
                        elapsed_ms,
                        version = %context.shared.policy.current().version(),
                        "session ended with copy error"
                    );
                }
                SessionError::Cancelled => {
                    context
                        .shared
                        .stats
                        .cancelled
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        listener = %context.name,
                        %peer,
                        session = session.id(),
                        reason = "cancelled",
                        elapsed_ms,
                        version = %context.shared.policy.current().version(),
                        "session cancelled"
                    );
                }
            }
        }
    }
}

/// Serves one session with raw byte relay or L7 inspection.
async fn relay(
    context: &ListenerContext,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> Result<(u64, u64), SessionError> {
    if let Some(state) = context.l7.as_ref() {
        return relay_l7(context, state, &mut stream, peer).await;
    }
    let mut upstream = connect_endpoint(
        &context.upstream,
        context.shared.config.limits.connect_timeout(),
    )
    .await
    .map_err(SessionError::Connect)?;
    copy_streams(&mut stream, &mut upstream, context.bandwidth.as_ref())
        .await
        .map_err(SessionError::Copy)
}

/// Inspects one HTTP request and relays it when the policy allows.
async fn relay_l7(
    context: &ListenerContext,
    inspector: &l7::L7Context,
    stream: &mut TcpStream,
    peer: SocketAddr,
) -> Result<(u64, u64), SessionError> {
    let policy = context.shared.policy.current();
    let outcome = l7::serve(
        stream,
        &context.upstream,
        context.shared.config.limits.connect_timeout(),
        inspector,
        &policy,
        context.bandwidth.as_ref(),
    )
    .await
    .map_err(|error| match error {
        l7::Error::Connect(error) => SessionError::Connect(error),
        l7::Error::Io(error) => SessionError::Copy(error),
    })?;
    let stats = &context.shared.stats;
    if outcome.forwarded || outcome.reject.is_some() {
        stats.l7_requests.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(reject) = outcome.reject {
        record_l7_rejection(context, peer, reject);
    }
    if outcome.forwarded {
        stats.l7_forwarded.fetch_add(1, Ordering::Relaxed);
    }
    Ok((outcome.to_upstream, outcome.to_client))
}

/// Counts and logs one rejected L7 request.
fn record_l7_rejection(context: &ListenerContext, peer: SocketAddr, reject: l7::Reject) {
    let stats = &context.shared.stats;
    let counter = match reject {
        l7::Reject::Malformed => &stats.l7_rejected_malformed,
        l7::Reject::Header => &stats.l7_rejected_header,
        l7::Reject::Body => &stats.l7_rejected_body,
        l7::Reject::Timeout => &stats.l7_rejected_timeout,
        l7::Reject::Method => &stats.l7_rejected_method,
        l7::Reject::Encoding => &stats.l7_rejected_encoding,
        l7::Reject::Upgrade => &stats.l7_rejected_upgrade,
        l7::Reject::Json => &stats.l7_rejected_json,
        l7::Reject::Policy => &stats.l7_rejected_policy,
        l7::Reject::Quota => &stats.l7_rejected_quota,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    debug!(
        listener = %context.name,
        %peer,
        reason = reject.reason(),
        version = %context.shared.policy.current().version(),
        "rpc request rejected"
    );
}

/// Failure while serving one session.
enum SessionError {
    Connect(ConnectError),
    Copy(io::Error),
    Cancelled,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(error) => error.fmt(formatter),
            Self::Copy(error) => write!(formatter, "copy failed: {error}"),
            Self::Cancelled => formatter.write_str("session cancelled"),
        }
    }
}

/// Failure while establishing the upstream connection.
#[derive(Debug)]
pub(crate) enum ConnectError {
    /// The connect deadline elapsed before the socket was ready.
    Timeout,
    /// The socket could not be opened.
    Failed(io::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("connect deadline exceeded"),
            Self::Failed(error) => write!(formatter, "connect failed: {error}"),
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Timeout => None,
        }
    }
}

/// Resolves and connects to `endpoint` within `deadline`.
pub(crate) async fn connect_endpoint(
    endpoint: &Endpoint,
    deadline: Duration,
) -> Result<TcpStream, ConnectError> {
    with_deadline(
        deadline,
        TcpStream::connect((endpoint.host(), endpoint.port())),
    )
    .await
}

/// Applies a connect deadline to a future that opens a socket.
async fn with_deadline<T, F>(deadline: Duration, future: F) -> Result<T, ConnectError>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout(deadline, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(source)) => Err(ConnectError::Failed(source)),
        Err(_) => Err(ConnectError::Timeout),
    }
}

/// Copies bytes in both directions until both halves close.
///
/// Without a bandwidth budget this delegates to `copy_bidirectional`, which
/// uses 8 KiB buffers and propagates half-close. With a budget, the same
/// semantics are implemented with explicit 8 KiB buffers and read delays.
async fn copy_streams<A, B>(
    client: &mut A,
    upstream: &mut B,
    bandwidth: Option<&BandwidthLimiter>,
) -> io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let Some(bandwidth) = bandwidth else {
        return tokio::io::copy_bidirectional(client, upstream).await;
    };

    let (mut client_reader, mut client_writer) = tokio::io::split(client);
    let (mut upstream_reader, mut upstream_writer) = tokio::io::split(upstream);
    let to_upstream = copy_direction(&mut client_reader, &mut upstream_writer, bandwidth);
    let to_client = copy_direction(&mut upstream_reader, &mut client_writer, bandwidth);
    tokio::try_join!(to_upstream, to_client)
}

/// Copies one direction, delaying reads to honor the byte budget.
///
/// Bytes are never dropped. Waiting before the next read applies backpressure
/// to the sender, which is the only safe way to throttle an encrypted stream.
async fn copy_direction<R, W>(
    reader: &mut R,
    writer: &mut W,
    bandwidth: &BandwidthLimiter,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
    let mut total = 0_u64;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bandwidth.acquire(read).await;
        writer.write_all(&buffer[..read]).await?;
        total += read as u64;
    }
    if let Err(error) = writer.shutdown().await {
        if error.kind() != io::ErrorKind::NotConnected {
            return Err(error);
        }
    }
    Ok(total)
}

/// Listener-wide byte budget shared by every session and both directions.
///
/// Tokens refill continuously. A read waits until enough tokens exist for the
/// chunk it just read, so data already read is never discarded.
pub(crate) struct BandwidthLimiter {
    bytes_per_second: u64,
    capacity: u64,
    state: tokio::sync::Mutex<Bucket>,
}

struct Bucket {
    tokens: u64,
    updated: Instant,
}

impl BandwidthLimiter {
    /// Returns a limiter, or `None` when the budget is unlimited.
    fn new(bytes_per_second: u64) -> Option<Self> {
        if bytes_per_second == 0 {
            return None;
        }
        let capacity = bytes_per_second.max(COPY_BUFFER_SIZE as u64);
        Some(Self {
            bytes_per_second,
            capacity,
            state: tokio::sync::Mutex::new(Bucket {
                tokens: capacity,
                updated: Instant::now(),
            }),
        })
    }

    /// Waits until `bytes` may be written to the other side.
    pub(crate) async fn acquire(&self, bytes: usize) {
        debug_assert!(bytes <= COPY_BUFFER_SIZE);
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        loop {
            let wait = {
                let mut bucket = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(bucket.updated);
                let refill = elapsed.as_nanos() * u128::from(self.bytes_per_second) / 1_000_000_000;
                if refill > 0 {
                    let refill = u64::try_from(refill).unwrap_or(u64::MAX);
                    bucket.tokens = bucket.tokens.saturating_add(refill).min(self.capacity);
                    bucket.updated = now;
                }
                if bucket.tokens >= bytes {
                    bucket.tokens -= bytes;
                    return;
                }
                let deficit = bytes - bucket.tokens;
                let nanos = u128::from(deficit) * 1_000_000_000 / u128::from(self.bytes_per_second);
                Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX).max(1))
            };
            sleep(wait).await;
        }
    }
}

/// Exponential retry delay for repeated accept errors.
struct Backoff {
    current: Duration,
}

impl Backoff {
    const fn new() -> Self {
        Self {
            current: MIN_ACCEPT_BACKOFF,
        }
    }

    const fn reset(&mut self) {
        self.current = MIN_ACCEPT_BACKOFF;
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_ACCEPT_BACKOFF);
        delay
    }
}

/// Counters maintained by the relay.
#[derive(Debug, Default)]
struct Stats {
    accepted: AtomicU64,
    rejected_policy: AtomicU64,
    rejected_budget: AtomicU64,
    rejected_per_ip: AtomicU64,
    rejected_rate: AtomicU64,
    rejected_stopped: AtomicU64,
    rejected_sessions: AtomicU64,
    connect_failed: AtomicU64,
    connect_timeout: AtomicU64,
    copy_failed: AtomicU64,
    cancelled: AtomicU64,
    completed: AtomicU64,
    accept_errors: AtomicU64,
    bytes_to_upstream: AtomicU64,
    bytes_to_client: AtomicU64,
    policy_version: AtomicU64,
    l7_requests: AtomicU64,
    l7_forwarded: AtomicU64,
    l7_rejected_malformed: AtomicU64,
    l7_rejected_header: AtomicU64,
    l7_rejected_body: AtomicU64,
    l7_rejected_timeout: AtomicU64,
    l7_rejected_method: AtomicU64,
    l7_rejected_encoding: AtomicU64,
    l7_rejected_upgrade: AtomicU64,
    l7_rejected_json: AtomicU64,
    l7_rejected_policy: AtomicU64,
    l7_rejected_quota: AtomicU64,
}

/// A point-in-time copy of relay counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Sessions that are registered and relaying right now.
    pub active_sessions: u64,
    /// Sessions that passed policy and budget checks.
    pub accepted: u64,
    /// Connections rejected by the address policy.
    pub rejected_policy: u64,
    /// Connections rejected because no session slot was free.
    pub rejected_budget: u64,
    /// Connections rejected by the per-source session limit.
    pub rejected_per_ip: u64,
    /// Connections rejected by the new-connection rate limit.
    pub rejected_rate: u64,
    /// Connections rejected because their listener was stopped.
    pub rejected_stopped: u64,
    /// Connections rejected because the session registry was full.
    pub rejected_sessions: u64,
    /// Upstream connections that failed.
    pub connect_failed: u64,
    /// Upstream connections that exceeded the connect deadline.
    pub connect_timeout: u64,
    /// Sessions that ended with a copy error.
    pub copy_failed: u64,
    /// Sessions stopped by a cancellation token.
    pub cancelled: u64,
    /// Sessions that completed normally.
    pub completed: u64,
    /// Accept calls that returned an error.
    pub accept_errors: u64,
    /// Bytes copied from clients to upstreams.
    pub bytes_to_upstream: u64,
    /// Bytes copied from upstreams to clients.
    pub bytes_to_client: u64,
    /// Version of the policy that is currently active.
    pub policy_version: u64,
    /// HTTP requests inspected on L7 listeners.
    pub l7_requests: u64,
    /// HTTP requests forwarded to an upstream.
    pub l7_forwarded: u64,
    /// L7 requests rejected as malformed.
    pub l7_rejected_malformed: u64,
    /// L7 requests rejected for exceeding the header limit.
    pub l7_rejected_header: u64,
    /// L7 requests rejected for exceeding the body limit.
    pub l7_rejected_body: u64,
    /// L7 requests rejected because receiving them timed out.
    pub l7_rejected_timeout: u64,
    /// L7 requests rejected because the HTTP method is unsupported.
    pub l7_rejected_method: u64,
    /// L7 requests rejected for using an unsupported encoding.
    pub l7_rejected_encoding: u64,
    /// L7 requests rejected because the client attempted an upgrade.
    pub l7_rejected_upgrade: u64,
    /// L7 requests rejected because the JSON was invalid or too large.
    pub l7_rejected_json: u64,
    /// L7 requests rejected by the method policy or emergency block.
    pub l7_rejected_policy: u64,
    /// L7 requests rejected by a per-method quota.
    pub l7_rejected_quota: u64,
}

impl Stats {
    fn snapshot(&self, active_sessions: u64) -> StatsSnapshot {
        StatsSnapshot {
            active_sessions,
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected_policy: self.rejected_policy.load(Ordering::Relaxed),
            rejected_budget: self.rejected_budget.load(Ordering::Relaxed),
            rejected_per_ip: self.rejected_per_ip.load(Ordering::Relaxed),
            rejected_rate: self.rejected_rate.load(Ordering::Relaxed),
            rejected_stopped: self.rejected_stopped.load(Ordering::Relaxed),
            rejected_sessions: self.rejected_sessions.load(Ordering::Relaxed),
            connect_failed: self.connect_failed.load(Ordering::Relaxed),
            connect_timeout: self.connect_timeout.load(Ordering::Relaxed),
            copy_failed: self.copy_failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            accept_errors: self.accept_errors.load(Ordering::Relaxed),
            bytes_to_upstream: self.bytes_to_upstream.load(Ordering::Relaxed),
            bytes_to_client: self.bytes_to_client.load(Ordering::Relaxed),
            policy_version: self.policy_version.load(Ordering::Relaxed),
            l7_requests: self.l7_requests.load(Ordering::Relaxed),
            l7_forwarded: self.l7_forwarded.load(Ordering::Relaxed),
            l7_rejected_malformed: self.l7_rejected_malformed.load(Ordering::Relaxed),
            l7_rejected_header: self.l7_rejected_header.load(Ordering::Relaxed),
            l7_rejected_body: self.l7_rejected_body.load(Ordering::Relaxed),
            l7_rejected_timeout: self.l7_rejected_timeout.load(Ordering::Relaxed),
            l7_rejected_method: self.l7_rejected_method.load(Ordering::Relaxed),
            l7_rejected_encoding: self.l7_rejected_encoding.load(Ordering::Relaxed),
            l7_rejected_upgrade: self.l7_rejected_upgrade.load(Ordering::Relaxed),
            l7_rejected_json: self.l7_rejected_json.load(Ordering::Relaxed),
            l7_rejected_policy: self.l7_rejected_policy.load(Ordering::Relaxed),
            l7_rejected_quota: self.l7_rejected_quota.load(Ordering::Relaxed),
        }
    }
}

/// Shared handle for reading relay counters from outside the runtime.
#[derive(Debug, Clone)]
pub struct StatsHandle {
    stats: Arc<Stats>,
    sessions: Arc<Registry>,
}

impl StatsHandle {
    /// Creates a handle over the counters and the session registry.
    ///
    /// The registry is the authority for the active-session gauge, so the
    /// gauge cannot drift when a session task ends in an unexpected way.
    #[must_use]
    fn new(stats: Arc<Stats>, sessions: Arc<Registry>) -> Self {
        Self { stats, sessions }
    }

    /// Returns a point-in-time copy of the counters.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        let active = u64::try_from(self.sessions.len()).unwrap_or(u64::MAX);
        self.stats.snapshot(active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::path::Path;
    use std::str::FromStr;
    use tokio::task::JoinHandle;

    const DEFAULT_LIMITS: &str = "\
max_connections = 8
max_connections_per_listener = 8
max_outbound_connections = 2
max_sessions = 16
";

    fn test_config_text(upstream: SocketAddr, limits: &str) -> String {
        format!(
            r#"
[limits]
{limits}

[policy]
allow = ["127.0.0.1/32", "::1/128"]

[[upstreams]]
name = "node"
address = "{upstream}"

[[listeners]]
name = "test"
listen = "127.0.0.1:26657"
upstream = "node"
"#
        )
    }

    fn test_config(upstream: SocketAddr, limits: &str) -> Arc<Config> {
        Arc::new(
            Config::parse(&test_config_text(upstream, limits), Path::new("test.toml"))
                .expect("test configuration"),
        )
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("l4guard-{}-{label}-{id}.toml", std::process::id()))
    }

    fn budget_config(global: usize, per_listener: usize, outbound: usize) -> Arc<Config> {
        let limits = format!(
            "max_connections = {global}\nmax_connections_per_listener = {per_listener}\nmax_outbound_connections = {outbound}\nmax_sessions = {global}\n"
        );
        test_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 1)), &limits)
    }

    struct TestRelay {
        address: SocketAddr,
        stats: StatsHandle,
        shared: Arc<Shared>,
        _task: JoinHandle<()>,
    }

    async fn start_relay(config: Arc<Config>, upstream: Endpoint) -> TestRelay {
        let shared = Arc::new(Shared::new(&config));
        let listener_config = config.listeners[0].clone();
        let context = Arc::new(ListenerContext::new(
            Arc::clone(&shared),
            &listener_config,
            upstream,
        ));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address");
        let stats = StatsHandle::new(Arc::clone(&shared.stats), Arc::clone(&shared.sessions));
        let task = tokio::spawn(serve_listener(context, listener));
        TestRelay {
            address,
            stats,
            shared,
            _task: task,
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..100 {
            if condition() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("condition was not met");
    }

    struct RunningRelay {
        address: SocketAddr,
        shared: Arc<Shared>,
        stop: tokio::sync::oneshot::Sender<()>,
        task: JoinHandle<Result<(), Error>>,
    }

    async fn start_running_relay(config: Arc<Config>, upstream: Endpoint) -> RunningRelay {
        let shared = Arc::new(Shared::new(&config));
        let context = Arc::new(ListenerContext::new(
            Arc::clone(&shared),
            &config.listeners[0],
            upstream,
        ));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address");
        let relay = Relay {
            shared: Arc::clone(&shared),
            listeners: vec![(context, listener)],
        };
        let (stop, wait) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(relay.run(async move {
            let _ = wait.await;
        }));
        RunningRelay {
            address,
            shared,
            stop,
            task,
        }
    }

    async fn spawn_echo_upstream() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind echo upstream");
        let address = listener.local_addr().expect("echo address");
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.split();
                    let _ = tokio::io::copy(&mut reader, &mut writer).await;
                    let _ = writer.shutdown().await;
                });
            }
        });
        address
    }

    async fn spawn_half_close_upstream() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind half-close upstream");
        let address = listener.local_addr().expect("half-close address");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .await
                .expect("read request");
            assert_eq!(request, b"hello");
            stream.write_all(b"done").await.expect("write response");
            stream.shutdown().await.expect("shutdown response");
        });
        address
    }

    #[tokio::test]
    async fn bind_failure_stops_startup() {
        let occupied = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind occupied address");
        let address = occupied.local_addr().expect("occupied address");
        let text = format!(
            r#"
[policy]
allow = ["127.0.0.1/32"]

[[upstreams]]
name = "node"
address = "127.0.0.1:1"

[[listeners]]
name = "test"
listen = "{address}"
upstream = "node"
"#
        );
        let config =
            Arc::new(Config::parse(&text, Path::new("test.toml")).expect("test configuration"));

        let result = Relay::bind(config).await;

        assert!(matches!(result, Err(Error::Bind { .. })));
    }

    #[tokio::test]
    async fn preserves_half_close() {
        let upstream = spawn_half_close_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"hello").await.expect("write request");
        client.shutdown().await.expect("half-close request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");

        assert_eq!(response, b"done");
        assert_eq!(relay.stats.snapshot().completed, 1);
    }

    #[tokio::test]
    async fn preserves_half_close_with_bandwidth_limit() {
        let upstream = spawn_half_close_upstream().await;
        let limits = format!("{DEFAULT_LIMITS}bandwidth_limit_bytes_per_second = 1000000\n");
        let config = test_config(upstream, &limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"hello").await.expect("write request");
        client.shutdown().await.expect("half-close request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");

        assert_eq!(response, b"done");
        assert_eq!(relay.stats.snapshot().completed, 1);
    }

    #[tokio::test]
    async fn relays_large_payload_in_both_directions() {
        const SIZE: usize = 4 * 1024 * 1024;
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let (mut reader, mut writer) = TcpStream::connect(relay.address)
            .await
            .expect("connect relay")
            .into_split();
        let sender = tokio::spawn(async move {
            let chunk = vec![0x5a_u8; 64 * 1024];
            let mut sent = 0;
            while sent < SIZE {
                let remaining = SIZE - sent;
                let length = remaining.min(chunk.len());
                writer.write_all(&chunk[..length]).await.expect("write");
                sent += length;
            }
            writer.shutdown().await.expect("shutdown");
        });

        let mut received = Vec::with_capacity(SIZE);
        reader
            .read_to_end(&mut received)
            .await
            .expect("read echoed bytes");
        sender.await.expect("sender task");

        assert_eq!(received.len(), SIZE);
        assert!(received.iter().all(|byte| *byte == 0x5a));
        let snapshot = relay.stats.snapshot();
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.bytes_to_upstream, SIZE as u64);
        assert_eq!(snapshot.bytes_to_client, SIZE as u64);
    }

    #[tokio::test]
    async fn rejects_connection_when_listener_budget_is_full() {
        let upstream = spawn_echo_upstream().await;
        let limits = "\
max_connections = 8
max_connections_per_listener = 1
max_outbound_connections = 2
max_sessions = 16
";
        let config = test_config(upstream, limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let _first = TcpStream::connect(relay.address)
            .await
            .expect("first connect");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        let mut second = TcpStream::connect(relay.address)
            .await
            .expect("second connect");
        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), second.read(&mut buffer))
            .await
            .expect("rejection timeout")
            .expect("read after rejection");

        assert_eq!(read, 0, "second connection must be closed immediately");
        assert_eq!(relay.stats.snapshot().rejected_budget, 1);
    }

    #[tokio::test]
    async fn rejects_connection_when_ip_budget_is_full() {
        let upstream = spawn_echo_upstream().await;
        let limits = "\
max_connections = 8
max_connections_per_listener = 8
max_outbound_connections = 2
max_sessions = 16
max_connections_per_ip = 1
";
        let config = test_config(upstream, limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut first = TcpStream::connect(relay.address)
            .await
            .expect("first connect");
        first.write_all(b"x").await.expect("write");
        let mut echoed = [0_u8; 1];
        first.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        let mut second = TcpStream::connect(relay.address)
            .await
            .expect("second connect");
        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), second.read(&mut buffer))
            .await
            .expect("rejection timeout")
            .expect("read after rejection");

        assert_eq!(read, 0, "second connection must be closed immediately");
        assert_eq!(relay.stats.snapshot().rejected_per_ip, 1);
    }

    #[tokio::test]
    async fn policy_change_cancels_existing_sessions() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        client.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        let mut next = (*relay.shared.config).clone();
        next.policy.allow = Vec::new();
        relay.shared.apply_reload(&next).expect("policy reload");

        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), client.read(&mut buffer))
            .await
            .expect("cancellation timeout")
            .expect("read after cancellation");
        assert_eq!(read, 0, "cancelled session must close the client");
        timeout(Duration::from_secs(1), relay.shared.sessions.wait_empty())
            .await
            .expect("registry drain");
        assert_eq!(relay.stats.snapshot().cancelled, 1);
    }

    #[tokio::test]
    async fn emergency_block_cancels_existing_sessions_and_rejects_new() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        client.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        let mut next = (*relay.shared.config).clone();
        next.policy.emergency_block = true;
        let outcome = relay.shared.apply_reload(&next).expect("emergency reload");
        assert_eq!(outcome.cancelled, 1);

        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), client.read(&mut buffer))
            .await
            .expect("cancellation timeout")
            .expect("read after cancellation");
        assert_eq!(read, 0);
        timeout(Duration::from_secs(1), relay.shared.sessions.wait_empty())
            .await
            .expect("registry drain");

        let mut late = TcpStream::connect(relay.address)
            .await
            .expect("late connect");
        let read = timeout(Duration::from_secs(1), late.read(&mut buffer))
            .await
            .expect("rejection timeout")
            .expect("read after rejection");
        assert_eq!(read, 0);
        assert_eq!(relay.stats.snapshot().rejected_policy, 1);
    }

    #[tokio::test]
    async fn stopping_a_listener_cancels_sessions_and_resuming_restores_service() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        client.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        let mut next = (*relay.shared.config).clone();
        next.listeners[0].enabled = false;
        let outcome = relay.shared.apply_reload(&next).expect("stop reload");
        assert_eq!(outcome.stopped, vec!["test".to_owned()]);
        assert_eq!(outcome.cancelled, 1);

        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), client.read(&mut buffer))
            .await
            .expect("cancellation timeout")
            .expect("read after cancellation");
        assert_eq!(read, 0);
        timeout(Duration::from_secs(1), relay.shared.sessions.wait_empty())
            .await
            .expect("registry drain");

        let mut late = TcpStream::connect(relay.address)
            .await
            .expect("late connect");
        let read = timeout(Duration::from_secs(1), late.read(&mut buffer))
            .await
            .expect("stopped rejection timeout")
            .expect("read after rejection");
        assert_eq!(read, 0);
        assert!(relay.stats.snapshot().rejected_stopped >= 1);

        let mut next = (*relay.shared.config).clone();
        next.listeners[0].enabled = true;
        let outcome = relay.shared.apply_reload(&next).expect("resume reload");
        assert_eq!(outcome.resumed, vec!["test".to_owned()]);

        let mut resumed = TcpStream::connect(relay.address)
            .await
            .expect("resumed connect");
        resumed.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        resumed
            .read_exact(&mut echoed)
            .await
            .expect("echo after resume");
        assert_eq!(&echoed, b"ping");
    }

    #[tokio::test]
    async fn rate_limit_rejects_connections_above_burst() {
        let upstream = spawn_echo_upstream().await;
        let limits = "\
max_connections = 8
max_connections_per_listener = 8
max_outbound_connections = 2
max_sessions = 16
max_new_connections_per_second = 1
new_connection_burst = 1
";
        let config = test_config(upstream, limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let mut first = TcpStream::connect(relay.address)
            .await
            .expect("first connect");
        first.write_all(b"x").await.expect("write");
        let mut echoed = [0_u8; 1];
        first.read_exact(&mut echoed).await.expect("echo");
        assert_eq!(relay.shared.sessions.len(), 1);

        let mut second = TcpStream::connect(relay.address)
            .await
            .expect("second connect");
        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(1), second.read(&mut buffer))
            .await
            .expect("rate rejection timeout")
            .expect("read after rejection");
        assert_eq!(read, 0);
        assert_eq!(relay.stats.snapshot().rejected_rate, 1);
    }

    #[tokio::test]
    async fn reload_rejects_fixed_settings_and_keeps_version() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;
        let before = relay.stats.snapshot().policy_version;

        let mut next = (*relay.shared.config).clone();
        next.limits.max_connections = 4;
        let error = relay
            .shared
            .apply_reload(&next)
            .expect_err("fixed setting must be rejected");

        assert!(matches!(error, ReloadError::RequiresRestart { .. }));
        assert_eq!(relay.stats.snapshot().policy_version, before);
        assert_eq!(relay.shared.policy.current().version().get(), before);
    }

    #[tokio::test]
    async fn reload_from_file_keeps_last_valid_policy_on_parse_error() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;
        let handle = RelayHandle {
            shared: Arc::clone(&relay.shared),
        };
        let before = relay.stats.snapshot().policy_version;

        let path = temp_path("invalid");
        std::fs::write(&path, "[limits]\nmax_connections = \"many\"\n").expect("write temp config");
        let result = handle.reload(&path).await;
        std::fs::remove_file(&path).expect("remove temp config");

        assert!(matches!(result, Err(ReloadError::Config(_))));
        assert_eq!(relay.stats.snapshot().policy_version, before);
    }

    #[tokio::test]
    async fn reload_from_file_applies_valid_policy_and_updates_version() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;
        let handle = RelayHandle {
            shared: Arc::clone(&relay.shared),
        };
        let before = relay.stats.snapshot().policy_version;

        let text = test_config_text(upstream, DEFAULT_LIMITS).replace(
            r#"allow = ["127.0.0.1/32", "::1/128"]"#,
            r#"allow = ["10.0.0.0/8"]"#,
        );
        let path = temp_path("valid");
        std::fs::write(&path, text).expect("write temp config");
        let outcome = handle.reload(&path).await.expect("valid reload");
        std::fs::remove_file(&path).expect("remove temp config");

        assert_ne!(outcome.version.get(), before);
        assert_eq!(outcome.version.get(), relay.stats.snapshot().policy_version);
        assert_eq!(relay.shared.policy.current().version(), outcome.version);
    }

    #[tokio::test]
    async fn shutdown_stops_accepts_and_cancels_remaining_sessions() {
        let upstream = spawn_echo_upstream().await;
        let limits = format!("{DEFAULT_LIMITS}drain_timeout_ms = 200\n");
        let config = test_config(upstream, &limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_running_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        client.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        relay.stop.send(()).expect("shutdown signal");
        wait_until(|| relay.shared.accept.is_cancelled()).await;

        // A connection attempted after shutdown began must never become a
        // session. The listener is usually already closed; if the kernel still
        // accepts it into the backlog, the guard must not serve it.
        if let Ok(mut late) = TcpStream::connect(relay.address).await {
            let mut buffer = [0_u8; 1];
            let _ = timeout(Duration::from_secs(2), late.read(&mut buffer)).await;
        }

        let result = timeout(Duration::from_secs(3), relay.task)
            .await
            .expect("shutdown timeout")
            .expect("relay task");
        result.expect("relay shutdown");

        assert!(relay.shared.sessions.is_empty());
        let snapshot = relay.shared.stats.snapshot(0);
        assert_eq!(snapshot.accepted, 1, "late connection must be rejected");
        assert_eq!(snapshot.cancelled, 1);
        assert!(
            TcpStream::connect(relay.address).await.is_err(),
            "listener must be closed after shutdown"
        );
    }

    #[tokio::test]
    async fn shutdown_waits_for_sessions_that_finish_during_drain() {
        let upstream = spawn_echo_upstream().await;
        let limits = format!("{DEFAULT_LIMITS}drain_timeout_ms = 1000\n");
        let config = test_config(upstream, &limits);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_running_relay(config, endpoint).await;

        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect relay");
        client.write_all(b"ping").await.expect("write");
        let mut echoed = [0_u8; 4];
        client.read_exact(&mut echoed).await.expect("echo");
        wait_until(|| relay.shared.sessions.len() == 1).await;

        relay.stop.send(()).expect("shutdown signal");
        wait_until(|| relay.shared.accept.is_cancelled()).await;
        drop(client);

        let result = timeout(Duration::from_secs(3), relay.task)
            .await
            .expect("shutdown timeout")
            .expect("relay task");
        result.expect("relay shutdown");

        let snapshot = relay.shared.stats.snapshot(0);
        assert_eq!(snapshot.completed, 1, "session must drain normally");
        assert_eq!(snapshot.cancelled, 0);
        assert!(relay.shared.sessions.is_empty());
    }

    #[test]
    fn outbound_budget_is_reserved_from_inbound_flood() {
        let config = budget_config(4, 4, 1);
        let shared = Shared::new(&config);
        let listener = Arc::new(Semaphore::new(4));

        let inbound = [
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .expect("first inbound"),
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .expect("second inbound"),
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .expect("third inbound"),
        ];
        assert!(
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .is_none(),
            "inbound budget must stop at max_connections - max_outbound_connections"
        );

        let outbound = shared
            .try_acquire(ListenerMode::Outbound, &listener)
            .expect("outbound slot must survive an inbound flood");
        assert!(
            shared
                .try_acquire(ListenerMode::Outbound, &listener)
                .is_none(),
            "outbound budget must have its own ceiling"
        );

        drop(inbound);
        drop(outbound);
        assert!(
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .is_some()
        );
    }

    #[test]
    fn per_listener_budget_is_enforced() {
        let config = budget_config(8, 1, 2);
        let shared = Shared::new(&config);
        let listener = Arc::new(Semaphore::new(1));

        let first = shared
            .try_acquire(ListenerMode::Inbound, &listener)
            .expect("first connection");
        assert!(
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .is_none()
        );
        drop(first);
        assert!(
            shared
                .try_acquire(ListenerMode::Inbound, &listener)
                .is_some()
        );
    }

    #[tokio::test]
    async fn connect_deadline_returns_timeout() {
        let result = with_deadline(
            Duration::from_millis(10),
            std::future::pending::<io::Result<TcpStream>>(),
        )
        .await;

        assert!(matches!(result, Err(ConnectError::Timeout)));
    }

    #[tokio::test]
    async fn reports_connection_failure() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind temporary listener");
        let address = listener.local_addr().expect("temporary address");
        drop(listener);
        let endpoint = Endpoint::from_str(&address.to_string()).expect("endpoint");

        let result = connect_endpoint(&endpoint, Duration::from_secs(1)).await;

        assert!(matches!(result, Err(ConnectError::Failed(_))));
    }

    #[test]
    fn accept_backoff_grows_and_resets() {
        let mut backoff = Backoff::new();

        assert_eq!(backoff.next_delay(), MIN_ACCEPT_BACKOFF);
        assert_eq!(backoff.next_delay(), MIN_ACCEPT_BACKOFF * 2);
        for _ in 0..32 {
            backoff.next_delay();
        }
        assert_eq!(backoff.current, MAX_ACCEPT_BACKOFF);
        backoff.reset();
        assert_eq!(backoff.next_delay(), MIN_ACCEPT_BACKOFF);
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_limiter_delays_reads() {
        let limiter = BandwidthLimiter::new(1_024).expect("bandwidth limiter");

        limiter.acquire(COPY_BUFFER_SIZE).await;
        let started = Instant::now();
        limiter.acquire(1_024).await;

        assert!(started.elapsed() >= Duration::from_secs(1));
    }

    #[test]
    fn zero_bandwidth_means_unlimited() {
        assert!(BandwidthLimiter::new(0).is_none());
    }

    #[tokio::test]
    async fn l7_forwards_an_allowed_request_as_a_new_request() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let body = r#"{"jsonrpc":"2.0","method":"eth_call","id":1}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Dropped: yes\r\n\r\n{body}",
            body.len()
        );
        let response = l7_request(relay.address, &request).await;

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "response: {response}"
        );
        let request = received.await.expect("captured request");
        let request = String::from_utf8(request).expect("request text");
        assert!(request.starts_with("POST / HTTP/1.1\r\n"));
        assert!(request.contains("Host: node\r\n"));
        assert!(request.contains("Content-Type: application/json\r\n"));
        assert!(request.contains(&format!("Content-Length: {}\r\n", body.len())));
        assert!(request.contains("Connection: close\r\n"));
        assert!(!request.contains("X-Dropped"));
        assert!(request.ends_with(body));

        wait_until(|| relay.stats.snapshot().l7_forwarded == 1).await;
        let snapshot = relay.stats.snapshot();
        assert_eq!(snapshot.l7_requests, 1);
        assert_eq!(snapshot.l7_rejected_policy, 0);
    }

    #[tokio::test]
    async fn l7_rejects_a_method_outside_the_allowlist() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let body = r#"{"jsonrpc":"2.0","method":"debug_trace","id":1}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = l7_request(relay.address, &request).await;

        assert!(
            response.starts_with("HTTP/1.1 403 Forbidden"),
            "response: {response}"
        );
        assert!(
            timeout(Duration::from_millis(100), received).await.is_err(),
            "the upstream must not be contacted"
        );
        wait_until(|| relay.stats.snapshot().l7_rejected_policy == 1).await;
    }

    #[tokio::test]
    async fn l7_rejects_a_batch_with_one_disallowed_item() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let body = r#"[{"method":"eth_call","id":1},{"method":"debug_trace","id":2}]"#;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = l7_request(relay.address, &request).await;

        assert!(
            response.starts_with("HTTP/1.1 403 Forbidden"),
            "response: {response}"
        );
        assert!(
            timeout(Duration::from_millis(100), received).await.is_err(),
            "the upstream must not be contacted"
        );
        wait_until(|| relay.stats.snapshot().l7_rejected_policy == 1).await;
    }

    #[tokio::test]
    async fn l7_rejects_upgrade_and_smuggling_shapes() {
        let (upstream, _received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let upgrade = l7_request(
            relay.address,
            "GET /status HTTP/1.1\r\nHost: node\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        )
        .await;
        assert!(upgrade.starts_with("HTTP/1.1 400 Bad Request"));

        let smuggling = l7_request(
            relay.address,
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}",
        )
        .await;
        assert!(smuggling.starts_with("HTTP/1.1 400 Bad Request"));

        wait_until(|| relay.stats.snapshot().l7_rejected_malformed == 1).await;
        let snapshot = relay.stats.snapshot();
        assert_eq!(snapshot.l7_rejected_upgrade, 1);
    }

    #[tokio::test]
    async fn l7_enforces_header_and_body_limits() {
        let (upstream, _received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "max_header_bytes = 256\nmax_body_bytes = 8\n");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let large_header = format!(
            "GET /status HTTP/1.1\r\nHost: node\r\nX-Pad: {}\r\n\r\n",
            "a".repeat(512)
        );
        let response = l7_request(relay.address, &large_header).await;
        assert!(response.starts_with("HTTP/1.1 431"), "response: {response}");

        let large_body = "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{}";
        let response = l7_request(relay.address, large_body).await;
        assert!(response.starts_with("HTTP/1.1 413"), "response: {response}");

        wait_until(|| relay.stats.snapshot().l7_rejected_body == 1).await;
        let snapshot = relay.stats.snapshot();
        assert_eq!(snapshot.l7_rejected_header, 1);
    }

    #[tokio::test]
    async fn l7_decodes_chunked_bodies_and_sets_content_length() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let body = r#"{"jsonrpc":"2.0","method":"eth_call","id":1}"#;
        let chunked = format!("{:x}\r\n{body}\r\n0\r\n\r\n", body.len());
        let request = format!(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{chunked}"
        );
        let response = l7_request(relay.address, &request).await;

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "response: {response}"
        );
        let request = received.await.expect("captured request");
        let request = String::from_utf8(request).expect("request text");
        assert!(request.contains(&format!("Content-Length: {}\r\n", body.len())));
        assert!(!request.contains("Transfer-Encoding"));
        assert!(request.ends_with(body));
    }

    #[tokio::test]
    async fn l7_supports_expect_continue() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let body = r#"{"jsonrpc":"2.0","method":"eth_call","id":1}"#;
        let head = format!(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
            body.len()
        );
        let mut client = TcpStream::connect(relay.address)
            .await
            .expect("connect L7 relay");
        client.write_all(head.as_bytes()).await.expect("write head");
        let mut interim = [0_u8; 64];
        let read = timeout(Duration::from_secs(1), client.read(&mut interim))
            .await
            .expect("continue timeout")
            .expect("read continue");
        assert!(
            String::from_utf8_lossy(&interim[..read]).starts_with("HTTP/1.1 100 Continue"),
            "expected 100 Continue"
        );

        client.write_all(body.as_bytes()).await.expect("write body");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("response timeout")
            .expect("read response");
        assert!(
            String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK"),
            "response: {}",
            String::from_utf8_lossy(&response)
        );

        let request = received.await.expect("captured request");
        assert!(
            String::from_utf8_lossy(&request).ends_with(body),
            "upstream request must include the body"
        );
    }

    #[tokio::test]
    async fn l7_inspects_cometbft_get_uri_calls() {
        let (upstream, received) = spawn_http_upstream().await;
        let config = l7_config(upstream, "");
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;

        let response = l7_request(
            relay.address,
            "GET /status?height=1 HTTP/1.1\r\nHost: node\r\n\r\n",
        )
        .await;

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "response: {response}"
        );
        let request = received.await.expect("captured request");
        let request = String::from_utf8(request).expect("request text");
        assert!(request.starts_with("GET /status?height=1 HTTP/1.1\r\n"));

        let blocked = l7_request(
            relay.address,
            "GET /debug_trace HTTP/1.1\r\nHost: node\r\n\r\n",
        )
        .await;
        assert!(
            blocked.starts_with("HTTP/1.1 403 Forbidden"),
            "response: {blocked}"
        );
    }

    fn l7_config(upstream: SocketAddr, extra: &str) -> Arc<Config> {
        let text = format!(
            r#"
[limits]
max_connections = 8
max_connections_per_listener = 8
max_outbound_connections = 2
max_sessions = 16

[policy]
allow = ["127.0.0.1/32", "::1/128"]

[[upstreams]]
name = "node"
address = "{upstream}"

[[listeners]]
name = "rpc"
listen = "127.0.0.1:26657"
upstream = "node"

[listeners.l7]
allow_methods = ["eth_call", "eth_blockNumber", "status"]
{extra}
"#
        );
        Arc::new(Config::parse(&text, Path::new("test.toml")).expect("L7 test configuration"))
    }

    async fn l7_request(address: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).await.expect("connect L7 relay");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .expect("L7 response timeout")
            .expect("read L7 response");
        String::from_utf8_lossy(&response).into_owned()
    }

    /// Accepts one request, reports it, and answers with a fixed response.
    async fn spawn_http_upstream() -> (SocketAddr, tokio::sync::oneshot::Receiver<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HTTP upstream");
        let address = listener.local_addr().expect("HTTP upstream address");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept L7 request");
            let request = read_http_request(&mut stream).await;
            let _ = sender.send(request);
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )
                .await;
            let _ = stream.shutdown().await;
        });
        (address, receiver)
    }

    async fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        let head_end = loop {
            let read = stream.read(&mut chunk).await.expect("read request");
            assert!(read > 0, "upstream closed before the head");
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let head = std::str::from_utf8(&request[..head_end]).expect("request head");
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .map_or(0, |value| value.parse::<usize>().expect("content length"));
        while request.len() < head_end + length {
            let read = stream.read(&mut chunk).await.expect("read request body");
            assert!(read > 0, "upstream closed before the body");
            request.extend_from_slice(&chunk[..read]);
        }
        request
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_counters_and_rejects_unknown_paths() {
        let upstream = spawn_echo_upstream().await;
        let config = test_config(upstream, DEFAULT_LIMITS);
        let endpoint = Endpoint::from_str(&upstream.to_string()).expect("endpoint");
        let relay = start_relay(config, endpoint).await;
        let server = crate::observe::MetricsServer::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            relay.stats.clone(),
        )
        .await
        .expect("bind metrics endpoint");
        let address = server.local_addr().expect("metrics address");
        let task = tokio::spawn(server.run());

        let response = http_get(address, "/metrics").await;
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("l4guard_active_sessions 0"));
        assert!(response.contains("l4guard_policy_version "));
        assert!(response.contains("l4guard_rejected_total{reason=\"policy\"} 0"));

        let health = http_get(address, "/healthz").await;
        assert!(health.contains("HTTP/1.1 200 OK"));
        let ready = http_get(address, "/readyz").await;
        assert!(ready.contains("HTTP/1.1 200 OK"));

        let missing = http_get(address, "/config").await;
        assert!(missing.contains("HTTP/1.1 404 Not Found"));

        task.abort();
    }

    async fn http_get(address: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect metrics endpoint");
        let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .expect("metrics response timeout")
            .expect("read metrics response");
        String::from_utf8_lossy(&response).into_owned()
    }
}
