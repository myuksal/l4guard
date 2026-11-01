//! Configuration loading and validation.
//!
//! The guard reads one `TOML` document that describes its listeners, fixed
//! upstreams, resource limits, address policy, and observation settings.
//! Parsing and validation both happen before the runtime starts: a document
//! that cannot be validated stops startup instead of degrading into an
//! open proxy. See `guard.toml.example` for a documented example.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ipnet::IpNet;
use serde::Deserialize;

/// A fully parsed and validated guard configuration.
///
/// Runtime tasks read an [`Arc<Config>`] snapshot so that a future reload can
/// replace the whole document at once. Build it through [`Config::load`] or
/// [`Config::parse`]; neither function exposes a partially validated value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Resource ceilings applied to connections and bookkeeping maps.
    #[serde(default)]
    pub limits: Limits,
    /// Source address policy. Deny entries win over allow entries.
    #[serde(default)]
    pub policy: Policy,
    /// Logging and metric emission settings.
    #[serde(default)]
    pub observe: Observe,
    /// Fixed destinations that listeners relay to.
    #[serde(default)]
    pub upstreams: Vec<Upstream>,
    /// Listening sockets exposed to inbound peers or to the local node.
    #[serde(default)]
    pub listeners: Vec<Listener>,
}

impl Config {
    /// Loads and validates a configuration file.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Read`] when the file cannot be read, [`Error::Parse`]
    /// when the document is not valid `TOML` or does not match the schema, and
    /// [`Error::Invalid`] when a parsed value fails semantic validation.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text, path)
    }

    /// Loads a validated configuration snapshot for sharing with runtime tasks.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Config::load`].
    pub fn load_shared(path: impl AsRef<Path>) -> Result<Arc<Self>, Error> {
        Self::load(path).map(Arc::new)
    }

    /// Parses and validates configuration from `TOML` text.
    ///
    /// `source` names the document in error messages and is usually the file
    /// path that `text` was read from.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Parse`] when the document is not valid `TOML` or does
    /// not match the schema, and [`Error::Invalid`] when a parsed value fails
    /// semantic validation.
    pub fn parse(text: &str, source: &Path) -> Result<Self, Error> {
        let config: Self =
            toml::from_str(text).map_err(|error| Error::from_toml(source, text, &error))?;
        config.validate(source)?;
        Ok(config)
    }

    fn validate(&self, source: &Path) -> Result<(), Error> {
        if self.listeners.is_empty() {
            return Err(Error::invalid(
                source,
                "listeners",
                "at least one listener is required",
            ));
        }
        let upstreams = self.upstream_names(source)?;
        self.validate_listeners(source, &upstreams)?;
        self.limits.validate(source)?;
        self.observe.validate(source)?;
        self.validate_budgets(source)?;
        Ok(())
    }

    /// Returns the first setting that a hot reload cannot apply, if any.
    ///
    /// Hot reload covers the address policy, the per-source and new-connection
    /// quotas, and listener stop/resume. Listener addresses, upstreams,
    /// connection budgets, and observation settings are fixed for the process
    /// lifetime, so a document that changes them requires a restart.
    #[must_use]
    pub fn non_reloadable_change(&self, next: &Self) -> Option<String> {
        if self.observe != next.observe {
            return Some("observe".to_owned());
        }
        let limits = &self.limits;
        let next_limits = &next.limits;
        let changes = [
            (
                "limits.max_connections",
                limits.max_connections != next_limits.max_connections,
            ),
            (
                "limits.max_connections_per_listener",
                limits.max_connections_per_listener != next_limits.max_connections_per_listener,
            ),
            (
                "limits.max_outbound_connections",
                limits.max_outbound_connections != next_limits.max_outbound_connections,
            ),
            (
                "limits.connect_timeout_ms",
                limits.connect_timeout_ms != next_limits.connect_timeout_ms,
            ),
            (
                "limits.drain_timeout_ms",
                limits.drain_timeout_ms != next_limits.drain_timeout_ms,
            ),
            (
                "limits.max_sessions",
                limits.max_sessions != next_limits.max_sessions,
            ),
            (
                "limits.max_ip_entries",
                limits.max_ip_entries != next_limits.max_ip_entries,
            ),
            (
                "limits.ip_entry_ttl_seconds",
                limits.ip_entry_ttl_seconds != next_limits.ip_entry_ttl_seconds,
            ),
            (
                "limits.bandwidth_limit_bytes_per_second",
                limits.bandwidth_limit_bytes_per_second
                    != next_limits.bandwidth_limit_bytes_per_second,
            ),
        ];
        if let Some((key, _)) = changes.iter().find(|(_, changed)| *changed) {
            return Some((*key).to_owned());
        }

        let upstreams: BTreeMap<&str, &Endpoint> = self
            .upstreams
            .iter()
            .map(|upstream| (upstream.name.as_str(), &upstream.address))
            .collect();
        let next_upstreams: BTreeMap<&str, &Endpoint> = next
            .upstreams
            .iter()
            .map(|upstream| (upstream.name.as_str(), &upstream.address))
            .collect();
        if upstreams != next_upstreams {
            return Some("upstreams".to_owned());
        }

        let listeners: BTreeMap<&str, (SocketAddr, ListenerMode, &str, &Option<L7>)> = self
            .listeners
            .iter()
            .map(|listener| {
                (
                    listener.name.as_str(),
                    (
                        listener.listen,
                        listener.mode,
                        listener.upstream.as_str(),
                        &listener.l7,
                    ),
                )
            })
            .collect();
        let next_listeners: BTreeMap<&str, (SocketAddr, ListenerMode, &str, &Option<L7>)> = next
            .listeners
            .iter()
            .map(|listener| {
                (
                    listener.name.as_str(),
                    (
                        listener.listen,
                        listener.mode,
                        listener.upstream.as_str(),
                        &listener.l7,
                    ),
                )
            })
            .collect();
        if listeners != next_listeners {
            for (name, route) in &listeners {
                if next_listeners.get(name) != Some(route) {
                    return Some(format!("listeners.{name}"));
                }
            }
            return Some("listeners".to_owned());
        }
        None
    }

    fn upstream_names<'config>(
        &'config self,
        source: &Path,
    ) -> Result<HashSet<&'config str>, Error> {
        let mut names = HashSet::with_capacity(self.upstreams.len());
        for (index, upstream) in self.upstreams.iter().enumerate() {
            let key = format!("upstreams[{index}].name");
            validate_name(source, &key, &upstream.name)?;
            if !names.insert(upstream.name.as_str()) {
                return Err(Error::invalid(
                    source,
                    key,
                    format!("duplicate upstream name \"{}\"", upstream.name),
                ));
            }
        }
        Ok(names)
    }

    fn validate_budgets(&self, source: &Path) -> Result<(), Error> {
        let has_inbound = self
            .listeners
            .iter()
            .any(|listener| listener.mode == ListenerMode::Inbound);
        let has_outbound = self
            .listeners
            .iter()
            .any(|listener| listener.mode == ListenerMode::Outbound);
        if has_outbound && self.limits.max_outbound_connections == 0 {
            return Err(Error::invalid(
                source,
                "limits.max_outbound_connections",
                "must be greater than zero when outbound listeners exist",
            ));
        }
        if has_inbound && self.limits.max_outbound_connections >= self.limits.max_connections {
            return Err(Error::invalid(
                source,
                "limits.max_outbound_connections",
                "must leave at least one connection for inbound listeners",
            ));
        }
        Ok(())
    }

    fn validate_listeners(&self, source: &Path, upstreams: &HashSet<&str>) -> Result<(), Error> {
        let mut names = HashSet::with_capacity(self.listeners.len());
        let mut addresses = HashSet::with_capacity(self.listeners.len());
        for (index, listener) in self.listeners.iter().enumerate() {
            let name_key = format!("listeners[{index}].name");
            validate_name(source, &name_key, &listener.name)?;
            if !names.insert(listener.name.as_str()) {
                return Err(Error::invalid(
                    source,
                    name_key,
                    format!("duplicate listener name \"{}\"", listener.name),
                ));
            }
            let listen_key = format!("listeners[{index}].listen");
            if listener.listen.port() == 0 {
                return Err(Error::invalid(source, listen_key, "port must not be zero"));
            }
            if !addresses.insert(listener.listen) {
                return Err(Error::invalid(
                    source,
                    listen_key,
                    format!("duplicate listen address {}", listener.listen),
                ));
            }
            if !upstreams.contains(listener.upstream.as_str()) {
                return Err(Error::invalid(
                    source,
                    format!("listeners[{index}].upstream"),
                    format!("unknown upstream \"{}\"", listener.upstream),
                ));
            }
            if let Some(l7) = &listener.l7 {
                l7.validate(source, &format!("listeners[{index}].l7"))?;
            }
        }
        Ok(())
    }
}

/// Resource ceilings for connections and bookkeeping maps.
///
/// Every field must be greater than zero except
/// `bandwidth_limit_bytes_per_second`, where zero means "no byte budget".
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    /// Maximum number of concurrent sessions across all listeners.
    pub max_connections: usize,
    /// Maximum number of concurrent sessions for one listener.
    pub max_connections_per_listener: usize,
    /// Concurrent sessions reserved for outbound listeners.
    ///
    /// Inbound listeners may use at most `max_connections - max_outbound_connections`
    /// sessions, so an inbound flood cannot consume every slot that a fixed peer
    /// needs for reconnection. Zero is allowed only when no outbound listener
    /// exists; a configuration that defines one must reserve at least one slot.
    pub max_outbound_connections: usize,
    /// Maximum number of concurrent sessions from one source address.
    ///
    /// Applies to inbound listeners. Outbound listeners serve the local node,
    /// so their sessions do not consume per-source slots.
    pub max_connections_per_ip: usize,
    /// Sustained new-connection rate per listener, in connections per second.
    pub max_new_connections_per_second: u32,
    /// Token bucket burst size for new connections.
    pub new_connection_burst: u32,
    /// Deadline for establishing the upstream connection, in milliseconds.
    pub connect_timeout_ms: u64,
    /// Time allowed for active sessions to finish during shutdown, in
    /// milliseconds. Sessions still open after this window are cancelled.
    pub drain_timeout_ms: u64,
    /// Maximum number of entries in the session registry.
    pub max_sessions: usize,
    /// Maximum number of tracked per-IP entries.
    pub max_ip_entries: usize,
    /// Time-to-live for idle per-IP entries, in seconds.
    pub ip_entry_ttl_seconds: u64,
    /// Per-listener byte budget in both directions, or zero for no budget.
    pub bandwidth_limit_bytes_per_second: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_connections_per_listener: 256,
            max_outbound_connections: 0,
            max_connections_per_ip: 8,
            max_new_connections_per_second: 50,
            new_connection_burst: 100,
            connect_timeout_ms: 5_000,
            drain_timeout_ms: 10_000,
            max_sessions: 512,
            max_ip_entries: 4_096,
            ip_entry_ttl_seconds: 600,
            bandwidth_limit_bytes_per_second: 0,
        }
    }
}

impl Limits {
    /// Returns the upstream connection deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    /// Returns the time allowed for active sessions to drain during shutdown.
    #[must_use]
    pub const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }

    /// Returns the time-to-live for idle per-IP entries.
    #[must_use]
    pub const fn ip_entry_ttl(&self) -> Duration {
        Duration::from_secs(self.ip_entry_ttl_seconds)
    }

    /// Returns the session budget available to inbound listeners.
    ///
    /// Outbound sessions use [`Limits::max_outbound_connections`] instead, so
    /// the two budgets always add up to [`Limits::max_connections`].
    #[must_use]
    pub const fn inbound_connections(&self) -> usize {
        self.max_connections
            .saturating_sub(self.max_outbound_connections)
    }

    fn validate(&self, source: &Path) -> Result<(), Error> {
        require_positive(source, "limits.max_connections", &self.max_connections)?;
        require_positive(
            source,
            "limits.max_connections_per_listener",
            &self.max_connections_per_listener,
        )?;
        require_positive(
            source,
            "limits.max_connections_per_ip",
            &self.max_connections_per_ip,
        )?;
        require_positive(
            source,
            "limits.max_new_connections_per_second",
            &self.max_new_connections_per_second,
        )?;
        require_positive(
            source,
            "limits.new_connection_burst",
            &self.new_connection_burst,
        )?;
        require_positive(
            source,
            "limits.connect_timeout_ms",
            &self.connect_timeout_ms,
        )?;
        require_positive(source, "limits.drain_timeout_ms", &self.drain_timeout_ms)?;
        require_positive(source, "limits.max_sessions", &self.max_sessions)?;
        require_positive(source, "limits.max_ip_entries", &self.max_ip_entries)?;
        require_positive(
            source,
            "limits.ip_entry_ttl_seconds",
            &self.ip_entry_ttl_seconds,
        )?;
        if self.max_connections_per_listener > self.max_connections {
            return Err(Error::invalid(
                source,
                "limits.max_connections_per_listener",
                "must not exceed limits.max_connections",
            ));
        }
        if self.max_outbound_connections > self.max_connections {
            return Err(Error::invalid(
                source,
                "limits.max_outbound_connections",
                "must not exceed limits.max_connections",
            ));
        }
        if self.max_sessions < self.max_connections {
            return Err(Error::invalid(
                source,
                "limits.max_sessions",
                "must be at least limits.max_connections",
            ));
        }
        Ok(())
    }
}

/// Source address policy.
///
/// An address is allowed only when it matches at least one allow entry and no
/// deny entry. The default allow list is empty, so a configuration that omits
/// it denies every source (fail-closed).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Policy {
    /// CIDR blocks that may connect. Both IPv4 and IPv6 are accepted.
    pub allow: Vec<IpNet>,
    /// CIDR blocks that are always rejected, even when allowed.
    pub deny: Vec<IpNet>,
    /// When true, every new connection is rejected.
    pub emergency_block: bool,
}

/// Logging and metric emission settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Observe {
    /// Minimum log level.
    pub level: LogLevel,
    /// Maximum number of log events emitted per second.
    pub log_rate_limit_per_second: u32,
    /// Maximum number of queued log events.
    pub log_queue_capacity: usize,
    /// Optional address for the metrics endpoint.
    ///
    /// When set, the guard serves Prometheus text metrics at `GET /metrics`.
    /// Bind a loopback or pod-internal address and expose it through the
    /// existing observation path rather than a public listener.
    pub metrics_listen: Option<SocketAddr>,
}

impl Default for Observe {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            log_rate_limit_per_second: 100,
            log_queue_capacity: 1_024,
            metrics_listen: None,
        }
    }
}

impl Observe {
    fn validate(&self, source: &Path) -> Result<(), Error> {
        require_positive(
            source,
            "observe.log_rate_limit_per_second",
            &self.log_rate_limit_per_second,
        )?;
        require_positive(
            source,
            "observe.log_queue_capacity",
            &self.log_queue_capacity,
        )
    }
}

/// Minimum log level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Finest-grained diagnostics.
    Trace,
    /// Development diagnostics.
    Debug,
    /// Normal operational events.
    #[default]
    Info,
    /// Recoverable problems.
    Warn,
    /// Failed operations.
    Error,
}

/// A fixed destination that listeners relay to.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Operator-facing name referenced by listeners.
    pub name: String,
    /// Destination host and port. Hostnames are resolved when connecting.
    pub address: Endpoint,
}

/// A listening socket and the upstream it relays to.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    /// Operator-facing name used in logs and metrics.
    pub name: String,
    /// Local socket to accept connections on.
    pub listen: SocketAddr,
    /// Whether this listener faces public peers or the local node.
    #[serde(default)]
    pub mode: ListenerMode,
    /// Name of the upstream that accepted connections are relayed to.
    pub upstream: String,
    /// Optional HTTP/1.1 JSON-RPC inspection.
    ///
    /// When present, the listener parses every accepted connection as one
    /// HTTP/1.1 request, applies the method policy, and forwards a newly
    /// constructed request to the upstream. When absent, the listener relays
    /// raw bytes.
    #[serde(default)]
    pub l7: Option<L7>,
    /// Whether the listener accepts connections.
    ///
    /// A reload can stop or resume one listener by changing this value. A
    /// stopped listener keeps its socket bound but closes accepted connections
    /// immediately and cancels its existing sessions.
    #[serde(default = "listener_enabled")]
    pub enabled: bool,
}

/// Default for [`Listener::enabled`]: listeners are active unless disabled.
const fn listener_enabled() -> bool {
    true
}

/// HTTP/1.1 JSON-RPC inspection settings for one listener.
///
/// The listener buffers one request at a time, so every bound here is also a
/// per-session memory bound. Method policy fails closed: an empty
/// `allow_methods` list denies every request. See `docs/l7.md`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct L7 {
    /// Maximum bytes accepted before the request head ends.
    pub max_header_bytes: usize,
    /// Maximum number of request headers.
    pub max_header_count: usize,
    /// Time allowed to receive the request head, in milliseconds.
    pub header_timeout_ms: u64,
    /// Maximum decoded body bytes accepted for one request.
    pub max_body_bytes: usize,
    /// Time allowed to receive the whole body, in milliseconds.
    pub body_timeout_ms: u64,
    /// Maximum JSON nesting depth, counting the request object as one.
    pub max_json_depth: usize,
    /// Maximum elements in any JSON array and entries in any JSON object.
    pub max_json_array: usize,
    /// Maximum JSON-RPC calls in one batch.
    pub max_batch: usize,
    /// Methods that may run. Empty means every method is denied.
    pub allow_methods: Vec<String>,
    /// Methods that are always denied, even when allowed.
    pub deny_methods: Vec<String>,
    /// Per-method rate and concurrency limits for expensive methods.
    pub expensive_methods: BTreeMap<String, MethodLimit>,
}

impl Default for L7 {
    fn default() -> Self {
        Self {
            max_header_bytes: 16 * 1024,
            max_header_count: 64,
            header_timeout_ms: 5_000,
            max_body_bytes: 1024 * 1024,
            body_timeout_ms: 10_000,
            max_json_depth: 32,
            max_json_array: 256,
            max_batch: 32,
            allow_methods: Vec::new(),
            deny_methods: Vec::new(),
            expensive_methods: BTreeMap::new(),
        }
    }
}

impl L7 {
    fn validate(&self, source: &Path, key: &str) -> Result<(), Error> {
        require_positive(
            source,
            &format!("{key}.max_header_bytes"),
            &self.max_header_bytes,
        )?;
        require_positive(
            source,
            &format!("{key}.max_header_count"),
            &self.max_header_count,
        )?;
        require_positive(
            source,
            &format!("{key}.header_timeout_ms"),
            &self.header_timeout_ms,
        )?;
        require_positive(
            source,
            &format!("{key}.max_body_bytes"),
            &self.max_body_bytes,
        )?;
        require_positive(
            source,
            &format!("{key}.body_timeout_ms"),
            &self.body_timeout_ms,
        )?;
        require_positive(
            source,
            &format!("{key}.max_json_depth"),
            &self.max_json_depth,
        )?;
        require_positive(
            source,
            &format!("{key}.max_json_array"),
            &self.max_json_array,
        )?;
        require_positive(source, &format!("{key}.max_batch"), &self.max_batch)?;

        require_at_most(
            source,
            &format!("{key}.max_header_bytes"),
            self.max_header_bytes,
            MAX_L7_HEADER_BYTES,
        )?;
        require_at_most(
            source,
            &format!("{key}.max_header_count"),
            self.max_header_count,
            MAX_L7_HEADER_COUNT,
        )?;
        require_at_most(
            source,
            &format!("{key}.max_body_bytes"),
            self.max_body_bytes,
            MAX_L7_BODY_BYTES,
        )?;
        require_at_most(
            source,
            &format!("{key}.max_json_depth"),
            self.max_json_depth,
            MAX_L7_JSON_DEPTH,
        )?;
        require_at_most(
            source,
            &format!("{key}.max_json_array"),
            self.max_json_array,
            MAX_L7_JSON_ARRAY,
        )?;
        if self.max_batch > self.max_json_array {
            return Err(Error::invalid(
                source,
                format!("{key}.max_batch"),
                "must not exceed max_json_array",
            ));
        }

        validate_method_list(source, &format!("{key}.allow_methods"), &self.allow_methods)?;
        validate_method_list(source, &format!("{key}.deny_methods"), &self.deny_methods)?;
        for (method, limit) in &self.expensive_methods {
            validate_method_name(source, &format!("{key}.expensive_methods"), method)?;
            limit.validate(source, &format!("{key}.expensive_methods.{method}"))?;
        }
        Ok(())
    }
}

/// Largest request head the guard accepts, regardless of configuration.
const MAX_L7_HEADER_BYTES: usize = 1024 * 1024;

/// Largest header count the guard accepts, regardless of configuration.
const MAX_L7_HEADER_COUNT: usize = 256;

/// Largest buffered body the guard accepts, regardless of configuration.
const MAX_L7_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Deepest JSON nesting the guard accepts, matching the parser's own limit.
const MAX_L7_JSON_DEPTH: usize = 128;

/// Largest JSON array or object the guard accepts, regardless of configuration.
const MAX_L7_JSON_ARRAY: usize = 65_536;

/// Rate and concurrency limit for one expensive JSON-RPC method.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MethodLimit {
    /// Sustained requests per second for this method.
    pub rate_per_second: u32,
    /// Requests of this method that may run at the same time.
    pub concurrent: usize,
}

impl MethodLimit {
    fn validate(&self, source: &Path, key: &str) -> Result<(), Error> {
        require_positive(
            source,
            &format!("{key}.rate_per_second"),
            &self.rate_per_second,
        )?;
        require_positive(source, &format!("{key}.concurrent"), &self.concurrent)
    }
}

fn validate_method_list(source: &Path, key: &str, methods: &[String]) -> Result<(), Error> {
    let mut seen = HashSet::with_capacity(methods.len());
    for method in methods {
        validate_method_name(source, key, method)?;
        if !seen.insert(method.as_str()) {
            return Err(Error::invalid(
                source,
                key,
                format!("duplicate method \"{method}\""),
            ));
        }
    }
    Ok(())
}

fn validate_method_name(source: &Path, key: &str, method: &str) -> Result<(), Error> {
    if method.is_empty() {
        return Err(Error::invalid(source, key, "method name must not be empty"));
    }
    if method.len() > 128 {
        return Err(Error::invalid(
            source,
            key,
            "method name must not exceed 128 bytes",
        ));
    }
    if !method.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::invalid(
            source,
            key,
            "method name must contain printable ASCII characters only",
        ));
    }
    Ok(())
}

fn require_at_most(source: &Path, key: &str, value: usize, maximum: usize) -> Result<(), Error> {
    if value > maximum {
        return Err(Error::invalid(
            source,
            key,
            format!("must not exceed {maximum}"),
        ));
    }
    Ok(())
}

/// Direction a listener serves.
///
/// Inbound listeners accept traffic from untrusted peers; outbound listeners
/// accept connections from the local node and relay them to a fixed peer. The
/// two modes have separate connection budgets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenerMode {
    /// Accepts connections from public peers.
    #[default]
    Inbound,
    /// Accepts connections from the local node toward a fixed peer.
    Outbound,
}

impl ListenerMode {
    /// Returns the configuration spelling of the mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

/// A `host:port` pair accepted by an upstream definition.
///
/// IPv6 addresses must be written in brackets, for example `[::1]:26656`.
/// Hostnames are kept as written; resolution happens when the guard connects.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    fn try_new(host: String, port: u16) -> Result<Self, InvalidEndpoint> {
        if port == 0 {
            return Err(InvalidEndpoint::new("port must not be zero"));
        }
        Ok(Self { host, port })
    }

    fn try_from_socket_addr(address: SocketAddr) -> Result<Self, InvalidEndpoint> {
        Self::try_new(address.ip().to_string(), address.port())
    }

    /// Returns the host portion as written in configuration.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the TCP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for Endpoint {
    type Err = InvalidEndpoint;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Ok(address) = value.parse::<SocketAddr>() {
            return Self::try_from_socket_addr(address);
        }
        let (host, port) = value
            .rsplit_once(':')
            .ok_or_else(|| InvalidEndpoint::new("expected host:port"))?;
        if host.is_empty() {
            return Err(InvalidEndpoint::new("host must not be empty"));
        }
        if host.contains(':') {
            return Err(InvalidEndpoint::new(
                "IPv6 address must be enclosed in brackets",
            ));
        }
        if !host.chars().all(is_host_character) {
            return Err(InvalidEndpoint::new("host contains unsupported characters"));
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| InvalidEndpoint::new("port must be a number between 1 and 65535"))?;
        Self::try_new(host.to_owned(), port)
    }
}

impl TryFrom<String> for Endpoint {
    type Error = InvalidEndpoint;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

const fn is_host_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '.' | '_')
}

/// Error returned when an upstream endpoint string cannot be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidEndpoint {
    reason: &'static str,
}

impl InvalidEndpoint {
    const fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

impl fmt::Display for InvalidEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl std::error::Error for InvalidEndpoint {}

/// Configuration failure reported before the guard starts.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The configuration file could not be read.
    Read {
        /// File that could not be read.
        path: PathBuf,
        /// Underlying I/O failure.
        source: io::Error,
    },
    /// The document is not valid `TOML` or does not match the schema.
    Parse {
        /// File that contains the invalid document.
        path: PathBuf,
        /// One-based line of the first parse error, when known.
        line: Option<usize>,
        /// One-based column of the first parse error, when known.
        column: Option<usize>,
        /// Single-line parser message.
        message: String,
    },
    /// A parsed value failed semantic validation.
    Invalid {
        /// File that contains the invalid value.
        path: PathBuf,
        /// Dotted configuration key, for example `listeners[0].listen`.
        key: String,
        /// Why the value was rejected.
        reason: String,
    },
}

impl Error {
    fn from_toml(path: &Path, text: &str, error: &toml::de::Error) -> Self {
        let (line, column) = error
            .span()
            .and_then(|span| text.get(..span.start))
            .map(|prefix| {
                let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let column = prefix
                    .rsplit('\n')
                    .next()
                    .map_or(0, |line| line.chars().count())
                    + 1;
                (line, column)
            })
            .unzip();
        Self::Parse {
            path: path.to_path_buf(),
            line,
            column,
            message: error.message().to_owned(),
        }
    }

    fn invalid(path: &Path, key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.to_path_buf(),
            key: key.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "{}: cannot read configuration: {source}",
                    path.display()
                )
            }
            Self::Parse {
                path,
                line: Some(line),
                column: Some(column),
                message,
            } => write!(formatter, "{}:{line}:{column}: {message}", path.display()),
            Self::Parse { path, message, .. } => {
                write!(formatter, "{}: {message}", path.display())
            }
            Self::Invalid { path, key, reason } => {
                write!(formatter, "{}: {key}: {reason}", path.display())
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse { .. } | Self::Invalid { .. } => None,
        }
    }
}

fn validate_name(source: &Path, key: &str, name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::invalid(source, key, "must not be empty"));
    }
    if name.trim() != name {
        return Err(Error::invalid(
            source,
            key,
            "must not contain leading or trailing whitespace",
        ));
    }
    if name.chars().any(char::is_whitespace) {
        return Err(Error::invalid(source, key, "must not contain whitespace"));
    }
    if name.chars().any(char::is_control) {
        return Err(Error::invalid(
            source,
            key,
            "must not contain control characters",
        ));
    }
    Ok(())
}

fn require_positive<T>(source: &Path, key: &str, value: &T) -> Result<(), Error>
where
    T: PartialEq + Default,
{
    if value == &T::default() {
        return Err(Error::invalid(source, key, "must be greater than zero"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[listeners]]
name = "public"
listen = "127.0.0.1:26657"
upstream = "node"
"#;

    fn parse(text: &str) -> Result<Config, Error> {
        Config::parse(text, Path::new("test.toml"))
    }

    fn error_text(text: &str) -> String {
        parse(text)
            .expect_err("configuration must be rejected")
            .to_string()
    }

    fn assert_contains(message: &str, needle: &str) {
        assert!(
            message.contains(needle),
            "expected {message:?} to contain {needle:?}"
        );
    }

    #[test]
    fn parses_minimal_config_with_conservative_defaults() {
        let config = parse(MINIMAL).expect("minimal configuration must be valid");

        assert_eq!(config.limits.max_connections, 256);
        assert_eq!(config.limits.max_outbound_connections, 0);
        assert_eq!(config.limits.inbound_connections(), 256);
        assert_eq!(config.limits.max_connections_per_ip, 8);
        assert_eq!(config.limits.connect_timeout(), Duration::from_secs(5));
        assert_eq!(config.limits.drain_timeout(), Duration::from_secs(10));
        assert_eq!(config.policy.allow, Vec::<IpNet>::new());
        assert!(!config.policy.emergency_block);
        assert_eq!(config.observe.level, LogLevel::Info);
        assert_eq!(config.observe.metrics_listen, None);
        assert_eq!(config.listeners.len(), 1);
        assert_eq!(config.listeners[0].mode, ListenerMode::Inbound);
        assert_eq!(config.listeners[0].upstream, "node");
        assert!(config.listeners[0].l7.is_none());
        assert!(config.listeners[0].enabled);
    }

    #[test]
    fn parses_listener_enabled_flag() {
        let text = format!(
            "{MINIMAL}\n[[listeners]]\nname = \"disabled\"\nlisten = \"127.0.0.1:26658\"\nupstream = \"node\"\nenabled = false\n"
        );

        let config = parse(&text).expect("disabled listener must be valid");

        assert!(!config.listeners[1].enabled);
    }

    #[test]
    fn parses_metrics_endpoint() {
        let text = format!("{MINIMAL}\n[observe]\nmetrics_listen = \"127.0.0.1:9100\"\n");

        let config = parse(&text).expect("metrics endpoint must be valid");

        assert_eq!(
            config.observe.metrics_listen,
            Some(SocketAddr::from(([127, 0, 0, 1], 9100)))
        );
    }

    #[test]
    fn parses_example_file() {
        let config = Config::parse(
            include_str!("../guard.toml.example"),
            Path::new("guard.toml.example"),
        )
        .expect("example configuration must be valid");

        assert_eq!(config.listeners.len(), 2);
        assert_eq!(config.listeners[0].mode, ListenerMode::Inbound);
        assert_eq!(config.listeners[1].mode, ListenerMode::Outbound);
        assert_eq!(config.upstreams.len(), 2);
    }

    #[test]
    fn rejects_missing_listeners() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"
"#;

        let message = error_text(text);

        assert_contains(&message, "test.toml");
        assert_contains(&message, "listeners");
        assert_contains(&message, "at least one listener");
    }

    #[test]
    fn rejects_duplicate_listen_address() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[listeners]]
name = "first"
listen = "127.0.0.1:26657"
upstream = "node"

[[listeners]]
name = "second"
listen = "127.0.0.1:26657"
upstream = "node"
"#;

        let message = error_text(text);

        assert_contains(&message, "listeners[1].listen");
        assert_contains(&message, "duplicate listen address");
    }

    #[test]
    fn rejects_unknown_upstream_reference() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[listeners]]
name = "public"
listen = "127.0.0.1:26657"
upstream = "missing"
"#;

        let message = error_text(text);

        assert_contains(&message, "listeners[0].upstream");
        assert_contains(&message, "unknown upstream");
    }

    #[test]
    fn rejects_duplicate_upstream_name() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[upstreams]]
name = "node"
address = "127.0.0.1:26657"

[[listeners]]
name = "public"
listen = "127.0.0.1:26658"
upstream = "node"
"#;

        let message = error_text(text);

        assert_contains(&message, "upstreams[1].name");
        assert_contains(&message, "duplicate upstream name");
    }

    #[test]
    fn rejects_zero_limits() {
        let text = format!("{MINIMAL}\n[limits]\nmax_connections = 0\n");

        let message = error_text(&text);

        assert_contains(&message, "limits.max_connections");
        assert_contains(&message, "greater than zero");
    }

    #[test]
    fn rejects_zero_drain_timeout() {
        let text = format!("{MINIMAL}\n[limits]\ndrain_timeout_ms = 0\n");

        let message = error_text(&text);

        assert_contains(&message, "limits.drain_timeout_ms");
        assert_contains(&message, "greater than zero");
    }

    #[test]
    fn rejects_listener_budget_above_global_budget() {
        let text = format!(
            "{MINIMAL}\n[limits]\nmax_connections = 10\nmax_connections_per_listener = 11\nmax_outbound_connections = 2\nmax_sessions = 10\n"
        );

        let message = error_text(&text);

        assert_contains(&message, "limits.max_connections_per_listener");
        assert_contains(&message, "must not exceed");
    }

    #[test]
    fn rejects_outbound_budget_above_global_budget() {
        let text = format!(
            "{MINIMAL}\n[limits]\nmax_connections = 10\nmax_connections_per_listener = 10\nmax_outbound_connections = 11\nmax_sessions = 10\n"
        );

        let message = error_text(&text);

        assert_contains(&message, "limits.max_outbound_connections");
        assert_contains(&message, "must not exceed");
    }

    #[test]
    fn rejects_outbound_budget_without_inbound_reserve() {
        let text = format!(
            "{MINIMAL}\n[limits]\nmax_connections = 10\nmax_connections_per_listener = 10\nmax_outbound_connections = 10\nmax_sessions = 10\n"
        );

        let message = error_text(&text);

        assert_contains(&message, "limits.max_outbound_connections");
        assert_contains(&message, "inbound listeners");
    }

    #[test]
    fn rejects_outbound_listener_without_reserved_budget() {
        let text = r#"
[[upstreams]]
name = "peer"
address = "203.0.113.10:26656"

[[listeners]]
name = "egress"
listen = "127.0.0.1:31001"
mode = "outbound"
upstream = "peer"

[limits]
max_connections = 10
max_connections_per_listener = 10
max_sessions = 10
"#;

        let message = error_text(text);

        assert_contains(&message, "limits.max_outbound_connections");
        assert_contains(&message, "greater than zero");
    }

    #[test]
    fn allows_full_budget_for_outbound_only_deployment() {
        let text = r#"
[[upstreams]]
name = "peer"
address = "203.0.113.10:26656"

[[listeners]]
name = "egress"
listen = "127.0.0.1:31001"
mode = "outbound"
upstream = "peer"

[limits]
max_connections = 10
max_connections_per_listener = 10
max_outbound_connections = 10
max_sessions = 10
"#;

        let config = parse(text).expect("outbound-only budget must be valid");

        assert_eq!(config.limits.inbound_connections(), 0);
    }

    #[test]
    fn rejects_session_registry_smaller_than_connection_budget() {
        let text = format!(
            "{MINIMAL}\n[limits]\nmax_connections = 10\nmax_connections_per_listener = 10\nmax_outbound_connections = 2\nmax_sessions = 9\n"
        );

        let message = error_text(&text);

        assert_contains(&message, "limits.max_sessions");
        assert_contains(&message, "at least");
    }

    #[test]
    fn rejects_invalid_cidr() {
        let text = format!("{MINIMAL}\n[policy]\nallow = [\"10.0.0.0/33\"]\n");

        let message = error_text(&text);

        assert_contains(&message, "test.toml");
        assert!(
            message.contains("CIDR") || message.contains("address"),
            "unexpected parse error: {message}"
        );
    }

    #[test]
    fn rejects_unknown_field() {
        let text = format!("{MINIMAL}\n[limits]\nmax_connection = 10\n");

        let message = error_text(&text);

        assert_contains(&message, "unknown field");
        assert_contains(&message, "max_connection");
    }

    #[test]
    fn rejects_listener_port_zero() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[listeners]]
name = "public"
listen = "127.0.0.1:0"
upstream = "node"
"#;

        let message = error_text(text);

        assert_contains(&message, "listeners[0].listen");
        assert_contains(&message, "port must not be zero");
    }

    #[test]
    fn rejects_missing_required_listener_field() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26656"

[[listeners]]
name = "public"
listen = "127.0.0.1:26657"
"#;

        let message = error_text(text);

        assert_contains(&message, "missing field");
        assert_contains(&message, "upstream");
    }

    #[test]
    fn error_message_is_single_line() {
        let text = format!("{MINIMAL}\n[policy]\nallow = [\"10.0.0.0/33\"]\n");

        let message = error_text(&text);

        assert!(!message.contains('\n'), "error must stay on one line");
    }

    #[test]
    fn parses_l7_listener_settings() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26657"

[[listeners]]
name = "rpc"
listen = "127.0.0.1:26657"
upstream = "node"

[listeners.l7]
max_header_bytes = 8192
max_header_count = 32
header_timeout_ms = 2000
max_body_bytes = 65536
body_timeout_ms = 3000
max_json_depth = 16
max_json_array = 128
max_batch = 8
allow_methods = ["eth_call", "eth_getLogs"]
deny_methods = ["debug_traceCall"]

[listeners.l7.expensive_methods.eth_call]
rate_per_second = 20
concurrent = 8
"#;

        let config = parse(text).expect("L7 configuration must be valid");
        let l7 = config.listeners[0].l7.as_ref().expect("l7 settings");

        assert_eq!(l7.max_header_bytes, 8192);
        assert_eq!(l7.max_header_count, 32);
        assert_eq!(l7.header_timeout_ms, 2000);
        assert_eq!(l7.max_body_bytes, 65_536);
        assert_eq!(l7.body_timeout_ms, 3000);
        assert_eq!(l7.max_json_depth, 16);
        assert_eq!(l7.max_json_array, 128);
        assert_eq!(l7.max_batch, 8);
        assert_eq!(l7.allow_methods, vec!["eth_call", "eth_getLogs"]);
        assert_eq!(l7.deny_methods, vec!["debug_traceCall"]);
        let expensive = l7.expensive_methods.get("eth_call").expect("limit");
        assert_eq!(expensive.rate_per_second, 20);
        assert_eq!(expensive.concurrent, 8);
    }

    #[test]
    fn l7_defaults_deny_every_method() {
        let text = r#"
[[upstreams]]
name = "node"
address = "127.0.0.1:26657"

[[listeners]]
name = "rpc"
listen = "127.0.0.1:26657"
upstream = "node"

[listeners.l7]
"#;

        let config = parse(text).expect("default L7 configuration must be valid");
        let l7 = config.listeners[0].l7.as_ref().expect("l7 settings");

        assert_eq!(l7.allow_methods, Vec::<String>::new());
        assert_eq!(l7.max_header_bytes, 16 * 1024);
        assert_eq!(l7.max_body_bytes, 1024 * 1024);
        assert_eq!(l7.max_batch, 32);
    }

    #[test]
    fn rejects_zero_l7_limits() {
        let text = format!("{MINIMAL}\n[listeners.l7]\nmax_body_bytes = 0\n");

        let message = error_text(&text);

        assert_contains(&message, "listeners[0].l7.max_body_bytes");
        assert_contains(&message, "greater than zero");
    }

    #[test]
    fn rejects_l7_batch_above_array_limit() {
        let text = format!("{MINIMAL}\n[listeners.l7]\nmax_json_array = 4\nmax_batch = 5\n");

        let message = error_text(&text);

        assert_contains(&message, "listeners[0].l7.max_batch");
        assert_contains(&message, "max_json_array");
    }

    #[test]
    fn rejects_invalid_l7_method_names() {
        let text = format!("{MINIMAL}\n[listeners.l7]\nallow_methods = [\"eth call\"]\n");

        let message = error_text(&text);

        assert_contains(&message, "listeners[0].l7.allow_methods");
        assert_contains(&message, "printable ASCII");
    }

    #[test]
    fn rejects_duplicate_l7_allow_methods() {
        let text =
            format!("{MINIMAL}\n[listeners.l7]\nallow_methods = [\"eth_call\", \"eth_call\"]\n");

        let message = error_text(&text);

        assert_contains(&message, "listeners[0].l7.allow_methods");
        assert_contains(&message, "duplicate method");
    }

    #[test]
    fn rejects_incomplete_expensive_method_limit() {
        let text = format!(
            "{MINIMAL}\n[listeners.l7]\nallow_methods = [\"eth_call\"]\n\n[listeners.l7.expensive_methods.eth_call]\nrate_per_second = 10\n"
        );

        let message = error_text(&text);

        assert_contains(
            &message,
            "listeners[0].l7.expensive_methods.eth_call.concurrent",
        );
        assert_contains(&message, "greater than zero");
    }

    #[test]
    fn reload_rejects_l7_changes() {
        let base = parse(MINIMAL).expect("minimal configuration");
        let mut next = base.clone();
        next.listeners[0].l7 = Some(L7 {
            allow_methods: vec!["eth_call".to_owned()],
            ..L7::default()
        });

        assert_eq!(
            base.non_reloadable_change(&next).as_deref(),
            Some("listeners.public")
        );
    }

    #[test]
    fn parses_endpoint_forms() {
        let ipv4: Endpoint = "203.0.113.10:26656".parse().expect("IPv4 endpoint");
        assert_eq!(ipv4.host(), "203.0.113.10");
        assert_eq!(ipv4.port(), 26_656);

        let ipv6: Endpoint = "[2001:db8::1]:26656".parse().expect("IPv6 endpoint");
        assert_eq!(ipv6.host(), "2001:db8::1");
        assert_eq!(ipv6.to_string(), "[2001:db8::1]:26656");

        let hostname: Endpoint = "peer-a.internal:26656".parse().expect("hostname endpoint");
        assert_eq!(hostname.host(), "peer-a.internal");
    }

    #[test]
    fn rejects_invalid_endpoint_forms() {
        for value in [
            "peer",
            ":26656",
            "peer:0",
            "peer:70000",
            "peer:port",
            "::1:26656",
            "peer name:26656",
            "peer/name:26656",
        ] {
            assert!(
                value.parse::<Endpoint>().is_err(),
                "endpoint {value} must be rejected"
            );
        }
    }

    #[test]
    fn parses_outbound_listener_mode() {
        let text = r#"
[[upstreams]]
name = "peer"
address = "203.0.113.10:26656"

[[listeners]]
name = "egress"
listen = "127.0.0.1:31001"
mode = "outbound"
upstream = "peer"

[limits]
max_outbound_connections = 8
"#;

        let config = parse(text).expect("outbound listener must be valid");

        assert_eq!(config.listeners[0].mode, ListenerMode::Outbound);
        assert_eq!(config.listeners[0].mode.as_str(), "outbound");
    }

    #[test]
    fn reload_accepts_policy_quota_and_listener_state_changes() {
        let base = parse(MINIMAL).expect("minimal configuration");
        let mut next = base.clone();
        next.policy.allow = vec![IpNet::from_str("10.0.0.0/8").expect("test CIDR")];
        next.policy.emergency_block = true;
        next.limits.max_connections_per_ip = 4;
        next.limits.max_new_connections_per_second = 10;
        next.limits.new_connection_burst = 20;
        next.listeners[0].enabled = false;

        assert_eq!(base.non_reloadable_change(&next), None);
    }

    #[test]
    fn reload_rejects_connection_budget_changes() {
        let base = parse(MINIMAL).expect("minimal configuration");
        let mut next = base.clone();
        next.limits.max_connections = 128;

        assert_eq!(
            base.non_reloadable_change(&next).as_deref(),
            Some("limits.max_connections")
        );
    }

    #[test]
    fn reload_rejects_listener_address_changes() {
        let base = parse(MINIMAL).expect("minimal configuration");
        let mut next = base.clone();
        next.listeners[0].listen = "127.0.0.1:26658".parse().expect("socket address");

        assert_eq!(
            base.non_reloadable_change(&next).as_deref(),
            Some("listeners.public")
        );
    }

    #[test]
    fn reload_rejects_upstream_changes() {
        let base = parse(MINIMAL).expect("minimal configuration");
        let mut next = base.clone();
        next.upstreams[0].address = "127.0.0.1:26659".parse().expect("endpoint");

        assert_eq!(
            base.non_reloadable_change(&next).as_deref(),
            Some("upstreams")
        );
    }
}
