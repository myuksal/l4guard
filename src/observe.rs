//! Bounded logging and optional metric exposure.
//!
//! Logging must not become a second failure domain. Every event passes through
//! a per-second rate limit and a bounded queue; when either limit is reached
//! the event is dropped and counted, and the writer emits a summary line at
//! most once per second. A busy reject path therefore cannot exhaust memory or
//! flood the collector with its own diagnostics.
//!
//! The optional metrics endpoint serves a small Prometheus text document. It
//! exposes counters and gauges only: configuration values, policy rules, and
//! peer addresses never appear in the response. See `docs/observe.md`.
//!
//! Structured events carry `listener`, `peer`, `reason`, `version`, and
//! `session` fields from the emitting modules. Logs intentionally avoid
//! dumping whole configuration or policy documents.

use std::fmt::Write as _;
use std::io;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::Level;

use crate::config::LogLevel;
use crate::config::Observe;
use crate::relay::StatsHandle;
use crate::relay::StatsSnapshot;

/// Log events are summarized at most once per this interval.
const SUMMARY_INTERVAL: Duration = Duration::from_secs(1);

/// How long the writer waits for a queued event before checking for shutdown.
const WRITER_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Maximum number of metrics connections served at once.
const METRICS_MAX_CONNECTIONS: usize = 8;

/// Maximum request bytes read before the metrics endpoint responds.
const METRICS_REQUEST_LIMIT: usize = 1_024;

/// Time allowed to send a metrics request.
const METRICS_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Delay before retrying a failed metrics accept.
const METRICS_ACCEPT_BACKOFF: Duration = Duration::from_millis(200);

/// Keeps the log writer alive and flushes queued events on shutdown.
///
/// Drop this guard after the last log event. Events emitted afterwards are
/// discarded because the writer has stopped.
#[derive(Debug)]
pub struct LogGuard {
    sink: Arc<LogSink>,
    writer: Option<JoinHandle<()>>,
}

impl LogGuard {
    /// Flushes queued events and stops the writer thread.
    ///
    /// Calling this more than once is harmless.
    pub fn shutdown(&mut self) {
        self.sink.closed.store(true, Ordering::Release);
        if let Some(writer) = self.writer.take() {
            if writer.join().is_err() {
                eprintln!("l4guard: log writer thread panicked");
            }
        }
    }
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Installs the global tracing subscriber with bounded, rate-limited output.
///
/// The writer thread is created only when the process can spawn it. If that
/// fails, events are written to standard error directly, still under the rate
/// limit, so the guard keeps producing diagnostics.
#[must_use]
pub fn init_logging(observe: &Observe) -> LogGuard {
    let (sender, receiver) = sync_channel(observe.log_queue_capacity);
    let sink = Arc::new(LogSink {
        queue: sender,
        direct: AtomicBool::new(false),
        limit: observe.log_rate_limit_per_second,
        dropped: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        window: Mutex::new(RateWindow::new()),
    });
    let writer_sink = Arc::clone(&sink);
    let writer = std::thread::Builder::new()
        .name("l4guard-log".to_owned())
        .spawn(move || writer_loop(&receiver, &writer_sink))
        .ok();
    if writer.is_none() {
        sink.direct.store(true, Ordering::Release);
    }
    if let Err(error) = tracing_subscriber::fmt()
        .with_max_level(level_of(observe.level))
        .with_target(false)
        .with_ansi(false)
        .with_writer(SinkWriter(Arc::clone(&sink)))
        .try_init()
    {
        eprintln!("l4guard: cannot initialize logging: {error}");
    }
    LogGuard { sink, writer }
}

fn level_of(level: LogLevel) -> Level {
    match level {
        LogLevel::Trace => Level::TRACE,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Info => Level::INFO,
        LogLevel::Warn => Level::WARN,
        LogLevel::Error => Level::ERROR,
    }
}

/// Shared state between the formatting layer and the writer thread.
#[derive(Debug)]
struct LogSink {
    queue: SyncSender<Vec<u8>>,
    direct: AtomicBool,
    limit: u32,
    dropped: AtomicU64,
    closed: AtomicBool,
    window: Mutex<RateWindow>,
}

impl LogSink {
    /// Applies the rate limit and either queues one event or counts a drop.
    fn submit(&self, bytes: &[u8]) {
        if bytes.is_empty() || self.closed.load(Ordering::Acquire) {
            return;
        }
        let allowed = {
            let mut window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
            window.allow(self.limit)
        };
        if !allowed {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.direct.load(Ordering::Acquire) {
            let _ = io::stderr().write_all(bytes);
            return;
        }
        if self.queue.try_send(bytes.to_vec()).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Fixed-window per-second limiter.
#[derive(Debug)]
struct RateWindow {
    started: Instant,
    count: u32,
}

impl RateWindow {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            count: 0,
        }
    }

    fn allow(&mut self, limit: u32) -> bool {
        if self.started.elapsed() >= SUMMARY_INTERVAL {
            self.started = Instant::now();
            self.count = 0;
        }
        if self.count < limit {
            self.count += 1;
            true
        } else {
            false
        }
    }
}

/// Buffers one formatted event and submits it when the formatter drops it.
struct LogWriter<'a> {
    sink: &'a LogSink,
    buffer: Vec<u8>,
}

impl io::Write for LogWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LogWriter<'_> {
    fn drop(&mut self) {
        self.sink.submit(&self.buffer);
    }
}

/// Adapter that gives the formatter one [`LogWriter`] per event.
struct SinkWriter(Arc<LogSink>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SinkWriter {
    type Writer = LogWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            sink: &self.0,
            buffer: Vec::with_capacity(256),
        }
    }
}

/// Drains queued events and reports suppressed counts.
fn writer_loop(receiver: &Receiver<Vec<u8>>, sink: &LogSink) {
    let mut stderr = io::stderr();
    let mut last_summary = Instant::now();
    loop {
        if sink.closed.load(Ordering::Acquire) {
            while let Ok(bytes) = receiver.try_recv() {
                let _ = stderr.write_all(&bytes);
            }
            break;
        }
        match receiver.recv_timeout(WRITER_POLL_INTERVAL) {
            Ok(bytes) => {
                let _ = stderr.write_all(&bytes);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        let dropped = sink.dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 && last_summary.elapsed() >= SUMMARY_INTERVAL {
            let _ = writeln!(
                stderr,
                "l4guard: suppressed {dropped} log event(s) by observe limits"
            );
            last_summary = Instant::now();
        }
    }
    let dropped = sink.dropped.swap(0, Ordering::Relaxed);
    if dropped > 0 {
        let _ = writeln!(
            stderr,
            "l4guard: suppressed {dropped} log event(s) by observe limits"
        );
    }
}

/// Bound metrics endpoint.
///
/// The server is created before the relay starts serving so a bind failure
/// stops startup instead of leaving the endpoint silently unavailable.
#[derive(Debug)]
pub struct MetricsServer {
    listener: TcpListener,
    stats: StatsHandle,
}

impl MetricsServer {
    /// Binds the configured metrics address.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the address cannot be bound.
    pub async fn bind(address: SocketAddr, stats: StatsHandle) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener, stats })
    }

    /// Returns the bound address when a test binds port `0`.
    #[cfg(test)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves metrics until the task is aborted.
    pub async fn run(self) {
        let limit = Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS));
        loop {
            let (stream, peer) = match self.listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(error = %error, "metrics accept failed; retrying");
                    tokio::time::sleep(METRICS_ACCEPT_BACKOFF).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
                // Shed the request instead of queueing unbounded tasks.
                continue;
            };
            let stats = self.stats.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = serve_metrics(stream, &stats).await {
                    tracing::debug!(%peer, error = %error, "metrics connection failed");
                }
            });
        }
    }
}

/// Reads one bounded HTTP request and answers with metrics or `404`.
async fn serve_metrics(mut stream: TcpStream, stats: &StatsHandle) -> io::Result<()> {
    let mut buffer = [0_u8; METRICS_REQUEST_LIMIT];
    let length = match timeout(METRICS_READ_TIMEOUT, read_request(&mut stream, &mut buffer)).await {
        Ok(Ok(length)) => length,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Ok(()),
    };
    let request = String::from_utf8_lossy(&buffer[..length]);
    let path = request.lines().next().and_then(|line| {
        let mut parts = line.split_whitespace();
        let method = parts.next()?;
        let path = parts.next()?;
        (method == "GET").then_some(path)
    });
    let (code, media_type, body) = match path {
        Some("/metrics") => (
            "200 OK",
            "text/plain; version=0.0.4",
            render_metrics(&stats.snapshot()),
        ),
        // Liveness and readiness are served by the same task as metrics. They
        // deliberately do not touch the backend: a Guard is ready when its
        // listeners are bound and this task is running.
        Some("/healthz" | "/readyz") => ("200 OK", "text/plain", "ok\n".to_owned()),
        _ => ("404 Not Found", "text/plain", "not found\n".to_owned()),
    };
    let response = format!(
        "HTTP/1.1 {code}\r\nContent-Type: {media_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Reads until the request headers end or the buffer is full.
async fn read_request(stream: &mut TcpStream, buffer: &mut [u8]) -> io::Result<usize> {
    let mut length = 0;
    loop {
        let read = stream.read(&mut buffer[length..]).await?;
        if read == 0 {
            return Ok(length);
        }
        length += read;
        if length == buffer.len()
            || buffer[..length]
                .windows(4)
                .any(|window| window == b"\r\n\r\n")
        {
            return Ok(length);
        }
    }
}

/// Renders the Prometheus text representation of one snapshot.
///
/// Only counters, gauges, and the policy version are emitted. No configuration
/// value, policy rule, or peer address is included.
#[must_use]
pub fn render_metrics(snapshot: &StatsSnapshot) -> String {
    let mut out = String::with_capacity(1_024);
    render_gauges(&mut out, snapshot);
    render_counters(&mut out, snapshot);
    out
}

fn render_gauges(out: &mut String, snapshot: &StatsSnapshot) {
    gauge(
        out,
        "l4guard_active_sessions",
        "Sessions currently registered and relaying.",
        snapshot.active_sessions,
    );
    gauge(
        out,
        "l4guard_ip_entries",
        "Source addresses currently tracked by the per-IP state map.",
        snapshot.ip_entries,
    );
    gauge(
        out,
        "l4guard_policy_version",
        "Deterministic version of the active policy snapshot.",
        snapshot.policy_version,
    );
}

fn render_counters(out: &mut String, snapshot: &StatsSnapshot) {
    counter(
        out,
        "l4guard_accepted_total",
        "Sessions admitted by policy and budgets.",
        snapshot.accepted,
    );
    render_rejections(out, snapshot);
    render_l7(out, snapshot);
    render_failures(out, snapshot);
    render_bytes(out, snapshot);
}

fn render_l7(out: &mut String, snapshot: &StatsSnapshot) {
    counter(
        out,
        "l4guard_l7_requests_total",
        "HTTP requests inspected on L7 listeners.",
        snapshot.l7_requests,
    );
    counter(
        out,
        "l4guard_l7_forwarded_total",
        "HTTP requests forwarded to an upstream.",
        snapshot.l7_forwarded,
    );
    labeled_header(
        out,
        "l4guard_l7_rejected_total",
        "counter",
        "L7 requests rejected, by reason.",
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "malformed",
        snapshot.l7_rejected_malformed,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "header",
        snapshot.l7_rejected_header,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "body",
        snapshot.l7_rejected_body,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "timeout",
        snapshot.l7_rejected_timeout,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "method",
        snapshot.l7_rejected_method,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "encoding",
        snapshot.l7_rejected_encoding,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "upgrade",
        snapshot.l7_rejected_upgrade,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "json",
        snapshot.l7_rejected_json,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "policy",
        snapshot.l7_rejected_policy,
    );
    labeled_value(
        out,
        "l4guard_l7_rejected_total",
        "quota",
        snapshot.l7_rejected_quota,
    );
}

fn render_rejections(out: &mut String, snapshot: &StatsSnapshot) {
    labeled_header(
        out,
        "l4guard_rejected_total",
        "counter",
        "Connections rejected, by reason.",
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "policy",
        snapshot.rejected_policy,
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "budget",
        snapshot.rejected_budget,
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "per_ip",
        snapshot.rejected_per_ip,
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "rate",
        snapshot.rejected_rate,
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "stopped",
        snapshot.rejected_stopped,
    );
    labeled_value(
        out,
        "l4guard_rejected_total",
        "sessions",
        snapshot.rejected_sessions,
    );
}

fn render_failures(out: &mut String, snapshot: &StatsSnapshot) {
    labeled_header(
        out,
        "l4guard_upstream_failures_total",
        "counter",
        "Upstream connection failures, by reason.",
    );
    labeled_value(
        out,
        "l4guard_upstream_failures_total",
        "connect",
        snapshot.connect_failed,
    );
    labeled_value(
        out,
        "l4guard_upstream_failures_total",
        "timeout",
        snapshot.connect_timeout,
    );
    counter(
        out,
        "l4guard_copy_failures_total",
        "Sessions that ended with a copy error.",
        snapshot.copy_failed,
    );
    counter(
        out,
        "l4guard_cancelled_total",
        "Sessions cancelled by policy, listener stop, or shutdown.",
        snapshot.cancelled,
    );
    counter(
        out,
        "l4guard_completed_total",
        "Sessions that completed normally.",
        snapshot.completed,
    );
    counter(
        out,
        "l4guard_accept_errors_total",
        "Accept calls that failed.",
        snapshot.accept_errors,
    );
}

fn render_bytes(out: &mut String, snapshot: &StatsSnapshot) {
    labeled_header(
        out,
        "l4guard_bytes_total",
        "counter",
        "Bytes relayed, by direction.",
    );
    labeled_direction(
        out,
        "l4guard_bytes_total",
        "upstream",
        snapshot.bytes_to_upstream,
    );
    labeled_direction(
        out,
        "l4guard_bytes_total",
        "client",
        snapshot.bytes_to_client,
    );
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn labeled_header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn labeled_value(out: &mut String, name: &str, label: &str, value: u64) {
    labeled(out, name, "reason", label, value);
}

fn labeled_direction(out: &mut String, name: &str, label: &str, value: u64) {
    labeled(out, name, "direction", label, value);
}

fn labeled(out: &mut String, name: &str, key: &str, label: &str, value: u64) {
    let _ = writeln!(out, "{name}{{{key}=\"{label}\"}} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_drops_excess_events() {
        let (sender, receiver) = sync_channel(8);
        let sink = LogSink {
            queue: sender,
            direct: AtomicBool::new(false),
            limit: 2,
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            window: Mutex::new(RateWindow::new()),
        };

        for _ in 0..5 {
            sink.submit(b"event\n");
        }

        assert_eq!(receiver.try_iter().count(), 2);
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn full_queue_drops_without_blocking() {
        let (sender, receiver) = sync_channel(1);
        let sink = LogSink {
            queue: sender,
            direct: AtomicBool::new(false),
            limit: 100,
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            window: Mutex::new(RateWindow::new()),
        };

        for _ in 0..3 {
            sink.submit(b"event\n");
        }

        assert_eq!(receiver.try_iter().count(), 1);
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn metrics_expose_counters_and_version_only() {
        let snapshot = StatsSnapshot {
            active_sessions: 3,
            accepted: 10,
            rejected_policy: 4,
            rejected_budget: 1,
            rejected_per_ip: 2,
            rejected_rate: 5,
            rejected_stopped: 1,
            rejected_sessions: 1,
            connect_failed: 2,
            connect_timeout: 1,
            copy_failed: 3,
            cancelled: 6,
            completed: 7,
            accept_errors: 1,
            bytes_to_upstream: 1_024,
            bytes_to_client: 2_048,
            policy_version: 42,
            l7_requests: 9,
            l7_forwarded: 7,
            l7_rejected_policy: 2,
            ..StatsSnapshot::default()
        };

        let rendered = render_metrics(&snapshot);

        assert!(rendered.contains("l4guard_active_sessions 3"));
        assert!(rendered.contains("l4guard_rejected_total{reason=\"policy\"} 4"));
        assert!(rendered.contains("l4guard_upstream_failures_total{reason=\"timeout\"} 1"));
        assert!(rendered.contains("l4guard_bytes_total{direction=\"upstream\"} 1024"));
        assert!(rendered.contains("l4guard_policy_version 42"));
        assert!(rendered.contains("l4guard_l7_requests_total 9"));
        assert!(rendered.contains("l4guard_l7_forwarded_total 7"));
        assert!(rendered.contains("l4guard_l7_rejected_total{reason=\"policy\"} 2"));
        assert!(!rendered.contains("allow"));
        assert!(!rendered.contains("127.0.0.1"));
    }
}
