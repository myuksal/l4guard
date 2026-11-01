//! `l4guard` — an L4 TCP guard proxy for blockchain nodes.
//!
//! See `plan.md` for the design. This entry point loads and validates
//! configuration, binds every listener, and runs the relay until a shutdown
//! signal arrives. On Unix, `SIGHUP` reloads the reloadable policy settings
//! without dropping the listeners.

mod config;
mod observe;
mod policy;
mod relay;
mod sessions;

use std::process::ExitCode;
use std::sync::Arc;

use config::Config;
use tracing::info;

const USAGE: &str = "\
usage: l4guard --config <path>

options:
  -c, --config <path>  configuration file to load
  -h, --help           print this help
  -V, --version        print the version

signals:
  SIGHUP               reload policy, quotas, and listener state
  SIGINT, SIGTERM      shut down after draining active sessions";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("l4guard: {message}");
            ExitCode::FAILURE
        }
    }
}

#[tokio::main]
async fn run() -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-c" | "--config" => {
                if config_path.is_some() {
                    return Err(format!("--config given more than once\n{USAGE}"));
                }
                let path = arguments
                    .next()
                    .ok_or_else(|| format!("--config requires a path\n{USAGE}"))?;
                config_path = Some(path);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "-V" | "--version" => {
                println!("l4guard {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}\n{USAGE}")),
        }
    }
    let path = config_path.ok_or_else(|| format!("missing --config\n{USAGE}"))?;
    let path = std::path::PathBuf::from(path);
    let config = Config::load_shared(&path).map_err(|error| error.to_string())?;
    let log_guard = observe::init_logging(&config.observe);
    print_summary(&config);

    let relay = relay::Relay::bind(Arc::clone(&config))
        .await
        .map_err(|error| error.to_string())?;
    let stats = relay.stats();
    let metrics_task = match config.observe.metrics_listen {
        Some(address) => {
            let server = observe::MetricsServer::bind(address, stats.clone())
                .await
                .map_err(|error| format!("cannot bind metrics endpoint {address}: {error}"))?;
            info!(address = %address, "metrics endpoint ready");
            Some(tokio::spawn(server.run()))
        }
        None => None,
    };
    #[cfg(unix)]
    let reload_task = spawn_reload_task(relay.handle(), path);
    let result = relay.run(shutdown_signal()).await;
    #[cfg(unix)]
    reload_task.abort();
    if let Some(task) = metrics_task {
        task.abort();
    }
    result.map_err(|error| error.to_string())?;
    let snapshot = stats.snapshot();
    info!(
        active_sessions = snapshot.active_sessions,
        accepted = snapshot.accepted,
        rejected_policy = snapshot.rejected_policy,
        rejected_budget = snapshot.rejected_budget,
        rejected_per_ip = snapshot.rejected_per_ip,
        rejected_rate = snapshot.rejected_rate,
        rejected_stopped = snapshot.rejected_stopped,
        rejected_sessions = snapshot.rejected_sessions,
        connect_failed = snapshot.connect_failed,
        connect_timeout = snapshot.connect_timeout,
        copy_failed = snapshot.copy_failed,
        cancelled = snapshot.cancelled,
        completed = snapshot.completed,
        bytes_to_upstream = snapshot.bytes_to_upstream,
        bytes_to_client = snapshot.bytes_to_client,
        policy_version = snapshot.policy_version,
        "shutdown complete"
    );
    drop(log_guard);
    Ok(())
}

/// Waits for `SIGINT` or, on Unix, `SIGTERM`.
async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %error, "cannot wait for interrupt signal; stopping");
        }
    };
    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    let _ = signal.recv().await;
                }
                Err(error) => {
                    tracing::error!(error = %error, "cannot wait for termination signal");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            () = interrupt => {}
            () = terminate => {}
        }
    }
    #[cfg(not(unix))]
    interrupt.await;
}

/// Reloads policy from `path` on every `SIGHUP`.
#[cfg(unix)]
fn spawn_reload_task(
    handle: relay::RelayHandle,
    path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(error = %error, "cannot install reload signal handler");
                return;
            }
        };
        while hangup.recv().await.is_some() {
            match handle.reload(&path).await {
                Ok(outcome) => info!(
                    previous_version = %outcome.previous_version,
                    version = %outcome.version,
                    cancelled = outcome.cancelled,
                    stopped = ?outcome.stopped,
                    resumed = ?outcome.resumed,
                    "policy reloaded"
                ),
                Err(error) => tracing::warn!(
                    error = %error,
                    "policy reload failed; keeping last valid policy"
                ),
            }
        }
    })
}

fn print_summary(config: &Config) {
    println!(
        "l4guard {}: loaded {} listener(s), {} upstream(s)",
        env!("CARGO_PKG_VERSION"),
        config.listeners.len(),
        config.upstreams.len()
    );
    for listener in &config.listeners {
        println!(
            "  listener {} ({}) {} -> {}",
            listener.name,
            listener.mode.as_str(),
            listener.listen,
            listener.upstream
        );
    }
    for upstream in &config.upstreams {
        println!(
            "  upstream {} {}:{}",
            upstream.name,
            upstream.address.host(),
            upstream.address.port()
        );
    }
    println!(
        "  limits connections={} inbound={} outbound={} per_listener={} per_ip={} sessions={} rate={}/s burst={} connect_timeout={:?} drain_timeout={:?} ip_ttl={:?}",
        config.limits.max_connections,
        config.limits.inbound_connections(),
        config.limits.max_outbound_connections,
        config.limits.max_connections_per_listener,
        config.limits.max_connections_per_ip,
        config.limits.max_sessions,
        config.limits.max_new_connections_per_second,
        config.limits.new_connection_burst,
        config.limits.connect_timeout(),
        config.limits.drain_timeout(),
        config.limits.ip_entry_ttl()
    );
    println!(
        "  policy allow={} deny={} emergency_block={}",
        config.policy.allow.len(),
        config.policy.deny.len(),
        config.policy.emergency_block
    );
    match config.observe.metrics_listen {
        Some(address) => println!(
            "  observe level={:?} log_rate={}/s log_queue={} metrics=http://{address}/metrics",
            config.observe.level,
            config.observe.log_rate_limit_per_second,
            config.observe.log_queue_capacity
        ),
        None => println!(
            "  observe level={:?} log_rate={}/s log_queue={} metrics=disabled",
            config.observe.level,
            config.observe.log_rate_limit_per_second,
            config.observe.log_queue_capacity
        ),
    }
}
