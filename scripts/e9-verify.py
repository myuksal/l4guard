#!/usr/bin/env python3
"""E9 load, failure, and bypass verification harness.

This harness exercises the guard as a black box: it starts the compiled
binary, a few local upstreams, and checks both observable behavior and the
rendered Kubernetes manifests. It covers:

* ``load``       E9-T1: a connection flood stays inside every bound.
* ``emergency``  E9-T2: emergency block and listener stop reach new and
                 existing sessions.
* ``bypass``     E9-T3: manifest audit, IPv6 policy, and UDP non-relay.
* ``replace``    E9-T4: the backend survives a guard kill and reconnects to a
                 replacement guard.
* ``rpc-bypass`` E9-T6: batch, escape, duplicate-key, URI, and Upgrade shapes
                 cannot reach an L7 upstream.
* ``replicas``   E9-T7: identical policies report the same version on every
                 replica, and a changed policy reports a different version.

Run every check with ``./scripts/e9-verify.py all``. The full CometBFT
recovery measurement (E9-T5) is documented in ``docs/e9.md`` and is not part
of ``all`` because it needs a longer-lived chain environment.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
GUARD_BINARY = ROOT / "target" / "debug" / "l4guard"
_RESOLVED_BINARY: Path | None = None


# ---------------------------------------------------------------------------
# Process and metric helpers
# ---------------------------------------------------------------------------


def guard_binary() -> Path:
    global _RESOLVED_BINARY
    if _RESOLVED_BINARY is not None:
        return _RESOLVED_BINARY
    if os.environ.get("E9_SKIP_BUILD") != "1":
        subprocess.run(["cargo", "build", "--quiet"], cwd=ROOT, check=True)
    elif not GUARD_BINARY.exists():
        raise AssertionError("E9_SKIP_BUILD=1 but target/debug/l4guard is missing")
    _RESOLVED_BINARY = GUARD_BINARY
    return _RESOLVED_BINARY


def free_port(host: str = "127.0.0.1", family: int = socket.AF_INET) -> int:
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def wait_until(
    description: str,
    predicate: Callable[[], bool],
    timeout: float = 10.0,
    interval: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {description}")


def write_config(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text)
    os.replace(temporary, path)


def parse_metrics(text: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.rpartition(" ")
        if not separator:
            continue
        try:
            values[name] = float(value)
        except ValueError:
            continue
    return values


def process_fds(pid: int) -> int | None:
    proc = Path(f"/proc/{pid}/fd")
    if proc.exists():
        try:
            return len(list(proc.iterdir()))
        except OSError:
            return None
    lsof = shutil.which("lsof")
    if lsof is None:
        return None
    result = subprocess.run(
        [lsof, "-a", "-p", str(pid), "-d", "0-9999", "-Fn"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return sum(1 for line in result.stdout.splitlines() if line.startswith("f"))


def process_rss_kib(pid: int) -> int | None:
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
        return None
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return int(value) if value else None


class Guard:
    """A running guard process with an optional metrics endpoint."""

    def __init__(
        self,
        config_path: Path,
        log_path: Path,
        metrics_port: int | None = None,
        listener_port: int | None = None,
    ) -> None:
        self.config_path = config_path
        self.log_path = log_path
        self.metrics_port = metrics_port
        self.listener_port = listener_port
        self.log = log_path.open("wb")
        self.process = subprocess.Popen(
            [str(guard_binary()), "--config", str(config_path)],
            cwd=ROOT,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )

    def wait_ready(self, timeout: float = 10.0) -> None:
        def ready() -> bool:
            if self.process.poll() is not None:
                return False
            if self.metrics_port is not None:
                return self._http_ok("/healthz")
            if self.listener_port is not None:
                try:
                    with socket.create_connection(
                        ("127.0.0.1", self.listener_port), timeout=0.2
                    ):
                        return True
                except OSError:
                    return False
            return True

        try:
            wait_until("guard readiness", ready, timeout=timeout)
        except AssertionError:
            self.fail("guard did not become ready")

    def _http_ok(self, path: str) -> bool:
        if self.metrics_port is None:
            return False
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.metrics_port}{path}", timeout=0.5
            ) as response:
                return response.status == 200
        except (OSError, urllib.error.URLError):
            return False

    def metrics(self) -> dict[str, float]:
        if self.metrics_port is None:
            raise AssertionError("guard has no metrics endpoint")
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.metrics_port}/metrics", timeout=1.0
        ) as response:
            return parse_metrics(response.read().decode())

    def metric(self, name: str) -> float:
        return self.metrics().get(name, 0.0)

    def wait_metric(
        self,
        description: str,
        predicate: Callable[[dict[str, float]], bool],
        timeout: float = 10.0,
    ) -> dict[str, float]:
        result: dict[str, float] = {}

        def check() -> bool:
            nonlocal result
            if self.process.poll() is not None:
                return False
            try:
                result = self.metrics()
            except OSError:
                return False
            return predicate(result)

        try:
            wait_until(description, check, timeout=timeout)
        except AssertionError:
            self.fail(f"timed out waiting for {description}")
        return result

    def send_signal(self, number: int) -> None:
        if self.process.poll() is None:
            self.process.send_signal(number)

    def reload(self) -> None:
        self.send_signal(signal.SIGHUP)

    def stop(self, number: int | None = None, timeout: float = 10.0) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM if number is None else number)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.log.close()

    def fail(self, message: str) -> None:
        self.stop()
        text = ""
        try:
            text = self.log_path.read_text(errors="replace")
        except OSError:
            pass
        raise AssertionError(f"{message}\n--- guard log ---\n{text}")


# ---------------------------------------------------------------------------
# Local upstreams
# ---------------------------------------------------------------------------


class EchoServer:
    """Accepts TCP connections and echoes every byte."""

    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(128)
        self.address = self.socket.getsockname()
        self.accepted = 0
        self.stopped = False
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while not self.stopped:
            try:
                connection, _ = self.socket.accept()
            except OSError:
                break
            self.accepted += 1
            threading.Thread(
                target=self._echo, args=(connection,), daemon=True
            ).start()

    @staticmethod
    def _echo(connection: socket.socket) -> None:
        try:
            while True:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                connection.sendall(chunk)
        except OSError:
            pass
        finally:
            connection.close()

    def close(self) -> None:
        self.stopped = True
        self.socket.close()


class HttpUpstream:
    """Accepts one request per connection and counts forwarded requests."""

    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(128)
        self.address = self.socket.getsockname()
        self.hits = 0
        self.requests: list[bytes] = []
        self.lock = threading.Lock()
        self.stopped = False
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while not self.stopped:
            try:
                connection, _ = self.socket.accept()
            except OSError:
                break
            threading.Thread(
                target=self._handle, args=(connection,), daemon=True
            ).start()

    def _handle(self, connection: socket.socket) -> None:
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = connection.recv(65536)
                if not chunk:
                    return
                data += chunk
            head, rest = data.split(b"\r\n\r\n", 1)
            length = 0
            for line in head.split(b"\r\n")[1:]:
                name, _, value = line.partition(b":")
                if name.strip().lower() == b"content-length":
                    length = int(value.strip())
            while len(rest) < length:
                chunk = connection.recv(65536)
                if not chunk:
                    return
                rest += chunk
            with self.lock:
                self.hits += 1
                self.requests.append(head + b"\r\n\r\n" + rest[:length])
            connection.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: 2\r\nConnection: close\r\n\r\nok"
            )
        except (OSError, ValueError):
            pass
        finally:
            connection.close()

    def close(self) -> None:
        self.stopped = True
        self.socket.close()


COUNTER_BACKEND = r"""
import socket
import sys

port = int(sys.argv[1])
path = sys.argv[2]
try:
    count = int(open(path).read())
except (OSError, ValueError):
    count = 0

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", port))
listener.listen(16)
while True:
    connection, _ = listener.accept()
    try:
        reader = connection.makefile("rwb")
        for line in reader:
            if line.strip() != b"inc":
                continue
            count += 1
            with open(path, "w") as handle:
                handle.write(str(count))
            reader.write(f"{count}\n".encode())
            reader.flush()
    except OSError:
        pass
    finally:
        connection.close()
"""


class CounterBackend:
    """A separate process that keeps a counter in a data file."""

    def __init__(self, port: int, data_path: Path) -> None:
        self.data_path = data_path
        self.process = subprocess.Popen(
            [sys.executable, "-c", COUNTER_BACKEND, str(port), str(data_path)],
            cwd=ROOT,
        )
        wait_until("counter backend", self._ready)

    def _ready(self) -> bool:
        if self.process.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                return True
        except OSError:
            return False

    @property
    def port(self) -> int:
        return int(self.process.args[3])

    def value(self) -> int:
        try:
            return int(self.data_path.read_text())
        except (OSError, ValueError):
            return 0

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


# ---------------------------------------------------------------------------
# Socket helpers
# ---------------------------------------------------------------------------


def read_exactly(connection: socket.socket, length: int) -> bytes:
    data = b""
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data


def recv_until_close(connection: socket.socket, timeout: float = 5.0) -> bytes:
    connection.settimeout(timeout)
    data = b""
    while True:
        try:
            chunk = connection.recv(65536)
        except (ConnectionResetError, socket.timeout):
            break
        if not chunk:
            break
        data += chunk
    return data


def http_request(port: int, request: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(request)
        return recv_until_close(connection)


def echo_round_trip(port: int, payload: bytes = b"ping") -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(payload)
        return read_exactly(connection, len(payload))


def echo_round_trip_ipv6(port: int, payload: bytes = b"ping") -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as connection:
            connection.settimeout(2)
            connection.connect(("::1", port))
            connection.sendall(payload)
            return read_exactly(connection, len(payload)) == payload
    except OSError:
        return False


def expect_closed(port: int, timeout: float = 2.0) -> None:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        try:
            data = connection.recv(1)
        except (ConnectionResetError, socket.timeout):
            data = b""
    if data:
        raise AssertionError("expected the guard to close the connection")


# ---------------------------------------------------------------------------
# Configuration text
# ---------------------------------------------------------------------------


def guard_config(
    listener: str,
    upstream: str,
    metrics: str,
    *,
    policy: str | None = None,
    limits: str | None = None,
    listener_extra: str = "",
    observe_extra: str = "",
) -> str:
    policy_text = policy or 'allow = ["127.0.0.1/32", "::1/128"]'
    limits_text = limits or """
max_connections = 32
max_connections_per_listener = 32
max_outbound_connections = 0
max_sessions = 32
max_connections_per_ip = 32
max_new_connections_per_second = 10000
new_connection_burst = 10000
max_ip_entries = 16
ip_entry_ttl_seconds = 1
"""
    return f"""
[limits]
{limits_text.strip()}

[policy]
{policy_text}

[observe]
metrics_listen = "{metrics}"
{observe_extra}

[[upstreams]]
name = "upstream"
address = "{upstream}"

[[listeners]]
name = "test"
listen = "{listener}"
upstream = "upstream"
{listener_extra}
"""


# ---------------------------------------------------------------------------
# E9-T1: connection flood
# ---------------------------------------------------------------------------


def check_load() -> None:
    echo = EchoServer()
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-load.") as temporary:
        directory = Path(temporary)
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        listener_port = free_port()
        write_config(
            config_path,
            guard_config(
                f"127.0.0.1:{listener_port}",
                f"127.0.0.1:{echo.address[1]}",
                f"127.0.0.1:{metrics_port}",
                limits="""
max_connections = 32
max_connections_per_listener = 32
max_outbound_connections = 0
max_sessions = 32
max_connections_per_ip = 1000
max_new_connections_per_second = 10000
new_connection_burst = 10000
max_ip_entries = 16
ip_entry_ttl_seconds = 1
""",
            ),
        )
        guard = Guard(config_path, log_path, metrics_port, listener_port)
        try:
            guard.wait_ready()
            baseline_fds = process_fds(guard.process.pid)
            baseline_rss = process_rss_kib(guard.process.pid)

            clients: list[socket.socket] = []
            for _ in range(256):
                try:
                    clients.append(
                        socket.create_connection(
                            ("127.0.0.1", listener_port), timeout=2
                        )
                    )
                except OSError:
                    pass

            guard.wait_metric(
                "the flood to fill the session budget",
                lambda metrics: int(metrics.get("l4guard_active_sessions", 0)) == 32,
            )
            metrics = guard.metrics()
            accepted = int(metrics.get("l4guard_accepted_total", 0))
            rejected = int(
                metrics.get('l4guard_rejected_total{reason="budget"}', 0)
            )
            ip_entries = int(metrics.get("l4guard_ip_entries", 0))
            active = 0
            fds = baseline_fds
            rss = baseline_rss
            for _ in range(5):
                metrics = guard.metrics()
                active = max(
                    active, int(metrics.get("l4guard_active_sessions", 0))
                )
                if baseline_fds is not None:
                    fds = max(fds or 0, process_fds(guard.process.pid) or 0)
                if baseline_rss is not None:
                    rss = max(rss or 0, process_rss_kib(guard.process.pid) or 0)
                time.sleep(0.1)

            assert active == 32, f"active sessions must stay capped, got {active}"
            assert accepted <= 32, f"admitted sessions must be capped, got {accepted}"
            assert rejected > 0, "the flood must be rejected by the budget"
            assert ip_entries <= 16, f"IP map exceeded its cap: {ip_entries}"
            if baseline_fds is not None and fds is not None:
                assert fds <= baseline_fds + 2 * 32 + 32, (
                    f"file descriptors grew beyond the bound: "
                    f"baseline={baseline_fds} now={fds}"
                )
            if rss is not None:
                assert rss < 256 * 1024, f"RSS exceeded 256 MiB: {rss} KiB"

            for client in clients:
                client.close()
            guard.wait_metric(
                "the flood to drain",
                lambda metrics: int(metrics.get("l4guard_active_sessions", 0)) == 0,
                timeout=5,
            )
            if baseline_fds is not None:
                wait_until(
                    "file descriptors to return",
                    lambda: (process_fds(guard.process.pid) or baseline_fds)
                    <= baseline_fds + 16,
                    timeout=5,
                )

            print(
                "PASS load: "
                f"active={active} accepted={accepted} rejected_budget={rejected} "
                f"ip_entries={ip_entries} fds={fds}/{baseline_fds} "
                f"rss_kib={rss}/{baseline_rss}"
            )
        finally:
            guard.stop()
            echo.close()


# ---------------------------------------------------------------------------
# E9-T2: emergency block and listener stop
# ---------------------------------------------------------------------------


def check_emergency() -> None:
    echo = EchoServer()
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-emergency.") as temporary:
        directory = Path(temporary)
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        listener_port = free_port()

        def config(emergency: bool, enabled: bool = True) -> str:
            return guard_config(
                f"127.0.0.1:{listener_port}",
                f"127.0.0.1:{echo.address[1]}",
                f"127.0.0.1:{metrics_port}",
                policy=(
                    'allow = ["127.0.0.1/32", "::1/128"]\n'
                    f"emergency_block = {str(emergency).lower()}"
                ),
                listener_extra=f"enabled = {str(enabled).lower()}",
            )

        write_config(config_path, config(False))
        guard = Guard(config_path, log_path, metrics_port, listener_port)
        try:
            guard.wait_ready()
            with socket.create_connection(
                ("127.0.0.1", listener_port), timeout=5
            ) as client:
                client.sendall(b"ping")
                assert read_exactly(client, 4) == b"ping"
                guard.wait_metric(
                    "one active session",
                    lambda metrics: int(metrics.get("l4guard_active_sessions", 0))
                    == 1,
                )
                version = int(guard.metric("l4guard_policy_version"))

                write_config(config_path, config(True))
                guard.reload()
                guard.wait_metric(
                    "the emergency policy to apply",
                    lambda metrics: int(metrics.get("l4guard_policy_version", 0))
                    != version,
                )
                guard.wait_metric(
                    "the existing session to be cancelled",
                    lambda metrics: int(metrics.get("l4guard_cancelled_total", 0))
                    >= 1,
                )
                client.settimeout(2)
                try:
                    closed = client.recv(1)
                except (ConnectionResetError, socket.timeout):
                    closed = b""
                assert closed == b"", "emergency block must close existing sessions"

            expect_closed(listener_port)
            guard.wait_metric(
                "a new connection to be rejected",
                lambda metrics: int(
                    metrics.get('l4guard_rejected_total{reason="policy"}', 0)
                )
                >= 1,
            )

            write_config(config_path, config(False))
            guard.reload()
            guard.wait_metric(
                "the original policy version",
                lambda metrics: int(metrics.get("l4guard_policy_version", 0))
                == version,
            )
            assert echo_round_trip(listener_port) == b"ping"

            write_config(config_path, config(False, enabled=False))
            guard.reload()
            guard.wait_metric(
                "the listener stop to apply",
                lambda metrics: int(metrics.get("l4guard_policy_version", 0))
                != version,
            )
            expect_closed(listener_port)
            guard.wait_metric(
                "the listener stop rejection",
                lambda metrics: int(
                    metrics.get('l4guard_rejected_total{reason="stopped"}', 0)
                )
                >= 1,
            )

            write_config(config_path, config(False, enabled=True))
            guard.reload()
            guard.wait_metric(
                "the listener to resume",
                lambda metrics: int(metrics.get("l4guard_policy_version", 0))
                == version,
            )
            wait_until(
                "echo after resume",
                lambda: echo_round_trip(listener_port) == b"ping",
                timeout=5,
            )

            print(
                "PASS emergency: existing sessions cancelled, new connections "
                "rejected, listener stop and resume applied"
            )
        finally:
            guard.stop()
            echo.close()


# ---------------------------------------------------------------------------
# E9-T3: bypass paths
# ---------------------------------------------------------------------------


def _yaml_documents(text: str) -> list[str]:
    return [document for document in text.split("\n---\n") if document.strip()]


def _document(documents: list[str], kind: str, name: str) -> str:
    for document in documents:
        if f"kind: {kind}" in document and f"name: {name}" in document:
            return document
    raise AssertionError(f"missing {kind} {name} in rendered manifests")


def check_manifests() -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise AssertionError("kubectl is required for the manifest audit")
    base = subprocess.run(
        [kubectl, "kustomize", "deploy/base"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    backend = subprocess.run(
        [kubectl, "kustomize", "deploy/backend"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    assert "hostPort" not in base, "Guard manifests must not use hostPort"
    assert "hostPort" not in backend, "backend manifests must not use hostPort"
    backend_service = _document(
        _yaml_documents(backend), "Service", "blockchain-node"
    )
    assert "type: ClusterIP" in backend_service
    assert "type: NodePort" not in backend_service
    assert "type: LoadBalancer" not in backend_service

    for name in ("l4guard-p2p", "l4guard-rpc"):
        service = _document(_yaml_documents(base), "Service", name)
        assert "type: LoadBalancer" in service
        assert "externalTrafficPolicy: Local" in service
    for name in ("l4guard-private", "rpc-provider-a"):
        service = _document(_yaml_documents(base), "Service", name)
        assert "type: ClusterIP" in service

    backend_policy = _document(
        _yaml_documents(backend), "NetworkPolicy", "blockchain-node"
    )
    _, _, backend_egress = backend_policy.partition("  egress:")
    backend_egress = backend_egress.split("\n  ingress:")[0]
    assert "0.0.0.0/0" not in backend_egress
    assert "::/0" not in backend_egress
    assert "kubernetes.io/metadata.name: l4guard" in backend_policy
    # The backend must not have a direct P2P path. It reaches peers only
    # through Guard's private 31001/31002 listeners.
    assert "port: 26656" not in backend_egress
    assert "port: 30303" not in backend_egress

    guard_policy = _document(
        _yaml_documents(base), "NetworkPolicy", "l4guard"
    )
    assert "kubernetes.io/metadata.name: blockchain" in guard_policy
    assert "port: 31001" in guard_policy
    assert "port: 32001" in guard_policy

    print(
        "PASS manifests: backend stays ClusterIP, public services use "
        "externalTrafficPolicy Local, backend egress has no default route"
    )


def check_ipv6() -> None:
    if not socket.has_ipv6:
        print("SKIP ipv6: Python reports no IPv6 support")
        return
    echo = EchoServer()
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-ipv6.") as temporary:
        directory = Path(temporary)
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        try:
            listener_port = free_port("::1", socket.AF_INET6)
        except OSError:
            print("SKIP ipv6: loopback IPv6 is not available")
            echo.close()
            return

        def config(deny: str) -> str:
            return guard_config(
                f"[::1]:{listener_port}",
                f"127.0.0.1:{echo.address[1]}",
                f"127.0.0.1:{metrics_port}",
                policy=f'allow = ["::1/128"]\n{deny}',
            )

        write_config(config_path, config(""))
        guard = Guard(config_path, log_path, metrics_port)
        try:
            guard.wait_ready()
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as client:
                client.settimeout(5)
                client.connect(("::1", listener_port))
                client.sendall(b"ipv6")
                assert read_exactly(client, 4) == b"ipv6"

                write_config(config_path, config('deny = ["::1/128"]'))
                guard.reload()
                guard.wait_metric(
                    "the IPv6 deny to cancel the existing session",
                    lambda metrics: int(metrics.get("l4guard_cancelled_total", 0))
                    >= 1,
                )
                client.settimeout(2)
                try:
                    closed = client.recv(1)
                except (ConnectionResetError, socket.timeout):
                    closed = b""
                assert closed == b"", "IPv6 deny must cancel the existing session"

            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as late:
                late.settimeout(2)
                late.connect(("::1", listener_port))
                try:
                    assert late.recv(1) == b""
                except (ConnectionResetError, socket.timeout):
                    pass
            guard.wait_metric(
                "the IPv6 rejection counter",
                lambda metrics: int(
                    metrics.get('l4guard_rejected_total{reason="policy"}', 0)
                )
                >= 1,
            )

            write_config(config_path, config(""))
            guard.reload()
            wait_until(
                "the IPv6 allow policy to return",
                lambda: echo_round_trip_ipv6(listener_port),
                timeout=5,
            )
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as client:
                client.settimeout(5)
                client.connect(("::1", listener_port))
                client.sendall(b"ok")
                assert read_exactly(client, 2) == b"ok"

            print("PASS ipv6: IPv6 allow/deny reaches new and existing sessions")
        finally:
            guard.stop()
            echo.close()


def check_udp() -> None:
    echo = EchoServer()
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-udp.") as temporary:
        directory = Path(temporary)
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        listener_port = free_port()
        write_config(
            config_path,
            guard_config(
                f"127.0.0.1:{listener_port}",
                f"127.0.0.1:{echo.address[1]}",
                f"127.0.0.1:{metrics_port}",
            ),
        )
        guard = Guard(config_path, log_path, metrics_port, listener_port)
        try:
            guard.wait_ready()
            accepted_before = int(guard.metric("l4guard_accepted_total"))
            echo_before = echo.accepted
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as datagram:
                datagram.sendto(b"udp-must-not-relay", ("127.0.0.1", listener_port))
            time.sleep(0.5)
            accepted_after = int(guard.metric("l4guard_accepted_total"))
            assert accepted_after == accepted_before, "UDP must not create a session"
            assert echo.accepted == echo_before, "UDP must not reach the upstream"
            print("PASS udp: UDP datagrams to the TCP listener are not relayed")
        finally:
            guard.stop()
            echo.close()


def check_bypass() -> None:
    check_manifests()
    check_ipv6()
    check_udp()


# ---------------------------------------------------------------------------
# E9-T4: guard replacement
# ---------------------------------------------------------------------------


def check_replace() -> None:
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-replace.") as temporary:
        directory = Path(temporary)
        data_path = directory / "counter.txt"
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        listener_port = free_port()
        backend_port = free_port()
        write_config(
            config_path,
            guard_config(
                f"127.0.0.1:{listener_port}",
                f"127.0.0.1:{backend_port}",
                f"127.0.0.1:{metrics_port}",
            ),
        )
        backend = CounterBackend(backend_port, data_path)
        guard = Guard(config_path, log_path, metrics_port, listener_port)
        try:
            guard.wait_ready()
            with socket.create_connection(
                ("127.0.0.1", listener_port), timeout=5
            ) as client:
                client.sendall(b"inc\n")
                assert read_exactly(client, 2) == b"1\n"
            assert backend.value() == 1

            backend_pid = backend.process.pid
            guard.stop(signal.SIGKILL)
            assert backend.process.poll() is None, "the backend must survive"
            assert backend.value() == 1, "backend data must survive"

            down_start = time.monotonic()
            replacement = Guard(config_path, log_path, metrics_port, listener_port)
            try:
                replacement.wait_ready()
                with socket.create_connection(
                    ("127.0.0.1", listener_port), timeout=5
                ) as client:
                    client.sendall(b"inc\n")
                    assert read_exactly(client, 2) == b"2\n"
                reconnect_seconds = time.monotonic() - down_start
                assert backend.process.pid == backend_pid
                assert backend.value() == 2
                print(
                    "PASS replace: backend process and data survived; "
                    f"reconnect took {reconnect_seconds:.3f}s"
                )
            finally:
                replacement.stop()
        finally:
            if guard.process.poll() is None:
                guard.stop()
            backend.close()


# ---------------------------------------------------------------------------
# E9-T6: RPC policy bypass shapes
# ---------------------------------------------------------------------------


def check_rpc_bypass() -> None:
    upstream = HttpUpstream()
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-rpc.") as temporary:
        directory = Path(temporary)
        config_path = directory / "guard.toml"
        log_path = directory / "guard.log"
        metrics_port = free_port()
        listener_port = free_port()
        write_config(
            config_path,
            guard_config(
                f"127.0.0.1:{listener_port}",
                f"127.0.0.1:{upstream.address[1]}",
                f"127.0.0.1:{metrics_port}",
                listener_extra="""
[listeners.l7]
allow_methods = ["status", "eth_call"]
deny_methods = ["debug_traceCall"]
max_batch = 4
""",
            ),
        )
        guard = Guard(config_path, log_path, metrics_port, listener_port)
        try:
            guard.wait_ready()

            def request(raw: str) -> bytes:
                return http_request(listener_port, raw.encode())

            allowed = request("GET /status HTTP/1.1\r\nHost: node\r\n\r\n")
            assert allowed.startswith(b"HTTP/1.1 200"), allowed
            assert upstream.hits == 1

            rejected = [
                (
                    "GET /%73tatus HTTP/1.1\r\nHost: node\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
                (
                    "GET //status HTTP/1.1\r\nHost: node\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
                (
                    "GET http://node/status HTTP/1.1\r\nHost: node\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
                (
                    "GET /status/ HTTP/1.1\r\nHost: node\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
                (
                    "GET /debug_traceCall HTTP/1.1\r\nHost: node\r\n\r\n",
                    b"HTTP/1.1 403",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\n"
                    "Content-Type: application/json\r\nContent-Length: 50\r\n\r\n"
                    '[{"method":"status"},{"method":"debug_traceCall"}]',
                    b"HTTP/1.1 403",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\n"
                    "Content-Type: application/json\r\nContent-Length: 46\r\n\r\n"
                    '{"method":"status","method":"debug_traceCall"}',
                    b"HTTP/1.1 400",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\n"
                    "Content-Type: application/json\r\nContent-Length: 33\r\n\r\n"
                    '{"method":"debug_trace\\u0043all"}',
                    b"HTTP/1.1 403",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\nUpgrade: websocket\r\n"
                    "Content-Length: 0\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\nContent-Encoding: gzip\r\n"
                    "Content-Length: 0\r\n\r\n",
                    b"HTTP/1.1 415",
                ),
                (
                    "POST / HTTP/1.1\r\nHost: node\r\nContent-Length: 1\r\n"
                    "Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                    b"HTTP/1.1 400",
                ),
            ]
            for raw, expected in rejected:
                response = request(raw)
                assert response.startswith(expected), (
                    f"expected {expected!r} for {raw!r}, got {response!r}"
                )
            assert upstream.hits == 1, "rejected shapes must not reach the upstream"

            escaped = request(
                "POST / HTTP/1.1\r\nHost: node\r\n"
                "Content-Type: application/json\r\nContent-Length: 24\r\n\r\n"
                '{"method":"\\u0073tatus"}'
            )
            assert escaped.startswith(b"HTTP/1.1 200"), escaped
            assert upstream.hits == 2

            chunked = request(
                "POST / HTTP/1.1\r\nHost: node\r\n"
                "Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n"
                "15\r\n{\"method\":\"eth_call\"}\r\n0\r\n\r\n"
            )
            assert chunked.startswith(b"HTTP/1.1 200"), chunked
            assert upstream.hits == 3

            metrics = guard.metrics()
            assert int(metrics.get('l4guard_l7_rejected_total{reason="policy"}', 0)) >= 2
            assert int(metrics.get('l4guard_l7_rejected_total{reason="json"}', 0)) >= 1
            assert int(metrics.get('l4guard_l7_rejected_total{reason="upgrade"}', 0)) >= 1
            print(
                "PASS rpc-bypass: batch, escape, duplicate-key, URI, encoding, "
                "and Upgrade shapes were rejected before the upstream"
            )
        finally:
            guard.stop()
            upstream.close()


# ---------------------------------------------------------------------------
# E9-T7: replica policy version
# ---------------------------------------------------------------------------


def check_replicas() -> None:
    with tempfile.TemporaryDirectory(prefix="l4guard-e9-replicas.") as temporary:
        directory = Path(temporary)
        ports = [free_port() for _ in range(6)]
        metrics = [free_port() for _ in range(3)]
        configs = [directory / f"guard-{index}.toml" for index in range(3)]
        logs = [directory / f"guard-{index}.log" for index in range(3)]

        def config(index: int, emergency: bool) -> str:
            return guard_config(
                f"127.0.0.1:{ports[index]}",
                "127.0.0.1:1",
                f"127.0.0.1:{metrics[index]}",
                policy=(
                    'allow = ["127.0.0.1/32"]\n'
                    f"emergency_block = {str(emergency).lower()}"
                ),
            )

        for index in range(3):
            write_config(configs[index], config(index, index == 2))

        first = Guard(configs[0], logs[0], metrics[0], ports[0])
        second = Guard(configs[1], logs[1], metrics[1], ports[1])
        changed = Guard(configs[2], logs[2], metrics[2], ports[2])
        try:
            first.wait_ready()
            second.wait_ready()
            changed.wait_ready()
            same = int(first.metric("l4guard_policy_version"))
            assert same == int(second.metric("l4guard_policy_version"))
            assert same != int(changed.metric("l4guard_policy_version"))

            write_config(configs[0], config(0, True))
            first.reload()
            first.wait_metric(
                "the changed replica version",
                lambda metrics_snapshot: int(
                    metrics_snapshot.get("l4guard_policy_version", 0)
                )
                == int(changed.metric("l4guard_policy_version")),
            )

            second.stop()
            restarted = Guard(configs[1], logs[1], metrics[1], ports[1])
            try:
                restarted.wait_ready()
                assert int(restarted.metric("l4guard_policy_version")) == same
            finally:
                restarted.stop()

            print(
                "PASS replicas: identical policies share a version, a changed "
                "policy differs, and a restart preserves the version"
            )
        finally:
            first.stop()
            second.stop()
            changed.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


CHECKS: dict[str, Callable[[], None]] = {
    "load": check_load,
    "emergency": check_emergency,
    "bypass": check_bypass,
    "replace": check_replace,
    "rpc-bypass": check_rpc_bypass,
    "replicas": check_replicas,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "check",
        choices=[*CHECKS, "all"],
        help="verification check to run",
    )
    arguments = parser.parse_args()
    names = list(CHECKS) if arguments.check == "all" else [arguments.check]
    for name in names:
        print(f"== {name} ==")
        try:
            CHECKS[name]()
        except AssertionError as error:
            print(f"FAIL {name}: {error}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
