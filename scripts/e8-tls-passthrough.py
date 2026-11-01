#!/usr/bin/env python3
"""E8-T4: verify that state-sync RPC TLS passes through the guard.

The guard is a raw TCP relay. This harness starts a TLS server with a
self-signed certificate for ``localhost``, points the guard's fixed egress
listener at it, and connects through the guard with SNI ``localhost``. The
server reports the SNI and HTTP Host it saw. A successful run proves that the
guard does not terminate TLS, rewrite the Host header, or break certificate
verification.
"""

from __future__ import annotations

import http.server
import json
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAST_SNI: dict[str, str | None] = {"value": None}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        body = json.dumps(
            {
                "sni": LAST_SNI["value"],
                "host": self.headers.get("Host"),
                "path": self.path,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def build_cert(directory: Path) -> tuple[Path, Path]:
    cert = directory / "cert.pem"
    key = directory / "key.pem"
    config = directory / "openssl.cnf"
    config.write_text(
        "[req]\n"
        "distinguished_name = dn\n"
        "x509_extensions = ext\n"
        "prompt = no\n"
        "[dn]\n"
        "CN = localhost\n"
        "[ext]\n"
        "subjectAltName = DNS:localhost\n"
        "basicConstraints = CA:TRUE\n"
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-config",
            str(config),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return cert, key


def guard_binary() -> Path:
    for candidate in ("target/debug/l4guard", "target/release/l4guard"):
        path = ROOT / candidate
        if path.exists():
            return path
    subprocess.run(["cargo", "build"], cwd=ROOT, check=True)
    return ROOT / "target/debug/l4guard"


def main() -> int:
    if shutil.which("openssl") is None:
        print("openssl is required", file=sys.stderr)
        return 1

    temp = Path(tempfile.mkdtemp(prefix="l4guard-e8-tls."))
    guard: subprocess.Popen[bytes] | None = None
    httpd: http.server.ThreadingHTTPServer | None = None
    try:
        cert, key = build_cert(temp)
        tls_port = free_port()
        guard_port = free_port()

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=cert, keyfile=key)

        def record_sni(_sslobj: ssl.SSLSocket, server_name: str | None, _ctx: ssl.SSLContext) -> None:
            LAST_SNI["value"] = server_name

        context.set_servername_callback(record_sni)

        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", tls_port), Handler)
        httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        config = temp / "guard.toml"
        config.write_text(
            f"""
[limits]
max_connections = 8
max_connections_per_listener = 8
max_outbound_connections = 8
max_sessions = 8

[policy]
allow = ["127.0.0.1/32"]

[[upstreams]]
name = "provider"
address = "localhost:{tls_port}"

[[listeners]]
name = "provider-egress"
listen = "127.0.0.1:{guard_port}"
mode = "outbound"
upstream = "provider"
"""
        )

        log = (temp / "guard.log").open("wb")
        guard = subprocess.Popen(
            [str(guard_binary()), "--config", str(config)],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if guard.poll() is not None:
                print("guard exited during startup", file=sys.stderr)
                print(log.name, file=sys.stderr)
                return 1
            try:
                with socket.create_connection(("127.0.0.1", guard_port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            print("guard listener did not become ready", file=sys.stderr)
            return 1

        client_context = ssl.create_default_context(cafile=str(cert))
        client_context.check_hostname = True
        with socket.create_connection(("127.0.0.1", guard_port), timeout=5) as raw:
            with client_context.wrap_socket(raw, server_hostname="localhost") as tls:
                request = (
                    f"GET /status HTTP/1.1\r\nHost: localhost:{tls_port}\r\n"
                    "Connection: close\r\n\r\n"
                )
                tls.sendall(request.encode())
                response = b""
                while True:
                    chunk = tls.recv(4096)
                    if not chunk:
                        break
                    response += chunk

        head, _, body = response.partition(b"\r\n\r\n")
        if not (head.startswith(b"HTTP/1.") and b" 200 " in head):
            print(f"unexpected response: {head!r}", file=sys.stderr)
            return 1
        seen = json.loads(body)
        if seen.get("sni") != "localhost":
            print(f"SNI was not preserved: {seen!r}", file=sys.stderr)
            return 1
        if seen.get("host") != f"localhost:{tls_port}":
            print(f"Host header was not preserved: {seen!r}", file=sys.stderr)
            return 1

        print(
            "PASS: TLS/SNI passed through the guard "
            f"(sni={seen['sni']!r}, host={seen['host']!r})"
        )
        return 0
    finally:
        if guard is not None:
            guard.send_signal(signal.SIGTERM)
            try:
                guard.wait(timeout=10)
            except subprocess.TimeoutExpired:
                guard.kill()
                guard.wait()
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
