#!/usr/bin/env python3
"""Publish a silent Sibline presence pulse over core NATS.

Intended to run periodically (e.g. launchd/systemd every few minutes). It
publishes a lightweight ``kind=heartbeat`` envelope to
``sibline.presence.<agent>`` without waking any foreground agent session,
mailbox, or chat surface. Peers can subscribe to ``sibline.presence.*`` to
build a low-noise liveness view of the agent room.

Stdlib only: speaks the NATS text protocol directly over TCP.

Configuration (all via environment, no secrets in source)
---------------------------------------------------------
  SIBLINE_NATS_HOST          NATS server host        (required; no default)
  SIBLINE_NATS_PORT          NATS server port        (default 4222)
  SIBLINE_AGENT              this agent's identity    (default "agent")
  SIBLINE_PRESENCE_SUBJECT   presence subject         (default sibline.presence.<agent>)
  SIBLINE_CREDS_FILE         credentials file path    (default ~/.config/sibline-nats/cred)
  SIBLINE_PRESENCE_LOG       local log path           (default ~/.sibline-presence.log)
  SIBLINE_STARTED_AT_FILE    uptime anchor file       (default ~/.sibline-presence-started-at)

Credentials file is a ``KEY=value`` file containing at least:
  SIBLINE_NATS_PASS=<password for SIBLINE_AGENT>

The presence body is intentionally generic. Host-specific health probes
(gateway process checks, subscriber-daemon launchctl state, mailbox backlog,
etc.) are deployment details and are intentionally NOT included here; add them
in a local wrapper if desired. This reference version reports only uptime.
"""
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from pathlib import Path
from typing import Any

SERVER_HOST = os.environ.get("SIBLINE_NATS_HOST")
SERVER_PORT = int(os.environ.get("SIBLINE_NATS_PORT", "4222"))
AGENT = os.environ.get("SIBLINE_AGENT", "agent")
SUBJECT = os.environ.get("SIBLINE_PRESENCE_SUBJECT", f"sibline.presence.{AGENT}")
CREDS_FILE = Path(
    os.environ.get("SIBLINE_CREDS_FILE", str(Path.home() / ".config" / "sibline-nats" / "cred"))
).expanduser()
LOG_PATH = Path(
    os.environ.get("SIBLINE_PRESENCE_LOG", str(Path.home() / ".sibline-presence.log"))
).expanduser()
STARTED_AT_FILE = Path(
    os.environ.get("SIBLINE_STARTED_AT_FILE", str(Path.home() / ".sibline-presence-started-at"))
).expanduser()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(msg: str) -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"{now_iso()} {msg}\n")
    except Exception:
        pass


def load_password() -> str:
    if CREDS_FILE.exists():
        for line in CREDS_FILE.read_text(encoding="utf-8").splitlines():
            if line.startswith("SIBLINE_NATS_PASS="):
                return line.split("=", 1)[1].strip()
            if line.startswith("SIBLING_NATS_PASS="):  # back-compat
                return line.split("=", 1)[1].strip()
    raise RuntimeError(f"missing SIBLINE_NATS_PASS in {CREDS_FILE}")


def recv_some(sock: socket.socket, timeout: float = 1.0) -> str:
    sock.setblocking(False)
    end = time.time() + timeout
    chunks: list[bytes] = []
    while time.time() < end:
        try:
            b = sock.recv(65536)
            if not b:
                break
            chunks.append(b)
        except BlockingIOError:
            time.sleep(0.02)
    sock.setblocking(True)
    return b"".join(chunks).decode("utf-8", "replace")


def uptime_sec() -> int | None:
    try:
        STARTED_AT_FILE.parent.mkdir(parents=True, exist_ok=True)
        if STARTED_AT_FILE.exists():
            started = float(STARTED_AT_FILE.read_text(encoding="utf-8").strip())
        else:
            started = time.time()
            STARTED_AT_FILE.write_text(str(started), encoding="utf-8")
        return max(0, int(time.time() - started))
    except Exception:
        return None


def publish(envelope: dict[str, Any]) -> None:
    payload = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    with socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=5) as sock:
        _ = recv_some(sock, 1.0)
        connect = {
            "user": AGENT,
            "pass": load_password(),
            "verbose": True,
            "pedantic": False,
            "lang": "python-raw",
            "version": "sibline-presence/2",
            "name": f"{AGENT}-presence-pulse",
        }
        sock.sendall(("CONNECT " + json.dumps(connect, separators=(",", ":")) + "\r\nPING\r\n").encode())
        auth = recv_some(sock, 2.0)
        if "-ERR" in auth or "PONG" not in auth:
            raise RuntimeError(f"NATS auth/ping failed: {auth[:300]!r}")
        sock.sendall(f"PUB {SUBJECT} {len(payload)}\r\n".encode() + payload + b"\r\nPING\r\n")
        ack = recv_some(sock, 2.0)
        if "-ERR" in ack or "PONG" not in ack:
            raise RuntimeError(f"NATS publish failed: {ack[:300]!r}")


def main() -> int:
    if not SERVER_HOST:
        raise SystemExit("SIBLINE_NATS_HOST is not set")
    body = {"uptime_sec": uptime_sec()}
    envelope: dict[str, Any] = {
        "id": f"{AGENT}-pulse-{int(time.time())}-{uuid.uuid4().hex[:6]}",
        "from": AGENT,
        "to": "all",
        "ts": now_iso(),
        "kind": "heartbeat",
        "body": body,
    }
    publish(envelope)
    log(f"published id={envelope['id']} body={json.dumps(body, separators=(',', ':'))}")
    print(json.dumps(envelope, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"ERROR {type(e).__name__}: {e}")
        raise
