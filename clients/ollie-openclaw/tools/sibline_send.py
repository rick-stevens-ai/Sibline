#!/usr/bin/env python3
"""Send a Sibline v1 envelope over NATS using only the Python standard library.

This is the stdlib-only *send* CLI counterpart to the subscriber daemon in
``clients/ollie-openclaw/sibline_subscriber.py``. It publishes a single
Sibline v1 envelope to a peer inbox (or the broadcast subject) and optionally
mirrors a copy to the sender's own outbox for auditing.

No third-party dependencies: it speaks the NATS text protocol directly over a
TCP socket, which keeps it usable from constrained agent/cron contexts.

Routes
------
  direct:     sibline.<to>.inbox
  broadcast:  sibline.broadcast
  audit copy: sibline.<self>.outbox  (on by default for direct sends)

Envelope (Sibline v1): {id, from, to, ts, reply_to?, kind, body}

Configuration (all via environment, no secrets in source)
---------------------------------------------------------
  SIBLINE_NATS_HOST     NATS server host         (required; no default)
  SIBLINE_NATS_PORT     NATS server port         (default 4222)
  SIBLINE_AGENT         this agent's identity     (default "agent")
  SIBLINE_CREDS_FILE    path to credentials file  (default ~/.config/sibline-nats/cred)

The credentials file is a simple ``KEY=value`` file containing at least:
  SIBLINE_NATS_PASS=<password for SIBLINE_AGENT>

Keep the credentials file out of version control (chmod 600). This tool never
embeds a password, host IP, or token in source.

Instrumentation: ``recv_some`` short-circuits as soon as a complete NATS
control-line frame ("PONG\\r\\n" / "+OK\\r\\n" / "-ERR ...") is seen rather than
busy-waiting until the deadline, so timeouts can be distinguished from real
NATS issues. Set SIBLINE_DEBUG=1 to append diagnostics to SIBLINE_DEBUG_LOG.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import uuid
from pathlib import Path
from typing import Any

SERVER_HOST = os.environ.get("SIBLINE_NATS_HOST")
SERVER_PORT = int(os.environ.get("SIBLINE_NATS_PORT", "4222"))
AGENT = os.environ.get("SIBLINE_AGENT", "agent")
CREDS_FILE = Path(
    os.environ.get("SIBLINE_CREDS_FILE", str(Path.home() / ".config" / "sibline-nats" / "cred"))
).expanduser()
DEBUG = os.environ.get("SIBLINE_DEBUG", "").strip() in {"1", "true", "True", "yes"}
DEBUG_LOG = Path(
    os.environ.get("SIBLINE_DEBUG_LOG", str(Path.home() / ".sibline-send-debug.log"))
).expanduser()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def debug(msg: str) -> None:
    if not DEBUG:
        return
    try:
        DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        with DEBUG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"{now_iso()}\tDEBUG sibline-send pid={os.getpid()} {msg}\n")
    except Exception:
        pass


def load_password() -> str:
    if CREDS_FILE.exists():
        for line in CREDS_FILE.read_text(encoding="utf-8").splitlines():
            if line.startswith("SIBLINE_NATS_PASS="):
                return line.split("=", 1)[1].strip()
            # Back-compat with an older key spelling.
            if line.startswith("SIBLING_NATS_PASS="):
                return line.split("=", 1)[1].strip()
    raise SystemExit(f"missing SIBLINE_NATS_PASS in {CREDS_FILE}")


# NATS terminators meaning "the response we asked for is complete".
_TERMINATORS = (b"PONG\r\n", b"+OK\r\n", b"-ERR ")


def recv_some(sock: socket.socket, timeout: float = 1.0, *, expect_terminator: bool = True) -> str:
    """Read bytes for up to ``timeout`` seconds.

    When ``expect_terminator`` is True (default), return as soon as a complete
    control-line frame appears. Otherwise drain until the deadline (used for the
    initial INFO read before the first CONNECT).
    """
    sock.setblocking(False)
    end = time.time() + timeout
    chunks: list[bytes] = []
    while time.time() < end:
        try:
            b = sock.recv(65536)
            if not b:
                break
            chunks.append(b)
            if expect_terminator and any(t in b"".join(chunks) for t in _TERMINATORS):
                break
        except BlockingIOError:
            time.sleep(0.02)
    sock.setblocking(True)
    return b"".join(chunks).decode("utf-8", "replace")


def nats_publish(subject: str, payload: bytes, *, password: str, sock: socket.socket | None = None) -> str:
    own_sock = sock is None
    if sock is None:
        t0 = time.time()
        sock = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=5)
        debug(f"connect ok subject={subject} {(time.time()-t0)*1000:.0f}ms")
        _ = recv_some(sock, 1.0, expect_terminator=False)  # INFO
        connect = {
            "user": AGENT,
            "pass": password,
            "verbose": True,
            "pedantic": False,
            "lang": "python-raw",
            "version": "sibline-send/1",
            "name": f"{AGENT}-sibline-send",
        }
        sock.sendall(("CONNECT " + json.dumps(connect, separators=(",", ":")) + "\r\nPING\r\n").encode())
        auth = recv_some(sock, 2.0)
        if "-ERR" in auth or "PONG" not in auth:
            raise RuntimeError(f"NATS auth/ping failed: {auth[:300]!r}")
    t1 = time.time()
    sock.sendall(f"PUB {subject} {len(payload)}\r\n".encode() + payload + b"\r\nPING\r\n")
    ack = recv_some(sock, 2.0)
    debug(f"publish {subject} ack={(time.time()-t1)*1000:.0f}ms ok={'PONG' in ack}")
    if "-ERR" in ack or "PONG" not in ack:
        raise RuntimeError(f"NATS publish failed for {subject}: {ack[:300]!r}")
    if own_sock:
        sock.close()
    return ack


def parse_body(s: str) -> Any:
    if s == "-":
        return sys.stdin.read()
    try:
        return json.loads(s)
    except Exception:
        return s


def main() -> int:
    ap = argparse.ArgumentParser(description="Send a Sibline v1 envelope")
    ap.add_argument("--to", required=True, help="peer agent id, or 'all' for broadcast")
    ap.add_argument("--kind", default="direct")
    ap.add_argument("--body", required=True, help="String body, JSON body, or '-' for stdin")
    ap.add_argument("--reply-to")
    ap.add_argument("--from", dest="frm", default=AGENT, help="sender id (default $SIBLINE_AGENT)")
    ap.add_argument("--id", default=None)
    ap.add_argument("--no-audit", action="store_true")
    ap.add_argument("--json", action="store_true", help="print JSON result")
    args = ap.parse_args()

    if not SERVER_HOST:
        raise SystemExit("SIBLINE_NATS_HOST is not set")

    frm = args.frm
    msg_id = args.id or f"{frm}-{uuid.uuid4().hex[:12]}"
    t_start = time.time()
    subject = "sibline.broadcast" if args.to == "all" else f"sibline.{args.to}.inbox"
    envelope: dict[str, Any] = {
        "id": msg_id,
        "from": frm,
        "to": args.to,
        "ts": now_iso(),
        "kind": args.kind,
        "body": parse_body(args.body),
    }
    if args.reply_to:
        envelope["reply_to"] = args.reply_to
    payload = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    password = load_password()
    debug(f"start to={args.to} kind={args.kind} id={msg_id} subject={subject} bytes={len(payload)}")

    sent = []
    audit_subject = f"sibline.{frm}.outbox"
    try:
        with socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=5) as sock:
            _ = recv_some(sock, 1.0, expect_terminator=False)
            connect = {"user": AGENT, "pass": password, "verbose": True, "pedantic": False, "name": f"{frm}-sibline-send"}
            sock.sendall(("CONNECT " + json.dumps(connect, separators=(",", ":")) + "\r\nPING\r\n").encode())
            auth = recv_some(sock, 2.0)
            if "-ERR" in auth or "PONG" not in auth:
                raise RuntimeError(f"NATS auth/ping failed: {auth[:300]!r}")
            nats_publish(subject, payload, password=password, sock=sock)
            sent.append(subject)
            if not args.no_audit and subject != audit_subject:
                nats_publish(audit_subject, payload, password=password, sock=sock)
                sent.append(audit_subject)
    except Exception as e:
        debug(f"FAIL after {(time.time()-t_start)*1000:.0f}ms: {type(e).__name__}: {e}")
        raise

    debug(f"done total={(time.time()-t_start)*1000:.0f}ms subjects={','.join(sent)}")
    result = {"ok": True, "id": msg_id, "sent": sent, "envelope": envelope}
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"sent id={msg_id} subjects={','.join(sent)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
