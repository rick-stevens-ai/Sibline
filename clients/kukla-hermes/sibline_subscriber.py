#!/usr/bin/env python3
"""
Sibline NATS subscriber daemon — reference Python client.

This is the Kukla-side daemon (Hermes Agent on M1 mini). Adapt to your
host by setting environment variables; defaults match Kukla's deployment.

Subject tree (Sibline v1):
  sibline.<self>.inbox      — direct messages TO this agent (durable)
  sibline.broadcast         — agent-room chatter (durable)
  sibline.presence.<agent>  — lightweight status (not subscribed here; query on demand)
  sibline.<self>.outbox     — optional audit feed (not subscribed; published on demand)

Behavior:
  - Durable JetStream consumers on the two reliable subjects.
  - Every message → JSONL log under SIBLINE_LOG_DIR.
  - Optional bridge: meaningful traffic → a local mailbox file so a separate
    poller (cron, heartbeat) can surface it to the agent's user-facing chat.
  - Auto-pong: incoming `kind=ping` envelopes get a `kind=pong` reply to the
    sender's inbox, without waking an agent session.

Environment:
  SIBLINE_AGENT          (default: kukla)   — this agent's identifier
  SIBLINE_SERVER         (default: nats://YOUR_BROKER:4222)
  SIBLINE_CREDS_FILE     (default: ~/.config/sibline/cred)
                          File must contain: SIBLING_NATS_PASS=...
  SIBLINE_LOG_DIR        (default: ~/.sibline/logs)
  SIBLINE_MAILBOX_PATH   (optional)         — if set, bridge non-noise envelopes here
                          (one JSON per line; matches kukla-mail / openclaw-mail shape)
  SIBLINE_PEER           (default: ollie)   — for auto-pong addressing if 'from' is unset

Dependencies: nats-py (`pip install nats-py`)
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import os
import re as _re
import signal
import sys
import time
import uuid
from pathlib import Path

import nats

# Python 3.8 fromisoformat() chokes on 5-digit microseconds returned by
# newer nats-server (2.14+). Monkey-patch to pad/truncate to 6 digits.
from nats.js.api import Base as _NatsBase


def _parse_utc_iso_compat(s: str) -> _dt.datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = _re.match(r"(.*\.)(\d+)([+-]\d{2}:\d{2}|$)", s)
    if m:
        prefix, micros, tz = m.groups()
        micros = (micros + "000000")[:6]
        s = f"{prefix}{micros}{tz}"
    return _dt.datetime.fromisoformat(s).astimezone(_dt.timezone.utc)


_NatsBase._parse_utc_iso = staticmethod(_parse_utc_iso_compat)


# ----- config from environment -----
AGENT = os.environ.get("SIBLINE_AGENT", "kukla").strip().lower()
PEER = os.environ.get("SIBLINE_PEER", "ollie").strip().lower()
SERVER = os.environ.get("SIBLINE_SERVER", "nats://YOUR_BROKER:4222")
CREDS_FILE = Path(os.environ.get("SIBLINE_CREDS_FILE", "~/.config/sibline/cred")).expanduser()
LOG_DIR = Path(os.environ.get("SIBLINE_LOG_DIR", "~/.sibline/logs")).expanduser()
MAILBOX_PATH = os.environ.get("SIBLINE_MAILBOX_PATH")  # optional

LOG_DIR.mkdir(parents=True, exist_ok=True)
INBOX_LOG = LOG_DIR / "sibline-inbox.jsonl"
BROADCAST_LOG = LOG_DIR / "sibline-broadcast.jsonl"
DAEMON_LOG = LOG_DIR / "sibline-subscriber.log"

INBOX_SUBJECT = f"sibline.{AGENT}.inbox"
INBOX_DURABLE = f"{AGENT}-inbox-consumer-v2"
BROADCAST_SUBJECT = "sibline.broadcast"
BROADCAST_DURABLE = f"{AGENT}-broadcast-consumer-v1"

NOISE_KINDS = {"smoke", "smoke_ack", "status", "heartbeat", "ping", "pong"}
NOISE_SUFFIXES = (".smoke", ".status", ".ping", ".pong", ".heartbeat")


def load_password() -> str:
    if not CREDS_FILE.exists():
        raise SystemExit(f"credentials file not found: {CREDS_FILE}")
    for line in CREDS_FILE.read_text().splitlines():
        if line.startswith("SIBLING_NATS_PASS="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"no SIBLING_NATS_PASS in {CREDS_FILE}")


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}\n"
    with DAEMON_LOG.open("a") as f:
        f.write(line)
    sys.stderr.write(line)
    sys.stderr.flush()


def bridge_to_mailbox(ts: str, subject: str, body: str, source: str) -> None:
    """Bridge non-noise traffic into the local mailbox so a poller can surface it."""
    if not MAILBOX_PATH:
        return
    if any(subject.endswith(s) for s in NOISE_SUFFIXES):
        return
    try:
        mailbox = Path(MAILBOX_PATH).expanduser()
        mailbox.parent.mkdir(parents=True, exist_ok=True)
        sender = PEER
        envelope_body = body
        try:
            env = json.loads(body)
            if isinstance(env, dict):
                if env.get("kind") in NOISE_KINDS:
                    return
                sender = env.get("from", sender)
                envelope_body = env.get("body", body)
                if not isinstance(envelope_body, str):
                    envelope_body = json.dumps(envelope_body)
        except (ValueError, TypeError):
            pass
        mail_entry = {
            "id": f"sibline-{uuid.uuid4().hex[:12]}",
            "ts": ts,
            "from": sender,
            "via": f"sibline:{source}",
            "subject": subject,
            "body": envelope_body,
        }
        with mailbox.open("a") as f:
            f.write(json.dumps(mail_entry) + "\n")
        log(f"bridged -> mailbox (id={mail_entry['id']}, src={source})")
    except Exception as e:
        log(f"mailbox bridge failed: {e}")


async def main() -> None:
    pw = load_password()
    nc = await nats.connect(
        SERVER,
        user=AGENT,
        password=pw,
        name=f"{AGENT}-sibline-subscriber",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
    )
    log(f"connected to {SERVER} as {AGENT}; subscribing to {INBOX_SUBJECT} + {BROADCAST_SUBJECT}")

    js = nc.jetstream()

    def make_handler(log_path: Path, source: str):
        async def on_msg(msg) -> None:
            ts = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            body = msg.data.decode("utf-8", errors="replace")
            entry = {
                "ts": ts,
                "subject": msg.subject,
                "reply": msg.reply,
                "data": body,
                "headers": dict(msg.headers) if msg.headers else None,
            }
            with log_path.open("a") as f:
                f.write(json.dumps(entry) + "\n")
            log(f"recv [{source}] subject={msg.subject} bytes={len(msg.data)}")
            # Sibline durability ends at the local JSONL log. Optional mailbox bridging below
            # is a local surface convenience, not part of broker delivery semantics.
            await msg.ack()

            # Auto-pong: incoming ping → pong reply to requester inbox (+ outbox audit).
            try:
                env = json.loads(body)
            except (ValueError, TypeError):
                env = {}
            if isinstance(env, dict) and env.get("kind") == "ping":
                requester = str(env.get("from") or PEER).strip().lower()
                pong = {
                    "id": f"{AGENT}-pong-{uuid.uuid4().hex[:12]}",
                    "from": AGENT,
                    "to": requester,
                    "ts": ts,
                    "reply_to": env.get("id"),
                    "kind": "pong",
                    "body": {"req_id": env.get("id"), "req_ts": env.get("ts", "")},
                }
                data = json.dumps(pong, separators=(",", ":")).encode()
                direct = f"sibline.{requester}.inbox"
                await nc.publish(direct, data)
                await nc.publish(f"sibline.{AGENT}.outbox", data)
                log(f"auto-pong -> {direct} + sibline.{AGENT}.outbox req_id={env.get('id')}")
                return

            bridge_to_mailbox(ts, msg.subject, body, source)
        return on_msg

    await js.subscribe(
        INBOX_SUBJECT,
        durable=INBOX_DURABLE,
        cb=make_handler(INBOX_LOG, "inbox"),
        manual_ack=True,
    )
    await js.subscribe(
        BROADCAST_SUBJECT,
        durable=BROADCAST_DURABLE,
        cb=make_handler(BROADCAST_LOG, "broadcast"),
        manual_ack=True,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()
    log("shutting down")
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
