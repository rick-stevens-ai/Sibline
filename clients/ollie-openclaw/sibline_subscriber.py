#!/usr/bin/env python3
"""Ollie/OpenClaw Sibline subscriber daemon — reference Python client.

This is the OpenClaw-side daemon currently used by Ollie. It runs durable
JetStream consumers for direct inbox + broadcast, logs every delivered message,
bridges non-noise traffic into an OpenClaw-readable JSONL inbox, and answers
`kind=ping` without waking a foreground agent session.

Subject tree (Sibline v1):
  sibline.<self>.inbox      — direct messages TO this agent (durable)
  sibline.broadcast         — agent-room chatter (durable)
  sibline.presence.<peer>   — lightweight peer status (core NATS, optional)
  sibline.<self>.outbox     — optional audit feed for pongs/status

Dependencies: nats-py (`pip install nats-py`)
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import os
import re
import signal
import sys
import time
import uuid
from pathlib import Path

# Some OpenClaw installs keep nats-py in a local venv beside the workspace.
# If SIBLINE_NATS_SITE_DIR is set, prepend it; otherwise try the historical
# `.nats-venv` convention and fall back to system site-packages.
site_dir = os.environ.get("SIBLINE_NATS_SITE_DIR")
if site_dir:
    sys.path.insert(0, site_dir)
else:
    for p in (Path(__file__).resolve().parent.parent.parent.parent / ".nats-venv" / "lib").glob("python*/site-packages"):
        sys.path.insert(0, str(p))

import nats  # type: ignore
from nats.js.api import AckPolicy, Base as _NatsBase, ConsumerConfig, DeliverPolicy  # type: ignore


# ----- config from environment -----
AGENT = os.environ.get("SIBLINE_AGENT", "ollie").strip().lower()
PEER = os.environ.get("SIBLINE_PEER", "kukla").strip().lower()
SERVER = os.environ.get("SIBLINE_SERVER", "nats://YOUR_BROKER:4222")
CREDS_FILE = Path(os.environ.get("SIBLINE_CREDS_FILE", "~/.config/sibline/cred")).expanduser()
LOG_DIR = Path(os.environ.get("SIBLINE_LOG_DIR", "~/.openclaw/logs")).expanduser()
WORKSPACE = Path(os.environ.get("OPENCLAW_WORKSPACE", "~/.openclaw/workspace")).expanduser()
MAILBOX_PATH = Path(os.environ.get(
    "SIBLINE_MAILBOX_PATH",
    str(WORKSPACE / "memory" / "kukla-background-inbox.jsonl"),
)).expanduser()
PRESENCE_SUBJECT = os.environ.get("SIBLINE_PEER_PRESENCE", f"sibline.presence.{PEER}")

INBOX_SUBJECT = f"sibline.{AGENT}.inbox"
OUTBOX_SUBJECT = f"sibline.{AGENT}.outbox"
BROADCAST_SUBJECT = "sibline.broadcast"
INBOX_STREAM = os.environ.get("SIBLINE_INBOX_STREAM", f"sibline-{AGENT}")
BROADCAST_STREAM = os.environ.get("SIBLINE_BROADCAST_STREAM", "sibline-broadcast")
INBOX_DURABLE = os.environ.get("SIBLINE_INBOX_DURABLE", f"{AGENT}-inbox-durable")
BROADCAST_DURABLE = os.environ.get("SIBLINE_BROADCAST_DURABLE", f"{AGENT}-bcast-durable")

LOG_DIR.mkdir(parents=True, exist_ok=True)
DAEMON_LOG = LOG_DIR / "sibline-subscriber.log"
NATS_ONLY_KINDS = {"smoke", "smoke_ack", "status", "heartbeat", "ping", "pong"}


# ----- nats-py timestamp compatibility -----
_FRAC_RE = re.compile(r"(\.\d+)([Z+\-]|$)")


def _normalize_frac(iso_str: str) -> str:
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"

    def _pad(m: re.Match[str]) -> str:
        frac_digits = m.group(1)[1:]
        return "." + (frac_digits + "000000")[:6] + m.group(2)

    return _FRAC_RE.sub(_pad, iso_str)


def _parse_utc_iso_compat(iso_str: str):
    return _dt.datetime.fromisoformat(_normalize_frac(iso_str)).astimezone(_dt.timezone.utc)


_NatsBase._parse_utc_iso = staticmethod(_parse_utc_iso_compat)


# ----- helpers -----
def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(msg: str) -> None:
    line = f"{now_iso()} {msg}\n"
    print(line, end="", flush=True)
    with DAEMON_LOG.open("a", encoding="utf-8") as f:
        f.write(line)


def load_password() -> str:
    if CREDS_FILE.exists():
        for line in CREDS_FILE.read_text(encoding="utf-8").splitlines():
            if line.startswith("SIBLING_NATS_PASS="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError(f"missing SIBLING_NATS_PASS in {CREDS_FILE}")


def append_mailbox(record: dict) -> None:
    MAILBOX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MAILBOX_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")


async def handle_js(msg, nc) -> None:
    subject = msg.subject
    raw = msg.data
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw.decode("utf-8", "replace")}

    ts = now_iso()
    kind = payload.get("kind", "")
    msg_id = payload.get("id", f"sibline-{uuid.uuid4().hex[:12]}")
    log(f"js-recv subject={subject!r} kind={kind!r} id={msg_id!r} size={len(raw)}")

    # Sibline v1 requires ACK after local log write. If mailbox bridging fails
    # below, we leave the message unacked so JetStream can redeliver.
    if kind == "ping":
        requester = str(payload.get("from") or PEER).strip().lower()
        direct_subject = f"sibline.{requester}.inbox"
        pong_obj = {
            "id": f"{AGENT}-pong-{uuid.uuid4().hex[:12]}",
            "from": AGENT,
            "to": requester,
            "ts": ts,
            "reply_to": msg_id,
            "kind": "pong",
            "body": {"req_id": msg_id, "req_ts": payload.get("ts", "")},
        }
        pong = json.dumps(pong_obj, separators=(",", ":")).encode()
        await nc.publish(direct_subject, pong)
        await nc.publish(OUTBOX_SUBJECT, pong)
        await msg.ack()
        log(f"ponged direct={direct_subject!r} audit={OUTBOX_SUBJECT!r} req_id={msg_id!r}")
        return

    if kind in NATS_ONLY_KINDS:
        await msg.ack()
        log(f"nats-only kind={kind!r} subject={subject!r}, skipping mailbox")
        return

    body = payload.get("body", payload.get("text", f"[nats:{subject}]"))
    record = {
        "ts": ts,
        "path": PEER,
        "source": f"nats:{subject}",
        "from": payload.get("from", PEER),
        "via": "sibline-js",
        "id": msg_id,
        "subject": subject,
        "kind": kind or "message",
        "text": body if isinstance(body, str) else json.dumps(body),
        "payload": payload,
    }
    append_mailbox(record)
    await msg.ack()
    log(f"mailbox appended id={msg_id!r} subject={subject!r} kind={kind!r}")


async def handle_presence(msg) -> None:
    try:
        payload = json.loads(msg.data)
    except Exception:
        payload = {}
    log(f"presence subject={msg.subject!r} agent={payload.get('agent','?')} status={payload.get('status','?')}")


async def run() -> None:
    log(f"starting sibline subscriber: agent={AGENT} server={SERVER}")
    stop_event = asyncio.Event()

    def _signal_handler(sig, _frame):
        log(f"signal {sig} received, stopping")
        stop_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    reconnect_attempts = 0
    while not stop_event.is_set():
        nc = None
        try:
            nc = await nats.connect(
                SERVER,
                user=AGENT,
                password=load_password(),
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                ping_interval=20,
                max_outstanding_pings=3,
                name=f"{AGENT}-sibline-subscriber",
            )
            reconnect_attempts = 0
            log("connected")
            js = nc.jetstream()

            subscriptions = []
            for stream, durable, filter_subj in [
                (INBOX_STREAM, INBOX_DURABLE, INBOX_SUBJECT),
                (BROADCAST_STREAM, BROADCAST_DURABLE, BROADCAST_SUBJECT),
            ]:
                cfg = ConsumerConfig(
                    durable_name=durable,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    filter_subject=filter_subj,
                )
                sub = await js.subscribe(
                    filter_subj,
                    durable=durable,
                    stream=stream,
                    config=cfg,
                    cb=lambda m, _nc=nc: asyncio.ensure_future(handle_js(m, _nc)),
                )
                subscriptions.append(sub)
                log(f"js-subscribed stream={stream!r} durable={durable!r} filter={filter_subj!r}")

            presence_sub = await nc.subscribe(PRESENCE_SUBJECT, cb=handle_presence)
            log(f"presence subscribed {PRESENCE_SUBJECT!r}")

            while not stop_event.is_set() and nc.is_connected:
                await asyncio.sleep(1)

            for sub in subscriptions:
                try:
                    await sub.unsubscribe()
                except Exception:
                    pass
            try:
                await presence_sub.unsubscribe()
            except Exception:
                pass
            await nc.drain()
            log("drained")

        except Exception as e:
            reconnect_attempts += 1
            wait = min(30, 2 ** min(reconnect_attempts, 5))
            log(f"connection error (attempt {reconnect_attempts}): {e}; retrying in {wait}s")
            await asyncio.sleep(wait)
        finally:
            if nc and not nc.is_closed:
                await nc.close()

    log("stopped")


if __name__ == "__main__":
    asyncio.run(run())
