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
import hashlib
import hmac
import json
import os
import re as _re
import shlex as _shlex
import signal
import sys
import time
import urllib.error
import urllib.request
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

# Liveness/probe traffic is kept NATS-only (never bridged to the local mailbox).
NOISE_KINDS = {"smoke", "smoke_ack", "status", "heartbeat", "ping", "pong", "rr_probe"}
# Recognized agents in the Sibline mesh. Config-driven so onboarding a new peer
# is a one-line env change (or a default bump) rather than a 3-file hardcode edit.
# Elders: ollie (CherryRd), kukla (m1). Trickster trio: ikto, tsisdu, yeil (sparks).
# Fleet agents: paradise (nuc13), lost (nuc7), prokko (home host).
_DEFAULT_ROSTER = "ollie,kukla,ikto,tsisdu,yeil,paradise,lost,prokko"
AGENT_NAMES = {
    a.strip() for a in os.environ.get("SIBLINE_ROSTER", _DEFAULT_ROSTER).split(",") if a.strip()
}
# Envelope kinds that trigger an auto-pong liveness reply.
PING_KINDS = {"ping", "rr_probe"}
NOISE_SUFFIXES = (".smoke", ".status", ".ping", ".pong", ".heartbeat")

# ----- task worker (delegated-work RPC) -----
# When another agent publishes a kind=task_request envelope, this daemon can run
# the task via a local worker command and publish the lifecycle back
# (task_accepted -> task_progress... -> task_result/task_error), per
# spec/sibline-task-rpc-v1.md. Opt-in: set SIBLINE_WORKER_CMD to enable.
#   SIBLINE_WORKER_CMD   e.g. "hermes --yolo -z {prompt}"  ({prompt} is shell-safe-substituted)
#   SIBLINE_WORKER_TIMEOUT  seconds (default 300)
#   SIBLINE_WORKER_SHELL    if "1", run WORKER_CMD via a login shell (for PATH/env). default 1.
TASK_KINDS = {"task_request"}
WORKER_CMD = os.environ.get("SIBLINE_WORKER_CMD", "").strip()
WORKER_TIMEOUT = float(os.environ.get("SIBLINE_WORKER_TIMEOUT", "300"))
WORKER_SHELL = os.environ.get("SIBLINE_WORKER_SHELL", "1").strip() == "1"
# Dedupe: remember req_ids we've already started so a redelivery doesn't double-run.
_SEEN_TASKS: set = set()

# ----- push-to-wake (optional) -----
# Hosts whose agent must be WOKEN on an inbound note (rather than running the task
# in-daemon) set SIBLINE_WAKE_URL to a local Hermes webhook that spawns an agent
# turn. Best-effort, 429-aware. If SIBLINE_WORKER_CMD is also set, task_request is
# handled by the worker and wake is used only for other meaningful notes.
#   SIBLINE_WAKE_URL      e.g. http://127.0.0.1:8644/webhooks/ikto-sibling
#   SIBLINE_WEBHOOK_SECRET  HMAC secret for X-Hub-Signature-256
#   SIBLINE_WAKE_ON_BROADCAST  "1" to also wake on broadcast notes (default 0)
WAKE_URL = os.environ.get("SIBLINE_WAKE_URL", "").strip()
WEBHOOK_SECRET = os.environ.get("SIBLINE_WEBHOOK_SECRET", "").strip()
WAKE_ON_BROADCAST = os.environ.get("SIBLINE_WAKE_ON_BROADCAST", "0").strip() == "1"


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


async def fire_push_to_wake(env: dict, source: str) -> None:
    """POST a note to the local Hermes webhook so the agent wakes and acts.

    Best-effort; never raises. 429-aware with exponential backoff so a restart
    replay burst drains gracefully. Enabled only when SIBLINE_WAKE_URL is set.
    """
    if not WAKE_URL:
        return
    if not WEBHOOK_SECRET:
        log("push-to-wake SKIPPED: SIBLINE_WAKE_URL set but no SIBLINE_WEBHOOK_SECRET")
        return
    mid = env.get("id") if isinstance(env, dict) else "?"
    sender = env.get("from", PEER) if isinstance(env, dict) else PEER
    body = env.get("body") if isinstance(env, dict) else None
    text = body if isinstance(body, str) else json.dumps(body if body is not None else env)
    payload = json.dumps({
        "text": text,
        "_meta": {
            "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "from": sender, "source": source,
            "msg_id": mid, "kind": env.get("kind") if isinstance(env, dict) else None,
        },
    }).encode()
    sig = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()

    def _post():
        req = urllib.request.Request(
            WAKE_URL, data=payload,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig})
        return urllib.request.urlopen(req, timeout=10)

    delay = 0.5
    for attempt in range(1, 6):
        try:
            r = await asyncio.to_thread(_post)
            log(f"push-to-wake fired (id={mid}) -> {WAKE_URL} HTTP {r.status}"
                + (f" after {attempt} tries" if attempt > 1 else ""))
            return
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 5:
                ra = e.headers.get("Retry-After") if e.headers else None
                wait = float(ra) if (ra and ra.isdigit()) else delay
                log(f"push-to-wake 429 (id={mid}) attempt {attempt}/5, backoff {wait}s")
                await asyncio.sleep(wait)
                delay = min(delay * 2, 8.0)
                continue
            log(f"push-to-wake HTTP {e.code}: {e.reason} (id={mid} url={WAKE_URL})")
            return
        except Exception as e:
            log(f"push-to-wake failed (id={mid}): {e}")
            return


async def run_task_worker(nc, env: dict, source: str) -> None:
    """Run a delegated task via the local worker command and publish the
    task-RPC lifecycle back to the requester, per spec/sibline-task-rpc-v1.md.

    Lifecycle: task_accepted -> task_progress (heartbeats) -> task_result|task_error.
    """
    req_id = str(env.get("req_id") or env.get("id") or "")
    requester = str(env.get("from") or PEER).strip().lower()
    body = env.get("body")
    if isinstance(body, dict):
        task_text = str(body.get("task") or body.get("prompt") or "")
        deadline_s = float(body.get("deadline_s") or WORKER_TIMEOUT)
    else:
        task_text = str(body or "")
        deadline_s = WORKER_TIMEOUT
    if not req_id or not task_text:
        log(f"task_request ignored (missing req_id/task) from={requester}")
        return
    if requester not in AGENT_NAMES:
        log(f"task_request from unknown agent '{requester}' rejected")
        return
    if req_id in _SEEN_TASKS:
        log(f"task_request {req_id} already seen; skipping redelivery")
        return
    _SEEN_TASKS.add(req_id)

    reply_subj = f"sibline.{requester}.inbox"

    def _now() -> str:
        return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    async def _emit(kind: str, body_obj) -> None:
        envlp = {
            "id": f"{AGENT}-{kind}-{uuid.uuid4().hex[:10]}",
            "from": AGENT, "to": requester, "ts": _now(),
            "reply_to": env.get("id"), "req_id": req_id,
            "kind": kind, "body": body_obj,
        }
        await nc.publish(reply_subj, json.dumps(envlp, separators=(",", ":")).encode())
        await nc.flush()

    if not WORKER_CMD:
        await _emit("task_error", {"error": "no_worker", "detail": f"{AGENT} has no SIBLINE_WORKER_CMD configured"})
        log(f"task {req_id}: no worker configured -> task_error")
        return

    await _emit("task_accepted", {"worker": AGENT, "deadline_s": deadline_s})
    log(f"task {req_id} ACCEPTED from={requester}: {task_text[:80]!r}")

    # Build the prompt: the task + an instruction NOT to try to reply over sibline
    # (the daemon handles the reply); just produce the answer as final text.
    prompt = (
        task_text
        + "\n\n---\nYou are completing a delegated task. Do the work using your tools as needed, "
          "then end your reply with the final answer/result as plain text. Do not attempt to "
          "publish anything to sibline yourself; the result is captured automatically."
    )
    cmd = WORKER_CMD.replace("{prompt}", _shlex.quote(prompt)) if "{prompt}" in WORKER_CMD \
        else f"{WORKER_CMD} {_shlex.quote(prompt)}"

    if WORKER_SHELL:
        argv = ["bash", "-lc", cmd]
    else:
        argv = _shlex.split(cmd)

    log(f"task {req_id}: launching worker")
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )

    # Heartbeat while the worker runs, enforce deadline.
    hb = 0
    start = time.time()
    try:
        while True:
            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                break
            except asyncio.TimeoutError:
                hb += 1
                elapsed = time.time() - start
                if elapsed > deadline_s:
                    proc.kill()
                    await proc.wait()
                    await _emit("task_error", {"error": "timeout", "detail": f"exceeded {deadline_s}s"})
                    log(f"task {req_id}: TIMEOUT after {elapsed:.0f}s")
                    return
                await _emit("task_progress", {"heartbeat": hb, "elapsed_s": round(elapsed, 1)})
                log(f"task {req_id}: heartbeat {hb} ({elapsed:.0f}s)")
    except Exception as e:
        await _emit("task_error", {"error": "worker_exception", "detail": repr(e)[:300]})
        log(f"task {req_id}: worker exception {e!r}")
        return

    out = (stdout or b"").decode("utf-8", errors="replace").strip()
    rc = proc.returncode
    if rc != 0:
        await _emit("task_error", {"error": "worker_nonzero", "rc": rc, "detail": out[-1000:]})
        log(f"task {req_id}: worker rc={rc} -> task_error")
        return
    await _emit("task_result", {"result": out[-8000:], "rc": rc, "elapsed_s": round(time.time() - start, 1)})
    log(f"task {req_id}: RESULT published ({len(out)} chars, {time.time()-start:.0f}s)")


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

            # Auto-pong: incoming ping/rr_probe from a known mesh agent → pong
            # reply to requester inbox (+ outbox audit). Symmetric liveness across
            # the 5-agent mesh (elders + trickster trio).
            try:
                env = json.loads(body)
            except (ValueError, TypeError):
                env = {}
            if isinstance(env, dict) and env.get("kind") in PING_KINDS:
                requester = str(env.get("from") or PEER).strip().lower()
                if requester in AGENT_NAMES:
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

            # Delegated-work RPC: a task_request from a known agent.
            #  - if a worker command is configured, the daemon runs it and
            #    publishes the lifecycle back (self-contained, no agent wake).
            #  - else, fall through to push-to-wake so the host's agent handles it
            #    (it must follow the sibline-help skill to reply).
            if isinstance(env, dict) and env.get("kind") in TASK_KINDS:
                if WORKER_CMD:
                    asyncio.create_task(run_task_worker(nc, env, source))
                    return
                # no worker -> wake the agent to handle the task
                bridge_to_mailbox(ts, msg.subject, body, source)
                if WAKE_URL:
                    await fire_push_to_wake(env, source)
                return

            bridge_to_mailbox(ts, msg.subject, body, source)

            # Push-to-wake for other meaningful notes (not noise), if enabled.
            if WAKE_URL and not any(msg.subject.endswith(s) for s in NOISE_SUFFIXES):
                if isinstance(env, dict) and env.get("kind") in NOISE_KINDS:
                    return
                if source == "broadcast" and not WAKE_ON_BROADCAST:
                    return
                await fire_push_to_wake(env, source)
        return on_msg

    # Push consumers need an explicit deliver_subject on newer nats-py (the
    # auto-generated default was removed); older clients ignored it. Pass it via
    # ConsumerConfig so this works across nats-py versions. durable+deliver_subject
    # together bind/create a push consumer.
    from nats.js.api import ConsumerConfig as _CC
    await js.subscribe(
        INBOX_SUBJECT,
        durable=INBOX_DURABLE,
        cb=make_handler(INBOX_LOG, "inbox"),
        manual_ack=True,
        config=_CC(durable_name=INBOX_DURABLE, deliver_subject=nc.new_inbox()),
    )
    await js.subscribe(
        BROADCAST_SUBJECT,
        durable=BROADCAST_DURABLE,
        cb=make_handler(BROADCAST_LOG, "broadcast"),
        manual_ack=True,
        config=_CC(durable_name=BROADCAST_DURABLE, deliver_subject=nc.new_inbox()),
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
