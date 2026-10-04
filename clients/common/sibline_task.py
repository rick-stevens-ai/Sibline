#!/usr/bin/env python3
"""
Sibline Task-RPC — requester library + CLI.

Ask another agent to do real work over sibline and reliably get the result back,
following spec/sibline-task-rpc-v1.md:

  task_request ──▶ [task_accepted] ──▶ [task_progress...] ──▶ task_result | task_error

Reliability:
  - correlate strictly by req_id (shared inbox)
  - ACCEPT_TIMEOUT: no task_accepted  → retry once, then give up as 'no_accept'
  - HEARTBEAT_TIMEOUT: accepted but silent past deadline → 'timeout'
  - every call returns a typed terminal: result | error('no_accept'|'timeout'|...)

Library:
  result = await ask_agent(js, requester, worker, task, deadline_s=180, ...)
  # result = {"ok": bool, "result"|"error": ..., "detail": str, "events": [...]}

CLI:
  SIBLINE_SERVER=nats://HOST:4222 SIBLINE_USER=kukla SIBLING_NATS_PASS=... \
  python3 clients/common/sibline_task.py --worker ollie \
      --task "Report the current time and your hostname." --deadline 120
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys
import time
import uuid

try:
    import nats
except ImportError:
    sys.exit("nats-py required: pip install nats-py")

# --- py3.8 compat for nats-server 2.14+ N-digit-microsecond timestamps (no-op on 3.11+)
if sys.version_info < (3, 11):
    import datetime as _dt
    import re as _re
    from nats.js import api as _jsapi

    def _parse_utc_iso_compat(s):
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        m = _re.search(r"\.(\d+)", s)
        if m:
            fixed = (m.group(1) + "000000")[:6]
            s = s[: m.start()] + "." + fixed + s[m.start() + 1 + len(m.group(1)):]
        return _dt.datetime.fromisoformat(s).astimezone(_dt.timezone.utc)

    _jsapi.Base._parse_utc_iso = staticmethod(_parse_utc_iso_compat)

from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy  # noqa: E402

ACCEPT_TIMEOUT = float(os.environ.get("SIBLINE_ACCEPT_TIMEOUT", "20"))
HEARTBEAT_TIMEOUT = float(os.environ.get("SIBLINE_HEARTBEAT_TIMEOUT", "45"))

TERMINAL = {"task_result", "task_error"}
LIFECYCLE = {"task_accepted", "task_progress", "task_result", "task_error"}


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _req_envelope(requester, worker, req_id, task, deadline_s, want, context):
    return {
        "id": req_id,
        "from": requester,
        "to": worker,
        "ts": _now_iso(),
        "reply_to": req_id,
        "kind": "task_request",
        "body": {
            "req_id": req_id,
            "task": task,
            "deadline_s": deadline_s,
            "want": want,
            "context": context or {},
        },
    }


async def ask_agent(js, requester, worker, task, deadline_s=180,
                    want="text", context=None, on_event=None, retries=1):
    """Delegate a task to `worker` and await a terminal reply, correlated by req_id.

    Returns: {"ok": bool, "result"|"error": ..., "detail": str,
              "req_id": str, "worker": str, "events": [kinds], "elapsed_s": float}
    `on_event(env)` is an optional callback invoked for each lifecycle event.
    """
    req_id = f"{requester}-task-{uuid.uuid4().hex[:8]}"
    inbox = f"sibline.{requester}.inbox"
    worker_inbox = f"sibline.{worker}.inbox"

    # Subscribe NEW-only BEFORE publishing so we catch the worker's replies and
    # are not drowned by inbox backlog. (Durable is unique per req_id.)
    durable = f"taskrpc-{req_id}"
    sub = await js.pull_subscribe(
        inbox, durable=durable,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW,
                              ack_policy=AckPolicy.EXPLICIT),
    )

    events_seen = []
    t_start = time.time()

    async def _drain_until_terminal():
        accepted = False
        last_alive = time.time()
        while True:
            # Budget: before accept use ACCEPT_TIMEOUT; after, use heartbeat+deadline.
            if not accepted and (time.time() - t_start) > ACCEPT_TIMEOUT:
                return {"ok": False, "error": "no_accept",
                        "detail": f"no task_accepted within {ACCEPT_TIMEOUT}s"}
            if accepted:
                if (time.time() - last_alive) > HEARTBEAT_TIMEOUT and \
                   (time.time() - t_start) > deadline_s:
                    return {"ok": False, "error": "timeout",
                            "detail": f"no terminal/heartbeat; past deadline {deadline_s}s"}
            try:
                msgs = await sub.fetch(1, timeout=2.0)
            except Exception:
                continue
            for m in msgs:
                try:
                    env = json.loads(m.data.decode())
                except Exception:
                    env = {}
                await m.ack()
                if env.get("reply_to") != req_id and (env.get("body") or {}).get("req_id") != req_id:
                    continue  # not ours
                kind = env.get("kind")
                if kind not in LIFECYCLE:
                    continue
                events_seen.append(kind)
                last_alive = time.time()
                if on_event:
                    try:
                        on_event(env)
                    except Exception:
                        pass
                if kind == "task_accepted":
                    accepted = True
                elif kind == "task_result":
                    b = env.get("body") or {}
                    return {"ok": True, "result": b.get("result"),
                            "detail": "ok", "elapsed_s": b.get("elapsed_s")}
                elif kind == "task_error":
                    b = env.get("body") or {}
                    return {"ok": False, "error": b.get("error", "error"),
                            "detail": b.get("detail", "")}

    outcome = {"ok": False, "error": "no_accept", "detail": "no attempt made"}
    for attempt in range(retries + 1):
        env = _req_envelope(requester, worker, req_id, task, deadline_s, want, context)
        await js.publish(worker_inbox, json.dumps(env).encode())
        outcome = await _drain_until_terminal()
        if outcome.get("ok") or outcome.get("error") != "no_accept":
            break  # only retry the no-accept case
        # retry: same req_id so a worker that WAS slow to accept dedupes
    try:
        await js.delete_consumer(f"sibline-{requester}", durable)
    except Exception:
        pass

    outcome.update({"req_id": req_id, "worker": worker,
                    "events": events_seen,
                    "elapsed_s": outcome.get("elapsed_s") or round(time.time() - t_start, 1)})
    return outcome


async def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", required=True, help="agent to ask")
    ap.add_argument("--task", required=True, help="task description")
    ap.add_argument("--deadline", type=int, default=180)
    ap.add_argument("--want", default="text", choices=["text", "json", "number", "path"])
    ap.add_argument("--context", default="", help="JSON string of structured context")
    args = ap.parse_args()

    server = os.environ.get("SIBLINE_SERVER", "nats://100.86.220.115:4222")
    user = os.environ.get("SIBLINE_USER", "kukla")
    pw = os.environ.get("SIBLINE_PASS") or os.environ.get("SIBLING_NATS_PASS", "")
    requester = os.environ.get("SIBLINE_AGENT", user)
    if not pw:
        sys.exit("set SIBLINE_PASS or SIBLING_NATS_PASS")
    context = json.loads(args.context) if args.context else None

    nc = await nats.connect(server, user=user, password=pw, connect_timeout=8)
    js = nc.jetstream()

    def show(env):
        k = env.get("kind")
        b = env.get("body") or {}
        if k == "task_accepted":
            print(f"  · accepted by {b.get('worker', env.get('from'))} (eta {b.get('eta_s','?')}s)")
        elif k == "task_progress":
            print(f"  · progress {b.get('pct','?')}% — {b.get('note','')}")

    print(f"→ asking {args.worker}: {args.task!r}  (deadline {args.deadline}s)")
    out = await ask_agent(js, requester, args.worker, args.task,
                          deadline_s=args.deadline, want=args.want,
                          context=context, on_event=show)
    await nc.close()

    print()
    if out["ok"]:
        print(f"✓ RESULT from {out['worker']} in {out['elapsed_s']}s "
              f"(events: {'→'.join(out['events'])}):\n")
        r = out["result"]
        print(json.dumps(r, indent=2) if isinstance(r, (dict, list)) else str(r))
    else:
        print(f"✗ {out['error'].upper()} from {out['worker']}: {out['detail']} "
              f"(events: {'→'.join(out['events']) or 'none'})")
    sys.exit(0 if out["ok"] else 2)


if __name__ == "__main__":
    asyncio.run(_cli())
