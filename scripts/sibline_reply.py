#!/usr/bin/env python3
"""
Sibline task-RPC reply helper (WORKER side).

A worker agent uses this to publish lifecycle envelopes back to a requester,
per spec/sibline-task-rpc-v1.md. Shell-friendly so an agent can call it in one
line from its turn.

Subcommands:
  accepted  --to R --req-id ID [--eta 60]
  progress  --to R --req-id ID --pct 50 [--note "..."]
  result    --to R --req-id ID --result 'TEXT or JSON' [--elapsed 42]
  error     --to R --req-id ID --error TYPE --detail 'why'

Env: SIBLINE_SERVER (default nats://100.86.220.115:4222),
     SIBLINE_USER / SIBLINE_AGENT (this worker's name),
     SIBLING_NATS_PASS or SIBLINE_PASS, or creds file ~/.config/sibline/cred.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

try:
    import nats
except ImportError:
    sys.exit("nats-py required: pip install nats-py")


def _load_pw():
    pw = os.environ.get("SIBLING_NATS_PASS") or os.environ.get("SIBLINE_PASS")
    if pw:
        return pw
    for p in (os.environ.get("SIBLINE_CREDS_FILE"),
              str(Path.home() / ".config" / "sibline" / "cred"),
              "/tmp/.nats_creds"):
        if p and Path(p).exists():
            for line in Path(p).read_text().splitlines():
                if line.startswith("SIBLING_NATS_PASS="):
                    return line.split("=", 1)[1].strip()
    sys.exit("no password: set SIBLING_NATS_PASS or provide ~/.config/sibline/cred")


def _maybe_json(s):
    try:
        return json.loads(s)
    except Exception:
        return s


async def _publish(worker, to, kind, body):
    server = os.environ.get("SIBLINE_SERVER", "nats://100.86.220.115:4222")
    user = os.environ.get("SIBLINE_USER") or os.environ.get("SIBLINE_AGENT") or worker
    pw = _load_pw()
    nc = await nats.connect(server, user=user, password=pw, connect_timeout=8)
    js = nc.jetstream()
    env = {
        "id": f"{worker}-{kind}-{uuid.uuid4().hex[:10]}",
        "from": worker,
        "to": to,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "reply_to": body["req_id"],
        "kind": kind,
        "body": body,
    }
    ack = await js.publish(f"sibline.{to}.inbox", json.dumps(env).encode())
    # audit copy to our own outbox (best-effort; ignore if not permitted)
    try:
        await js.publish(f"sibline.{worker}.outbox", json.dumps(env).encode())
    except Exception:
        pass
    await nc.close()
    print(f"{kind} -> sibline.{to}.inbox (seq {ack.seq}, req_id {body['req_id']})")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("accepted", "progress", "result", "error"):
        p = sub.add_parser(c)
        p.add_argument("--to", required=True, help="requester agent name")
        p.add_argument("--req-id", required=True)
        p.add_argument("--worker", default=os.environ.get("SIBLINE_AGENT")
                       or os.environ.get("SIBLINE_USER") or "")
        if c == "accepted":
            p.add_argument("--eta", type=int, default=None)
        if c == "progress":
            p.add_argument("--pct", type=int, default=None)
            p.add_argument("--note", default="")
        if c == "result":
            p.add_argument("--result", required=True)
            p.add_argument("--elapsed", type=float, default=None)
        if c == "error":
            p.add_argument("--error", required=True,
                           help="unsupported_task|refused|tool_failed|timeout|bad_request")
            p.add_argument("--detail", default="")
    args = ap.parse_args()

    worker = args.worker or os.environ.get("SIBLINE_USER") or ""
    if not worker:
        sys.exit("set --worker or SIBLINE_AGENT/SIBLINE_USER to your agent name")

    body = {"req_id": args.req_id, "worker": worker}
    kind = {"accepted": "task_accepted", "progress": "task_progress",
            "result": "task_result", "error": "task_error"}[args.cmd]

    if args.cmd == "accepted" and args.eta is not None:
        body["eta_s"] = args.eta
    elif args.cmd == "progress":
        if args.pct is not None:
            body["pct"] = args.pct
        body["note"] = args.note
    elif args.cmd == "result":
        body["ok"] = True
        body["result"] = _maybe_json(args.result)
        if args.elapsed is not None:
            body["elapsed_s"] = args.elapsed
    elif args.cmd == "error":
        body["ok"] = False
        body["error"] = args.error
        body["detail"] = args.detail

    asyncio.run(_publish(worker, args.to, kind, body))


if __name__ == "__main__":
    main()
