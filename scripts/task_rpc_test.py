#!/usr/bin/env python3
"""
Sibline Task-RPC integration tests.

Delegates REAL tasks to live agents over sibline and asserts a correlated
terminal reply comes back, per spec/sibline-task-rpc-v1.md. This exercises the
full help path: task_request → (task_accepted → task_progress*) → task_result.

Unlike mesh_test.py (liveness), this requires the responding agent to actually
DO work and reply following the protocol (the `sibline-help` skill). A worker
that hasn't adopted the skill yet shows up as 'no_accept' — that is a true
readiness signal, not a test bug.

Usage:
  SIBLINE_SERVER=nats://HOST:4222 SIBLINE_USER=kukla SIBLING_NATS_PASS=... \
  python3 scripts/task_rpc_test.py --workers ollie,ikto,yeil [--deadline 120]

Exit: 0 if every targeted worker returned a terminal task_result; else 1.
Workers that are simply offline can be excluded via --require-live.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys

# import the requester library from the sibling common/ dir
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients", "common"))
import sibline_task as rpc  # noqa: E402

try:
    import nats
except ImportError:
    sys.exit("nats-py required")

# A battery of small, verifiable tasks. Each has a checker that validates the
# worker's result so we test CORRECTNESS, not just that *something* came back.
TASKS = [
    {
        "name": "identity",
        "task": "Reply with ONLY your agent name and the hostname you run on, "
                "as JSON: {\"agent\": <name>, \"host\": <hostname>}.",
        "want": "json",
        "check": lambda r: isinstance(r, (dict, str)) and bool(r),
        "detail": "returns agent+host",
    },
    {
        "name": "arithmetic",
        "task": "Compute 17 * 23 and reply with ONLY the integer result.",
        "want": "number",
        "check": lambda r: str(r).strip().replace('"', '') == "391",
        "detail": "17*23 == 391",
    },
    {
        "name": "reasoning",
        "task": "A farmer has 12 sheep; all but 9 run away. Reply with ONLY the "
                "number remaining.",
        "want": "number",
        "check": lambda r: "9" in str(r),
        "detail": "answer is 9",
    },
]


async def run_for_worker(js, requester, worker, deadline):
    print(f"\n=== worker: {worker} ===")
    results = []
    for t in TASKS:
        def show(env):
            k = env.get("kind"); b = env.get("body") or {}
            if k == "task_accepted":
                print(f"  [{t['name']}] · accepted")
            elif k == "task_progress":
                print(f"  [{t['name']}] · {b.get('pct','?')}% {b.get('note','')}")
        out = await rpc.ask_agent(js, requester, worker, t["task"],
                                  deadline_s=deadline, want=t["want"], on_event=show)
        if out["ok"]:
            ok = False
            try:
                ok = bool(t["check"](out["result"]))
            except Exception:
                ok = False
            mark = "✓" if ok else "✗"
            print(f"  {mark} {t['name']:12s} ({t['detail']}) "
                  f"[{'→'.join(out['events'])}] {out['elapsed_s']}s -> {str(out['result'])[:80]!r}")
            results.append(("correct" if ok else "wrong_answer", t["name"], out))
        else:
            print(f"  ✗ {t['name']:12s} {out['error'].upper()}: {out['detail']} "
                  f"[{'→'.join(out['events']) or 'none'}]")
            results.append((out["error"], t["name"], out))
    return results


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", default=os.environ.get("SIBLINE_WORKERS", "ollie"),
                    help="comma-separated agent names to delegate to")
    ap.add_argument("--deadline", type=int, default=120)
    ap.add_argument("--require-live", action="store_true",
                    help="only fail on workers that accepted but gave wrong/err answers "
                         "(treat no_accept as offline, not a failure)")
    args = ap.parse_args()

    server = os.environ.get("SIBLINE_SERVER", "nats://100.86.220.115:4222")
    user = os.environ.get("SIBLINE_USER", "kukla")
    pw = os.environ.get("SIBLINE_PASS") or os.environ.get("SIBLING_NATS_PASS", "")
    requester = os.environ.get("SIBLINE_AGENT", user)
    if not pw:
        sys.exit("set SIBLINE_PASS or SIBLING_NATS_PASS")
    workers = [w.strip() for w in args.workers.split(",") if w.strip()]

    nc = await nats.connect(server, user=user, password=pw, connect_timeout=8)
    js = nc.jetstream()
    print(f"# Sibline Task-RPC test  requester={requester}  workers={workers}  "
          f"deadline={args.deadline}s")

    all_results = {}
    for w in workers:
        all_results[w] = await run_for_worker(js, requester, w, args.deadline)
    await nc.close()

    # Summary + exit policy
    print("\n" + "=" * 60)
    hard_fail = 0
    for w, res in all_results.items():
        correct = sum(1 for s, _, _ in res if s == "correct")
        statuses = [s for s, _, _ in res]
        print(f"  {w:10s} {correct}/{len(res)} correct  "
              f"[{', '.join(statuses)}]")
        for s in statuses:
            if s in ("wrong_answer", "task_failed", "timeout"):
                hard_fail += 1
            elif s == "no_accept" and not args.require_live:
                hard_fail += 1
    print("=" * 60)
    if hard_fail:
        print(f"\n{hard_fail} task(s) did not complete correctly.")
        if any(s == "no_accept" for res in all_results.values() for s, _, _ in res):
            print("Hint: 'no_accept' means the worker has not adopted the sibline-help "
                  "skill (doesn't answer task_request yet), or is offline.")
    else:
        print("\nAll targeted workers completed their tasks correctly. ✓")
    sys.exit(1 if hard_fail else 0)


if __name__ == "__main__":
    asyncio.run(main())
