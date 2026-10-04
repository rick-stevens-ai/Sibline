#!/usr/bin/env python3
"""Idempotently add sibline task-RPC worker support to Ollie's OpenClaw
nats-subscriber.py. Backs up first. Safe to re-run."""
import re, sys, time, shutil
from pathlib import Path

P = Path.home() / ".openclaw" / "workspace" / "scripts" / "nats-subscriber.py"
src = P.read_text()

if "run_task_worker" in src:
    print("already patched; nothing to do")
    sys.exit(0)

shutil.copy2(P, P.with_suffix(f".py.bak-{int(time.time())}"))

# 1) config constant: after PING_KINDS definition
anchor_cfg = 'PING_KINDS = {"ping", "rr_probe"}'
cfg_block = anchor_cfg + '''

# ── sibline task-RPC worker (delegated work) ─────────────────────────────────
import os as _os, shlex as _shlex, asyncio as _asyncio
TASK_KINDS = {"task_request"}
WORKER_CMD = _os.environ.get(
    "SIBLINE_WORKER_CMD",
    str(Path.home() / ".openclaw" / "workspace" / "scripts" / "ollie-worker.sh"),
).strip()
WORKER_TIMEOUT = float(_os.environ.get("SIBLINE_WORKER_TIMEOUT", "600"))
_SEEN_TASKS = set()


async def run_task_worker(nc, payload, ts):
    """Run a delegated task via WORKER_CMD and publish the task-RPC lifecycle
    (task_accepted -> task_progress -> task_result/task_error) per spec."""
    req_id = str(payload.get("req_id") or payload.get("id") or "")
    requester = str(payload.get("from") or "kukla").strip().lower()
    body = payload.get("body")
    if isinstance(body, dict):
        task_text = str(body.get("task") or body.get("prompt") or "")
        deadline_s = float(body.get("deadline_s") or WORKER_TIMEOUT)
    else:
        task_text = str(body or payload.get("text") or "")
        deadline_s = WORKER_TIMEOUT
    if not req_id or not task_text:
        log(f"task_request ignored (missing req_id/task) from={requester}")
        return
    if req_id in _SEEN_TASKS:
        log(f"task_request {req_id} already seen; skipping")
        return
    _SEEN_TASKS.add(req_id)
    reply_subj = f"sibline.{requester}.inbox"

    async def _emit(kind, body_obj):
        envlp = {
            "id": f"ollie-{kind}-{uuid.uuid4().hex[:10]}",
            "from": "ollie", "to": requester, "ts": ts,
            "reply_to": payload.get("id"), "req_id": req_id,
            "kind": kind, "body": body_obj,
        }
        await nc.publish(reply_subj, json.dumps(envlp, separators=(",", ":")).encode())
        try:
            await nc.flush()
        except Exception:
            pass

    if not WORKER_CMD:
        await _emit("task_error", {"error": "no_worker", "detail": "ollie WORKER_CMD empty"})
        return
    await _emit("task_accepted", {"worker": "ollie", "deadline_s": deadline_s})
    log(f"task {req_id} ACCEPTED from={requester}: {task_text[:80]!r}")

    prompt = (task_text + "\\n\\n---\\nYou are completing a delegated task. Do the work "
              "using your tools as needed, then end with the final answer as plain text. "
              "Do not publish anything to sibline yourself; the result is captured automatically.")
    argv = _shlex.split(WORKER_CMD) + [prompt]
    proc = await _asyncio.create_subprocess_exec(
        *argv, stdout=_asyncio.subprocess.PIPE, stderr=_asyncio.subprocess.STDOUT)
    hb = 0
    start = time.time()
    try:
        while True:
            try:
                stdout, _ = await _asyncio.wait_for(proc.communicate(), timeout=15)
                break
            except _asyncio.TimeoutError:
                hb += 1
                elapsed = time.time() - start
                if elapsed > deadline_s:
                    proc.kill(); await proc.wait()
                    await _emit("task_error", {"error": "timeout", "detail": f"exceeded {deadline_s}s"})
                    log(f"task {req_id}: TIMEOUT {elapsed:.0f}s")
                    return
                await _emit("task_progress", {"heartbeat": hb, "elapsed_s": round(elapsed, 1)})
    except Exception as e:
        await _emit("task_error", {"error": "worker_exception", "detail": repr(e)[:300]})
        return
    out = (stdout or b"").decode("utf-8", errors="replace").strip()
    rc = proc.returncode
    if rc != 0:
        await _emit("task_error", {"error": "worker_nonzero", "rc": rc, "detail": out[-1000:]})
        log(f"task {req_id}: worker rc={rc}")
        return
    await _emit("task_result", {"result": out[-8000:], "rc": rc, "elapsed_s": round(time.time() - start, 1)})
    log(f"task {req_id}: RESULT published ({len(out)} chars, {time.time()-start:.0f}s)")
'''
assert anchor_cfg in src, "PING_KINDS anchor not found"
src = src.replace(anchor_cfg, cfg_block, 1)

# 2) dispatch hook: AFTER the existing ack block (Ollie's subscriber already acks
#    right after the js-recv log), before the _actionable/wake logic.
anchor_disp = '''    # ACK first so broker doesn't redeliver during slow processing
    try:
        await msg.ack()
    except Exception as e:
        log(f"WARN: ack failed: {e}")'''
disp_block = anchor_disp + '''

    # ── sibline task-RPC: handle delegated work in-daemon, before wake/mailbox ──
    if kind in TASK_KINDS:
        await run_task_worker(nc, payload, ts)
        return
'''
assert anchor_disp in src, "ack-block anchor not found"
src = src.replace(anchor_disp, disp_block, 1)

P.write_text(src)
import py_compile
py_compile.compile(str(P), doraise=True)
print("patched + compiles OK:", P)
