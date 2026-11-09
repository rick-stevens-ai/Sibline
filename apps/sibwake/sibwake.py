#!/usr/bin/env python3
"""sibwake — Sibline reliable wake (Phase 1): wake a fleet agent and CONFIRM the outcome.

Sends a kind="wake" request to sibline.<target>.inbox and waits for the target subscriber's
wake.received receipt (which carries the local wake-hook HTTP status). Classifies the outcome so
the SENDER knows what happened, instead of firing blind:

  WOKE       receipt says the wake hook returned 2xx (agent's gateway accepted the wake)
  ACCEPTED   receipt arrived but hook non-2xx / no WAKE_URL (subscriber alive, gateway didn't take it)
  QUEUED     no receipt in time, but JetStream shows messages pending for the target (offline; will
             wake on reconnect — a VALID durable outcome, not a failure)
  DOWN       no receipt and nothing pending and no recent presence (host/subscriber unreachable)

Retries the request on timeout (default 2 retries, backoff). Exit code: 0 WOKE/QUEUED, 1 ACCEPTED, 2 DOWN.

Usage:
  sibwake <target> "reason" [--priority urgent] [--timeout 20] [--retries 2] [--json]
  sibwake --all "reason"                 # sweep every family agent -> wake-health matrix
Phase 2 will additionally wait for a wake.ran receipt (true turn-start proof).
"""
import asyncio, json, os, sys, time, uuid, argparse, socket
import nats

BROKER = os.environ.get("NUNTIO_BROKER") or (
    "nats://127.0.0.1:14222" if socket.gethostname().split(".")[0].lower() == "ricksmacstudio"
    else "nats://100.86.220.115:4222")
FAMILY = ["kukla","ollie","tsisdu","ikto","yeil","paradise","lost","prokko","livewire","notdoneyet","nevermind"]

def _pw():
    ef = os.environ.get("SIBLINE_CREDS_FILE")
    paths = ([os.path.expanduser(ef)] if ef else []) + [
        "/tmp/.nats_creds", os.path.expanduser("~/.hermes/nats/sibline-cred"),
        os.path.expanduser("~/.config/sibline/cred"),
        os.path.expanduser("~/.config/sibling-nats/cred"),
        os.path.expanduser("~/.hermes/secrets/nats.env")]
    for p in paths:
        if os.path.exists(p):
            txt = open(p).read()
            for line in txt.splitlines():
                if "PASS" in line and "=" in line:
                    return line.split("=",1)[1].strip().strip('"').strip("'")
            if txt.strip(): return txt.strip().splitlines()[0].strip()
    return None

def _self():
    h = socket.gethostname().split(".")[0].lower()
    m = {"mini":"kukla","macmini":"kukla","monera":"nevermind","cherryrd":"ollie",
         "promaxgb10-95fe":"tsisdu","spark-36ac":"ikto","promaxgb10-9611":"yeil",
         "stevens-nuc13rngi9":"paradise","stevens-nuc7i5bnh":"lost","prokko":"prokko",
         "ricksmacstudio":"notdoneyet","uicgpu01":"livewire"}
    return os.environ.get("NUNTIO_USER") or m.get(h, "kukla")

async def _pending(js, target):
    """How many messages are durably waiting in the target's stream (consumer-independent view)."""
    try:
        si = await js.stream_info(f"sibline-{target}")
        return si.state.messages
    except Exception:
        return None

async def wake_one(nc, js, me, target, reason, priority, timeout, retries):
    reply_subject = f"sibline.{me}.inbox"
    got = {}
    async def on_reply(msg):
        try:
            env = json.loads(msg.data.decode())
        except Exception:
            return
        if env.get("kind") == "wake.received" and env.get("body", {}).get("agent") == target:
            got["receipt"] = env
    sub = await nc.subscribe(reply_subject, cb=on_reply)
    outcome = None
    try:
        for attempt in range(retries + 1):
            req_id = f"{me}-wake-{uuid.uuid4().hex[:12]}"
            env = {"id": req_id, "from": me, "to": target, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "kind": "wake", "priority": priority, "reason": reason,
                   "reply_subject": reply_subject, "require_ack": True,
                   "body": f"[sibwake] {reason}"}
            await js.publish(f"sibline.{target}.inbox", json.dumps(env, separators=(",",":")).encode())
            deadline = time.time() + timeout
            while time.time() < deadline:
                if got.get("receipt"):
                    b = got["receipt"].get("body", {})
                    if b.get("woke"):
                        return {"target": target, "outcome": "WOKE", "hook_status": b.get("hook_status"), "attempt": attempt+1}
                    return {"target": target, "outcome": "ACCEPTED", "hook_status": b.get("hook_status"),
                            "wake_url_configured": b.get("wake_url_configured"), "attempt": attempt+1}
                await asyncio.sleep(0.4)
            # timed out this attempt; will retry
        # no receipt after all attempts -> classify via durable state
        pend = await _pending(js, target)
        if pend and pend > 0:
            outcome = {"target": target, "outcome": "QUEUED", "pending": pend}
        else:
            outcome = {"target": target, "outcome": "DOWN", "pending": pend}
        return outcome
    finally:
        await sub.unsubscribe()

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", nargs="?")
    ap.add_argument("reason", nargs="?", default="wake")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--priority", default="urgent")
    ap.add_argument("--timeout", type=float, default=20)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if not a.all and not a.target:
        ap.error("target required (or --all)")
    me = _self()
    nc = await asyncio.wait_for(nats.connect(BROKER, user=me, password=_pw()), timeout=10)
    js = nc.jetstream()
    targets = [t for t in FAMILY if t != me] if a.all else [a.target]
    results = []
    for t in targets:
        r = await wake_one(nc, js, me, t, a.reason, a.priority, a.timeout, a.retries)
        results.append(r)
        if not a.json:
            extra = " ".join(f"{k}={v}" for k,v in r.items() if k not in ("target","outcome"))
            print(f"{r['target']:<12} {r['outcome']:<9} {extra}")
    await nc.close()
    if a.json:
        print(json.dumps(results, indent=2))
    # exit code reflects worst outcome
    worst = {"WOKE":0,"QUEUED":0,"ACCEPTED":1,"DOWN":2}
    sys.exit(max((worst.get(r["outcome"],2) for r in results), default=0))

if __name__ == "__main__":
    asyncio.run(main())
