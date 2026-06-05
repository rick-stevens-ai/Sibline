#!/usr/bin/env python3
"""
Sibline smoke test — publish a probe to your own inbox via JetStream and
verify it gets stored and acked.

Run from any host that can reach the broker (with the kukla or ollie
credentials). This does NOT require a subscriber daemon to be running —
JetStream persists the message on the broker, and we verify by reading
the stream state, not by consuming.

Usage:
    SIBLINE_AGENT=kukla \\
    SIBLINE_SERVER=nats://YOUR_BROKER:4222 \\
    SIBLINE_CREDS_FILE=~/.config/sibline/cred \\
    python3 scripts/smoke.py
"""
import asyncio
import json
import os
import time
import uuid
from pathlib import Path

try:
    import nats
except ImportError:
    raise SystemExit("nats-py not installed. Run: python3 -m pip install nats-py")


AGENT = os.environ.get("SIBLINE_AGENT", "kukla").strip().lower()
SERVER = os.environ.get("SIBLINE_SERVER", "nats://YOUR_BROKER:4222")
CREDS_FILE = Path(os.environ.get("SIBLINE_CREDS_FILE", "~/.config/sibline/cred")).expanduser()


def load_password() -> str:
    if not CREDS_FILE.exists():
        raise SystemExit(f"credentials file not found: {CREDS_FILE}")
    for line in CREDS_FILE.read_text().splitlines():
        if line.startswith("SIBLING_NATS_PASS="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"no SIBLING_NATS_PASS in {CREDS_FILE}")


async def main() -> None:
    pw = load_password()
    nc = await nats.connect(SERVER, user=AGENT, password=pw, name=f"{AGENT}-smoke")
    js = nc.jetstream()

    msg_id = f"smoke-{AGENT}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    envelope = {
        "id": msg_id,
        "from": AGENT,
        "to": AGENT,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "kind": "smoke",
        "body": f"sibline smoke probe from {AGENT} at {time.strftime('%H:%M:%SZ', time.gmtime())}",
    }
    data = json.dumps(envelope, separators=(",", ":")).encode()
    subject = f"sibline.{AGENT}.inbox"

    print(f"→ publishing to {subject} (id={msg_id}, {len(data)}B)")
    ack = await js.publish(subject, data)
    print(f"✓ broker ack: stream={ack.stream} seq={ack.seq} duplicate={ack.duplicate}")

    # Check stream state
    stream = f"sibline-{AGENT}"
    try:
        info = await js.stream_info(stream)
        print(f"✓ stream {stream}: {info.state.messages} msgs, {info.state.bytes}B")
    except Exception as e:
        print(f"! could not read stream info ({e}) — admin perms not granted to {AGENT}, that's fine")

    await nc.drain()
    print(f"OK — Sibline broker reachable from {AGENT}, JetStream working.")


if __name__ == "__main__":
    asyncio.run(main())
