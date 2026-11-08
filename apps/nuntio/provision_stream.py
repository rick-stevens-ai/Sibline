#!/usr/bin/env python3
"""Provision the nuntio JetStream stream (owner, one-time; idempotent).
Run with a python that has nats-py and the Sibline credential available.
  python3 provision_stream.py
Creates stream 'sibline-nuntio' (subjects sibline.nuntio.>) if absent; prints current state.
NOTE: max_age is passed in SECONDS (nats-py converts to ns). Passing ns overflows.
"""
import asyncio, os, nats
from nats.js.api import StreamConfig, StorageType, RetentionPolicy

BROKER = os.environ.get("NUNTIO_BROKER", "nats://100.86.220.115:4222")
USER   = os.environ.get("NUNTIO_USER", "kukla")

def _pw():
    ef = os.environ.get("SIBLINE_CREDS_FILE")
    paths = ([os.path.expanduser(ef)] if ef else []) + [
        "/tmp/.nats_creds",
        os.path.expanduser("~/.config/sibline/cred"),
        os.path.expanduser("~/.config/sibling-nats/cred"),
        os.path.expanduser("~/.hermes/secrets/nats.env"),
    ]
    for p in paths:
        if os.path.exists(p):
            txt = open(p).read()
            for line in txt.splitlines():
                if "PASS" in line and "=" in line:
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
            if txt.strip():
                return txt.strip().splitlines()[0].strip()
    return None

async def main():
    nc = await nats.connect(BROKER, user=USER, password=_pw(), connect_timeout=10)
    js = nc.jetstream()
    try:
        si = await js.stream_info("sibline-nuntio")
        print("exists:", si.config.name, si.config.subjects, "msgs", si.state.messages)
    except Exception:
        si = await js.add_stream(StreamConfig(
            name="sibline-nuntio", subjects=["sibline.nuntio.>"],
            storage=StorageType.FILE, retention=RetentionPolicy.LIMITS,
            max_msgs=50000, max_age=90 * 24 * 3600, max_bytes=512 * 1024 * 1024))
        print("created:", si.config.name, si.config.subjects, "max_age_s", si.config.max_age)
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
