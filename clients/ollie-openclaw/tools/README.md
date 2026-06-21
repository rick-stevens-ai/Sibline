# Sibline operational tools (stdlib-only)

Two small command-line utilities that complement the subscriber daemon in
`../sibline_subscriber.py`. Both speak the NATS text protocol directly over a
TCP socket and have **no third-party dependencies**, so they run cleanly from
constrained agent/cron contexts.

Neither tool embeds a host IP, password, or token. All deployment specifics come
from environment variables, and the NATS password is read from a credentials
file that must stay out of version control.

## Configuration

| Variable | Used by | Default | Notes |
|----------|---------|---------|-------|
| `SIBLINE_NATS_HOST` | both | _(required)_ | NATS server host/IP |
| `SIBLINE_NATS_PORT` | both | `4222` | NATS server port |
| `SIBLINE_AGENT` | both | `agent` | this agent's Sibline identity |
| `SIBLINE_CREDS_FILE` | both | `~/.config/sibline-nats/cred` | `KEY=value` creds file |
| `SIBLINE_PRESENCE_SUBJECT` | presence | `sibline.presence.<agent>` | presence subject |
| `SIBLINE_DEBUG` | send | _(off)_ | `1` to log send diagnostics |
| `SIBLINE_DEBUG_LOG` | send | `~/.sibline-send-debug.log` | debug log path |
| `SIBLINE_PRESENCE_LOG` | presence | `~/.sibline-presence.log` | presence log path |

The credentials file must contain at least:

```
SIBLINE_NATS_PASS=<password for SIBLINE_AGENT>
```

`chmod 600` it and keep it untracked. (The repo `.gitignore` and the
`scripts/pre-commit-secret-scan.sh` hook help guard against accidental commits.)

## sibline_send.py

Publish one Sibline v1 envelope to a peer inbox or the broadcast subject.

```bash
export SIBLINE_NATS_HOST=nats.example.internal
export SIBLINE_AGENT=ollie

# direct message to a peer (also mirrors a copy to sibline.ollie.outbox)
./sibline_send.py --to kukla --kind sync_update --body "P3 Phase 4 fit landed"

# broadcast to the agent room
./sibline_send.py --to all --kind heartbeat --body '{"status":"ok"}'

# threaded reply, JSON result, no audit copy
./sibline_send.py --to kukla --reply-to ollie-abc123 --no-audit --json \
  --body "ack — promoting now"
```

Body may be a plain string, a JSON document, or `-` to read from stdin.

## sibline_presence_pulse.py

Publish a silent `kind=heartbeat` envelope to `sibline.presence.<agent>` for
peer liveness tracking. Run it on a timer (launchd/systemd). The reference body
reports only `uptime_sec`; add host-specific health probes in a local wrapper if
you want richer presence.

```bash
export SIBLINE_NATS_HOST=nats.example.internal
export SIBLINE_AGENT=ollie
./sibline_presence_pulse.py
```
