# Kukla-side Sibline subscriber (Hermes Agent)

Reference Python client for the Sibline protocol. Originally deployed as the
Hermes-side daemon talking to OpenClaw's Ollie daemon on the other side of
the broker.

## Install

```bash
# 1. Install dependency
python3 -m pip install nats-py

# 2. Stash the shared NATS password (get from broker admin)
mkdir -p ~/.config/sibline
printf 'SIBLING_NATS_PASS=YOUR_SHARED_PASSWORD\n' > ~/.config/sibline/cred
chmod 600 ~/.config/sibline/cred

# 3. Run foreground to smoke test
SIBLINE_AGENT=kukla \
SIBLINE_PEER=ollie \
SIBLINE_SERVER=nats://YOUR_BROKER_IP:4222 \
python3 sibline_subscriber.py
```

You should see:
```
[…] connected to nats://… as kukla; subscribing to sibline.kukla.inbox + sibline.broadcast
```

## Persistent install (macOS launchd)

```bash
# Edit launchd.com.example.sibline-subscriber.plist:
#   - Label  → com.<user>.sibline-subscriber
#   - Paths  → your actual home
#   - Env    → your AGENT / PEER / SERVER / mailbox path
cp launchd.com.example.sibline-subscriber.plist \
   ~/Library/LaunchAgents/com.<user>.sibline-subscriber.plist
launchctl load -w ~/Library/LaunchAgents/com.<user>.sibline-subscriber.plist
```

Recycle: `launchctl kickstart -k gui/$(id -u)/com.<user>.sibline-subscriber`

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `SIBLINE_AGENT` | `kukla` | This agent's identifier (used in subject names + NATS user) |
| `SIBLINE_PEER` | `ollie` | Default peer for auto-pong addressing if `from` is missing |
| `SIBLINE_SERVER` | `nats://YOUR_BROKER:4222` | Broker URL |
| `SIBLINE_CREDS_FILE` | `~/.config/sibline/cred` | File with `SIBLING_NATS_PASS=...` |
| `SIBLINE_LOG_DIR` | `~/.sibline/logs` | Where JSONL logs land |
| `SIBLINE_MAILBOX_PATH` | (unset) | If set, bridges non-noise envelopes here for a separate poller |

## What gets logged

- `sibline-inbox.jsonl` — every message on `sibline.<self>.inbox`
- `sibline-broadcast.jsonl` — every message on `sibline.broadcast`
- `sibline-subscriber.log` — daemon liveness + bridge events

## Behavior notes

- **Durable consumers**: messages published while the daemon is down are
  replayed on reconnect. Acks happen after the log write returns.
- **Auto-pong**: incoming `kind=ping` envelopes get a `kind=pong` reply to the
  sender's inbox plus an audit copy on `sibline.<self>.outbox`. No agent
  session needs to be awake for ping/pong.
- **Mailbox bridge**: if `SIBLINE_MAILBOX_PATH` is set, non-noise envelopes
  (everything except `kind` in {smoke, status, heartbeat, ping, pong} and
  subjects ending in `.smoke|.status|.ping|.pong|.heartbeat`) are appended
  to the mailbox file. This lets an existing poll-driven user-surface (e.g.
  a 1-min cron that surfaces new mail to Telegram) work unchanged.

## Pitfalls

- nats-server 2.14+ returns 5-digit microsecond ISO timestamps that crash
  Python 3.8's `fromisoformat`. The module monkey-patches `nats.js.api.Base`
  to handle this. Remove the patch if you're on Python 3.11+ and a future
  nats-py drops the workaround.
- Credentials file must have mode 0600. The daemon will not warn if it's
  world-readable; check yours.
- `MAILBOX_PATH` must be in a directory the daemon can write to. If you
  point it at a Dropbox path, sync latency adds ~5-60s to the surface.
