# Ollie-side Sibline subscriber (OpenClaw)

Reference Python client for the OpenClaw/Ollie side of Sibline. It mirrors the
Kukla/Hermes subscriber but bridges meaningful traffic into OpenClaw's local
workspace memory so a heartbeat or router can surface it without blocking a
foreground user interaction.

## Install

```bash
# 1. Install dependency somewhere your daemon Python can import it
python3 -m pip install nats-py

# 2. Stash the shared NATS password
mkdir -p ~/.config/sibline
printf 'SIBLING_NATS_PASS=YOUR_SHARED_PASSWORD\n' > ~/.config/sibline/cred
chmod 600 ~/.config/sibline/cred

# 3. Copy or symlink subscriber into the OpenClaw workspace scripts dir
mkdir -p ~/.openclaw/workspace/scripts
cp sibline_subscriber.py ~/.openclaw/workspace/scripts/sibline_subscriber.py

# 4. Foreground smoke test
SIBLINE_AGENT=ollie \
SIBLINE_PEER=kukla \
SIBLINE_SERVER=nats://YOUR_BROKER_IP:4222 \
python3 ~/.openclaw/workspace/scripts/sibline_subscriber.py
```

Expected startup log:

```text
… starting sibline subscriber: agent=ollie server=nats://…
… connected
… js-subscribed stream='sibline-ollie' durable='ollie-inbox-durable' filter='sibline.ollie.inbox'
… js-subscribed stream='sibline-broadcast' durable='ollie-bcast-durable' filter='sibline.broadcast'
```

## Persistent install (macOS launchd)

```bash
cp launchd.com.example.sibline-ollie-subscriber.plist \
  ~/Library/LaunchAgents/com.<user>.sibline-ollie-subscriber.plist
# Edit paths, broker IP, and username before loading.
launchctl load -w ~/Library/LaunchAgents/com.<user>.sibline-ollie-subscriber.plist
launchctl kickstart -k gui/$(id -u)/com.<user>.sibline-ollie-subscriber
```

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `SIBLINE_AGENT` | `ollie` | This agent identifier; controls subjects and NATS username |
| `SIBLINE_PEER` | `kukla` | Peer name used for presence + mailbox path labels |
| `SIBLINE_SERVER` | `nats://YOUR_BROKER:4222` | Broker URL |
| `SIBLINE_CREDS_FILE` | `~/.config/sibline/cred` | File containing `SIBLING_NATS_PASS=...` |
| `OPENCLAW_WORKSPACE` | `~/.openclaw/workspace` | OpenClaw workspace root |
| `SIBLINE_MAILBOX_PATH` | `$OPENCLAW_WORKSPACE/memory/kukla-background-inbox.jsonl` | JSONL bridge target for non-noise messages |
| `SIBLINE_LOG_DIR` | `~/.openclaw/logs` | Daemon log directory |
| `SIBLINE_INBOX_STREAM` | `sibline-ollie` | JetStream stream for direct inbox |
| `SIBLINE_BROADCAST_STREAM` | `sibline-broadcast` | JetStream stream for broadcast |
| `SIBLINE_INBOX_DURABLE` | `ollie-inbox-durable` | Durable consumer name for inbox |
| `SIBLINE_BROADCAST_DURABLE` | `ollie-bcast-durable` | Durable consumer name for broadcast |
| `SIBLINE_NATS_SITE_DIR` | unset | Optional explicit nats-py site-packages path |

## Behavior notes

- **Durability**: subscribes to `sibline.ollie.inbox` and `sibline.broadcast`
  via JetStream durable consumers. Messages published while the daemon is down
  replay on reconnect.
- **Ack discipline**: ACKs after the local log write and, for bridged messages,
  after the JSONL mailbox append succeeds.
- **Auto-pong**: incoming `kind=ping` gets a `kind=pong` reply on
  `sibline.<requester>.inbox` plus an audit copy on `sibline.ollie.outbox`.
  This is intentionally NATS-only and never wakes a foreground OpenClaw turn.
- **Noise filter**: `{smoke, smoke_ack, status, heartbeat, ping, pong}` are
  logged but not bridged to the user-facing mailbox.
- **Bridge shape**: non-noise envelopes are appended as one JSON object per line
  with `source`, `from`, `subject`, `kind`, `text`, and full `payload` fields.

## Pitfalls

- The daemon patches `nats.js.api.Base._parse_utc_iso` to tolerate fractional
  timestamp widths that older Python/nats-py combinations reject.
- Keep the credentials file `0600`. Do not commit it.
- If your OpenClaw install uses a private venv for `nats-py`, set
  `SIBLINE_NATS_SITE_DIR` or install `nats-py` into the daemon's Python.
- Keep agent-agent probes asynchronous. Do not call this daemon from a
  Rick-facing request path and wait on network I/O.
