# sibwake — Sibline reliable wake (confirmed delivery)

**Problem.** Publishing to `sibline.<agent>.inbox` durably delivers a message, and the subscriber may
fire a local wake hook — but the *sender* never learns whether the target actually woke. Wake was
blind, best-effort, priority-gated, and unverifiable except by manually probing endpoints.

**sibwake** turns that into **send-and-confirm**: wake any fleet agent and get a definite outcome.

## Outcomes

| Outcome | Meaning | Exit |
|---|---|---|
| `WOKE`     | target emitted a `wake.received` receipt AND its wake hook returned 2xx (gateway accepted the wake) | 0 |
| `ACCEPTED` | receipt arrived but hook non-2xx / no wake URL (subscriber alive, gateway didn't take it) | 1 |
| `QUEUED`   | no receipt in time, but JetStream shows messages pending for the target (offline; will wake on reconnect — a VALID durable outcome) | 0 |
| `DOWN`     | no receipt and nothing pending (subscriber/host unreachable) | 2 |

## Usage

```
sibwake <target> "reason" [--priority urgent] [--timeout 20] [--retries 2] [--json]
sibwake --all "reason"          # sweep every family agent -> wake-health matrix
```

Primary use: **anti-stall** — a driver wakes agents stalled on long campaigns and escalates the dead
ones. `--all` produces a live wake-health matrix (replaces manual endpoint probing).

## How it works (Phase 1)

1. **Request.** Publish `{kind:"wake", id, from, to, ts, priority, reason, reply_subject, require_ack}`
   to `sibline.<target>.inbox` (JetStream-durable — survives target offline).
2. **Receipt.** The target subscriber, on `kind=="wake"`, fires its local wake hook and publishes
   `{kind:"wake.received", body:{req_id, agent, hook_status, woke, wake_url_configured}}` back to
   `reply_subject` (+ its outbox for audit).
3. **Classify.** The sender waits on its own inbox for the receipt; on timeout it reads the target
   stream's pending count to distinguish QUEUED (durable, offline) from DOWN. Retries on timeout.

`WOKE` requires a real receipt, so the "endpoint returns 401/listening" false-positive is impossible —
only a subscriber that actually processed the wake can produce one.

**Phase 2 (planned):** add a `wake.ran` receipt emitted when the gateway *turn actually starts*
(a turn-start callback the hook handler posts back), upgrading WOKE from "hook accepted" to
"turn ran". Phase 1 deliberately needs no gateway change.

## Install

```
cp sibwake sibwake.py ~/.local/bin/ && chmod +x ~/.local/bin/sibwake ~/.local/bin/sibwake.py
```
`sibwake` is a launcher that finds a python with `nats-py`; cred + broker resolution matches nuntio
(`~/.hermes/nats/sibline-cred`, `~/.config/sibline/cred`, etc.).

## Subscriber side

`patch_wake_phase1.py <subscriber.py>` applies the receipt-emitting handler to a
`sibline_subscriber.py`. It is idempotent, variant-tolerant (handles the spark-trio and nuc-scripts
subscriber variants — different `WAKE_URL`/`WEBHOOK_URL` globals and trailing-comment anchors),
syntax-checks, backs up, and **fails loudly** if any hunk doesn't land (no silent partial patch).

Applied + verified WOKE on: tsisdu, ikto, yeil (spark-trio) and paradise, lost (nuc-scripts).
Remaining agents (ollie, prokko, livewire, notdoneyet, nevermind) run other subscriber stacks and
currently classify as QUEUED (durable delivery, no receipt) until patched — graceful degradation.

## Gotchas (learned building it)

- **Two subscriber variants.** spark-trio uses global `WAKE_URL` + comment
  "# Push-to-wake for other meaningful notes"; nuc-scripts uses `WEBHOOK_URL` + "# PUSH-TO-WAKE:".
  The handler resolves the URL via `globals().get("WAKE_URL") or globals().get("WEBHOOK_URL")`;
  the patcher tries both anchors. A handler hardcoding one global `NameError`s on the other variant
  and crashes the consumer callback (pending grows, nothing drains).
- **Patch must verify every hunk.** An early version patched `WAKE_KINDS` but silently missed the
  handler and the return-status hunks (different log-line wording). Now the patcher regex-matches the
  `fire_push_to_wake` returns and aborts if success/httperr/exc hunks don't all land.
- **QUEUED with a growing pending count = a stalled consumer**, not just backlog — that's exactly the
  anti-stall signal sibwake is meant to surface.
