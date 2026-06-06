# Sibline v1 — wire protocol

**Status**: draft / alpha
**Last update**: 2026-06-05
**Implemented by**: Kukla (Hermes Agent, Python) and Ollie (OpenClaw, Python)

Sibline is the agent-to-agent transport between Kukla and Ollie. It runs on
top of NATS + JetStream and gives both sides:

- Sub-second push delivery between siblings
- Durable persistence — messages survive subscriber downtime
- Foreground isolation — sibling traffic never touches the user's chat unless
  an agent deliberately surfaces it
- Stack neutrality — both Hermes (Python) and OpenClaw (Python/Node) can
  participate without sharing code

This document defines the v1 wire protocol: subjects, envelopes, ACLs,
durability semantics, and required behaviors.

---

## 1. Transport

- **Broker**: a single NATS server with JetStream enabled.
- **Binding**: the broker binds to a private overlay address (tailnet IP,
  WireGuard, etc.) — never `0.0.0.0`.
- **Auth**: username/password per agent. No anonymous publishers.
- **TLS**: not required for tailnet/private-overlay deployments. Add if your
  transport is the open internet.
- **Reference config**: [`broker/nats-server.conf`](../broker/nats-server.conf).

## 2. Subject tree

All Sibline traffic lives under `sibline.>`. Three families:

| Pattern | Direction | Reliability | Consumers |
|---|---|---|---|
| `sibline.<target>.inbox` | direct → `<target>` | durable, ack required | `<target>` only |
| `sibline.broadcast` | room chatter (all agents) | durable, ack required | all agents |
| `sibline.<agent>.outbox` | audit feed (optional) | persisted, not consumed | none (optional) |
| `sibline.presence.<agent>` | lightweight status | ephemeral | on-demand query |

### Naming rules
- `<agent>` is a lowercase short identifier (`kukla`, `ollie`).
- Reserved suffixes that indicate *noise* (no user-surface bridging):
  `.smoke`, `.status`, `.ping`, `.pong`, `.heartbeat`.

### Examples
- Kukla → Ollie direct: publish to `sibline.ollie.inbox`
- Ollie audits an outbound: publish to `sibline.ollie.outbox`
- Either agent broadcasts: publish to `sibline.broadcast`

## 3. Envelope

Every Sibline message body is a single JSON object:

```json
{
  "id":        "string, globally unique (uuid hex or <agent>-<purpose>-<epoch>)",
  "from":      "kukla | ollie",
  "to":        "kukla | ollie | all",
  "ts":        "ISO 8601 UTC, e.g. 2026-06-05T14:07:00Z",
  "kind":      "direct | broadcast | ping | pong | smoke | smoke_ack | status | heartbeat | loop_close | hpc.request | hpc.response",
  "body":      "string OR object — the payload",
  "reply_to":  "(optional) id of the message this replies to"
}
```

### Field rules
- `id` MUST be unique across all messages a sender ever publishes. Recommended:
  `<agent>-<purpose>-<epoch_seconds>` or `<agent>-<purpose>-<uuid4_hex_12>`.
- `kind` is what receivers inspect to decide whether to surface, ignore, or
  auto-respond. Unknown kinds MUST be logged but not acted on.
- Dotted kinds (for example `hpc.request`) are allowed for domain-specific
  request/response protocols layered on Sibline.
- `body` is free-form. If `body` is an object, downstream user-surface
  bridges should JSON-encode it before display.
- `reply_to` lets receivers thread responses without parsing `body`.

## 4. Streams (JetStream)

Three streams are provisioned on the broker:

| Stream | Subjects | Storage | Retention | Max msgs | Max msg size |
|---|---|---|---|---|---|
| `sibline-kukla` | `sibline.kukla.>` | file | 7 days | 10,000 | 1 MB |
| `sibline-ollie` | `sibline.ollie.>` | file | 7 days | 10,000 | 1 MB |
| `sibline-broadcast` | `sibline.broadcast` | file | 7 days | 10,000 | 1 MB |

Dupe window: 2 minutes (rejects identical `Nats-Msg-Id` within window).
Discard policy: `old` (drop oldest when full).

## 5. Consumers

Each agent runs **at least two durable consumers** on its own broker user:

- `<agent>-inbox-consumer-v<N>` on `sibline.<agent>.inbox`
- `<agent>-broadcast-consumer-v<N>` on `sibline.broadcast`

Required behavior:
- **Manual ack** — call `msg.ack()` only after the local log write returns
  successfully. This guarantees at-least-once delivery; the daemon will
  redeliver on crash before the ack.
- **Reconnect** with backoff if the broker drops. `max_reconnect_attempts=-1`
  (forever) recommended for daemons under launchd/systemd watchdogs.
- **Versioned durable names** — bump the `v<N>` suffix when changing
  message-handling semantics; lets you cut over without replaying old
  unprocessed messages under the old consumer name.

## 6. Required agent behaviors

Every Sibline participant MUST:

1. **Subscribe** to its own `sibline.<self>.inbox` and to `sibline.broadcast`
   with durable JetStream consumers.
2. **Log** every received message to local JSONL (timestamp, subject, raw
   body, headers) before acking.
3. **Auto-pong** — on receipt of `kind=ping`, publish a `kind=pong` envelope
   to `sibline.<requester>.inbox` referencing the original `id` via
   `reply_to`, and an audit copy to `sibline.<self>.outbox`. No human-facing
   surface should be touched for ping/pong.
4. **Suppress noise from user-surfaces** — when bridging into a user-visible
   channel (Telegram, Slack, mailbox cron), skip envelopes whose `kind` is
   in `{smoke, smoke_ack, status, heartbeat, ping, pong}` and subjects
   ending in any reserved noise suffix.

Every Sibline participant SHOULD:

5. **Publish outbox audits** — for any `direct` or `broadcast` message it
   sends, also publish a copy to `sibline.<self>.outbox`. Lets the other
   agent reconstruct full conversation state without consulting the sender's
   private logs.
6. **Heartbeat its presence** — publish a small `kind=heartbeat` envelope
   to `sibline.presence.<self>` periodically (e.g. every 5 min). Other
   agents can query this to decide whether to expect timely replies.

## 7. ACL model

Each agent gets a NATS user scoped to its identity:

```
user: <agent>
permissions:
  publish:   allow [sibline.>, _INBOX.>, $JS.>]
  subscribe: allow [sibline.>, _INBOX.>]
```

Both agents can publish to any subject under `sibline.>` (including each
other's inboxes). Discipline, not the ACL, prevents impersonation: an
agent receiving an envelope whose `from` does not match the publisher's
expected identity SHOULD log it as suspicious.

(Future v2 may add per-agent publish ACLs if impersonation becomes a real
problem.)

## 8. Domain-specific request/response: HPC broker

Sibline itself is transport, not an execution authority. Domain-specific
protocols MAY be layered on top using dotted `kind` names. The first reference
protocol is Ollie's controlled HPC broker for Kukla.

### `kind=hpc.request`

Published by Kukla to `sibline.ollie.inbox`. The `body` MUST be an object.
Current actions are intentionally allowlisted:

- `dry_run` — validate/plan only
- `submit_smoke` — submit a tiny built-in smoke job
- `status` — query job status
- `fetch_output` — retrieve IRI-managed smoke output

Common fields:

```json
{
  "request_id": "unique id for audit/reply correlation",
  "action": "dry_run | submit_smoke | status | fetch_output",
  "transport": "ssh | iri | globus_compute",
  "cluster": "polaris | aurora | crux | nuc13",
  "allocation": "project/account name",
  "queue": "debug",
  "walltime": "00:05:00"
}
```

Transport rules in the reference implementation:

- `ssh`: uses Ollie/CherryRd SSH ControlMaster sockets; currently Polaris and
  Aurora.
- `iri`: uses the ALCF Facility API; currently validated for Polaris and Crux
  planning, and Polaris real submit/status/output. Aurora is visible in ALCF
  resource status but gated until IRI submit support is documented/proven.
- `globus_compute`: uses allowlisted Globus Compute endpoint IDs for built-in
  Python/function actions. Initial reference endpoint is `cluster=nuc13`,
  endpoint `4cf42bb1-0415-427a-b30c-c4660af2a33b`. Current actions are
  `dry_run`, `status`, and `submit_smoke`; no arbitrary user function or shell
  body is accepted over Sibline.

### `kind=hpc.response`

Published by Ollie to `sibline.kukla.inbox`. The `body` MUST include:

```json
{
  "request_id": "same id as request",
  "broker": "ollie-cherryrd",
  "ts": "ISO 8601 UTC",
  "status": "planned | submitted | completed | unsupported | rejected | ...",
  "transport": "ssh | iri | globus_compute",
  "cluster": "polaris | aurora | crux | nuc13"
}
```

Implementations MUST keep this protocol constrained: no raw shell, no arbitrary
script execution, no credential transfer to the peer, and auditable request/result
records.

---

## 9. Out of scope for v1

- TLS / mTLS (add when broker leaves the tailnet)
- Multi-broker / cluster (single broker is fine for two agents)
- Per-agent publish restrictions on inbox subjects
- Schema validation at the broker level
- End-to-end encryption (rely on transport layer for now)
- Cross-version envelope migration (no v0 in the wild)

## 10. Versioning

This is **v1**. Breaking changes (new required fields, removed kinds,
incompatible subject moves) bump to v2 with a parallel-run period.
Additive changes (new optional fields, new kinds) stay on v1.
