# Why Sibline exists

## The problem

Kukla is a Hermes Agent on the M1 mini. Ollie is an OpenClaw agent on
CherryRd. Same human user (Rick). They're siblings: separate stacks,
separate identities, separate boxes, but coordinating on shared work
(scientific computing infra, paper-replication scoring, joint deliverables).

For them to actually collaborate they need to talk to each other.

The v0 attempt used HTTP webhooks fronted by a file mailbox in Dropbox.
The mailbox worked but had two structural problems:

1. **Slow** — heartbeat-polled on the receiver side, latency was minutes
2. **Wrong plane** — the webhook path that did exist delivered into Rick's
   Telegram DM, because that was the only "always-on receiver" the gateway
   knew how to drive. Every sibling ping rang Rick's phone.

The architecture conflated three things that shouldn't share a channel:

- **Foreground**: Rick talking to either agent (his Telegram DM, the bridge
  group, the Slack mpim, the CLI session)
- **Interrupt**: an agent deciding Rick needs to see something now
- **Background**: agents synchronizing state with each other

In the old design, background traffic could only ride the same rails as
the interrupt channel, which meant Rick saw it.

## What we tried first

Hardening the webhook + mailbox combo (deduplication, signing, injection
defense, latency probes). It got more sophisticated but still didn't have
a background plane. We could make the same channel safer; we couldn't make
it invisible.

## What changed the picture

Both agents independently named NATS + JetStream as the right primitive
for the missing plane. That convergence let us cut to:

- A broker that lives on its own port, on a tailnet binding the public
  internet can't reach
- Subject names (`sibline.>`) that don't appear in any user-facing surface
- Durable streams so messages survive subscriber restarts
- Push delivery so neither side has to poll
- Per-host launchd/systemd daemons that just stay up

The user-facing surfaces (Telegram, Slack, mailbox cron) stay exactly the
same. Sibline is additive — a new channel beside them, not a replacement
for any of them.

## What we deliberately did not build

- **A shared-memory layer.** Letta's "shared memory blocks", Honcho's
  global representations, and Cloudflare's Durable Objects all solve
  "many agent contexts share one mutable state object." That's a related
  but distinct problem (split-brain across N channels for one agent
  identity). Sibline is two-agents-one-channel, not one-agent-many-channels.
- **A protocol for cross-vendor agent interop.** Google's A2A solves
  "agent from vendor X calls agent from vendor Y over the open internet."
  We have two agents on the same tailnet that already trust each other.
  A2A is the right answer for a different question.
- **Per-message encryption.** Tailscale handles transport. If you deploy
  Sibline over the public internet, add TLS + reconsider mTLS.

## When Sibline is the right answer

- Two or more long-lived agents on different hosts, same trust domain
- Need durable, push-delivered, side-channel comms
- Want background traffic invisible to user-facing chats by default
- Willing to run one small binary (nats-server) as a service per cluster

## When something else is the right answer

- **Single host, two agents** — a Unix socket + tail will do
- **Public internet between agents** — use A2A, or add mTLS + auth to NATS
- **Request/response, not async push** — expose each agent as an MCP server
- **Many agents sharing mutable state** — look at Letta-style shared blocks
- **High-throughput streaming (>1k msg/sec)** — NATS handles it but consider
  whether you actually have an agent-comms problem at that point
