#!/usr/bin/env bash
# Provision Sibline JetStream streams for the whole agent fleet.
#
# Requires the NATS CLI and an admin (or $JS-capable) context.
# Examples:
#   export NATS_SERVER=nats://YOUR_BROKER:4222
#   export NATS_USER=admin
#   export NATS_PASSWORD=YOUR_ADMIN_PASSWORD
#   bash broker/provision-streams.sh
#
# Or:
#   NATS_CONTEXT=sibline-admin bash broker/provision-streams.sh
#
# Roster override (comma-separated per-peer inbox stream owners):
#   SIBLINE_ROSTER=ollie,kukla,ikto,tsisdu,yeil,paradise,lost,prokko bash broker/provision-streams.sh
#
# ---------------------------------------------------------------------------
# STORAGE NOTE (learned the hard way, 2026-10-04):
# JetStream sums every stream's max_bytes *reservation* against the server's
# `max_file_store` ceiling (nats-server.conf jetstream{}), NOT actual bytes on
# disk. Reserving a fixed max_bytes per stream (e.g. 1GiB) means you can only
# fit `max_file_store / per_stream_reservation` streams before new creates fail
# with err 10047 "insufficient storage resources available" — even though the
# disk is nearly empty. We therefore create streams with max_bytes=-1 (NO
# reservation), bounded instead by max_msgs + max_age. Size the fleet via the
# server's global max_file_store, not per-stream caps.
# ---------------------------------------------------------------------------

set -euo pipefail

CTX_ARGS=()
if [[ -n "${NATS_CONTEXT:-}" ]]; then
  CTX_ARGS=(--context "$NATS_CONTEXT")
elif [[ -n "${NATS_SERVER:-}" ]]; then
  CTX_ARGS=(--server "$NATS_SERVER")
  [[ -n "${NATS_USER:-}" ]] && CTX_ARGS+=(--user "$NATS_USER")
  [[ -n "${NATS_PASSWORD:-}" ]] && CTX_ARGS+=(--password "$NATS_PASSWORD")
fi

ROSTER="${SIBLINE_ROSTER:-ollie,kukla,ikto,tsisdu,yeil,paradise,lost,prokko}"

# Stream limits — reservation-free (see STORAGE NOTE above).
MAX_AGE="${SIBLINE_MAX_AGE:-7d}"
MAX_MSGS="${SIBLINE_MAX_MSGS:-10000}"
MAX_MSG_SIZE="${SIBLINE_MAX_MSG_SIZE:-1048576}"   # 1 MiB per message

need_nats() {
  command -v nats >/dev/null 2>&1 || {
    echo "nats CLI not found; install from https://github.com/nats-io/natscli" >&2
    exit 127
  }
}

ensure_stream() {
  local name="$1" subject="$2"
  if nats "${CTX_ARGS[@]}" stream info "$name" >/dev/null 2>&1; then
    echo "✓ stream exists: $name"
    return 0
  fi
  echo "→ creating stream $name ($subject)"
  nats "${CTX_ARGS[@]}" stream add "$name" \
    --subjects "$subject" \
    --storage file \
    --retention limits \
    --discard old \
    --max-age "$MAX_AGE" \
    --max-msgs "$MAX_MSGS" \
    --max-bytes=-1 \
    --max-msg-size "$MAX_MSG_SIZE" \
    --dupe-window 2m \
    --replicas 1 \
    --ack \
    --defaults
}

# Durable broadcast consumer so a peer never misses room chatter while offline.
ensure_broadcast_consumer() {
  local agent="$1" durable="${1}-broadcast-consumer-v1"
  if nats "${CTX_ARGS[@]}" consumer info sibline-broadcast "$durable" >/dev/null 2>&1; then
    echo "  ✓ broadcast consumer exists: $durable"
    return 0
  fi
  echo "  → creating broadcast consumer: $durable"
  nats "${CTX_ARGS[@]}" consumer add sibline-broadcast "$durable" \
    --ack explicit \
    --deliver new \
    --replay instant \
    --filter '' \
    --defaults
}

need_nats

# Shared broadcast stream (one, fleet-wide).
ensure_stream sibline-broadcast 'sibline.broadcast'

# Per-peer inbox stream + that peer's durable broadcast consumer.
IFS=',' read -ra PEERS <<< "$ROSTER"
for peer in "${PEERS[@]}"; do
  peer="$(echo "$peer" | xargs)"   # trim
  [[ -z "$peer" ]] && continue
  ensure_stream "sibline-${peer}" "sibline.${peer}.>"
  ensure_broadcast_consumer "$peer"
done

echo "OK — Sibline streams provisioned for: ${ROSTER}"
