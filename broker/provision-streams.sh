#!/usr/bin/env bash
# Provision Sibline JetStream streams.
#
# Requires the NATS CLI and an admin context or env-configured server/user/password.
# Examples:
#   export NATS_SERVER=nats://YOUR_BROKER:4222
#   export NATS_USER=admin
#   export NATS_PASSWORD=YOUR_ADMIN_PASSWORD
#   bash broker/provision-streams.sh
#
# Or:
#   NATS_CONTEXT=sibline-admin bash broker/provision-streams.sh

set -euo pipefail

CTX_ARGS=()
if [[ -n "${NATS_CONTEXT:-}" ]]; then
  CTX_ARGS=(--context "$NATS_CONTEXT")
elif [[ -n "${NATS_SERVER:-}" ]]; then
  CTX_ARGS=(--server "$NATS_SERVER")
  [[ -n "${NATS_USER:-}" ]] && CTX_ARGS+=(--user "$NATS_USER")
  [[ -n "${NATS_PASSWORD:-}" ]] && CTX_ARGS+=(--password "$NATS_PASSWORD")
fi

need_nats() {
  command -v nats >/dev/null 2>&1 || { echo "nats CLI not found; install from https://github.com/nats-io/natscli" >&2; exit 127; }
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
    --max-age 7d \
    --max-msgs 10000 \
    --max-msg-size 1048576 \
    --dupe-window 2m \
    --ack \
    --defaults
}

need_nats
ensure_stream sibline-kukla 'sibline.kukla.>'
ensure_stream sibline-ollie 'sibline.ollie.>'
ensure_stream sibline-broadcast 'sibline.broadcast'
echo 'OK — Sibline streams provisioned.'
