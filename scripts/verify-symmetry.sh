#!/usr/bin/env bash
# Sibline bidirectional symmetry test.
#
# Sends a ping from each agent to the other and waits for the auto-pong.
# Requires nats CLI contexts configured with username/password auth (not nkey --creds):
#   nats context save sibline-kukla --server nats://YOUR_BROKER:4222 --user kukla --password "$SIBLING_NATS_PASS"
#   nats context save sibline-ollie --server nats://YOUR_BROKER:4222 --user ollie --password "$SIBLING_NATS_PASS"
#
# Usage:
#   TIMEOUT=10 bash scripts/verify-symmetry.sh

set -euo pipefail

KUKLA_CTX="${KUKLA_CTX:-sibline-kukla}"
OLLIE_CTX="${OLLIE_CTX:-sibline-ollie}"
TIMEOUT="${TIMEOUT:-10}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$TIMEOUT" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$TIMEOUT" "$@"
  else
    "$@"
  fi
}

probe() {
    local from="$1" to="$2" ctx="$3"
    local id="probe-${from}-$(date +%s)-$$"
    local env
    env=$(printf '{"id":"%s","from":"%s","to":"%s","ts":"%s","kind":"ping","body":{"purpose":"symmetry-verify"}}' \
        "$id" "$from" "$to" "$(ts)")
    echo "→ ${from} ping → sibline.${to}.inbox  (id=${id})"
    nats --context "$ctx" pub "sibline.${to}.inbox" "$env" 2>&1 | tail -1
    echo "  waiting up to ${TIMEOUT}s for pong on sibline.${from}.inbox …"
    if run_with_timeout nats --context "$ctx" sub "sibline.${from}.inbox" --count 1 --raw 2>&1 \
        | grep -m1 "\"reply_to\":\"${id}\"" >/dev/null; then
      echo "✓ ${from} ← pong from ${to}"
    else
      echo "✗ no pong received within ${TIMEOUT}s"
      return 1
    fi
}

echo "=== Sibline symmetry verification ==="
probe kukla ollie "$KUKLA_CTX"
echo
probe ollie kukla "$OLLIE_CTX"
echo
echo "=== both directions OK ==="
