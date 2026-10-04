#!/bin/bash
# Sibline task-worker wrapper for Ollie (OpenClaw on CherryRd).
#
# The sibline subscriber calls this with the task prompt as $1 when a
# kind=task_request arrives. It runs a headless OpenClaw agent turn via the
# Gateway, extracts the reply text, and prints it to stdout — which the
# subscriber captures and publishes as task_result.
#
# Wire it up in the subscriber (env or constant):
#   SIBLINE_WORKER_CMD=/Users/stevens/.openclaw/workspace/scripts/ollie-worker.sh
#   SIBLINE_WORKER_SHELL=0
#   SIBLINE_WORKER_TIMEOUT=600   # openclaw/gpt-5.6 turns can take ~30-60s
#
# Each task gets an isolated session so delegated work never pollutes Ollie's
# main conversation. --json gives structured output; we extract the reply text.
set -euo pipefail
PROMPT="$1"
SID="sibline-task-$(date +%s)-$$"
OUT="$(openclaw agent --session-id "$SID" -m "$PROMPT" --json 2>/dev/null)"
printf '%s' "$OUT" | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin)
except Exception as e:
    sys.stderr.write("worker: bad json: %r\n"%e); sys.exit(2)
# reply text lives at .result.payloads[*].text
texts=[]
res=d.get("result") or {}
for p in (res.get("payloads") or []):
    t=p.get("text")
    if isinstance(t,str) and t.strip(): texts.append(t)
if not texts:
    sys.stderr.write("worker: no reply text in result\n"); sys.exit(3)
print("\n".join(texts))
'
