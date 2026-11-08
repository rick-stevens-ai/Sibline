#!/usr/bin/env bash
# nuntio-deploy-nucs: deploy nuntio to nuc13/nuc7 when they become reachable. Idempotent, silent-on-noop.
set -u
NP=~/.local/bin/nuntio.py; NW=~/.local/bin/nuntio
declare -A AGENT=( [nuc13]=paradise [nuc7]=lost )
out=""
for h in nuc13 nuc7; do
  ssh -o ConnectTimeout=8 -o BatchMode=yes "$h" 'true' 2>/dev/null || continue   # unreachable -> skip silently
  # already installed + working?
  if ssh -o ConnectTimeout=8 "$h" "NUNTIO_AGENT=${AGENT[$h]} ~/.local/bin/nuntio read 1 >/dev/null 2>&1" 2>/dev/null; then
    continue
  fi
  scp -o ConnectTimeout=8 "$NP" "$h:~/.local/bin/nuntio.py" >/dev/null 2>&1
  scp -o ConnectTimeout=8 "$NW" "$h:~/.local/bin/nuntio" >/dev/null 2>&1
  ssh -o ConnectTimeout=8 "$h" "chmod +x ~/.local/bin/nuntio ~/.local/bin/nuntio.py" >/dev/null 2>&1
  if ssh -o ConnectTimeout=8 "$h" "NUNTIO_AGENT=${AGENT[$h]} ~/.local/bin/nuntio read 1 >/dev/null 2>&1" 2>/dev/null; then
    out="${out}nuntio deployed+verified on $h (${AGENT[$h]})\n"
  else
    out="${out}nuntio copied to $h but read failed (check nats-py/cred on host)\n"
  fi
done
[ -n "$out" ] && printf "%b" "$out"   # empty => silent (watchdog pattern)
