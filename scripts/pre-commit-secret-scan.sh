#!/usr/bin/env bash
# Lightweight high-confidence secret scan for local pre-commit use.
set -euo pipefail
python3 - <<'PY'
import pathlib, re, sys
patterns=[
 re.compile(r'xox[baprs]-'), re.compile(r'ghp_[A-Za-z0-9_]{20,}'), re.compile(r'github_pat_'),
 re.compile(r'Bearer [A-Za-z0-9._-]{20,}'), re.compile(r'SIBLING_NATS_PASS=(?!YOUR_)(?!\.\.\.)[A-Za-z0-9+/]{12,}'),
 re.compile(r'password:\s*"(?!(REPLACE_|YOUR_))[^"\n]+"'),
]
bad=[]
for path in pathlib.Path('.').rglob('*'):
    if '.git' in path.parts or not path.is_file(): continue
    if str(path) in {'scripts/pre-commit-secret-scan.sh', '.github/workflows/ci.yml'}: continue
    try: text=path.read_text(errors='ignore')
    except Exception: continue
    for i,line in enumerate(text.splitlines(),1):
        if any(p.search(line) for p in patterns): bad.append(f'{path}:{i}:{line}')
if bad:
    print('Possible secret(s) found:', *bad, sep='\n'); sys.exit(1)
print('secret scan OK')
PY
