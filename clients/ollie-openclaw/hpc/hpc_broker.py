#!/usr/bin/env python3
"""CherryRd HPC broker for Kukla -> Ollie Sibline requests.

Test-safe v0:
  - Input: memory/kukla-background-inbox.jsonl records where kind == "hpc.request"
           or envelope payload.kind == "hpc.request".
  - Actions: dry_run, submit_smoke, status, fetch_output.
  - Transports: ssh, iri, globus_compute.
  - Clusters: polaris, aurora; crux via IRI only; nuc13 via Globus Compute only.
  - No arbitrary CherryRd shell and no arbitrary PBS script execution in v0.
  - Replies to Kukla using scripts/sibline-send.py.

Request body shape:
{
  "action": "dry_run" | "submit_smoke" | "status" | "fetch_output",
  "transport": "ssh" | "iri" | "globus_compute",
  "cluster": "polaris" | "aurora" | "crux" | "nuc13",
  "request_id": "optional stable id",
  "allocation": "optional allowlisted positive-balance project",
  "queue": "debug",
  "walltime": "00:05:00",
  "job_id": "for status"
}
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alcf_iri  # noqa: E402

HOME = Path.home()
WS = HOME / ".openclaw" / "workspace"
INBOX = WS / "memory" / "kukla-background-inbox.jsonl"
STATE_DIR = WS / "memory" / "hpc-broker"
JOBS_DIR = STATE_DIR / "jobs"
PROCESSED = STATE_DIR / "processed.json"
SIBLINE_SEND = WS / "scripts" / "sibline-send.py"

ALLOW_CLUSTERS = {"polaris", "aurora", "crux", "nuc13"}
ALLOW_ACTIONS = {"dry_run", "submit_smoke", "status", "fetch_output"}
ALLOW_TRANSPORTS = {"ssh", "iri", "globus_compute"}
ALLOW_QUEUES = {
    "polaris": {"debug", "preemptable"},
    "aurora": {"debug", "small", "capacity"},
    "crux": {"debug", "default", "workq"},
    "nuc13": {"local"},
}
# Use only known positive balances from 2026-06-06 checks. Revalidated before submit via sbank.
ALLOW_ALLOCATIONS = {
    "polaris": {"IMPROVE_Aim1", "ModCon", "AuroraGPT"},
    "aurora": {"AuroraGPT", "CompBioAffin", "datascience_collab", "ModCon"},
    "crux": {"IMPROVE_Aim1", "ModCon", "AuroraGPT"},
    "nuc13": {"local"},
}
DEFAULT_ALLOCATION = {
    "polaris": "IMPROVE_Aim1",
    "aurora": "AuroraGPT",
    "crux": "IMPROVE_Aim1",
    "nuc13": "local",
}
DEFAULT_QUEUE = {
    "polaris": "debug",
    "aurora": "debug",
    "crux": "debug",
    "nuc13": "local",
}

GLOBUS_COMPUTE_ENDPOINTS = {
    "polaris": {
        "endpoint_id": "b624baaa-d390-4b7b-b878-1a1c5afc7f2f",
        "host": "polaris",
        "venv_python": "/lus/eagle/projects/IMPROVE_Aim1/stevens/globus-compute-polaris/venv/bin/python",
        "description": "PBS-backed Globus Compute endpoint on Polaris using IMPROVE_Aim1/debug and 4x A100 per block",
        "user_endpoint_config": {
            "worker_init": "cd /lus/eagle/projects/IMPROVE_Aim1/stevens/globus-compute-polaris\nsource /lus/eagle/projects/IMPROVE_Aim1/stevens/globus-compute-polaris/venv/bin/activate\nexport TMPDIR=/tmp\n"
        },
    },
    "nuc13": {
        "endpoint_id": "4cf42bb1-0415-427a-b30c-c4660af2a33b",
        "host": "nuc13",
        "venv_python": "/home/stevens/.globus_compute_venv/bin/python",
        "description": "LocalProvider endpoint on stevens-NUC13RNGi9",
    },
}
JOBID_RE = re.compile(r"^[0-9]+(?:\.[A-Za-z0-9_.-]+)?$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
WALL_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run(cmd: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)


def ssh(cluster: str, remote: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run(["ssh", "-o", "BatchMode=yes", cluster, remote], timeout=timeout)


def load_processed() -> set[str]:
    try:
        data = json.loads(PROCESSED.read_text())
        return set(data.get("processed", []))
    except Exception:
        return set()


def save_processed(processed: set[str]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED.write_text(json.dumps({"processed": sorted(processed), "updated_at": now_iso()}, indent=2) + "\n")


def append_job_record(req_id: str, record: dict[str, Any]) -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    p = JOBS_DIR / f"{safe_filename(req_id)}.json"
    p.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


def safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]", "_", s)[:120]


def parse_body(record: dict[str, Any]) -> dict[str, Any] | None:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    body = payload.get("body", record.get("text"))
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        try:
            v = json.loads(body)
            if isinstance(v, dict):
                return v
        except Exception:
            return None
    return None


def iter_requests(limit: int | None = None) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    if not INBOX.exists():
        return []
    lines = INBOX.read_text(errors="replace").splitlines()
    if limit:
        lines = lines[-limit:]
    out = []
    for line in lines:
        try:
            rec = json.loads(line)
        except Exception:
            continue
        payload = rec.get("payload") if isinstance(rec.get("payload"), dict) else {}
        kind = rec.get("kind") or payload.get("kind")
        if kind != "hpc.request":
            continue
        body = parse_body(rec)
        if not body:
            continue
        req_id = str(body.get("request_id") or rec.get("id") or payload.get("id") or f"req-{len(out)}")
        out.append((req_id, rec, body))
    return out


def validate_common(body: dict[str, Any]) -> tuple[str, str, str, str, str]:
    action = str(body.get("action", "")).strip()
    cluster = str(body.get("cluster", "")).strip().lower()
    transport = str(body.get("transport") or "ssh").strip().lower()
    if action not in ALLOW_ACTIONS:
        raise ValueError(f"action not allowed: {action!r}")
    if transport not in ALLOW_TRANSPORTS:
        raise ValueError(f"transport not allowed: {transport!r}")
    if cluster not in ALLOW_CLUSTERS:
        raise ValueError(f"cluster not allowed: {cluster!r}")
    if transport == "ssh" and cluster == "crux":
        raise ValueError("ssh transport is not configured for crux in this broker; use transport='iri'")
    if transport == "ssh" and cluster == "nuc13":
        raise ValueError("ssh transport is intentionally not exposed through the Sibline HPC broker for nuc13; use transport='globus_compute'")
    if transport == "iri" and cluster == "nuc13":
        raise ValueError("IRI is ALCF-only and is not available for nuc13; use transport='globus_compute'")
    if transport == "globus_compute" and cluster not in GLOBUS_COMPUTE_ENDPOINTS:
        raise ValueError(f"Globus Compute endpoint not allowlisted for cluster {cluster!r}")
    queue = str(body.get("queue") or DEFAULT_QUEUE[cluster]).strip()
    if queue not in ALLOW_QUEUES[cluster]:
        raise ValueError(f"queue {queue!r} not allowed for {cluster}")
    alloc = str(body.get("allocation") or DEFAULT_ALLOCATION[cluster]).strip()
    if alloc not in ALLOW_ALLOCATIONS[cluster]:
        raise ValueError(f"allocation {alloc!r} not allowlisted for {cluster}")
    return action, cluster, queue, alloc, transport


def walltime_to_seconds(walltime: str) -> int:
    if not WALL_RE.match(walltime):
        raise ValueError("walltime must be HH:MM:SS")
    h, m, s = [int(x) for x in walltime.split(":")]
    return h * 3600 + m * 60 + s


def allocation_balance(cluster: str, alloc: str) -> tuple[bool, str, float | None]:
    cp = ssh(cluster, "sbank l a -u stevens", timeout=60)
    if cp.returncode != 0:
        return False, cp.stderr.strip() or cp.stdout.strip(), None
    matched = []
    for line in cp.stdout.splitlines():
        if f" {cluster} " in line and re.search(rf"\s{re.escape(alloc)}\s", line):
            matched.append(line)
    if not matched:
        return False, f"allocation {alloc!r} not found in sbank output", None
    best = None
    for line in matched:
        nums = re.findall(r"-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+\.\d+|-?\d+", line)
        # balance is last numeric column in sbank rows
        if nums:
            try:
                bal = float(nums[-1].replace(",", ""))
                if best is None or bal > best:
                    best = bal
            except Exception:
                pass
    if best is None:
        return False, "could not parse allocation balance", None
    return best > 0, "\n".join(matched), best


def remote_workdir(cluster: str, req_id: str) -> str:
    # Use an absolute remote path. Returning "$HOME/..." and then shell-quoting
    # it creates a literal /home/stevens/$HOME/... directory on PBS systems.
    return f"/home/stevens/projects/kukla-hpc-broker/{safe_filename(req_id)}"


def smoke_script(cluster: str, req_id: str, queue: str, alloc: str, walltime: str) -> str:
    jobname = ("k_hpc_" + safe_filename(req_id).replace(":", "_").replace(".", "_")).lower()[:14]
    lines = [
        "#!/bin/bash",
        f"#PBS -N {jobname}",
        "#PBS -l select=1",
        f"#PBS -l walltime={walltime}",
        f"#PBS -q {queue}",
        f"#PBS -A {alloc}",
        "#PBS -l filesystems=home",
        "#PBS -r y",
        "set -euo pipefail",
        "cd \"$PBS_O_WORKDIR\"",
        "echo BROKER_SMOKE_OK=1",
        f"echo CLUSTER={shlex.quote(cluster)}",
        f"echo REQUEST_ID={shlex.quote(req_id)}",
        "echo HOST=$(hostname)",
        "echo USER=$(whoami)",
        "echo PBS_JOBID=${PBS_JOBID:-unknown}",
        "date -u '+UTC=%Y-%m-%dT%H:%M:%SZ'",
    ]
    if cluster == "aurora":
        lines += [
            "echo AURORA_GPU_ENV_PROBE=1",
            "command -v xpu-smi >/dev/null 2>&1 && xpu-smi discovery -j || true",
        ]
    else:
        lines += [
            "echo POLARIS_GPU_ENV_PROBE=1",
            "command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L || true",
        ]
    return "\n".join(lines) + "\n"


def submit_smoke(req_id: str, cluster: str, queue: str, alloc: str, body: dict[str, Any]) -> dict[str, Any]:
    walltime = str(body.get("walltime") or "00:05:00")
    if not WALL_RE.match(walltime):
        raise ValueError("walltime must be HH:MM:SS")
    ok, bal_text, balance = allocation_balance(cluster, alloc)
    if not ok:
        raise ValueError(f"allocation check failed or non-positive for {alloc}: {bal_text}")
    wd = remote_workdir(cluster, req_id)
    script = smoke_script(cluster, req_id, queue, alloc, walltime)
    local_tmp = STATE_DIR / f"{safe_filename(req_id)}.pbs"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    local_tmp.write_text(script)
    # Stage script via stdin to avoid shell interpolation surprises.
    remote_cmd = f"mkdir -p {shlex.quote(wd)} && cat > {shlex.quote(wd + '/job.pbs')} && cd {shlex.quote(wd)} && qsub job.pbs"
    cp = subprocess.run(["ssh", "-o", "BatchMode=yes", cluster, remote_cmd], input=script, text=True, capture_output=True, timeout=60)
    if cp.returncode != 0:
        raise RuntimeError(f"qsub failed rc={cp.returncode}: stdout={cp.stdout!r} stderr={cp.stderr!r}")
    job_id = cp.stdout.strip().splitlines()[-1].strip()
    return {
        "status": "submitted",
        "cluster": cluster,
        "job_id": job_id,
        "queue": queue,
        "allocation": alloc,
        "allocation_balance": balance,
        "workdir": wd,
        "script_local": str(local_tmp),
    }


def plan_smoke(req_id: str, cluster: str, queue: str, alloc: str, body: dict[str, Any]) -> dict[str, Any]:
    """Validate a smoke submission without staging or qsub side effects."""
    walltime = str(body.get("walltime") or "00:05:00")
    if not WALL_RE.match(walltime):
        raise ValueError("walltime must be HH:MM:SS")
    ok, bal_text, balance = allocation_balance(cluster, alloc)
    if not ok:
        raise ValueError(f"allocation check failed or non-positive for {alloc}: {bal_text}")
    return {
        "status": "planned",
        "transport": "ssh",
        "cluster": cluster,
        "queue": queue,
        "allocation": alloc,
        "allocation_balance": balance,
        "workdir": remote_workdir(cluster, req_id),
        "walltime": walltime,
        "note": "dry-run plan only; no job staged or submitted",
        "script_preview": smoke_script(cluster, req_id, queue, alloc, walltime),
    }


def status_job(cluster: str, body: dict[str, Any]) -> dict[str, Any]:
    job_id = str(body.get("job_id", "")).strip()
    if not JOBID_RE.match(job_id):
        raise ValueError(f"invalid job_id: {job_id!r}")
    cp = ssh(cluster, f"qstat -f {shlex.quote(job_id)} 2>/dev/null || qstat {shlex.quote(job_id)} 2>&1", timeout=60)
    return {
        "status": "ok" if cp.returncode == 0 else "not_found_or_finished",
        "transport": "ssh",
        "cluster": cluster,
        "job_id": job_id,
        "returncode": cp.returncode,
        "stdout": cp.stdout[-6000:],
        "stderr": cp.stderr[-2000:],
    }


def iri_plan(req_id: str, cluster: str, queue: str, alloc: str, body: dict[str, Any]) -> dict[str, Any]:
    walltime = str(body.get("walltime") or "00:05:00")
    duration = int(body.get("duration") or walltime_to_seconds(walltime))
    result = alcf_iri.plan_smoke(cluster, req_id, alloc, queue, duration)
    result["note"] = result.get("note", "") + " Broker IRI path; no SSH login required for submit once token exists."
    return result


def iri_submit_smoke(req_id: str, cluster: str, queue: str, alloc: str, body: dict[str, Any]) -> dict[str, Any]:
    walltime = str(body.get("walltime") or "00:05:00")
    duration = int(body.get("duration") or walltime_to_seconds(walltime))
    return alcf_iri.submit_smoke(cluster, req_id, alloc, queue, duration)


def iri_status_job(cluster: str, body: dict[str, Any]) -> dict[str, Any]:
    job_id = str(body.get("job_id", "")).strip()
    if not job_id or len(job_id) > 200:
        raise ValueError(f"invalid IRI job_id: {job_id!r}")
    return alcf_iri.job_status(cluster, job_id, include_spec=bool(body.get("include_spec")))


def iri_fetch_output(body: dict[str, Any]) -> dict[str, Any]:
    path = str(body.get("path") or body.get("stdout_path") or "").strip()
    if not path.startswith("/home/stevens/iri-hpc-broker/"):
        raise ValueError("IRI fetch_output path must be under /home/stevens/iri-hpc-broker/")
    return alcf_iri.view_file(path, storage=str(body.get("storage") or "home"), wait=True, timeout_seconds=60)


def globus_compute_remote(cluster: str, code: str, timeout: int = 90) -> dict[str, Any]:
    cfg = GLOBUS_COMPUTE_ENDPOINTS[cluster]
    remote = f"{shlex.quote(cfg['venv_python'])} - <<'PY'\n{code}\nPY"
    cp = ssh(cfg["host"], remote, timeout=timeout)
    if cp.returncode != 0:
        raise RuntimeError(
            f"Globus Compute remote command failed rc={cp.returncode}: "
            f"stdout={cp.stdout[-2000:]!r} stderr={cp.stderr[-2000:]!r}"
        )
    try:
        return json.loads(cp.stdout.strip().splitlines()[-1])
    except Exception as e:
        raise RuntimeError(f"Could not parse Globus Compute JSON output: {e}; stdout={cp.stdout[-4000:]!r}") from e


def globus_compute_status(cluster: str, body: dict[str, Any]) -> dict[str, Any]:
    cfg = GLOBUS_COMPUTE_ENDPOINTS[cluster]
    endpoint_id = cfg["endpoint_id"]
    code = f'''
import json
from globus_compute_sdk import Client
endpoint_id = {endpoint_id!r}
cluster = {cluster!r}
client = Client()
out = {{"status": "ok", "transport": "globus_compute", "cluster": cluster, "endpoint_id": endpoint_id}}
try:
    out["endpoint_status"] = client.get_endpoint_status(endpoint_id)
except Exception as e:
    out["endpoint_status_error"] = "%s: %s" % (type(e).__name__, e)
try:
    out["endpoint_metadata"] = client.get_endpoint_metadata(endpoint_id)
except Exception as e:
    out["endpoint_metadata_error"] = "%s: %s" % (type(e).__name__, e)
print(json.dumps(out, default=str, sort_keys=True))
'''
    out = globus_compute_remote(cluster, code)
    out.setdefault("description", cfg.get("description"))
    return out


def globus_compute_plan(req_id: str, cluster: str, body: dict[str, Any]) -> dict[str, Any]:
    cfg = GLOBUS_COMPUTE_ENDPOINTS[cluster]
    return {
        "status": "planned",
        "transport": "globus_compute",
        "cluster": cluster,
        "endpoint_id": cfg["endpoint_id"],
        "endpoint_host": cfg["host"],
        "action": str(body.get("action") or "dry_run"),
        "note": "Globus Compute plan only; no function submitted",
        "supported_actions": ["dry_run", "status", "submit_smoke"],
    }


def globus_compute_submit_smoke(req_id: str, cluster: str, body: dict[str, Any]) -> dict[str, Any]:
    cfg = GLOBUS_COMPUTE_ENDPOINTS[cluster]
    endpoint_id = cfg["endpoint_id"]
    user_endpoint_config = cfg.get("user_endpoint_config")
    timeout = int(body.get("timeout_seconds") or 60)
    if timeout < 5 or timeout > 300:
        raise ValueError("timeout_seconds must be between 5 and 300")
    code = f'''
import json
from globus_compute_sdk import Client, Executor
endpoint_id = {endpoint_id!r}
request_id = {req_id!r}
cluster = {cluster!r}
user_endpoint_config = {user_endpoint_config!r}

def sibline_globus_compute_smoke(cluster=cluster, request_id=request_id):
    import os, socket, sys, time
    return {{
        "GLOBUS_COMPUTE_SMOKE_OK": 1,
        "cluster": cluster,
        "request_id": request_id,
        "host": socket.gethostname(),
        "user": os.getenv("USER"),
        "pid": os.getpid(),
        "python": sys.version.split()[0],
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }}

executor_kwargs = {{"endpoint_id": endpoint_id}}
if user_endpoint_config:
    executor_kwargs["user_endpoint_config"] = user_endpoint_config
with Executor(**executor_kwargs) as ex:
    fut = ex.submit(sibline_globus_compute_smoke)
    result = fut.result(timeout={timeout})
    task_id = getattr(fut, "task_id", None)
print(json.dumps({{"status": "completed", "transport": "globus_compute", "cluster": cluster, "endpoint_id": endpoint_id, "task_id": str(task_id) if task_id else None, "result": result}}, sort_keys=True))
'''
    return globus_compute_remote(cluster, code, timeout=timeout + 45)


def reply(req_id: str, body: dict[str, Any], dry_run: bool = False) -> None:
    envelope = {"request_id": req_id, "broker": "ollie-cherryrd", "ts": now_iso(), **body}
    if dry_run:
        print(json.dumps(envelope, indent=2))
        return
    cp = run([str(SIBLINE_SEND), "--to", "kukla", "--kind", "hpc.response", "--body", json.dumps(envelope)], timeout=30)
    if cp.returncode != 0:
        print(f"WARN: reply failed rc={cp.returncode}: {cp.stderr}", file=sys.stderr)


def handle(req_id: str, rec: dict[str, Any], body: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    if not SAFE_ID_RE.match(req_id):
        raise ValueError(f"unsafe request_id: {req_id!r}")
    action, cluster, queue, alloc, transport = validate_common(body)
    if action == "dry_run":
        if transport == "globus_compute":
            return globus_compute_plan(req_id, cluster, body)
        if transport == "iri":
            return iri_plan(req_id, cluster, queue, alloc, body)
        ok, bal_text, balance = allocation_balance(cluster, alloc)
        return {
            "status": "accepted" if ok else "rejected",
            "action": action,
            "transport": transport,
            "cluster": cluster,
            "queue": queue,
            "allocation": alloc,
            "allocation_balance": balance,
            "allocation_positive": ok,
            "note": "dry_run only; no job submitted",
            "sbank_evidence": bal_text[-2000:],
        }
    if action == "submit_smoke":
        if transport == "globus_compute":
            if dry_run:
                return globus_compute_plan(req_id, cluster, body)
            return globus_compute_submit_smoke(req_id, cluster, body)
        if transport == "iri":
            if dry_run:
                return iri_plan(req_id, cluster, queue, alloc, body)
            return iri_submit_smoke(req_id, cluster, queue, alloc, body)
        if dry_run:
            return plan_smoke(req_id, cluster, queue, alloc, body)
        return submit_smoke(req_id, cluster, queue, alloc, body)
    if action == "status":
        if transport == "globus_compute":
            return globus_compute_status(cluster, body)
        if transport == "iri":
            return iri_status_job(cluster, body)
        return status_job(cluster, body)
    if action == "fetch_output":
        if transport != "iri":
            raise ValueError("fetch_output is currently implemented for transport='iri' only")
        return iri_fetch_output(body)
    raise ValueError(f"unhandled action {action}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="process current unprocessed requests then exit")
    ap.add_argument("--limit", type=int, default=500, help="tail N inbox lines")
    ap.add_argument("--dry-run", action="store_true", help="validate/plan only: no qsub side effects and no Sibline reply")
    ap.add_argument("--request-json", help="process one request body JSON directly, for local testing")
    args = ap.parse_args()

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    processed = load_processed()
    handled = 0

    if args.request_json:
        body = json.loads(args.request_json)
        req_id = str(body.get("request_id") or f"manual-{int(time.time())}")
        result = handle(req_id, {}, body, dry_run=args.dry_run)
        reply(req_id, result, dry_run=args.dry_run)
        append_job_record(req_id, {"request": body, "result": result, "processed_at": now_iso()})
        return 0

    for req_id, rec, body in iter_requests(args.limit):
        if req_id in processed:
            continue
        try:
            result = handle(req_id, rec, body, dry_run=args.dry_run)
        except Exception as e:
            result = {"status": "rejected", "error": str(e), "request_body": body}
        reply(req_id, result, dry_run=args.dry_run)
        append_job_record(req_id, {"record": rec, "request": body, "result": result, "processed_at": now_iso()})
        processed.add(req_id)
        handled += 1
    save_processed(processed)
    print(f"hpc-broker handled={handled} processed_total={len(processed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
