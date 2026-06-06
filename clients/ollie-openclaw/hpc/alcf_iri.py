#!/usr/bin/env python3
"""Small ALCF IRI Facility API client for Ollie/Kukla HPC broker.

Docs: https://docs.alcf.anl.gov/services/iri-api/
OpenAPI: https://api.alcf.anl.gov/openapi.json

Auth:
  - Prefer env ALCF_IRI_TOKEN for noninteractive automation.
  - Else use Globus SDK public app credentials from ALCF's token helper.
  - Run `scripts/alcf_iri.py authenticate` once interactively to create ~/.globus tokens.

This module intentionally does not print access tokens.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

BASE_URL = "https://api.alcf.anl.gov"
AUTH_CLIENT_ID = "REMOVED_AUTH_CLIENT_ID"
APP_NAME = "alcf_facility_api_app"
SCOPE_CLIENT_ID = "6be511f6-a071-471f-9bc0-02a0d0836723"
SCOPE_STRING = f"https://auth.globus.org/scopes/{SCOPE_CLIENT_ID}/filesystem"
ALCF_IDP_POLICY = "a128e981-c9a5-417a-97ab-8571c9831bff"
TOKENS_PATH = Path.home() / ".globus" / "app" / AUTH_CLIENT_ID / APP_NAME / "tokens.json"

# Live public resource IDs seen 2026-06-06. Refreshed by resource_map() when online.
RESOURCE_FALLBACK = {
    "polaris": "55c1c993-1124-47f9-b823-514ba3849a9a",
    "aurora": "0325fc07-6fb7-4453-b772-3d5030b2df72",
    "crux": "8b9b42f7-572a-4909-8472-a0453436304c",
    "home": "6115bd2c-957a-4543-abff-5fae52992ff2",
    "eagle": "1c3ad9d4-2e91-42bc-becb-72b1fde1235c",
}
# Docs currently say compute submission supports Polaris and Crux. Aurora is visible
# in resource status, but keep real submit gated until an authenticated probe proves it.
IRI_COMPUTE_SUBMIT_SUPPORTED = {"polaris", "crux"}

SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _globus_app(force: bool = False):
    import globus_sdk
    from globus_sdk.login_flows import LocalServerLoginFlowManager  # noqa: F401 - ensures gare availability in some SDK versions

    ga_params = globus_sdk.gare.GlobusAuthorizationParameters(
        session_required_policies=[ALCF_IDP_POLICY]
    )

    class DomainBasedErrorHandler:
        def __call__(self, app, error):
            print(f"Encountered Globus auth error {error!r}; initiating ALCF login...", file=sys.stderr)
            app.login(auth_params=ga_params)

    app = globus_sdk.UserApp(
        APP_NAME,
        client_id=AUTH_CLIENT_ID,
        scope_requirements={SCOPE_CLIENT_ID: [SCOPE_STRING]},
        config=globus_sdk.GlobusAppConfig(
            request_refresh_tokens=True,
            token_validation_error_handler=DomainBasedErrorHandler(),
        ),
    )
    if force:
        app.login(auth_params=ga_params)
    return app


def authenticate() -> dict[str, Any]:
    _globus_app(force=True).get_authorizer(SCOPE_CLIENT_ID)
    return {"status": "authenticated", "tokens_path": str(TOKENS_PATH)}


def token_status() -> dict[str, Any]:
    if not TOKENS_PATH.exists():
        return {
            "status": "missing",
            "tokens_path": str(TOKENS_PATH),
            "next": "Run scripts/alcf_iri.py authenticate once interactively.",
        }
    try:
        app = _globus_app(force=False)
        auth = app.get_authorizer(SCOPE_CLIENT_ID)
        auth.ensure_valid_token()
        expires_at = getattr(auth, "expires_at", None)
        return {
            "status": "valid" if getattr(auth, "access_token", None) else "unknown",
            "tokens_path": str(TOKENS_PATH),
            "expires_at": expires_at,
            "seconds_remaining": round(expires_at - time.time(), 2) if expires_at else None,
        }
    except Exception as e:
        return {"status": "error", "tokens_path": str(TOKENS_PATH), "error": str(e)}


def get_access_token() -> str:
    env = os.environ.get("ALCF_IRI_TOKEN")
    if env:
        return env.strip()
    if not TOKENS_PATH.exists():
        raise RuntimeError(
            f"ALCF IRI token missing at {TOKENS_PATH}. Run scripts/alcf_iri.py authenticate once."
        )
    app = _globus_app(force=False)
    auth = app.get_authorizer(SCOPE_CLIENT_ID)
    auth.ensure_valid_token()
    token = getattr(auth, "access_token", None)
    if not token:
        raise RuntimeError("Globus authorizer did not return an access token")
    return token


def request(method: str, path: str, *, token: str | None = None, json_body: Any = None,
            query: dict[str, Any] | None = None, timeout: int = 60) -> Any:
    url = BASE_URL + path
    if query:
        clean = {k: v for k, v in query.items() if v is not None}
        if clean:
            url += "?" + urllib.parse.urlencode(clean, doseq=True)
    data = None
    headers = {"Accept": "application/json", "User-Agent": "ollie-hpc-broker/0.1"}
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return None
            ctype = resp.headers.get("content-type", "")
            if "json" in ctype:
                return json.loads(raw.decode("utf-8"))
            return raw.decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            detail = json.loads(raw)
        except Exception:
            detail = raw
        raise RuntimeError(f"IRI HTTP {e.code} for {method} {path}: {detail}") from e


def list_resources(resource_type: str | None = None) -> list[dict[str, Any]]:
    query = {"limit": 1000}
    if resource_type:
        query["resource_type"] = resource_type
    return request("GET", "/api/v1/status/resources", query=query)


def resource_map(resource_type: str | None = None) -> dict[str, str]:
    try:
        resources = list_resources(resource_type)
        return {str(r.get("name", "")).lower(): r["id"] for r in resources if r.get("name") and r.get("id")}
    except Exception:
        if resource_type == "compute":
            return {k: v for k, v in RESOURCE_FALLBACK.items() if k in {"polaris", "aurora", "crux"}}
        if resource_type == "storage":
            return {k: v for k, v in RESOURCE_FALLBACK.items() if k in {"home", "eagle"}}
        return dict(RESOURCE_FALLBACK)


def compute_resource_id(cluster: str) -> str:
    key = cluster.lower()
    return resource_map("compute").get(key) or RESOURCE_FALLBACK.get(key) or cluster


def safe_job_name(prefix: str, request_id: str, limit: int = 32) -> str:
    return (prefix + SAFE_NAME_RE.sub("-", request_id)).strip("-")[:limit]


def build_smoke_spec(cluster: str, request_id: str, allocation: str, queue: str = "debug",
                     duration: int = 300, directory: str = "/home/stevens") -> dict[str, Any]:
    cluster = cluster.lower()
    rid = SAFE_NAME_RE.sub("-", request_id)[:96]
    outdir = f"/home/stevens/iri-hpc-broker"
    cmd = [
        "set -euo pipefail",
        "mkdir -p /home/stevens/iri-hpc-broker",
        "echo IRI_BROKER_SMOKE_OK=1",
        f"echo CLUSTER={cluster}",
        f"echo REQUEST_ID={request_id}",
        "echo HOST=$(hostname)",
        "echo USER=$(whoami)",
        "date -u '+UTC=%Y-%m-%dT%H:%M:%SZ'",
    ]
    if cluster == "polaris":
        cmd += ["command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L || true"]
    elif cluster == "aurora":
        cmd += ["command -v xpu-smi >/dev/null 2>&1 && xpu-smi discovery -j || true"]
    shell = "; ".join(cmd)
    return {
        "executable": "/bin/bash",
        "arguments": ["-lc", shell],
        "directory": directory,
        "name": safe_job_name("iri-", rid),
        "inherit_environment": True,
        "stdout_path": f"{outdir}/{rid}.out",
        "stderr_path": f"{outdir}/{rid}.err",
        "resources": {
            "node_count": 1,
            "exclusive_node_use": True,
        },
        "attributes": {
            "duration": int(duration),
            "queue_name": queue,
            "account": allocation,
            # Scheduler-specific room for PBS extensions. Harmless if ignored.
            "custom_attributes": {"filesystems": "home"},
        },
    }


def plan_smoke(cluster: str, request_id: str, allocation: str, queue: str = "debug",
               duration: int = 300) -> dict[str, Any]:
    key = cluster.lower()
    spec = build_smoke_spec(key, request_id, allocation, queue, duration)
    supported = key in IRI_COMPUTE_SUBMIT_SUPPORTED
    return {
        "status": "planned" if supported else "unsupported",
        "transport": "iri",
        "cluster": key,
        "resource_id": compute_resource_id(key),
        "iri_submit_supported_by_docs": supported,
        "note": (
            "IRI compute submit is documented for this cluster."
            if supported else
            "Resource is visible, but ALCF docs currently list IRI compute submit support only for Polaris and Crux."
        ),
        "job_spec": spec,
    }


def submit_smoke(cluster: str, request_id: str, allocation: str, queue: str = "debug",
                 duration: int = 300) -> dict[str, Any]:
    key = cluster.lower()
    if key not in IRI_COMPUTE_SUBMIT_SUPPORTED:
        raise RuntimeError(
            f"IRI submit for {cluster!r} is not enabled; docs currently list Polaris and Crux only."
        )
    token = get_access_token()
    resource_id = compute_resource_id(key)
    spec = build_smoke_spec(key, request_id, allocation, queue, duration)
    job = request("POST", f"/api/v1/compute/job/{urllib.parse.quote(resource_id)}", token=token, json_body=spec)
    return {
        "status": "submitted",
        "transport": "iri",
        "cluster": key,
        "resource_id": resource_id,
        "job": job,
        "job_id": job.get("id") if isinstance(job, dict) else None,
        "job_spec": spec,
    }


def job_status(cluster: str, job_id: str, include_spec: bool = False) -> dict[str, Any]:
    token = get_access_token()
    resource_id = compute_resource_id(cluster.lower())
    path = f"/api/v1/compute/status/{urllib.parse.quote(resource_id)}/{urllib.parse.quote(job_id)}"
    query = {"historical": "true"}
    # The OpenAPI spec exposes include_spec, but the live ALCF endpoint returned
    # HTTP 501 "'include_spec' not supported yet" on 2026-06-06. Only send it
    # when explicitly requested, and retry without it if the deployment rejects it.
    if include_spec:
        query["include_spec"] = "true"
    try:
        job = request("GET", path, token=token, query=query)
    except RuntimeError as e:
        if include_spec and "include_spec" in str(e) and "501" in str(e):
            job = request("GET", path, token=token, query={"historical": "true"})
        else:
            raise
    return {"status": "ok", "transport": "iri", "cluster": cluster.lower(), "resource_id": resource_id, "job": job}


def task_status(task_id: str) -> dict[str, Any]:
    token = get_access_token()
    return request("GET", f"/api/v1/task/{urllib.parse.quote(task_id)}", token=token)


def view_file(path: str, storage: str = "home", size: int = 5242880, offset: int = 0,
              wait: bool = True, timeout_seconds: int = 60) -> dict[str, Any]:
    """View a small file through the IRI filesystem API.

    Filesystem operations are asynchronous. When wait=True, poll the returned
    task until completion/failure or timeout.
    """
    token = get_access_token()
    storage_id = resource_map("storage").get(storage.lower()) or RESOURCE_FALLBACK.get(storage.lower()) or storage
    submitted = request(
        "GET",
        f"/api/v1/filesystem/view/{urllib.parse.quote(storage_id)}",
        token=token,
        query={"path": path, "size": int(size), "offset": int(offset)},
    )
    out = {"status": "submitted", "transport": "iri", "storage": storage.lower(), "resource_id": storage_id, "task": submitted}
    if not wait:
        return out
    task_id = submitted.get("task_id") if isinstance(submitted, dict) else None
    if not task_id:
        return out
    deadline = time.time() + timeout_seconds
    while True:
        task = task_status(task_id)
        out["task"] = task
        state = task.get("status") if isinstance(task, dict) else None
        if state in {"completed", "failed", "canceled"}:
            out["status"] = state
            return out
        if time.time() >= deadline:
            out["status"] = "timeout"
            return out
        time.sleep(2)


def main() -> int:
    ap = argparse.ArgumentParser(description="ALCF IRI Facility API helper")
    ap.add_argument("action", choices=["authenticate", "token-status", "resources", "plan-smoke", "submit-smoke", "status", "view"])
    ap.add_argument("--cluster", default="polaris")
    ap.add_argument("--request-id", default=f"iri-smoke-{int(time.time())}")
    ap.add_argument("--allocation", default="IMPROVE_Aim1")
    ap.add_argument("--queue", default="debug")
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--job-id")
    ap.add_argument("--include-spec", action="store_true", help="request job spec when live API supports it")
    ap.add_argument("--path", help="remote file path for view")
    ap.add_argument("--storage", default="home", help="storage resource name/id for view")
    args = ap.parse_args()

    if args.action == "authenticate":
        out = authenticate()
    elif args.action == "token-status":
        out = token_status()
    elif args.action == "resources":
        out = {"compute": list_resources("compute"), "storage": list_resources("storage")}
    elif args.action == "plan-smoke":
        out = plan_smoke(args.cluster, args.request_id, args.allocation, args.queue, args.duration)
    elif args.action == "submit-smoke":
        out = submit_smoke(args.cluster, args.request_id, args.allocation, args.queue, args.duration)
    elif args.action == "status":
        if not args.job_id:
            raise SystemExit("--job-id required for status")
        out = job_status(args.cluster, args.job_id, include_spec=args.include_spec)
    elif args.action == "view":
        if not args.path:
            raise SystemExit("--path required for view")
        out = view_file(args.path, storage=args.storage)
    else:
        raise SystemExit(f"unhandled action {args.action}")
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
