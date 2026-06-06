# IRI HPC Broker Path — Kukla → Sibline → Ollie/CherryRd → ALCF IRI

Status: implemented for planning; real IRI submit needs one-time Globus auth on CherryRd.

## Purpose

Use the ALCF Facility / IRI API for jobs that do not need an active SSH login session or build steps on a login node. This complements the existing SSH/PBS broker path.

- Existing SSH transport: `transport: "ssh"` — uses CherryRd SSH ControlMaster sockets to Polaris/Aurora.
- New IRI transport: `transport: "iri"` — uses `https://api.alcf.anl.gov` + Globus token; no SSH login required once authenticated.

## Current IRI support from ALCF docs

Docs page: https://docs.alcf.anl.gov/services/iri-api/
OpenAPI: https://api.alcf.anl.gov/openapi.json

The docs say compute submission is currently supported for:

- Polaris
- Crux

The public status API also exposes Aurora as an `up` compute resource, but the docs do **not** list Aurora as IRI compute-submit supported yet. The broker therefore returns `status: "unsupported"` for `transport:"iri", cluster:"aurora"` until an authenticated probe or docs update says otherwise.

Public resource IDs observed 2026-06-06:

- Polaris: `55c1c993-1124-47f9-b823-514ba3849a9a`
- Crux: `8b9b42f7-572a-4909-8472-a0453436304c`
- Aurora: `0325fc07-6fb7-4453-b772-3d5030b2df72`
- Home: `6115bd2c-957a-4543-abff-5fae52992ff2`
- Eagle: `1c3ad9d4-2e91-42bc-becb-72b1fde1235c`

## Auth

Helper: `~/.openclaw/workspace/scripts/alcf_iri.py`

One-time interactive auth on CherryRd:

```bash
~/.openclaw/workspace/scripts/alcf_iri.py authenticate
```

This uses the ALCF public Globus app:

- client id: `REMOVED_AUTH_CLIENT_ID`
- scope client id: `6be511f6-a071-471f-9bc0-02a0d0836723`
- scope: `https://auth.globus.org/scopes/6be511f6-a071-471f-9bc0-02a0d0836723/filesystem`
- ALCF IdP policy: `a128e981-c9a5-417a-97ab-8571c9831bff`

Token cache:

```text
~/.globus/app/REMOVED_AUTH_CLIENT_ID/alcf_facility_api_app/tokens.json
```

Check status:

```bash
~/.openclaw/workspace/scripts/alcf_iri.py token-status
```

As of 2026-06-06 12:40 CDT, token is missing on CherryRd, so real IRI submit is blocked until Rick authenticates once.

## Kukla request format

Publish to `sibline.ollie.inbox`:

```json
{
  "id": "kukla-iri-plan-polaris-001",
  "from": "kukla",
  "to": "ollie",
  "ts": "2026-06-06T17:40:00Z",
  "kind": "hpc.request",
  "body": {
    "request_id": "kukla-iri-plan-polaris-001",
    "action": "dry_run",
    "transport": "iri",
    "cluster": "polaris",
    "allocation": "IMPROVE_Aim1",
    "queue": "debug",
    "walltime": "00:05:00"
  }
}
```

For real submission after auth:

```json
{
  "request_id": "kukla-iri-smoke-polaris-001",
  "action": "submit_smoke",
  "transport": "iri",
  "cluster": "polaris",
  "allocation": "IMPROVE_Aim1",
  "queue": "debug",
  "walltime": "00:05:00"
}
```

Response is sent to `sibline.kukla.inbox` as `kind: "hpc.response"`.

## Implemented files

- `scripts/alcf_iri.py` — ALCF IRI client/helper.
- `scripts/hpc-broker.py` — accepts `transport:"iri"` and routes to `alcf_iri`.
- Broker state/audit records: `memory/hpc-broker/jobs/*.json`.

## Validation completed 2026-06-06

- `alcf_iri.py token-status` works and reports missing token.
- Public resource discovery works without auth.
- Broker IRI planning works for Polaris and Crux.
- Broker correctly gates Aurora IRI as unsupported by docs.
- End-to-end Sibline planning test from Kukla/M1 succeeded:
  - `kukla-iri-plan-polaris-1780767611` → `planned`, IRI supported.
  - `kukla-iri-plan-crux-1780767611` → `planned`, IRI supported.
  - `kukla-iri-plan-aurora-1780767611` → `unsupported`, resource visible but IRI submit not documented.

## Notes

The IRI JobSpec uses the PSIJ-ish schema from OpenAPI:

```json
{
  "executable": "/bin/bash",
  "arguments": ["-lc", "..."],
  "directory": "/home/stevens",
  "name": "iri-...",
  "inherit_environment": true,
  "stdout_path": "/home/stevens/iri-hpc-broker/<request>.out",
  "stderr_path": "/home/stevens/iri-hpc-broker/<request>.err",
  "resources": {"node_count": 1, "exclusive_node_use": true},
  "attributes": {
    "duration": 300,
    "queue_name": "debug",
    "account": "IMPROVE_Aim1",
    "custom_attributes": {"filesystems": "home"}
  }
}
```

## Real IRI smoke validation — 2026-06-06 12:43 CDT

Rick completed Globus auth. Token status became valid at:

```text
~/.globus/app/REMOVED_AUTH_CLIENT_ID/alcf_facility_api_app/tokens.json
```

Real end-to-end Sibline → broker → IRI submit succeeded:

- Request: `kukla-iri-smoke-polaris-1780767827`
- Transport: `iri`
- Cluster: `polaris`
- Resource: `55c1c993-1124-47f9-b823-514ba3849a9a`
- Job id: `7186680.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov`
- IRI status: `completed`, `exit_code: 0`

Output fetched through IRI filesystem API, no SSH:

```text
IRI_BROKER_SMOKE_OK=1
CLUSTER=polaris
REQUEST_ID=kukla-iri-smoke-polaris-1780767827
HOST=x3006c0s13b0n0
USER=stevens
UTC=2026-06-06T17:43:57Z
GPU 0: NVIDIA A100-SXM4-40GB
GPU 1: NVIDIA A100-SXM4-40GB
GPU 2: NVIDIA A100-SXM4-40GB
GPU 3: NVIDIA A100-SXM4-40GB
```

### Live API quirk

The OpenAPI spec advertises `include_spec` for job status, but live ALCF returned HTTP 501: `'include_spec' not supported yet.` `scripts/alcf_iri.py` now omits it by default and retries without it if explicitly requested and rejected.

### Added broker action

`fetch_output` is implemented for `transport:"iri"` with paths restricted to `/home/stevens/iri-hpc-broker/`:

```json
{
  "request_id": "iri-fetch-output-001",
  "action": "fetch_output",
  "transport": "iri",
  "cluster": "polaris",
  "path": "/home/stevens/iri-hpc-broker/kukla-iri-smoke-polaris-1780767827.out"
}
```
