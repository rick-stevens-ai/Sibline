# Ollie HPC broker over Sibline

This directory contains Ollie-side broker helpers that let a peer agent (Kukla)
request small, controlled HPC operations over Sibline without getting SSH keys,
ALCF credentials, or direct shell access.

## Files

- `hpc_broker.py` — consumes `kind=hpc.request` envelopes from Ollie\x27s local
  Sibline mailbox and replies with `kind=hpc.response`.
- `alcf_iri.py` — small ALCF Facility / IRI API client used by the broker for
  API-backed submission/status/output retrieval.

## Transports

### `transport: "ssh"`

Uses CherryRd\x27s existing SSH configuration and ControlMaster sockets to stage
and submit PBS scripts. Current broker scope is intentionally narrow:

- `dry_run`
- `submit_smoke`
- `status`

This is the right path for jobs that need login-node build/staging behavior or
for resources not yet supported by the IRI API.

### `transport: "iri"`

Uses the ALCF Facility / IRI API at `https://api.alcf.anl.gov` with a Globus
access token cached on the Ollie host. Once authenticated, this path does not
need a live SSH login session.

Current validated scope:

- `dry_run`
- `submit_smoke`
- `status`
- `fetch_output`

ALCF docs currently list compute submission support for Polaris and Crux. Aurora
is visible in resource status but gated as unsupported for IRI submit until ALCF
docs or a live authenticated probe says otherwise.

### `transport: "globus_compute"`

Uses an allowlisted Globus Compute endpoint as a remote Python/function execution
fabric. This is not a general shell path and does not transfer credentials to the
peer. Current allowlist:

- `cluster: "nuc13"` → endpoint `4cf42bb1-0415-427a-b30c-c4660af2a33b`, name `nuc13`

Current validated scope:

- `dry_run` — return endpoint plan/metadata
- `status` — query endpoint status/metadata via the SDK
- `submit_smoke` — submit a tiny built-in Python function and return its result

This is the right path for reusable Python functions/workflows on known endpoints,
not for software builds or arbitrary shell execution.

## Request envelope

Publish to `sibline.ollie.inbox`:

```json
{
  "id": "kukla-iri-smoke-polaris-001",
  "from": "kukla",
  "to": "ollie",
  "ts": "2026-06-06T17:43:00Z",
  "kind": "hpc.request",
  "body": {
    "request_id": "kukla-iri-smoke-polaris-001",
    "action": "submit_smoke",
    "transport": "iri",
    "cluster": "polaris",
    "allocation": "IMPROVE_Aim1",
    "queue": "debug",
    "walltime": "00:05:00"
  }
}
```

The broker replies to `sibline.kukla.inbox` with `kind=hpc.response` and a JSON
body containing `status`, `transport`, `cluster`, and any job/task identifiers.

## Safety shape

- No arbitrary shell from the peer.
- No raw PBS script execution in the current default path.
- Allocations, queues, clusters, endpoints, and paths are allowlisted.
- IRI output fetch is restricted to `/home/stevens/iri-hpc-broker/`.
- Globus Compute requests are restricted to built-in broker functions on known endpoint IDs.
- Every request/result is written to `~/.openclaw/workspace/memory/hpc-broker/jobs/`.

## Auth

For IRI, run once on the Ollie host:

```bash
~/.openclaw/workspace/scripts/alcf_iri.py authenticate
```

Then check:

```bash
~/.openclaw/workspace/scripts/alcf_iri.py token-status
```

Access tokens are short-lived; the Globus SDK refreshes while the refresh token
remains valid.
