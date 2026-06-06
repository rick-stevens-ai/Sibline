# Globus Compute HPC broker path

Status: implemented for `nuc13`, `crux`, and the PBS-backed `polaris`/`aurora` endpoints.

## Purpose

Add Globus Compute as a third controlled mechanism behind Sibline's HPC broker:

- `transport: "ssh"` for login-node/PBS control and build/debug workflows.
- `transport: "iri"` for ALCF API-backed job submit/status/output retrieval.
- `transport: "globus_compute"` for remote Python/function execution on allowlisted endpoints.

This path is intentionally not a raw shell, arbitrary Python upload, or credential
sharing mechanism. The peer can request only broker-defined actions.

## Initial endpoint

| Cluster label | Endpoint name | Endpoint ID | Host |
|---|---|---|---|
| `nuc13` | `nuc13` | `4cf42bb1-0415-427a-b30c-c4660af2a33b` | `stevens-NUC13RNGi9` |
| `polaris` | `polaris` | `b624baaa-d390-4b7b-b878-1a1c5afc7f2f` | `polaris-login-04` / PBS workers (`4× A100`) |
| `aurora` | `aurora` | `5f931d91-04e9-4b19-ad59-c3923f3a1460` | `aurora-uan-0009` / PBS workers (Intel Max GPUs) |
| `crux` | `crux` | `1a30477c-7c30-421e-b688-5f36c8a86cbe` | `crux-uan-0001` LocalProvider |

Nuc13's endpoint runs from:

```text
/home/stevens/.globus_compute_venv/bin/globus-compute-endpoint start nuc13
```

SDK/endpoint versions verified 2026-06-06:

- `globus-compute-sdk`: 4.9.0
- `globus-compute-endpoint`: 4.9.0
- `globus-sdk`: 4.5.0

## Request examples

Dry run:

```json
{
  "request_id": "gc-plan-nuc13-001",
  "action": "dry_run",
  "transport": "globus_compute",
  "cluster": "nuc13"
}
```

Endpoint status:

```json
{
  "request_id": "gc-status-nuc13-001",
  "action": "status",
  "transport": "globus_compute",
  "cluster": "nuc13"
}
```

Smoke function submit:

```json
{
  "request_id": "gc-smoke-nuc13-001",
  "action": "submit_smoke",
  "transport": "globus_compute",
  "cluster": "nuc13",
  "timeout_seconds": 60
}
```

Response body includes `status`, `transport`, `cluster`, `endpoint_id`, optional
`task_id`, and the built-in smoke function result.

## Safety constraints

- Endpoint IDs are allowlisted in the broker.
- No arbitrary function source is accepted from Sibline.
- No arbitrary shell command is accepted from Sibline.
- Smoke/status logic is broker-defined and auditable.
- Requests/results are logged under `~/.openclaw/workspace/memory/hpc-broker/jobs/`.

## Validation

A direct SDK smoke on 2026-06-06 returned:

```text
SUBMIT_TEST_OK
host: stevens-NUC13RNGi9
user: stevens
python: 3.13.13
```


## Polaris notes

- Endpoint: `ollie-polaris` (`b624baaa-d390-4b7b-b878-1a1c5afc7f2f`).
- Runs from `/lus/eagle/projects/IMPROVE_Aim1/stevens/globus-compute-polaris`.
- Uses Cray Python 3.11.7 and Globus Compute 4.9.0.
- Worker config requests PBS `debug`, allocation `IMPROVE_Aim1`, `select=1:ncpus=64:ngpus=4`, `filesystems=home:eagle`, `walltime=00:10:00`.
- Verified smoke on 2026-06-06: task `2b2a0bbf-b4d8-4f52-ad85-b189083f9fbe` ran on `x3001c0s13b0n0` and saw four `NVIDIA A100-SXM4-40GB` GPUs.
- Client-side Python should match endpoint Python 3.11 when submitting arbitrary serialized functions; Python 3.13 clients can hit dill/serialization failures.


## Aurora notes

- Endpoint: `ollie-aurora` (`5f931d91-04e9-4b19-ad59-c3923f3a1460`).
- Runs from `/lus/flare/projects/AuroraGPT/stevens/globus-compute-aurora`.
- Manager config dir moved to `/lus/flare/projects/AuroraGPT/stevens/globus-compute-aurora/gc-config`.
- User endpoint runtime dir set via `GLOBUS_COMPUTE_USER_DIR=/lus/flare/projects/AuroraGPT/stevens/globus-compute-aurora/gc-user` to avoid Aurora home quota failures.
- Uses frameworks Python `3.12.12` and Globus Compute 4.9.0.
- Worker config requests PBS `debug`, allocation `AuroraGPT`, `select=1:ncpus=208:ngpus=1`, `filesystems=home:flare`, `walltime=00:10:00`.
- Verified smoke on 2026-06-06: ran on Aurora node `x4219c2s3b0n0`, Python `3.12.12`, and saw `/dev/dri` cards/render devices for Intel GPUs.


## Crux notes

- Endpoint: `ollie-crux` (`1a30477c-7c30-421e-b688-5f36c8a86cbe`).
- Runs from `/lus/eagle/projects/IMPROVE_Aim1/stevens/globus-compute-crux`.
- Uses Cray Python `3.11.7` and Globus Compute 4.9.0.
- LocalProvider endpoint on `crux-uan-0001` (`max_workers_per_node: 2`).
- Requires an interactive SSH ControlMaster to `crux` because the host offers keyboard-interactive/hostbased auth, not normal batch public-key auth.
- Verified smoke on 2026-06-06: task ran on `crux-uan-0001`, Python `3.11.7`.
- Tailscale is not installed; noninteractive `sudo -n true` fails, so Tailscale installation requires an interactive sudo/admin path or a site-supported install method.
