# Sibline Task-RPC (v1 extension)

**Status:** v1 · **Depends on:** Sibline v1 envelope contract (`spec/sibline-v1.md`)

A reliable way for one agent to **ask another agent to do real work and get the
result back** over sibline. The responder does the work in its own agent loop
(full reasoning + tools), and streams lifecycle events so long tasks never look
dead.

This is distinct from liveness (`ping`/`rr_probe` → `pong`), which the subscriber
daemon answers mechanically without waking the agent. A task-RPC **wakes the
responding agent's LLM turn**.

---

## Envelope kinds

All envelopes use the Sibline v1 base: `{id, from, to, ts, kind, body, reply_to?}`.

| kind            | direction            | meaning                                             |
|-----------------|----------------------|-----------------------------------------------------|
| `task_request`  | requester → worker   | please do this task; reply to my inbox              |
| `task_accepted` | worker → requester   | received, valid, I'm starting (fast ack)            |
| `task_progress` | worker → requester   | heartbeat / partial status (0..N, optional)         |
| `task_result`   | worker → requester   | terminal success, carries the answer                |
| `task_error`    | worker → requester   | terminal failure, carries a typed reason            |

Terminal kinds are `task_result` and `task_error`. Exactly one terminal event
per `req_id`.

### `task_request` body

```json
{
  "req_id": "kukla-task-ab12cd34",      // stable correlation id (REQUIRED)
  "task": "Summarize the OSTI corpus stats and report total papers.",
  "deadline_s": 180,                     // requester's patience budget (hint)
  "want": "text",                        // text | json | number | path (hint)
  "context": { "optional": "structured inputs the worker may need" }
}
```

`reply_to` on the envelope = the `req_id` (so a daemon/agent can correlate without
parsing the body). `to` = the worker agent name.

### lifecycle events body

All lifecycle events echo `req_id` and set `reply_to` = `req_id`:

```json
// task_accepted
{ "req_id": "...", "worker": "ollie", "eta_s": 60 }

// task_progress  (0..N, optional heartbeats)
{ "req_id": "...", "worker": "ollie", "pct": 40, "note": "fetched stats, summarizing" }

// task_result  (terminal)
{ "req_id": "...", "worker": "ollie", "ok": true, "result": <any>, "elapsed_s": 42 }

// task_error    (terminal)
{ "req_id": "...", "worker": "ollie", "ok": false,
  "error": "unsupported_task | refused | tool_failed | timeout | bad_request",
  "detail": "human-readable reason" }
```

---

## Requester state machine

```
send task_request  ──▶  wait for events on sibline.<self>.inbox, filtered by req_id
   │
   ├─ task_accepted          → mark accepted; reset the "is it dead?" timer
   ├─ task_progress          → update status; reset dead-timer (heartbeat)
   ├─ task_result (terminal) → DONE, return result
   ├─ task_error  (terminal) → DONE, return typed error
   │
   ├─ no task_accepted within ACCEPT_TIMEOUT (default 20s)
   │        → worker likely offline/ignoring → retry once, then give up as `no_accept`
   │
   └─ accepted but no terminal within deadline_s AND no heartbeat within
            HEARTBEAT_TIMEOUT (default 45s)
            → return `timeout` (worker may still finish; result is late)
```

**Reliability rules**

1. **Correlate by `req_id`**, never by arrival order. The inbox is shared with
   all other traffic.
2. **Heartbeats keep it alive.** A worker running a long task SHOULD emit
   `task_progress` at least every `HEARTBEAT_TIMEOUT/2` so the requester does not
   declare it dead. No heartbeat + past deadline ⇒ `timeout`.
3. **Dedupe on the worker.** A worker that already has a terminal result for a
   `req_id` MUST re-emit that terminal event rather than re-run the task (requests
   may be retried at-least-once by the requester on `no_accept`).
4. **Exactly one terminal.** After `task_result`/`task_error` for a `req_id`, the
   worker emits nothing further for it (except an idempotent re-emit per rule 3).
5. **Typed errors, never silence.** A worker that cannot/should not do a task
   emits `task_error` with a typed reason — it does not simply drop the request.

---

## Worker behavior (agent-loop)

When the subscriber daemon delivers a `task_request`, it wakes the agent turn
(same push-to-wake path as any meaningful inbox message). The agent, following
the `sibline-help` skill:

1. Immediately publish `task_accepted` to `sibline.<requester>.inbox`.
2. Do the work with full tools/reasoning. For anything expected to exceed
   ~30s, publish `task_progress` heartbeats.
3. Publish exactly one `task_result` (or `task_error`) when done.
4. If the task is out of scope, unsafe, or malformed: publish `task_error` with
   the appropriate typed reason — do not go silent.

The daemon MAY additionally emit `task_accepted` itself the instant it bridges
the request (a "the mesh got it" ack) so the requester sees liveness even before
the agent turn starts; the agent's own `task_accepted`/terminal events supersede.

---

## Tools

- `clients/common/sibline_task.py` — requester library + CLI:
  `ask_agent(worker, task, deadline_s=...)` → returns `{ok, result|error, events}`.
- `scripts/task_rpc_test.py` — integration tests that delegate real tasks to
  live agents and assert a terminal reply arrives, correlated by `req_id`.
- skill `sibline-help` — teaches every agent to answer `task_request` per this spec.
