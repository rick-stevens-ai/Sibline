#!/usr/bin/env python3
"""Idempotent patcher: add Sibline reliable-wake Phase 1 to a sibline_subscriber.py.

Phase 1 adds a `wake` message kind that, on receipt, emits a `wake.received` receipt back to the
sender's reply_subject carrying the local wake-hook HTTP status. This gives the SENDER delivery +
hook-status proof without any gateway change. (Phase 2 will add a true `wake.ran` turn-start receipt.)

Changes (all additive, guarded so re-runs are no-ops):
  1. fire_push_to_wake -> returns the hook HTTP status (int) or None, instead of None-always.
  2. In on_msg dispatch, before the generic mailbox bridge, handle kind=="wake":
       - fire the hook (reusing fire_push_to_wake), capture status
       - publish {kind:"wake.received", ...} to the sender's reply_subject (+ our outbox audit)
  3. WAKE_KINDS constant.

Usage:  python patch_wake_phase1.py /path/to/sibline_subscriber.py
Backs up to <file>.bak-wake-<ts> before writing. Prints APPLIED / ALREADY / FAILED.
"""
import sys, os, time, re

MARK = "# --- sibwake-phase1 ---"

def apply(path):
    src = open(path).read()
    if MARK in src:
        print("ALREADY: wake phase1 present in", path); return 0
    orig = src

    # 1) make fire_push_to_wake return the HTTP status.
    #    replace the two `return` lines inside the success/HTTPError paths with status returns.
    #    success path: after `log(... push-to-wake fired ... HTTP {r.status} ...)` there is a bare
    #    `return`. Both subscriber variants exist ({WAKE_URL} vs {WEBHOOK_URL}, with/without the
    #    "after N tries" continuation), so match with regex and turn the bare return into `return r.status`.
    import re as _re
    # success: a log line mentioning 'push-to-wake fired' then (optional continuation) then bare 'return'
    src, n1 = _re.subn(
        r'(log\(f"push-to-wake fired[^\n]*\n(?:[^\n]*\n)?)(\s*)return\n',
        r'\1\2return r.status  ' + MARK + '\n', src, count=1)
    # HTTPError final path: log mentioning 'push-to-wake HTTP {e.code}' then bare return
    src, n2 = _re.subn(
        r'(log\(f"push-to-wake HTTP \{e\.code\}[^\n]*\n)(\s*)return\n',
        r'\1\2return e.code  ' + MARK + '\n', src, count=1)
    # generic exception path: log mentioning 'push-to-wake failed' then bare return
    src, n3 = _re.subn(
        r'(log\(f"push-to-wake failed[^\n]*\n)(\s*)return\n',
        r'\1\2return None  ' + MARK + '\n', src, count=1)
    if not (n1 and n2 and n3):
        print(f"FAILED: fire_push_to_wake return-patch incomplete (success={n1} httperr={n2} exc={n3}) in", path)
        return 2

    # 2) add WAKE_KINDS constant next to TASK_KINDS/NOISE_KINDS
    if "WAKE_KINDS" not in src:
        src = src.replace(
            'NOISE_KINDS = {',
            'WAKE_KINDS = {"wake"}  %s\nNOISE_KINDS = {' % MARK, 1)

    # 3) insert the wake handler just before the final generic bridge_to_mailbox call in on_msg.
    #    We hook on the TASK_KINDS block's trailing structure: insert right after that block's `return`
    #    by anchoring on the comment that precedes the generic bridge.
    # Unique anchor: the GENERIC bridge line + the comment that immediately follows it.
    # Two known subscriber variants (spark-trio vs nuc-scripts) use different trailing comments.
    anchor_variants = [
        ("            bridge_to_mailbox(ts, msg.subject, body, source)\n"
         "\n"
         "            # Push-to-wake for other meaningful notes (not noise), if enabled.\n"),
        ("            bridge_to_mailbox(ts, msg.subject, body, source)\n"
         "\n"
         "            # PUSH-TO-WAKE: wake the agent on meaningful notes.\n"),
    ]
    anchor = next((a for a in anchor_variants if a in src), None)
    handler_prefix = (
        '            %s\n'
        '            if isinstance(env, dict) and env.get("kind") in WAKE_KINDS:\n'
        '                _wurl = globals().get("WAKE_URL") or globals().get("WEBHOOK_URL") or ""\n'
        '                status = await fire_push_to_wake(env, source) if _wurl else None\n'
        '                reply_subject = env.get("reply_subject") or (\n'
        '                    f"sibline.{str(env.get(\'from\')).strip().lower()}.inbox"\n'
        '                    if env.get("from") else None)\n'
        '                if reply_subject:\n'
        '                    receipt = {\n'
        '                        "id": f"{AGENT}-wakeack-{uuid.uuid4().hex[:12]}",\n'
        '                        "from": AGENT, "to": env.get("from"), "ts": ts,\n'
        '                        "reply_to": env.get("id"), "kind": "wake.received",\n'
        '                        "body": {"req_id": env.get("id"), "agent": AGENT,\n'
        '                                 "hook_status": status,\n'
        '                                 "woke": bool(status and 200 <= int(status) < 300),\n'
        '                                 "wake_url_configured": bool(_wurl)},\n'
        '                    }\n'
        '                    rdata = json.dumps(receipt, separators=(",", ":")).encode()\n'
        '                    try:\n'
        '                        await nc.publish(reply_subject, rdata)\n'
        '                        await nc.publish(f"sibline.{AGENT}.outbox", rdata)\n'
        '                        log(f"wake.received -> {reply_subject} status={status} req_id={env.get(\'id\')}")\n'
        '                    except Exception as e:\n'
        '                        log(f"wake.received publish failed: {e}")\n'
        '                return\n'
        % MARK)
    if anchor is None:
        print("FAILED: no known generic-bridge anchor found in", path); return 2
    handler = handler_prefix + anchor
    # replace only the FIRST occurrence (the generic bridge at end of on_msg)
    src = src.replace(anchor, handler, 1)

    if src == orig or "env.get(\"kind\") in WAKE_KINDS" not in src:
        print("FAILED: handler block did not insert in", path); return 2

    bak = path + ".bak-wake-" + time.strftime("%Y%m%dT%H%M%SZ")
    open(bak, "w").write(orig)
    open(path, "w").write(src)
    # syntax check
    import py_compile
    try:
        py_compile.compile(path, doraise=True)
    except Exception as e:
        open(path, "w").write(orig)  # roll back
        print("FAILED: syntax error, rolled back:", e); return 3
    print("APPLIED:", path, "(backup", bak + ")"); return 0

if __name__ == "__main__":
    sys.exit(apply(sys.argv[1]))
