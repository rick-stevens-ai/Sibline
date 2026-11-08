#!/usr/bin/env python3
"""nuntio: shared async fleet notice board over sibline/NATS JetStream (Dropbox-independent).
Any family agent posts FYIs others might want; everyone browses on their own schedule. Broadcast, not p2p.

Usage:
  nuntio post "text..." [--tag topic]              # --from is ignored; sender = authenticated agent
  nuntio read [N] [--tag topic] [--from agent]     # last N by timestamp (default 20)
  nuntio tail [N] [--tag topic] [--from agent]     # last N as raw JSON lines (for automation)
  nuntio topics                                    # tags + counts (scans recent window)

Backing: durable JetStream stream 'sibline-nuntio' (subjects sibline.nuntio.<tag>),
broker nats://100.86.220.115:4222. No Dropbox. 90-day / 50k-msg retention.
"""
import asyncio, json, time, uuid, os, sys, socket, argparse, re, hashlib
from datetime import datetime, timedelta, timezone
import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

def _broker():
    override=os.environ.get("NUNTIO_BROKER")
    if override:
        return override
    h=socket.gethostname().split(".")[0].lower()
    # Studio reaches the broker through its durable local mesh forward.
    if h == "ricksmacstudio":
        return "nats://127.0.0.1:14222"
    return "nats://100.86.220.115:4222"
STREAM="sibline-nuntio"
SUBJECT_PREFIX="sibline.nuntio"
TAG_RE=re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
def _pw():
    import os as _o
    paths=[]
    ef=_o.environ.get("SIBLINE_CREDS_FILE")
    if ef: paths.append(_o.path.expanduser(ef))
    paths += ["/tmp/.nats_creds", _o.path.expanduser("~/.config/sibline/cred"),
              _o.path.expanduser("~/.config/sibling-nats/cred"),
              _o.path.expanduser("~/.hermes/secrets/nats.env")]
    for p in paths:
        if _o.path.exists(p):
            txt=open(p).read()
            for line in txt.splitlines():
                if "PASS" in line and "=" in line:
                    return line.split("=",1)[1].strip().strip('"').strip("'")
            if txt.strip():
                return txt.strip().splitlines()[0].strip()
    return None

def _user():
    # user = agent id; broker ACL allows the known family users. default by hostname mapping.
    h=socket.gethostname().split(".")[0].lower()
    m={"mini":"kukla","macmini":"kukla","cherryrd":"ollie","promaxgb10-95fe":"tsisdu",
       "spark-36ac":"ikto","promaxgb10-9611":"yeil","nuc13":"paradise","nuc7":"lost",
       "prokko":"prokko","ricksmacstudio":"notdoneyet","uicgpu01":"livewire",
       "monera":"nevermind"}
    return os.environ.get("NUNTIO_USER") or m.get(h,"kukla")
def who(): return _user()

def valid_tag(tag):
    tag=(tag or "general").lower()
    if not TAG_RE.fullmatch(tag):
        raise ValueError("tag must match ^[a-z0-9][a-z0-9_-]{0,31}$")
    return tag

async def _conn():
    pw=_pw()
    nc=await asyncio.wait_for(nats.connect(_broker(), user=_user(), password=pw), timeout=10)
    js=nc.jetstream()
    # Publish/read clients are deliberately not stream administrators.
    # The owner-provisioned stream must already exist.
    await js.stream_info(STREAM)
    return nc, js

async def do_post(a):
    nc,js=await _conn()
    tag=valid_tag(a.tag)
    rec={"id":str(uuid.uuid4())[:8],"ts":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
         "from":who(),"tag":tag,"text":a.text}
    wire=json.dumps(rec,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
    # Exclude generated id/timestamp so an immediate retry of the same notice
    # deduplicates inside the stream's configured duplicate window.
    identity=json.dumps({"from":rec["from"],"tag":tag,"text":a.text},
        sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
    msg_id=hashlib.sha256(identity).hexdigest()
    ack=await js.publish(f"{SUBJECT_PREFIX}.{tag}", wire, headers={"Nats-Msg-Id":msg_id})
    print(json.dumps({"posted":rec["id"],"tag":tag,"from":rec["from"],"seq":ack.seq}))
    await nc.close()

async def _fetch_window(a):
    """Pull the notices in the --since window. Returns a list of records in STREAM order.
    Fetches the whole window (capped) so timestamp sorting + tail-trim are correct; the stream
    is retention-bounded (50k/90d) so this is safe. Raises on connection/stream error."""
    nc,js=await _conn()
    out=[]
    try:
        subject=f"{SUBJECT_PREFIX}.{valid_tag(a.tag)}" if a.tag else f"{SUBJECT_PREFIX}.>"
        start=datetime.now(timezone.utc)-timedelta(seconds=a.since)
        psub=await js.pull_subscribe(subject, durable=None,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_TIME,
                                  opt_start_time=start))
        # Hard cap to protect memory, but large enough that default N never truncates silently.
        hard_cap=5000
        while len(out)<hard_cap:
            try:
                msgs=await psub.fetch(100, timeout=1)
            except Exception:
                break
            if not msgs: break
            for m in msgs:
                try: out.append(json.loads(m.data.decode()))
                except Exception: pass
                await m.ack()
            if len(msgs)<100: break
    finally:
        await nc.close()
    if a.frm: out=[r for r in out if r.get("from")==a.frm]
    return out

def _trim(out, n):
    """Return the last N by TIMESTAMP (ascending). n<=0 -> empty."""
    srt=sorted(out, key=lambda r: r.get("ts",""))
    if n is None or n <= 0: return [] if (n is not None and n<=0) else srt
    return srt[-n:]

async def do_read(a):
    try:
        out=await _fetch_window(a)
    except Exception as e:
        print(f"(read error: {e})"); return
    recs=_trim(out, a.n)
    if not recs: print("(no notices)"); return
    for r in recs:
        print(f"{r.get('ts')}  {r.get('from','?'):<12} [{r.get('tag','?')}]  {r.get('text','')}")

async def do_tail(a):
    # Raw JSON, one record per line, time-sorted — safe to pipe into json.loads.
    try:
        out=await _fetch_window(a)
    except Exception as e:
        print(json.dumps({"error":str(e)})); return
    for r in _trim(out, a.n):
        print(json.dumps(r, ensure_ascii=False))

async def do_topics(a):
    from collections import Counter
    try:
        out=await _fetch_window(a)
    except Exception as e:
        print(f"(topics error: {e})"); return
    c=Counter(r.get("tag","?") for r in out)
    if not c: print("(no notices)"); return
    for tag,cnt in c.most_common():
        print(f"{cnt:>5}  {tag}")

def main():
    ap=argparse.ArgumentParser(prog="nuntio")
    s=ap.add_subparsers(dest="cmd")
    p=s.add_parser("post"); p.add_argument("text"); p.add_argument("--tag")
    r=s.add_parser("read"); r.add_argument("n",nargs="?",type=int,default=20); r.add_argument("--tag"); r.add_argument("--from",dest="frm"); r.add_argument("--since",type=int,default=86400,help="lookback seconds (default 86400)")
    t=s.add_parser("tail"); t.add_argument("n",nargs="?",type=int,default=20); t.add_argument("--tag"); t.add_argument("--from",dest="frm"); t.add_argument("--since",type=int,default=86400)
    tp=s.add_parser("topics"); tp.add_argument("--since",type=int,default=86400); tp.add_argument("--tag",default=None); tp.add_argument("--from",dest="frm",default=None)
    a=ap.parse_args()
    if not hasattr(a,"frm"): a.frm=None
    if not hasattr(a,"tag"): a.tag=None
    try:
        if a.tag is not None: valid_tag(a.tag)   # validate early, clean message
    except ValueError as e:
        print(f"nuntio: {e}", file=sys.stderr); sys.exit(2)
    try:
        if a.cmd=="post": asyncio.run(do_post(a))
        elif a.cmd=="read": asyncio.run(do_read(a))
        elif a.cmd=="tail": asyncio.run(do_tail(a))
        elif a.cmd=="topics": asyncio.run(do_topics(a))
        else: ap.print_help()
    except ValueError as e:
        print(f"nuntio: {e}", file=sys.stderr); sys.exit(2)
if __name__=="__main__": main()
