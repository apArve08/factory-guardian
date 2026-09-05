"""
Factory Guardian - operator console (Stage 8).

One page:
  - left  : live line telemetry, drawn from the simulator's own state (cosmetic;
            the agent still investigates the real data in Grafana Cloud)
  - right : chat with the investigation agent, with an inline Approve/Reject
            button when the agent proposes remediation

Run:
  source .venv/bin/activate && set -a && source .env && set +a
  PATH=/opt/homebrew/bin:$PATH uvicorn app:app --port 8080
Then open http://localhost:8080

Needs the simulator on :8000 and GRAFANA_* / GOOGLE_API_KEY in the env.
"""

import asyncio
import collections
import json
import os
import time
import uuid

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

load_dotenv()

from google.adk.runners import InMemoryRunner  # noqa: E402
from google.genai import types  # noqa: E402

from guardian.agent import root_agent  # noqa: E402

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://mintrabbit2277.grafana.net")
GRAFANA_TOKEN = os.environ.get("GRAFANA_SERVICE_ACCOUNT_TOKEN", "")
DASHBOARD_LINK = GRAFANA_URL + "/d/factory-guardian/"
SIMULATOR_URL = os.environ.get("SIMULATOR_URL", "http://localhost:8000")
_CONFIRM = "adk_request_confirmation"

app = FastAPI(title="Factory Guardian Console")
runner = InMemoryRunner(agent=root_agent, app_name="factory-guardian-console")

# session_id -> Future that /approve resolves with a bool
_pending: dict[str, asyncio.Future] = {}

# rolling telemetry buffer, filled by a background poll of the simulator
_HISTORY: collections.deque = collections.deque(maxlen=180)  # ~6 min at 2s
_poll_started = False


async def _poll_simulator():
    async with httpx.AsyncClient(timeout=4) as c:
        while True:
            try:
                d = (await c.get(f"{SIMULATOR_URL}/state")).json()
                _HISTORY.append({
                    "t": time.time(),
                    "incident": d.get("incident", False),
                    **d.get("readings", {}),
                })
            except Exception:
                pass
            await asyncio.sleep(2)


def _ensure_poll():
    global _poll_started
    if not _poll_started:
        _poll_started = True
        asyncio.create_task(_poll_simulator())


def _sse(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


async def _agent_stream(session_id: str, prompt: str):
    try:
        # Get-or-create, so follow-ups ("now fix it") keep the context of the
        # investigation that just ran instead of starting from scratch.
        # /reset drops the session when you want a clean slate.
        session = await runner.session_service.get_session(
            app_name="factory-guardian-console", user_id=session_id,
            session_id=session_id,
        )
        if session is None:
            session = await runner.session_service.create_session(
                app_name="factory-guardian-console", user_id=session_id,
                session_id=session_id,
            )
        msg = types.Content(role="user", parts=[types.Part(text=prompt)])

        while msg is not None:
            next_msg = None
            async for event in runner.run_async(
                user_id=session_id, session_id=session.id, new_message=msg
            ):
                if not (event.content and event.content.parts):
                    continue
                lr = getattr(event, "long_running_tool_ids", None) or []
                for p in event.content.parts:
                    fc, fr = p.function_call, p.function_response
                    if p.text:
                        yield _sse({"type": "text", "text": p.text})
                    elif fc and fc.name == _CONFIRM and fc.id in lr:
                        orig = (fc.args or {}).get("originalFunctionCall", {})
                        yield _sse({
                            "type": "approval",
                            "tool": orig.get("name", "tool"),
                            "args": orig.get("args", {}),
                        })
                        fut = asyncio.get_running_loop().create_future()
                        _pending[session_id] = fut
                        try:
                            approved = await asyncio.wait_for(fut, timeout=300)
                        except asyncio.TimeoutError:
                            approved = False
                        _pending.pop(session_id, None)
                        yield _sse({"type": "approval_result",
                                    "approved": approved})
                        next_msg = types.Content(
                            role="user",
                            parts=[types.Part(
                                function_response=types.FunctionResponse(
                                    id=fc.id, name=_CONFIRM,
                                    response={"confirmed": approved},
                                ))],
                        )
                    elif fc and fc.name == "submit_findings":
                        yield _sse({"type": "findings", **dict(fc.args or {})})
                    elif fc and fc.name == "submit_report":
                        yield _sse({"type": "report", **dict(fc.args or {})})
                    elif fc:
                        yield _sse({"type": "tool_call", "name": fc.name,
                                    "args": dict(fc.args or {})})
                    elif fr and fr.name not in (_CONFIRM, "submit_report",
                                                "submit_findings"):
                        yield _sse({"type": "tool_result", "name": fr.name,
                                    "text": str(fr.response)[:400]})
            msg = next_msg
        yield _sse({"type": "done"})
    except asyncio.CancelledError:
        raise
    except BaseException as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        yield _sse({"type": "error", "text": f"{type(e).__name__}: {str(e)[:600]}"})
        yield _sse({"type": "done"})


@app.post("/chat")
async def chat(req: Request):
    body = await req.json()
    sid = body.get("session_id") or uuid.uuid4().hex
    prompt = body.get("message", "")
    return StreamingResponse(
        _agent_stream(sid, prompt), media_type="text/event-stream",
        headers={"X-Session-Id": sid, "Cache-Control": "no-cache"},
    )


@app.post("/approve")
async def approve(req: Request):
    body = await req.json()
    sid, approved = body.get("session_id"), bool(body.get("approved"))
    fut = _pending.get(sid)
    if fut and not fut.done():
        fut.set_result(approved)
        return {"ok": True}
    return {"ok": False, "reason": "no pending approval"}


@app.post("/incident")
async def incident():
    _ensure_poll()
    async with httpx.AsyncClient(timeout=6) as c:
        r = await c.post(f"{SIMULATOR_URL}/incident")
    return r.json()


@app.post("/remediate")
async def remediate_ep():
    async with httpx.AsyncClient(timeout=6) as c:
        r = await c.post(f"{SIMULATOR_URL}/remediate")
    return r.json()


@app.post("/reset")
async def reset(req: Request):
    """Drop the conversation so the next question starts with a clean context."""
    sid = (await req.json()).get("session_id")
    try:
        await runner.session_service.delete_session(
            app_name="factory-guardian-console", user_id=sid, session_id=sid)
    except Exception:
        pass
    return {"ok": True}


@app.get("/metrics")
async def metrics():
    _ensure_poll()
    return JSONResponse(list(_HISTORY))


@app.get("/logs")
async def logs():
    try:
        async with httpx.AsyncClient(timeout=4) as c:
            return JSONResponse((await c.get(f"{SIMULATOR_URL}/logs")).json())
    except Exception:
        return JSONResponse({"logs": []})


@app.get("/alert-status")
async def alert_status():
    """The REAL Grafana alert state (not derived from local thresholds)."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(
                f"{GRAFANA_URL}/api/prometheus/grafana/api/v1/rules",
                headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"},
            )
        rules = [
            {"name": rule.get("name"), "state": rule.get("state"),
             "health": rule.get("health")}
            for g in r.json().get("data", {}).get("groups", [])
            for rule in g.get("rules", [])
        ]
        return JSONResponse({"rules": rules})
    except Exception as e:
        return JSONResponse({"rules": [], "error": str(e)[:200]})


@app.get("/", response_class=HTMLResponse)
async def index():
    _ensure_poll()
    return INDEX_HTML.replace("__DASHLINK__", DASHBOARD_LINK)


INDEX_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Factory Guardian</title>
<style>
  :root {
    color-scheme: dark;
    --bg:#0a0c11; --panel:#12151d; --panel2:#161a24; --line:#232935;
    --ink:#e8eaed; --dim:#8b93a1; --faint:#5b6270;
    --ok:#4ec98f; --warn:#f0b93b; --crit:#f16a6a; --accent:#5b9bff;
    --ok-bg:rgba(78,201,143,.10); --warn-bg:rgba(240,185,59,.12); --crit-bg:rgba(241,106,106,.13);
  }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 -apple-system,BlinkMacSystemFont,system-ui,"Segoe UI",sans-serif;
         background:var(--bg); color:var(--ink); height:100vh; display:flex; overflow:hidden; }

  /* ---- left: telemetry ------------------------------------------------ */
  #left { flex:1.5; display:flex; flex-direction:column; border-right:1px solid var(--line); min-width:0; }
  .bar { display:flex; align-items:center; gap:14px; padding:12px 18px;
         background:var(--panel); border-bottom:1px solid var(--line); }
  .bar h1 { font-size:14px; font-weight:650; margin:0; letter-spacing:.02em; }
  .bar h1 span { color:var(--faint); font-weight:400; }
  .pill { font-size:11px; font-weight:700; letter-spacing:.06em; text-transform:uppercase;
          padding:4px 10px; border-radius:999px; }
  .pill.ok { color:var(--ok); background:var(--ok-bg); }
  .pill.warn { color:var(--warn); background:var(--warn-bg); }
  .pill.crit { color:var(--crit); background:var(--crit-bg); }
  .pill.dim { color:var(--faint); background:var(--panel2); }
  .degrade { flex:1; height:6px; border-radius:3px; background:var(--line); overflow:hidden; max-width:220px; }
  .degrade > i { display:block; height:100%; width:0; background:linear-gradient(90deg,var(--warn),var(--crit)); transition:width .5s; }
  .bar .spacer { flex:1; }
  .bar button, .bar a { padding:6px 12px; border:1px solid var(--line); border-radius:7px;
         background:var(--panel2); color:var(--ink); cursor:pointer; font-size:12px;
         font-weight:550; text-decoration:none; white-space:nowrap; }
  .bar button:hover, .bar a:hover { border-color:var(--faint); }
  .bar button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }

  #scroll { flex:1; overflow-y:auto; padding:16px 18px 20px; }
  .section { font-size:11px; font-weight:700; letter-spacing:.09em; text-transform:uppercase;
             color:var(--faint); margin:18px 2px 9px; }
  .section:first-child { margin-top:2px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(210px,1fr)); gap:11px; }
  #flow { background:var(--panel); border:1px solid var(--line); border-radius:11px; padding:6px 10px; }
  #flow svg { display:block; width:100%; height:auto; }
  #flow .stname { font:600 12px system-ui; fill:var(--ink); }
  #flow .stval  { font:700 15px system-ui; font-variant-numeric:tabular-nums; }
  #flow .stunit { font:500 10px system-ui; fill:var(--faint); }
  #flow .stsub  { font:500 9.5px system-ui; fill:var(--faint); letter-spacing:.06em; }
  .tsrc { float:right; text-transform:none; letter-spacing:0; font-weight:500;
          color:var(--faint); font-family:ui-monospace,Menlo,monospace; font-size:10px; }
  /* ---- trace waterfall ---- */
  #trace { background:var(--panel); border:1px solid var(--line); border-radius:11px; padding:11px 14px; }
  .span { display:grid; grid-template-columns:170px 1fr 74px; align-items:center; gap:10px; margin:5px 0; }
  .span .sn { font:500 11.5px ui-monospace,Menlo,monospace; color:var(--dim);
              overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .span .sn.root { color:var(--ink); font-weight:600; }
  .span .track { height:15px; background:var(--bg); border-radius:4px; position:relative; overflow:hidden; }
  .span .track i { position:absolute; top:0; bottom:0; border-radius:4px; transition:left .4s, width .4s, background .4s; }
  .span .sd { font:600 11.5px system-ui; text-align:right; font-variant-numeric:tabular-nums; color:var(--dim); }
  .span.dom .sd { color:var(--crit); }
  .span.dom .sn { color:var(--ink); }
  /* ---- recovery strip ---- */
  #recovery { background:var(--panel); border:1px solid rgba(78,201,143,.35); border-radius:11px;
              padding:11px 14px; margin-top:11px; }
  #recovery .rvh { font-size:11px; font-weight:700; letter-spacing:.09em; text-transform:uppercase;
              color:var(--ok); margin-bottom:8px; }
  #recovery .rv { display:flex; justify-content:space-between; align-items:baseline;
              font-size:12.5px; padding:3px 0; border-bottom:1px solid var(--line); }
  #recovery .rv:last-child { border-bottom:0; }
  #recovery .rv b { font-weight:500; color:var(--dim); }
  #recovery .was { color:var(--crit); font-variant-numeric:tabular-nums; }
  #recovery .now { color:var(--ok); font-weight:650; font-variant-numeric:tabular-nums; }
  #recovery .arw { color:var(--faint); margin:0 7px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:11px;
          padding:12px 13px 8px; position:relative; overflow:hidden; }
  .card::before { content:""; position:absolute; left:0; top:0; bottom:0; width:3px; background:var(--faint); opacity:.5; }
  .card.ok::before { background:var(--ok); } .card.warn::before { background:var(--warn); } .card.crit::before { background:var(--crit); }
  .card .lbl { font-size:11px; color:var(--dim); font-weight:550; }
  .card .num { font-size:25px; font-weight:660; margin:1px 0 1px; font-variant-numeric:tabular-nums; letter-spacing:-.01em; }
  .card .num u { font-size:12px; font-weight:500; color:var(--faint); text-decoration:none; margin-left:3px; }
  .card.ok .num { color:var(--ok); } .card.warn .num { color:var(--warn); } .card.crit .num { color:var(--crit); }
  .card .sub { font-size:11px; color:var(--faint); }
  .card svg { display:block; width:100%; height:34px; margin-top:5px; }

  /* ---- logs --------------------------------------------------------- */
  #logs { border-top:1px solid var(--line); background:var(--panel); height:190px; display:flex; flex-direction:column; }
  #logs .h { font-size:11px; font-weight:700; letter-spacing:.09em; text-transform:uppercase;
             color:var(--faint); padding:8px 18px; border-bottom:1px solid var(--line);
             display:flex; justify-content:space-between; }
  #logs .h b { color:var(--dim); font-weight:600; }
  #logstream { flex:1; overflow-y:auto; padding:6px 0; font:12px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace; }
  .lg { padding:1px 18px; white-space:pre-wrap; word-break:break-word; }
  .lg time { color:var(--faint); margin-right:9px; }
  .lg.INFO { color:var(--dim); }
  .lg.WARNING { color:var(--warn); }
  .lg.ERROR { color:var(--crit); background:var(--crit-bg); }

  /* ---- right: agent ------------------------------------------------- */
  #right { flex:1; display:flex; flex-direction:column; min-width:400px; background:var(--bg); }
  #right .bar { border-bottom:1px solid var(--line); }
  /* ---- pinned findings panel ---- */
  .findings { border-bottom:1px solid var(--line); background:var(--panel); }
  .findings.empty { opacity:.55; }
  .findings .fh { display:flex; justify-content:space-between; align-items:center;
        padding:9px 18px; font-size:11px; font-weight:700; letter-spacing:.09em;
        text-transform:uppercase; color:var(--faint); }
  .fconf { font-size:10px; letter-spacing:.05em; padding:2px 8px; border-radius:999px;
        background:var(--panel2); color:var(--faint); }
  .fconf.high { background:var(--ok-bg); color:var(--ok); }
  .fconf.medium { background:var(--warn-bg); color:var(--warn); }
  .fconf.low { background:var(--crit-bg); color:var(--crit); }
  .findings .fb { padding:0 18px 12px; }
  .findings .flabel { font-size:10px; text-transform:uppercase; letter-spacing:.08em;
        color:var(--faint); font-weight:700; margin:6px 0 3px; }
  .findings .frc { font-size:13px; line-height:1.5; }
  .findings ul { margin:3px 0 0; padding-left:17px; font-size:12px; color:var(--dim); line-height:1.55; }
  #ffix { margin-top:10px; padding:7px 14px; border:1px solid rgba(240,185,59,.45);
        border-radius:7px; background:var(--warn-bg); color:var(--warn);
        font-weight:650; font-size:12px; cursor:pointer; }
  #ffix:hover { filter:brightness(1.15); }
  #log { flex:1; overflow-y:auto; padding:16px 18px; }
  .msg { margin-bottom:13px; }
  .msg .who { font-size:10px; text-transform:uppercase; letter-spacing:.08em; color:var(--faint); margin-bottom:3px; }
  .msg.agent .body { white-space:pre-wrap; }
  .tool { font:12px/1.5 ui-monospace,Menlo,monospace; color:var(--accent);
          background:var(--panel); border:1px solid var(--line); border-radius:7px;
          padding:6px 9px; margin-bottom:7px; }
  .tool .r { color:var(--faint); display:block; margin-top:3px; white-space:pre-wrap; word-break:break-all; }
  .approval { background:var(--panel); border:1px solid rgba(240,185,59,.35);
              border-radius:12px; margin-bottom:14px; overflow:hidden;
              box-shadow:0 0 0 1px rgba(240,185,59,.06), 0 6px 20px rgba(0,0,0,.25); }
  .approval .ah { display:flex; align-items:center; gap:9px; padding:11px 15px;
              background:var(--warn-bg); border-bottom:1px solid rgba(240,185,59,.25); }
  .approval .aicon { width:22px; height:22px; border-radius:50%; background:var(--warn);
              color:#241a05; display:flex; align-items:center; justify-content:center;
              font-weight:800; font-size:13px; flex-shrink:0; }
  .approval .atitle { font-weight:700; font-size:13px; color:var(--warn); letter-spacing:.01em; }
  .approval .atitle small { display:block; font-weight:500; color:var(--dim); font-size:11px; margin-top:1px; }
  .approval .abody { padding:13px 15px 14px; }
  .approval .action { font-size:14px; font-weight:650; margin-bottom:5px; }
  .approval .reason { font-size:12.5px; color:var(--dim); line-height:1.5; }
  .approval .af { display:flex; gap:9px; padding:0 15px 15px; }
  .approval button { flex:1; padding:9px 0; border:0; border-radius:8px; font-weight:650;
              font-size:13px; cursor:pointer; transition:filter .12s; }
  .approval button:hover { filter:brightness(1.08); }
  .approve { background:var(--ok); color:#08160f; } .reject { background:transparent; color:var(--dim); border:1px solid var(--line)!important; }
  .approval .decided { padding:11px 15px; font-size:12.5px; font-weight:650; display:flex; align-items:center; gap:7px; }
  .decided.yes { color:var(--ok); } .decided.no { color:var(--crit); }
  form { display:flex; gap:9px; padding:13px 18px; border-top:1px solid var(--line); background:var(--panel); }
  input[type=text] { flex:1; padding:10px 13px; border-radius:8px; border:1px solid var(--line); background:var(--bg); color:var(--ink); }
  input[type=text]:focus { outline:none; border-color:var(--accent); }
  button.send { padding:10px 18px; border:0; border-radius:8px; background:var(--accent); color:#fff; font-weight:650; cursor:pointer; }
  button.send.stop { background:var(--crit); }
  .think { color:var(--faint); font-style:italic; }
  .think::after { content:''; animation:dots 1.2s steps(4,end) infinite; }
  @keyframes dots { 0%{content:''} 25%{content:'.'} 50%{content:'..'} 75%{content:'...'} }

  /* ---- investigation report card ---- */
  .report { background:var(--panel); border:1px solid rgba(78,201,143,.35); border-radius:12px;
            margin-bottom:14px; overflow:hidden; box-shadow:0 6px 20px rgba(0,0,0,.25); }
  .report .rh { display:flex; align-items:center; gap:9px; padding:11px 15px;
            background:var(--ok-bg); border-bottom:1px solid rgba(78,201,143,.25); }
  .report .ricon { width:22px; height:22px; border-radius:50%; background:var(--ok); color:#08160f;
            display:flex; align-items:center; justify-content:center; font-weight:800; font-size:13px; }
  .report .rtitle { font-weight:700; font-size:13px; color:var(--ok); }
  .report .rtitle small { display:block; font-weight:500; color:var(--dim); font-size:11px; margin-top:1px; }
  .report .rbody { padding:13px 15px 6px; }
  .report .rlabel { font-size:10px; text-transform:uppercase; letter-spacing:.08em;
            color:var(--faint); font-weight:700; margin:10px 0 4px; }
  .report .rlabel:first-child { margin-top:0; }
  .report .rtext { font-size:13px; line-height:1.55; }
  .report ol { margin:4px 0 0; padding-left:18px; font-size:12.5px; color:var(--dim); line-height:1.6; }
  .report table { width:100%; border-collapse:collapse; font-size:12.5px; margin:4px 0 12px; }
  .report th { text-align:left; font-size:10px; text-transform:uppercase; letter-spacing:.06em;
            color:var(--faint); font-weight:700; padding:5px 8px; border-bottom:1px solid var(--line); }
  .report td { padding:6px 8px; border-bottom:1px solid var(--line); font-variant-numeric:tabular-nums; }
  .report td.b { color:var(--crit); } .report td.a { color:var(--ok); font-weight:600; }
  .report tr:last-child td { border-bottom:0; }
  code { background:var(--panel2); padding:1px 5px; border-radius:4px; font-size:12px; }
  ::-webkit-scrollbar { width:9px; height:9px; } ::-webkit-scrollbar-thumb { background:var(--line); border-radius:5px; }
</style></head><body>
<div id="left">
  <div class="bar">
    <h1>Production Line <span>/ LINE-01</span></h1>
    <span id="status" class="pill ok">Healthy</span>
    <div class="degrade" title="equipment degradation"><i id="degbar"></i></div>
    <span id="alertpill" class="pill dim" title="live state of the Grafana alert rule">Alert: —</span>
    <div class="spacer"></div>
    <button onclick="trigger()">Trigger incident</button>
    <button onclick="fix()">Remediate</button>
    <a href="__DASHLINK__" target="_blank">Grafana ↗</a>
  </div>
  <div id="scroll">
    <div class="section">Process flow</div>
    <div id="flow">
      <svg viewBox="0 0 900 150" preserveAspectRatio="xMidYMid meet"></svg>
    </div>
    <div class="section">Latest request trace <span class="tsrc">tempo · POST /api/production/status</span></div>
    <div id="trace"></div>
    <div id="recovery" style="display:none"></div>
    <div class="section">Equipment</div>
    <div class="grid" id="g-equip"></div>
    <div class="section">Production line</div>
    <div class="grid" id="g-line"></div>
    <div class="section">Application &amp; database</div>
    <div class="grid" id="g-app"></div>
  </div>
  <div id="logs">
    <div class="h"><span>Line logs</span><b id="logsrc">production-control-system</b></div>
    <div id="logstream"></div>
  </div>
</div>
<div id="right">
  <div class="bar">
    <h1>Factory Guardian <span>/ investigation agent</span></h1>
    <div class="spacer"></div>
    <button onclick="resetSession()" title="clear the conversation context">Reset</button>
    <button onclick="ask('The alert \\'API latency > 5s (LINE-01)\\' is firing. Investigate and report the root cause with evidence. Do NOT remediate.')">Investigate</button>
    <button class="primary" onclick="ask('The alert \\'API latency > 5s (LINE-01)\\' is firing. Investigate, propose remediation, verify recovery, then file the report.')">Investigate + Fix</button>
  </div>
  <div id="findings" class="findings empty">
    <div class="fh"><span>Findings</span><span id="fconf" class="fconf">awaiting investigation</span></div>
    <div class="fb">
      <div class="flabel">Root cause</div>
      <div class="frc" id="frc">—</div>
      <div class="flabel" id="fevlabel" style="display:none">Evidence</div>
      <ul id="fev"></ul>
      <button id="ffix" style="display:none" onclick="proposeFix()">Propose a fix →</button>
    </div>
  </div>
  <div id="log"><div id="hint" style="color:var(--faint);padding:8px 2px;font-size:13px">
    Trigger an incident on the left, then <b>Investigate alert</b> — the agent
    queries Grafana metrics, logs and traces, finds the root cause, proposes a
    fix for your approval, and verifies recovery.</div></div>
  <form onsubmit="event.preventDefault(); ask(this.q.value); this.q.value='';">
    <input type="text" name="q" placeholder="Ask the agent…" autocomplete="off">
    <button class="send">Send</button>
  </form>
</div>
<script>
// ---- live telemetry panels (cosmetic; fed by the simulator) ----------------
const METRICS = [
  {k:'equipment_motor_temperature_celsius', name:'Motor temperature', unit:'°C', grp:'equip', base:65, warn:85, crit:92},
  {k:'equipment_vibration_mm_per_second',   name:'Drive vibration',   unit:'mm/s', grp:'equip', base:0.2, warn:1.0, crit:1.5},
  {k:'production_processing_time_ms',       name:'Unit cycle time',   unit:'ms', grp:'line', base:120, warn:400, crit:800},
  {k:'production_output_units_per_hour',    name:'Throughput',        unit:'/hr', grp:'line', base:980, warn:780, crit:650, low:true},
  {k:'api_latency_milliseconds',            name:'API latency',       unit:'ms', grp:'app', base:200, warn:1000, crit:5000},
  {k:'api_error_rate_percent',              name:'API error rate',    unit:'%', grp:'app', base:0.2, warn:2, crit:5},
  {k:'database_connection_pool_percent',    name:'DB pool in use',    unit:'%', grp:'app', base:40, warn:80, crit:95},
];
const cards = {};
for(const m of METRICS){
  const el = document.createElement('div');
  el.className = 'card';
  el.innerHTML =
      '<div class="lbl">'+m.name+'</div>'
    + '<div class="num">–<u>'+m.unit+'</u></div>'
    + '<div class="sub">baseline '+m.base+'</div>'
    + '<svg viewBox="0 0 100 34" preserveAspectRatio="none">'
    +   '<defs><linearGradient id="f'+m.k+'" x1="0" x2="0" y1="0" y2="1">'
    +   '<stop offset="0" stop-color="currentColor" stop-opacity=".28"/>'
    +   '<stop offset="1" stop-color="currentColor" stop-opacity="0"/></linearGradient></defs>'
    +   '<path class="area" fill="url(#f'+m.k+')" stroke="none"/>'
    +   '<polyline class="line" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/>'
    +   '<line class="mark" y1="0" y2="34" stroke="#f16a6a" stroke-width="1"'
    +     ' stroke-dasharray="2 2" opacity=".75" style="display:none"/>'
    + '</svg>';
  document.getElementById('g-'+m.grp).appendChild(el);
  cards[m.k] = el;
}
function level(m, v){
  if(m.low) return v <= m.crit ? 'crit' : v <= m.warn ? 'warn' : 'ok';
  return v >= m.crit ? 'crit' : v >= m.warn ? 'warn' : 'ok';
}

// ---- interactive process-flow diagram ------------------------------------
const COLORS = {ok:'#4ec98f', warn:'#f0b93b', crit:'#f16a6a'};
const STATIONS = [
  {id:'motor', label:'Drive motor',  sub:'EQUIPMENT', k:'equipment_motor_temperature_celsius', unit:'°C'},
  {id:'proc',  label:'Processing',   sub:'LINE',      k:'production_processing_time_ms',       unit:'ms'},
  {id:'db',    label:'DB pool',      sub:'DATABASE',  k:'database_connection_pool_percent',    unit:'%'},
  {id:'api',   label:'API',          sub:'SERVICE',   k:'api_latency_milliseconds',            unit:'ms'},
  {id:'out',   label:'Output',       sub:'THROUGHPUT',k:'production_output_units_per_hour',    unit:'/hr'},
];
const flowSvg = document.querySelector('#flow svg');
(function buildFlow(){
  const W=900, boxW=140, boxH=64, y=44, gap=(W-STATIONS.length*boxW)/(STATIONS.length-1);
  let html='';
  STATIONS.forEach((s,i)=>{
    const x = i*(boxW+gap);
    if(i<STATIONS.length-1){
      const x1=x+boxW, x2=x+boxW+gap;
      html += '<line id="ln-'+s.id+'" x1="'+x1+'" y1="'+(y+boxH/2)+'" x2="'+x2+'" y2="'+(y+boxH/2)+'"'
            + ' stroke="#232935" stroke-width="2"/>'
            + '<circle id="dot-'+s.id+'" r="3" fill="#4ec98f">'
            + '<animate attributeName="cx" from="'+x1+'" to="'+x2+'" dur="1.6s" repeatCount="indefinite"/>'
            + '<animate attributeName="cy" from="'+(y+boxH/2)+'" to="'+(y+boxH/2)+'" dur="1.6s" repeatCount="indefinite"/>'
            + '</circle>';
    }
    html += '<g id="st-'+s.id+'">'
         +  '<rect x="'+x+'" y="'+y+'" width="'+boxW+'" height="'+boxH+'" rx="9"'
         +    ' fill="#161a24" stroke="#232935" stroke-width="1.5"/>'
         +  '<rect id="bar-'+s.id+'" x="'+x+'" y="'+y+'" width="3.5" height="'+boxH+'" rx="2" fill="#5b6270"/>'
         +  '<text class="stsub"  x="'+(x+14)+'" y="'+(y+16)+'">'+s.sub+'</text>'
         +  '<text class="stname" x="'+(x+14)+'" y="'+(y+33)+'">'+s.label+'</text>'
         +  '<text class="stval" id="v-'+s.id+'" x="'+(x+14)+'" y="'+(y+53)+'" fill="#8b93a1">–</text>'
         +  '<text class="stunit" id="u-'+s.id+'" x="'+(x+14)+'" y="'+(y+53)+'">'+s.unit+'</text>'
         + '</g>';
  });
  flowSvg.innerHTML = html;
})();
// Continuous 0..1 deviation from baseline toward the critical threshold, so
// colour tracks "how far from normal" rather than snapping between 3 buckets.
function deviation(m, v){
  const base = m.base, crit = m.crit;
  const d = (v - base) / ((crit - base) || 1);
  return Math.max(0, Math.min(1, d));
}
function devColor(d){
  // green -> amber -> red across the deviation range
  const stops = [[0,[78,201,143]], [0.5,[240,185,59]], [1,[241,106,106]]];
  let a = stops[0], b = stops[stops.length-1];
  for(let i=0;i<stops.length-1;i++)
    if(d >= stops[i][0] && d <= stops[i+1][0]){ a=stops[i]; b=stops[i+1]; break; }
  const t = (d - a[0]) / ((b[0]-a[0])||1);
  const c = a[1].map((v,i)=> Math.round(v + (b[1][i]-v)*t));
  return 'rgb('+c.join(',')+')';
}
function updateFlow(last){
  const devs = {};
  STATIONS.forEach((s,i)=>{
    const m = METRICS.find(x=>x.k===s.k);
    const v = last[s.k];
    const vt = document.getElementById('v-'+s.id);
    const ut = document.getElementById('u-'+s.id);
    if(typeof v!=='number' || !m) return;
    const d = deviation(m, v), col = devColor(d);
    devs[s.id] = d;
    vt.textContent = fmt(v); vt.setAttribute('fill', col);
    ut.setAttribute('x', 14 + i*(140+(900-STATIONS.length*140)/(STATIONS.length-1))
                        + vt.getComputedTextLength() + 4);
    const bar = document.getElementById('bar-'+s.id);
    bar.setAttribute('fill', col);
    bar.setAttribute('width', (3.5 + d*3).toFixed(1));
  });
  // Connectors carry the UPSTREAM stage's severity, so the cascade reads
  // left-to-right: motor goes red first, then processing, then DB, then API.
  STATIONS.slice(0,-1).forEach(s=>{
    const d = devs[s.id]; if(d===undefined) return;
    const col = devColor(d);
    const ln = document.getElementById('ln-'+s.id);
    if(ln){ ln.setAttribute('stroke', col); ln.setAttribute('stroke-width', (2+d*2).toFixed(1));
            ln.setAttribute('opacity', (0.35 + d*0.65).toFixed(2)); }
    const dot = document.getElementById('dot-'+s.id);
    if(dot){
      dot.setAttribute('fill', col);
      dot.setAttribute('r', (3 - d*0.8).toFixed(1));
      // flow slows as the stage degrades — visual backpressure
      dot.querySelectorAll('animate').forEach(a=>
        a.setAttribute('dur', (1.6 + d*4).toFixed(1)+'s'));
    }
  });
}

// ---- trace waterfall ------------------------------------------------------
// Mirrors exactly what telemetry.py emits to Tempo: a root request span with
// equipment.process_unit and db.query children.
const SPANS = [
  {name:'POST /api/production/status', root:true},
  {name:'  equipment.process_unit'},
  {name:'  db.query production_orders'},
];
function updateTrace(last){
  const total = last.api_latency_milliseconds;
  const proc  = last.production_processing_time_ms;
  if(typeof total!=='number' || typeof proc!=='number') return;
  const dbw = Math.max(5, total - proc - 20);
  const segs = [
    {i:0, off:0,        dur:total, label:SPANS[0].name, root:true},
    {i:1, off:10,       dur:proc,  label:SPANS[1].name},
    {i:2, off:10+proc,  dur:dbw,   label:SPANS[2].name},
  ];
  const rootDev = deviation(METRICS.find(m=>m.k==='api_latency_milliseconds'), total);
  // Only call out a "dominant" span once the request is actually slow —
  // at baseline nothing should look alarming.
  const dom = rootDev > 0.15 ? (segs[1].dur >= segs[2].dur ? 1 : 2) : -1;
  document.getElementById('trace').innerHTML = segs.map(s=>{
    const left = (s.off/total)*100, width = Math.max(0.6,(s.dur/total)*100);
    const col = s.root ? devColor(rootDev)
              : s.i===dom ? '#f16a6a' : '#5b9bff';
    return '<div class="span'+(s.i===dom?' dom':'')+'">'
      + '<div class="sn'+(s.root?' root':'')+'">'+esc(s.label)+'</div>'
      + '<div class="track"><i style="left:'+left.toFixed(1)+'%;width:'+width.toFixed(1)
      +   '%;background:'+col+'"></i></div>'
      + '<div class="sd">'+Math.round(s.dur).toLocaleString()+' ms</div></div>';
  }).join('');
}

// ---- recovery before/after -----------------------------------------------
const RECOVERY_KEYS = ['api_latency_milliseconds','api_error_rate_percent',
  'database_connection_pool_percent','equipment_motor_temperature_celsius',
  'production_output_units_per_hour'];
function updateRecovery(hist){
  const el = document.getElementById('recovery');
  const last = hist[hist.length-1];
  if(!last || last.incident){ el.style.display='none'; return; }
  const start = incidentStartIdx(hist);
  if(start < 0){ el.style.display='none'; return; }
  // peak (worst) value seen since the incident began
  const worst = {};
  for(let i=start;i<hist.length;i++)
    for(const k of RECOVERY_KEYS){
      const v = hist[i][k]; if(typeof v!=='number') continue;
      const m = METRICS.find(x=>x.k===k);
      if(!(k in worst)) worst[k]=v;
      else worst[k] = m && m.low ? Math.min(worst[k],v) : Math.max(worst[k],v);
    }
  const rows = RECOVERY_KEYS.filter(k=>k in worst && typeof last[k]==='number').map(k=>{
    const m = METRICS.find(x=>x.k===k);
    const [label,unit] = [m ? m.name : k, m ? m.unit : ''];
    return '<div class="rv"><b>'+esc(label)+'</b><span>'
      + '<span class="was">'+fmt(worst[k])+' '+unit+'</span>'
      + '<span class="arw">→</span>'
      + '<span class="now">'+fmt(last[k])+' '+unit+'</span></span></div>';
  }).join('');
  if(!rows){ el.style.display='none'; return; }
  el.innerHTML = '<div class="rvh">✓ Recovery verified — incident peak vs now</div>'+rows;
  el.style.display = '';
}
function draw(el, vals, markIdx){
  if(vals.length < 2) return;
  const lo = Math.min(...vals), hi = Math.max(...vals), span = (hi-lo)||1;
  const xy = vals.map((v,i)=>[ (i/(vals.length-1))*100, 32 - ((v-lo)/span)*30 ]);
  const line = xy.map(p=>p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
  el.querySelector('.line').setAttribute('points', line);
  el.querySelector('.area').setAttribute('d',
     'M0,34 L'+xy.map(p=>p[0].toFixed(1)+','+p[1].toFixed(1)).join(' L')+' L100,34 Z');
  // vertical marker at the tick where the incident began
  const mk = el.querySelector('.mark');
  if(markIdx >= 0 && vals.length > 1){
    const x = (markIdx/(vals.length-1))*100;
    mk.setAttribute('x1', x); mk.setAttribute('x2', x);
    mk.style.display = '';
  } else { mk.style.display = 'none'; }
}
// index of the tick where incident flipped false -> true (latest such flip)
function incidentStartIdx(hist){
  for(let i = hist.length-1; i > 0; i--)
    if(hist[i].incident && !hist[i-1].incident) return i;
  return (hist.length && hist[0].incident) ? 0 : -1;
}
function fmt(v){ return v>=100 ? Math.round(v).toLocaleString() : v.toFixed(v>=10?1:2); }
async function pollMetrics(){
  try{
    const hist = await (await fetch('/metrics')).json();
    const last = hist[hist.length-1] || {};
    const inc = !!last.incident;
    const startAbs = incidentStartIdx(hist);
    let worst = 'ok';
    for(const m of METRICS){
      const idx = [];
      const series = hist.map((h,i)=>{ if(typeof h[m.k]==='number'){ idx.push(i); return h[m.k]; } return null; })
                         .filter(v=>v!==null).slice(-90);
      const kept = idx.slice(-90);
      if(!series.length) continue;
      const cur = series[series.length-1], lv = level(m, cur);
      if(lv==='crit') worst='crit'; else if(lv==='warn'&&worst!=='crit') worst='warn';
      const el = cards[m.k];
      el.className = 'card ' + lv;
      const col = devColor(deviation(m, cur));
      const num = el.querySelector('.num');
      num.innerHTML = fmt(cur) + '<u>'+m.unit+'</u>';
      num.style.color = col;         // continuous deviation colour
      el.style.color = col;          // sparkline uses currentColor
      draw(el, series, kept.indexOf(startAbs));
    }
    updateFlow(last);
    updateTrace(last);
    updateRecovery(hist);
    const pill = document.getElementById('status');
    const st = worst==='crit' ? ['crit','Incident'] : worst==='warn'
             ? [inc?'warn':'warn', inc?'Degrading':'Recovering'] : ['ok','Healthy'];
    pill.className = 'pill ' + st[0]; pill.textContent = st[1];
    // degradation bar: infer from motor temp vs range
    const t = last.equipment_motor_temperature_celsius;
    if(typeof t==='number')
      document.getElementById('degbar').style.width = Math.max(0,Math.min(100,(t-65)/30*100))+'%';
  }catch(e){}
}
setInterval(pollMetrics, 2000); pollMetrics();

// ---- line logs ----------------------------------------------------------
let lastLogT = 0;
async function pollLogs(){
  try{
    const {logs} = await (await fetch('/logs')).json();
    const stream = document.getElementById('logstream');
    const atBottom = stream.scrollHeight - stream.scrollTop - stream.clientHeight < 40;
    for(const l of logs){
      if(l.t <= lastLogT) continue;
      lastLogT = l.t;
      const d = document.createElement('div');
      d.className = 'lg ' + l.level;
      const ts = new Date(l.t*1000).toTimeString().slice(0,8);
      d.innerHTML = '<time>'+ts+'</time>'+l.msg.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
      stream.appendChild(d);
    }
    while(stream.children.length > 200) stream.removeChild(stream.firstChild);
    if(atBottom) stream.scrollTop = stream.scrollHeight;
  }catch(e){}
}
setInterval(pollLogs, 2000); pollLogs();

// ---- real Grafana alert state (not derived) ------------------------------
async function pollAlert(){
  try{
    const {rules} = await (await fetch('/alert-status')).json();
    const r = rules.find(x=>x.name && x.name.includes('API latency')) || rules[0];
    const el = document.getElementById('alertpill');
    if(!r){ el.className='pill dim'; el.textContent='Alert: —'; return; }
    const map = {firing:['crit','Firing'], pending:['warn','Pending'], inactive:['ok','Normal'], normal:['ok','Normal']};
    const [cls,label] = map[r.state] || ['dim', r.state];
    el.className = 'pill ' + cls;
    el.textContent = 'Alert: ' + label;
  }catch(e){}
}
setInterval(pollAlert, 5000); pollAlert();

const log = document.getElementById('log');
let sid = localStorage.getItem('fg_sid') || (crypto.randomUUID().replace(/-/g,''));
localStorage.setItem('fg_sid', sid);

function add(cls, html){ const d=document.createElement('div'); d.className=cls;
  d.innerHTML=html; log.appendChild(d); log.scrollTop=log.scrollHeight; return d; }
function esc(s){ return (s||'').replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }

async function trigger(){
  await fetch('/incident',{method:'POST'});
  add('msg','<div class="tool">incident triggered — watch the panels degrade (~30s)</div>');
}
async function fix(){
  await fetch('/remediate',{method:'POST'});
  add('msg','<div class="tool">manual remediation sent — line should recover in ~20s</div>');
}

let busy = false, controller = null, watchdog = null;
const MAX_RUN_MS = 240000;   // hard ceiling so the UI can never lock up

function setBusy(on){
  busy = on;
  document.querySelector('button.send').textContent = on ? 'Stop' : 'Send';
  document.querySelector('button.send').classList.toggle('stop', on);
  document.querySelector('input[name=q]').placeholder =
    on ? 'Agent is working — click Stop to cancel' : 'Ask the agent…';
}
function stopRun(){ if(controller) controller.abort(); }

async function ask(q){
  if(busy){ stopRun(); return; }   // never silently swallow a click
  if(!q) return;
  setBusy(true);
  controller = new AbortController();
  clearTimeout(watchdog);
  watchdog = setTimeout(()=>{ if(controller) controller.abort(); }, MAX_RUN_MS);
  const hint = document.getElementById('hint'); if(hint) hint.remove();
  add('msg','<div class="who">you</div><div class="body">'+esc(q)+'</div>');
  const agentMsg = add('msg agent','<div class="who">agent</div><div class="body"><span class="think">working…</span></div>');
  const body = agentMsg.querySelector('.body');
  let gotText = false;
  try {
  const res = await fetch('/chat',{method:'POST',headers:{'Content-Type':'application/json'},
      signal: controller.signal,
      body: JSON.stringify({session_id: sid, message: q})});
  const reader = res.body.getReader(); const dec = new TextDecoder(); let buf='';
  while(true){
    const {value, done} = await reader.read(); if(done) break;
    buf += dec.decode(value, {stream:true});
    let i; while((i = buf.indexOf('\\n\\n')) >= 0){
      const line = buf.slice(0, i).trim(); buf = buf.slice(i+2);
      if(!line.startsWith('data:')) continue;
      const ev = JSON.parse(line.slice(5));
      if(ev.type==='text'){
        if(!gotText){ body.textContent=''; gotText=true; }
        body.textContent += ev.text; }
      else if(ev.type==='tool_call'){ add('msg','<div class="tool">→ '+esc(ev.name)+' '+esc(JSON.stringify(ev.args))+'</div>'); }
      else if(ev.type==='tool_result'){ add('msg','<div class="tool">← '+esc(ev.name)+'<span class="r">'+esc(ev.text)+'</span></div>'); }
      else if(ev.type==='error'){ add('msg','<div class="tool" style="color:#ff8a8a">error: '+esc(ev.text)+'</div>'); }
      else if(ev.type==='approval'){
        const args = ev.args || {};
        const actionLabel = (args.action || ev.tool).replace(/_/g,' ')
          .replace(/\\b\\w/g, c=>c.toUpperCase());
        const reason = args.reason ? esc(args.reason)
          : Object.keys(args).length ? '<code>'+esc(JSON.stringify(args))+'</code>' : '';
        const d = add('msg', ''
          +'<div class="approval">'
          +  '<div class="ah"><div class="aicon">!</div>'
          +    '<div class="atitle">Approval required<small>'+esc(ev.tool)+'()  ·  runs only if you approve</small></div>'
          +  '</div>'
          +  '<div class="abody">'
          +    '<div class="action">'+esc(actionLabel)+' — LINE-01</div>'
          +    (reason ? '<div class="reason">'+reason+'</div>' : '')
          +  '</div>'
          +  '<div class="af"><button class="approve">✓ Approve</button><button class="reject">Reject</button></div>'
          +'</div>');
        const approved = await new Promise(r=>{
          d.querySelector('.approve').onclick=()=>r(true);
          d.querySelector('.reject').onclick =()=>r(false);
        });
        await decide(approved);
        d.querySelector('.af').outerHTML =
          '<div class="decided '+(approved?'yes':'no')+'">'
          +(approved?'✓ Approved — executing…':'✕ Rejected')+'</div>';
      }
      else if(ev.type==='findings'){ renderFindings(ev); }
      else if(ev.type==='report'){ renderReport(ev); }
      else if(ev.type==='approval_result'){ /* reflected by the card itself above */ }
      else if(ev.type==='done'){ /* stream end */ }
    }
  }
  } catch(err){
    const aborted = err && err.name === 'AbortError';
    add('msg','<div class="tool" style="color:'+(aborted?'#8b93a1':'#ff8a8a')+'">'
      + (aborted ? 'run cancelled' : 'stream error: '+esc(String(err))) + '</div>');
  } finally {
    clearTimeout(watchdog); controller = null; setBusy(false);
    const t = body.querySelector('.think'); if(t) t.remove();
  }
}
const METRIC_LABELS = {
  api_latency_milliseconds:['API latency','ms'],
  api_error_rate_percent:['API error rate','%'],
  production_output_units_per_hour:['Throughput','/hr'],
  database_connection_pool_percent:['DB pool','%'],
  equipment_motor_temperature_celsius:['Motor temp','°C'],
  production_processing_time_ms:['Cycle time','ms'],
  equipment_vibration_mm_per_second:['Vibration','mm/s'],
};
function renderFindings(ev){
  const p = document.getElementById('findings');
  p.classList.remove('empty');
  document.getElementById('frc').textContent = ev.root_cause || '—';
  const conf = (ev.confidence || '').toLowerCase();
  const cf = document.getElementById('fconf');
  cf.textContent = conf ? conf + ' confidence' : 'filed';
  cf.className = 'fconf ' + (['high','medium','low'].includes(conf) ? conf : '');
  const ul = document.getElementById('fev');
  const ev_ = ev.evidence || [];
  ul.innerHTML = ev_.map(e=>'<li>'+esc(e)+'</li>').join('');
  document.getElementById('fevlabel').style.display = ev_.length ? '' : 'none';
  document.getElementById('ffix').style.display = '';
}
// Follow-up in the SAME session, so the agent already has the investigation
// in context and goes straight to proposing remediation (which pauses for
// your approval).
function proposeFix(){
  ask('Based on that root cause, propose remediation now, then verify recovery '
    + 'and file the report.');
}
function renderReport(ev){
  const before = ev.before || {}, after = ev.after || {};
  const keys = Object.keys(before).filter(k=>k in after);
  const rows = keys.map(k=>{
    const [label,unit] = METRIC_LABELS[k] || [k.replace(/_/g,' '), ''];
    const f = v => (typeof v==='number' ? (v>=100?Math.round(v).toLocaleString():v.toFixed(2)) : esc(String(v)));
    return '<tr><td>'+esc(label)+'</td><td class="b">'+f(before[k])+' '+unit
         + '</td><td class="a">'+f(after[k])+' '+unit+'</td></tr>';
  }).join('');
  const chain = (ev.causal_chain||[]).map(s=>'<li>'+esc(s)+'</li>').join('');
  const ok = ev.recovered !== false;
  add('msg', ''
    +'<div class="report" style="'+(ok?'':'border-color:rgba(241,106,106,.35)')+'">'
    +  '<div class="rh"'+(ok?'':' style="background:var(--crit-bg);border-bottom-color:rgba(241,106,106,.25)"')+'>'
    +    '<div class="ricon"'+(ok?'':' style="background:var(--crit);color:#1a0808"')+'>'+(ok?'✓':'!')+'</div>'
    +    '<div class="rtitle"'+(ok?'':' style="color:var(--crit)"')+'>'
    +      (ok?'Incident resolved':'Incident report — not fully recovered')
    +      '<small>LINE-01  ·  '+new Date().toLocaleTimeString()+'</small></div>'
    +  '</div>'
    +  '<div class="rbody">'
    +    '<div class="rlabel">Root cause</div>'
    +    '<div class="rtext">'+esc(ev.root_cause||'—')+'</div>'
    +    (chain ? '<div class="rlabel">Causal chain</div><ol>'+chain+'</ol>' : '')
    +    (rows ? '<div class="rlabel">Before / after</div><table>'
    +      '<tr><th>Metric</th><th>Incident</th><th>Now</th></tr>'+rows+'</table>' : '')
    +  '</div>'
    +'</div>');
}
async function resetSession(){
  if(busy) stopRun();
  await fetch('/reset',{method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({session_id: sid})});
  document.getElementById('log').innerHTML = '';
  const f = document.getElementById('findings');
  f.classList.add('empty');
  document.getElementById('frc').textContent = '—';
  document.getElementById('fev').innerHTML = '';
  document.getElementById('fevlabel').style.display = 'none';
  document.getElementById('ffix').style.display = 'none';
  const cf = document.getElementById('fconf');
  cf.className = 'fconf'; cf.textContent = 'awaiting investigation';
  add('msg','<div class="tool">conversation reset</div>');
}
async function decide(ok){
  await fetch('/approve',{method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({session_id: sid, approved: ok})});
}
</script>
</body></html>
"""
