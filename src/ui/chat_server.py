import os
import json
import asyncio
from types import SimpleNamespace

from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from ..ai.bedrock_agent import BedrockMCPAgent, load_config, unwrap_result, route_intent_sql


async def startup():
    cfg_path = os.path.join("config", "mcp_client.sample.json")
    cfg = load_config(cfg_path)
    db = cfg.get("db", {})
    ns = SimpleNamespace(
        server=cfg.get("server", "src.core.server"),
        python_cmd=cfg.get("python_cmd", "py -3.12"),
        url=cfg.get("url", "http://127.0.0.1:8000/mcp"),
        db_type=db.get("type"),
        db_host=db.get("host"),
        db_port=int(db.get("port") or 0),
        db_name=db.get("name"),
        db_user=db.get("user"),
        db_pass=db.get("pass"),
        ssl_mode=db.get("ssl_mode"),
    )
    region = os.getenv("AWS_REGION", "us-east-1")
    model_id = os.getenv("BEDROCK_MODEL", "anthropic.claude-3-5-sonnet-20240620-v1:0")
    app.state.ns = ns
    app.state.agent = BedrockMCPAgent(region=region, model_id=model_id, dry_run=False)
    app.state.session = None
    app.state.http_cm = None


async def shutdown():
    try:
        if app.state.http_cm is not None:
            await app.state.http_cm.__aexit__(None, None, None)
    except Exception:
        pass


async def index(request):
    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset=\"utf-8\" />
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
      <title>MCP AI Chat</title>
      <style>
        body{font-family:system-ui,Arial;padding:0;margin:0;background:#0f172a;color:#e2e8f0}
        header{padding:16px;background:#111827;border-bottom:1px solid #1f2937}
        main{max-width:800px;margin:0 auto;padding:16px}
        .chat{border:1px solid #1f2937;border-radius:8px;padding:12px;background:#111827}
        .msg{padding:8px 10px;margin:8px 0;border-radius:6px}
        .user{background:#1f2937}
        .bot{background:#0b1f2a}
        .input{display:flex;gap:8px;margin-top:12px}
        input[type=text]{flex:1;padding:10px;border-radius:6px;border:1px solid #374151;background:#0f172a;color:#e2e8f0}
        button{padding:10px 14px;border-radius:6px;border:1px solid #374151;background:#1f2937;color:#e2e8f0}
        pre{white-space:pre-wrap;word-wrap:break-word;margin:6px 0}
      </style>
    </head>
    <body>
      <header>
        <h2>MCP AI Chat</h2>
      </header>
      <main>
        <div class=\"chat\" id=\"chat\"></div>
        <div class=\"input\">
          <input type=\"text\" id=\"msg\" placeholder=\"Type a message...\" />
          <button id=\"send\">Send</button>
        </div>
      </main>
      <script>
        const chat = document.getElementById('chat');
        const msg = document.getElementById('msg');
        const send = document.getElementById('send');
        function add(role, text){
          const div = document.createElement('div');
          div.className = 'msg ' + (role==='user'?'user':'bot');
          const pre = document.createElement('pre');
          pre.textContent = text;
          div.appendChild(pre);
          chat.appendChild(div);
          chat.scrollTop = chat.scrollHeight;
        }
        async function post(){
          const v = msg.value.trim();
          if(!v) return;
          add('user', v);
          msg.value='';
          const res = await fetch('/api/sql', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ message: v }) });
          const data = await res.json();
          if(data.type==='sql') add('assistant', data.sql);
          else add('assistant', JSON.stringify(data, null, 2));
        }
        send.onclick = post;
        msg.addEventListener('keydown', e=>{ if(e.key==='Enter') post(); });
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


async def api_chat(request):
    data = await request.json()
    query = str(data.get("message") or "").strip()
    ns = getattr(app.state, "ns", None)
    if ns is None:
        cfg_path = os.path.join("config", "mcp_client.sample.json")
        cfg = load_config(cfg_path)
        db = cfg.get("db", {})
        ns = SimpleNamespace(
            server=cfg.get("server", "src.core.server"),
            python_cmd=cfg.get("python_cmd", "py -3.12"),
            url=cfg.get("url", "http://127.0.0.1:8000/mcp"),
            db_type=db.get("type"),
            db_host=db.get("host"),
            db_port=int(db.get("port") or 0),
            db_name=db.get("name"),
            db_user=db.get("user"),
            db_pass=db.get("pass"),
            ssl_mode=db.get("ssl_mode"),
        )
        region = os.getenv("AWS_REGION", "us-east-1")
        model_id = os.getenv("BEDROCK_MODEL", "anthropic.claude-3-5-sonnet-20240620-v1:0")
        app.state.ns = ns
        app.state.agent = BedrockMCPAgent(region=region, model_id=model_id, dry_run=False)
        app.state.http_cm = None
        app.state.session = None
    sess = getattr(app.state, "session", None)
    if sess is None:
        try:
            app.state.http_cm = streamablehttp_client(app.state.ns.url)
            read, write, _ = await app.state.http_cm.__aenter__()
            sess = ClientSession(read, write)
            await sess.initialize()
            app.state.session = sess
        except Exception:
            sess = None
            app.state.session = None
    session = sess
    agent = app.state.agent
    ns = app.state.ns
    tools = await session.list_tools()
    tool_names = [t.name for t in tools.tools]
    prompt = agent.build_prompt(query or "List tables", tool_names)
    try:
        out = agent.invoke_bedrock(prompt)
    except Exception as e:
        out = ""
        tname = "db_query"
        ql = query.lower()
        sql = "SELECT 1"
        if ("average" in ql or "avg" in ql) and "payment" in ql:
            sql = "SELECT DATE_TRUNC('month', payment_date) AS month, AVG(amount) AS avg_amount FROM payments_paymentrecord WHERE payment_date IS NOT NULL GROUP BY month ORDER BY month"
        res = await session.call_tool(tname, {
            "query": sql,
            "database_type": ns.db_type,
            "host": ns.db_host,
            "port": ns.db_port,
            "database": ns.db_name,
            "username": ns.db_user,
            "password": ns.db_pass,
            "parameters": None,
            "ssl_mode": ns.ssl_mode,
        })
        return JSONResponse({"type": "tool", "tool": tname, "result": unwrap_result(res)})
    call = None
    try:
        call = json.loads(out)
    except Exception:
        call = None
    if isinstance(call, dict) and call.get("tool"):
        tname = call["tool"]
        if tname == "db_list_tables":
            payload = {
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "schema": None,
                "ssl_mode": ns.ssl_mode,
            }
        elif tname == "db_schema":
            payload = {
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "ssl_mode": ns.ssl_mode,
            }
        elif tname == "db_query":
            args = call.get("args") or {}
            payload = {
                "query": args.get("query", "SELECT 1"),
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "parameters": args.get("parameters"),
                "ssl_mode": ns.ssl_mode,
            }
        else:
            payload = call.get("args") or {}
        res = await session.call_tool(tname, payload)
        return JSONResponse({"type": "tool", "tool": tname, "result": unwrap_result(res)})
    else:
        if query:
            tname, payload = await route_intent_sql(session, agent, ns, query)
            res = await session.call_tool(tname, payload)
            return JSONResponse({"type": "tool", "tool": tname, "result": unwrap_result(res)})
        return JSONResponse({"type": "text", "text": out})


async def api_sql(request):
    data = await request.json()
    query = str(data.get("message") or "").strip()
    ns = getattr(app.state, "ns", None)
    if ns is None:
        cfg_path = os.path.join("config", "mcp_client.sample.json")
        cfg = load_config(cfg_path)
        db = cfg.get("db", {})
        ns = SimpleNamespace(
            server=cfg.get("server", "src.core.server"),
            python_cmd=cfg.get("python_cmd", "py -3.12"),
            url=cfg.get("url", "http://127.0.0.1:8000/mcp"),
            db_type=db.get("type"),
            db_host=db.get("host"),
            db_port=int(db.get("port") or 0),
            db_name=db.get("name"),
            db_user=db.get("user"),
            db_pass=db.get("pass"),
            ssl_mode=db.get("ssl_mode"),
        )
        region = os.getenv("AWS_REGION", "us-east-1")
        model_id = os.getenv("BEDROCK_MODEL", "anthropic.claude-3-5-sonnet-20240620-v1:0")
        app.state.ns = ns
        app.state.agent = BedrockMCPAgent(region=region, model_id=model_id, dry_run=False)
    agent = app.state.agent
    try:
        sql = agent.generate_sql(query, [])
    except Exception:
        sql = "SELECT 1"
    ql = query.lower()
    if not sql.lower().strip().startswith("select") or sql.strip() == "SELECT 1":
        if ("average" in ql or "avg" in ql) and ("payment" in ql or "payments" in ql):
            sql = "SELECT DATE_TRUNC('month', payment_date) AS month, AVG(amount) AS avg_amount FROM payments_paymentrecord WHERE payment_date IS NOT NULL GROUP BY month ORDER BY month"
        elif "count" in ql:
            tbl = None
            for kw in ["from", "in", "table"]:
                if kw in ql:
                    try:
                        tbl = ql.split(kw, 1)[1].strip().split()[0]
                    except Exception:
                        tbl = None
                    break
            if tbl:
                sql = f"SELECT COUNT(*) AS count FROM {tbl}"
            else:
                sql = "SELECT COUNT(*) AS count FROM residents_residentprofile"
    return JSONResponse({"type": "sql", "sql": sql})

routes = [
    Route("/", endpoint=index),
    Route("/api/chat", endpoint=api_chat, methods=["POST"]),
    Route("/api/sql", endpoint=api_sql, methods=["POST"]),
]

app = Starlette(routes=routes)
