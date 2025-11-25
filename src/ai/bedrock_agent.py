import os
import json
import argparse
import asyncio
from types import SimpleNamespace

import boto3

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def unwrap_result(res: types.CallToolResult):
    if getattr(res, "structuredOutput", None) is not None:
        return res.structuredOutput
    for c in res.content:
        if isinstance(c, types.TextContent):
            t = c.text
            try:
                return json.loads(t)
            except Exception:
                return t
    return [c.model_dump() for c in res.content]


class BedrockMCPAgent:
    def __init__(self, region: str, model_id: str, dry_run: bool = False):
        self._dry = dry_run
        self._model = model_id
        self._client = None if dry_run else boto3.client("bedrock-runtime", region_name=region)

    def build_prompt(self, user_input: str, tools: list[str]) -> str:
        names = ", ".join(tools)
        return (
            "You can call MCP tools by replying with a JSON object {\"tool\": name, \"args\": {...}}. "
            + f"Available tools: {names}. "
            + "If no tool is needed, reply with plain text. "
            + user_input
        )

    def invoke_bedrock(self, prompt: str) -> str:
        if self._dry:
            return json.dumps({"tool": "db_list_tables", "args": {}})
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "max_tokens": 1024,
        }
        resp = self._client.invoke_model(modelId=self._model, body=json.dumps(body))
        data = resp.get("body")
        s = data.read().decode("utf-8")
        obj = json.loads(s)
        parts = obj.get("output", {}).get("message", {}).get("content", [])
        if not parts and isinstance(obj.get("content"), list):
            parts = obj.get("content")
        texts = [p.get("text") for p in parts if p.get("type") == "text" and isinstance(p.get("text"), str)]
        return texts[0].strip() if texts else ""
        
    def generate_sql(self, user_input: str, tables: list[str]) -> str:
        if self._dry:
            t = None
            for name in tables:
                if name.lower() in user_input.lower():
                    t = name
                    break
            if t:
                return f"SELECT COUNT(*) AS count FROM {t}"
            return "SELECT 1"
        prompt = (
            "Return a single valid SQL SELECT for the following request. "
            + "Use only tables from this list and prefer simple aggregates if counting is requested. "
            + "Return only the SQL string. "    
            + f"Tables: {', '.join(tables)}. "
            + f"Request: {user_input}"
        )
        out = self.invoke_bedrock(prompt)
        return out.strip()


async def run_tools_with_session(session: ClientSession, agent: BedrockMCPAgent, ns: SimpleNamespace, query: str | None):
    tools = await session.list_tools()
    tool_names = [t.name for t in tools.tools]
    prompt = agent.build_prompt(query or "List tables", tool_names)
    out = agent.invoke_bedrock(prompt)
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
        print(json.dumps(unwrap_result(res), ensure_ascii=False))
    else:
        print(out)


def _is_sql(q: str) -> bool:
    s = q.strip().lower()
    return s.startswith("select") or s.startswith("insert") or s.startswith("update") or s.startswith("delete") or s.startswith("create") or s.startswith("alter") or s.startswith("drop")


def route_intent(query: str, ns: SimpleNamespace) -> tuple[str, dict]:
    q = query.strip().lower()
    if "list tables" in q or ("show" in q and "tables" in q):
        schema = None
        for kw in ["in schema", "schema"]:
            if kw in q:
                try:
                    schema = q.split(kw, 1)[1].strip().split()[0]
                except Exception:
                    schema = None
                break
        return (
            "db_list_tables",
            {
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "schema": schema,
                "ssl_mode": ns.ssl_mode,
            },
        )
    if "schema" in q or "describe" in q or "structure" in q:
        return (
            "db_schema",
            {
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "ssl_mode": ns.ssl_mode,
            },
        )
    if _is_sql(query):
        return (
            "db_query",
            {
                "query": query,
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "parameters": None,
                "ssl_mode": ns.ssl_mode,
            },
        )
    if "count" in q:
        # naive table extraction after 'from' or 'in'
        tbl = None
        for kw in ["from", "in", "of", "table"]:
            if kw in q:
                try:
                    tbl = q.split(kw, 1)[1].strip().split()[0]
                except Exception:
                    tbl = None
                break
        sql = f"SELECT COUNT(*) FROM {tbl}" if tbl else "SELECT 1"
        return (
            "db_query",
            {
                "query": sql,
                "database_type": ns.db_type,
                "host": ns.db_host,
                "port": ns.db_port,
                "database": ns.db_name,
                "username": ns.db_user,
                "password": ns.db_pass,
                "parameters": None,
                "ssl_mode": ns.ssl_mode,
            },
        )
    return (
        "db_list_tables",
        {
            "database_type": ns.db_type,
            "host": ns.db_host,
            "port": ns.db_port,
            "database": ns.db_name,
            "username": ns.db_user,
            "password": ns.db_pass,
            "schema": None,
            "ssl_mode": ns.ssl_mode,
        },
    )


async def route_intent_sql(session: ClientSession, agent: BedrockMCPAgent, ns: SimpleNamespace, query: str) -> tuple[str, dict]:
    lp = {
        "database_type": ns.db_type,
        "host": ns.db_host,
        "port": ns.db_port,
        "database": ns.db_name,
        "username": ns.db_user,
        "password": ns.db_pass,
        "schema": None,
        "ssl_mode": ns.ssl_mode,
    }
    try:
        lres = await session.call_tool("db_list_tables", lp)
        data = unwrap_result(lres)
        tables = data.get("tables") if isinstance(data, dict) else []
    except Exception:
        tables = []
    sql = agent.generate_sql(query, tables or [])
    if not sql.lower().strip().startswith("select"):
        sql = "SELECT 1"
    return (
        "db_query",
        {
            "query": sql,
            "database_type": ns.db_type,
            "host": ns.db_host,
            "port": ns.db_port,
            "database": ns.db_name,
            "username": ns.db_user,
            "password": ns.db_pass,
            "parameters": None,
            "ssl_mode": ns.ssl_mode,
        },
    )

async def run_agent():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=os.path.join("config", "mcp_client.sample.json"))
    p.add_argument("--transport", default="http")
    p.add_argument("--url", default="http://127.0.0.1:8000/mcp")
    p.add_argument("--server", default="src.core.server")
    p.add_argument("--python_cmd", default="py -3.12")
    p.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    p.add_argument("--model", default=os.getenv("BEDROCK_MODEL", "anthropic.claude-3.5-sonnet-20241022"))
    p.add_argument("--query", required=False)
    p.add_argument("--tool", required=False)
    p.add_argument("--schema", required=False)
    p.add_argument("--sql", required=False)
    p.add_argument("--params", required=False)
    p.add_argument("--summarize", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    cfg = load_config(a.config)
    db = cfg.get("db", {})
    ns = SimpleNamespace(
        transport=a.transport,
        url=a.url,
        server=a.server,
        python_cmd=a.python_cmd,
        db_type=db.get("type"),
        db_host=db.get("host"),
        db_port=int(db.get("port") or 0),
        db_name=db.get("name"),
        db_user=db.get("user"),
        db_pass=db.get("pass"),
        ssl_mode=db.get("ssl_mode"),
    )

    agent = BedrockMCPAgent(region=a.region, model_id=a.model, dry_run=a.dry_run)
    if ns.transport in ("http", "streamable-http"):
        async with streamablehttp_client(ns.url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                if a.tool:
                    tname = a.tool
                    if tname == "db_list_tables":
                        payload = {
                            "database_type": ns.db_type,
                            "host": ns.db_host,
                            "port": ns.db_port,
                            "database": ns.db_name,
                            "username": ns.db_user,
                            "password": ns.db_pass,
                            "schema": a.schema or None,
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
                        payload = {
                            "query": a.sql or "SELECT 1",
                            "database_type": ns.db_type,
                            "host": ns.db_host,
                            "port": ns.db_port,
                            "database": ns.db_name,
                            "username": ns.db_user,
                            "password": ns.db_pass,
                            "parameters": json.loads(a.params) if a.params else None,
                            "ssl_mode": ns.ssl_mode,
                        }
                    else:
                        payload = {}
                    res = await session.call_tool(tname, payload)
                    data = unwrap_result(res)
                    print(json.dumps(data, ensure_ascii=False))
                    if a.summarize and not a.dry_run:
                        summary = agent.invoke_bedrock("Summarize: " + json.dumps(data, ensure_ascii=False))
                        print(summary)
                else:
                    if a.query:
                        tname, payload = await route_intent_sql(session, agent, ns, a.query)
                        res = await session.call_tool(tname, payload)
                        data = unwrap_result(res)
                        print(json.dumps(data, ensure_ascii=False))
                        if a.summarize and not a.dry_run:
                            summary = agent.invoke_bedrock("Summarize: " + json.dumps(data, ensure_ascii=False))
                            print(summary)
                    else:
                        await run_tools_with_session(session, agent, ns, a.query)
    else:
        sp = [ns.server] if ns.server.endswith(".py") else ["-m", ns.server]
        cmd_parts = ns.python_cmd.split()
        params = StdioServerParameters(command=cmd_parts[0], args=cmd_parts[1:] + sp)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                if a.tool:
                    tname = a.tool
                    if tname == "db_list_tables":
                        payload = {
                            "database_type": ns.db_type,
                            "host": ns.db_host,
                            "port": ns.db_port,
                            "database": ns.db_name,
                            "username": ns.db_user,
                            "password": ns.db_pass,
                            "schema": a.schema or None,
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
                        payload = {
                            "query": a.sql or "SELECT 1",
                            "database_type": ns.db_type,
                            "host": ns.db_host,
                            "port": ns.db_port,
                            "database": ns.db_name,
                            "username": ns.db_user,
                            "password": ns.db_pass,
                            "parameters": json.loads(a.params) if a.params else None,
                            "ssl_mode": ns.ssl_mode,
                        }
                    else:
                        payload = {}
                    res = await session.call_tool(tname, payload)
                    data = unwrap_result(res)
                    print(json.dumps(data, ensure_ascii=False))
                    if a.summarize and not a.dry_run:
                        summary = agent.invoke_bedrock("Summarize: " + json.dumps(data, ensure_ascii=False))
                        print(summary)
                else:
                    if a.query:
                        tname, payload = await route_intent_sql(session, agent, ns, a.query)
                        res = await session.call_tool(tname, payload)
                        data = unwrap_result(res)
                        print(json.dumps(data, ensure_ascii=False))
                        if a.summarize and not a.dry_run:
                            summary = agent.invoke_bedrock("Summarize: " + json.dumps(data, ensure_ascii=False))
                            print(summary)
                    else:
                        await run_tools_with_session(session, agent, ns, a.query)


if __name__ == "__main__":
    asyncio.run(run_agent())
