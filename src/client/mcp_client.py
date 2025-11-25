import os
import json
import argparse
import asyncio
from types import SimpleNamespace

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_args() -> SimpleNamespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=os.getenv("MCP_CLIENT_CONFIG", os.path.join("config", "mcp_client.json")))
    a = p.parse_args()
    cfg = load_config(a.config)
    db = cfg.get("db", {})
    return SimpleNamespace(
        transport=cfg.get("transport", "stdio"),
        url=cfg.get("url", "http://127.0.0.1:8000/mcp"),
        server=cfg.get("server", "src.core.server"),
        python_cmd=cfg.get("python_cmd", "python"),
        db_type=db.get("type"),
        db_host=db.get("host"),
        db_port=int(db.get("port") or 0),
        db_name=db.get("name"),
        db_user=db.get("user"),
        db_pass=db.get("pass"),
        ssl_mode=db.get("ssl_mode"),
        schema=db.get("schema"),
    )


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


async def run_stdio(server_path: str, python_cmd: str, args: SimpleNamespace):
    sp = [server_path] if server_path.endswith(".py") else ["-m", server_path]
    cmd_parts = python_cmd.split()
    params = StdioServerParameters(command=cmd_parts[0], args=cmd_parts[1:] + sp)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", [t.name for t in tools.tools])
            await maybe_test_db(session, args)


async def run_http(url: str, args: SimpleNamespace):
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", [t.name for t in tools.tools])
            await maybe_test_db(session, args)


async def maybe_test_db(session: ClientSession, args: SimpleNamespace):
    ok = all([args.db_type, args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass])
    if not ok:
        print("skip db tests; set DB_* env or CLI flags to enable")
        return

    test_res = await session.call_tool(
        "db_test_connection",
        {
            "database_type": args.db_type,
            "host": args.db_host,
            "port": args.db_port,
            "database": args.db_name,
            "username": args.db_user,
            "password": args.db_pass,
            "ssl_mode": args.ssl_mode,
        },
    )
    print("db_test_connection:", unwrap_result(test_res))

    validate_res = await session.call_tool(
        "db_validate",
        {
            "query": "SELECT 1",
            "database_type": args.db_type,
            "host": args.db_host,
            "port": args.db_port,
            "database": args.db_name,
            "username": args.db_user,
            "password": args.db_pass,
            "ssl_mode": args.ssl_mode,
        },
    )
    print("db_validate:", unwrap_result(validate_res))

    schema_res = await session.call_tool(
        "db_schema",
        {
            "database_type": args.db_type,
            "host": args.db_host,
            "port": args.db_port,
            "database": args.db_name,
            "username": args.db_user,
            "password": args.db_pass,
            "ssl_mode": args.ssl_mode,
        },
    )
    print("db_schema tables:", len(unwrap_result(schema_res).get("tables", [])))

    list_res = await session.call_tool(
        "db_list_tables",
        {
            "database_type": args.db_type,
            "host": args.db_host,
            "port": args.db_port,
            "database": args.db_name,
            "username": args.db_user,
            "password": args.db_pass,
            "schema": args.schema,
            "ssl_mode": args.ssl_mode,
        },
    )
    print("db_list_tables:", unwrap_result(list_res))

    query_res = await session.call_tool(
        "db_query",
        {
            "query": "SELECT 1",
            "database_type": args.db_type,
            "host": args.db_host,
            "port": args.db_port,
            "database": args.db_name,
            "username": args.db_user,
            "password": args.db_pass,
            "parameters": None,
            "ssl_mode": args.ssl_mode,
        },
    )
    print("db_query:", unwrap_result(query_res))


async def main():
    args = build_args()
    if args.transport == "http" or args.transport == "streamable-http":
        await run_http(args.url, args)
    else:
        await run_stdio(args.server, args.python_cmd, args)


if __name__ == "__main__":
    asyncio.run(main())
