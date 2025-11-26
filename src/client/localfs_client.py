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
    p.add_argument("--config", default=os.getenv("MCP_CLIENT_CONFIG", os.path.join("config", "localfile_mcp.json")))
    p.add_argument("--python_cmd", default=os.getenv("PY_CMD", "py -3.12"))
    a = p.parse_args()
    cfg = load_config(a.config)
    return SimpleNamespace(
        transport=cfg.get("transport", "http"),
        url=cfg.get("url", "http://127.0.0.1:8003/mcp"),
        server=cfg.get("server", "src.localfs.server"),
        python_cmd=a.python_cmd,
        path=cfg.get("path", "."),
        types=cfg.get("types"),
        ignore=cfg.get("ignore"),
        read=cfg.get("read"),
        max_depth=int(cfg.get("max_depth") or 5),
        max_items=int(cfg.get("max_items") or 200),
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
            await run_tests(session, args)


async def run_http(url: str, args: SimpleNamespace):
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", [t.name for t in tools.tools])
            await run_tests(session, args)


async def run_tests(session: ClientSession, args: SimpleNamespace):
    lst = await session.call_tool(
        "localfs_list",
        {
            "base_path": args.path,
            "types": args.types,
            "ignore": args.ignore,
            "max_depth": args.max_depth,
            "max_items": args.max_items,
        },
    )
    print("localfs_list:", json.dumps(unwrap_result(lst), ensure_ascii=False))
    if args.read:
        rd = await session.call_tool(
            "localfs_read",
            {
                "base_path": args.path,
                "rel_path": args.read,
                "max_bytes": None,
                "decode_text": True,
            },
        )
        print("localfs_read:", json.dumps(unwrap_result(rd), ensure_ascii=False)[:2000])


async def main():
    args = build_args()
    if args.transport in ("http", "streamable-http"):
        await run_http(args.url, args)
    else:
        await run_stdio(args.server, args.python_cmd, args)


if __name__ == "__main__":
    asyncio.run(main())
