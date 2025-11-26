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
    p.add_argument("--config", default=os.getenv("MCP_CLIENT_CONFIG", os.path.join("config", "mc_S3.json")))
    p.add_argument("--python_cmd", default=os.getenv("PY_CMD", "py -3.12"))
    a = p.parse_args()
    cfg = load_config(a.config)
    return SimpleNamespace(
        transport=cfg.get("transport", "http"),
        url=cfg.get("url", "http://127.0.0.1:8002/mcp"),
        server=cfg.get("server", "src.s3.server"),
        python_cmd=a.python_cmd,
        bucket=cfg.get("bucket"),
        prefix=cfg.get("prefix"),
        key=cfg.get("key"),
        max_bytes=int(cfg.get("max_bytes") or 0) or None,
        decode_text=bool(cfg.get("decode_text", True)),
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
            await maybe_test_s3(session, args)


async def run_http(url: str, args: SimpleNamespace):
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", [t.name for t in tools.tools])
            await maybe_test_s3(session, args)


async def maybe_test_s3(session: ClientSession, args: SimpleNamespace):
    lb = await session.call_tool("s3_list_buckets", {})
    print("s3_list_buckets:", json.dumps(unwrap_result(lb), ensure_ascii=False))
    if args.bucket:
        lo = await session.call_tool(
            "s3_list_objects",
            {"bucket": args.bucket, "prefix": args.prefix or None},
        )
        print("s3_list_objects:", json.dumps(unwrap_result(lo), ensure_ascii=False))
    if args.bucket and args.key:
        ro = await session.call_tool(
            "s3_read_object",
            {
                "bucket": args.bucket,
                "key": args.key,
                "max_bytes": args.max_bytes,
                "decode_text": args.decode_text,
            },
        )
        print("s3_read_object:", json.dumps(unwrap_result(ro), ensure_ascii=False))


async def main():
    args = build_args()
    if args.transport in ("http", "streamable-http"):
        await run_http(args.url, args)
    else:
        await run_stdio(args.server, args.python_cmd, args)


if __name__ == "__main__":
    asyncio.run(main())
