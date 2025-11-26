import os
import base64
from typing import AsyncIterator, Optional, Dict, List
from contextlib import asynccontextmanager

import boto3
from mcp.server.fastmcp import FastMCP, Context


class S3Context:
    def __init__(self, client):
        self.client = client


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[S3Context]:
    region = os.getenv("AWS_REGION", "us-east-1")
    endpoint = os.getenv("AWS_S3_ENDPOINT")
    if endpoint:
        client = boto3.client("s3", region_name=region, endpoint_url=endpoint)
    else:
        client = boto3.client("s3", region_name=region)
    try:
        yield S3Context(client)
    finally:
        pass


_HOST = os.getenv("MCP_HOST", "127.0.0.1")
_PORT = int(os.getenv("MCP_HTTP_PORT") or os.getenv("MCP_PORT") or os.getenv("PORT") or 8000)
mcp = FastMCP(
    name="S3MCP",
    lifespan=lifespan,
    dependencies=[
        "boto3",
    ],
    host=_HOST,
    port=_PORT,
)


@mcp.tool()
async def s3_list_buckets(ctx: Context) -> Dict:
    res = ctx.request_context.lifespan_context.client.list_buckets()
    names: List[str] = [b["Name"] for b in res.get("Buckets", [])]
    return {"buckets": names}


@mcp.tool()
async def s3_list_objects(
    ctx: Context,
    bucket: str,
    prefix: Optional[str] = None,
    continuation_token: Optional[str] = None,
    max_keys: int = 1000,
) -> Dict:
    kwargs: Dict[str, object] = {"Bucket": bucket, "MaxKeys": max_keys}
    if prefix:
        kwargs["Prefix"] = prefix
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token
    out = ctx.request_context.lifespan_context.client.list_objects_v2(**kwargs)
    items: List[Dict] = []
    for o in out.get("Contents", []) or []:
        items.append(
            {
                "key": o.get("Key"),
                "size": o.get("Size"),
                "last_modified": o.get("LastModified").isoformat() if o.get("LastModified") else None,
                "etag": o.get("ETag"),
            }
        )
    return {
        "objects": items,
        "is_truncated": bool(out.get("IsTruncated")),
        "next_continuation_token": out.get("NextContinuationToken"),
    }


@mcp.tool()
async def s3_read_object(
    ctx: Context,
    bucket: str,
    key: str,
    max_bytes: Optional[int] = None,
    decode_text: bool = True,
) -> Dict:
    client = ctx.request_context.lifespan_context.client
    res = client.get_object(Bucket=bucket, Key=key)
    body = res["Body"].read(max_bytes if max_bytes is not None else None)
    ct = res.get("ContentType")
    size = res.get("ContentLength")
    truncated = max_bytes is not None and isinstance(size, int) and size > len(body)
    if decode_text:
        try:
            content = body.decode("utf-8")
            encoding = "utf-8"
        except Exception:
            content = base64.b64encode(body).decode("ascii")
            encoding = "base64"
    else:
        content = base64.b64encode(body).decode("ascii")
        encoding = "base64"
    return {
        "bucket": bucket,
        "key": key,
        "content": content,
        "encoding": encoding,
        "content_type": ct,
        "size": size,
        "truncated": truncated,
    }


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport in ("http", "streamable-http"):
        mcp.run(transport="streamable-http")
    else:
        mcp.run()
