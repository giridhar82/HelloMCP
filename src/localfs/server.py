import os
import base64
import fnmatch
from typing import AsyncIterator, Optional, Dict, List
from contextlib import asynccontextmanager
from pathlib import Path

from mcp.server.fastmcp import FastMCP, Context


class FSContext:
    def __init__(self):
        pass


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[FSContext]:
    try:
        yield FSContext()
    finally:
        pass


_HOST = os.getenv("MCP_HOST", "127.0.0.1")
_PORT = int(os.getenv("MCP_HTTP_PORT") or os.getenv("MCP_PORT") or os.getenv("PORT") or 8000)
mcp = FastMCP(
    name="LocalFSMCP",
    lifespan=lifespan,
    dependencies=[],
    host=_HOST,
    port=_PORT,
)


def _norm_types(types: Optional[List[str]]) -> Optional[List[str]]:
    if not types:
        return None
    out: List[str] = []
    for t in types:
        s = (t or "").strip().lower()
        if not s:
            continue
        s = s[1:] if s.startswith(".") else s
        out.append(s)
    return out or None


def _is_ignored(rel: str, patterns: Optional[List[str]]) -> bool:
    if not patterns:
        return False
    for p in patterns:
        if fnmatch.fnmatch(rel, p):
            return True
    return False


@mcp.tool()
async def localfs_list(
    ctx: Context,
    base_path: str,
    types: Optional[List[str]] = None,
    ignore: Optional[List[str]] = None,
    max_depth: int = 10,
    max_items: int = 1000,
) -> Dict:
    root = Path(base_path).expanduser().resolve()
    tps = _norm_types(types)
    items: List[Dict] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dp = Path(dirpath)
        depth = len(dp.relative_to(root).parts) if dp != root else 0
        if depth > max_depth:
            continue
        for name in filenames:
            fp = dp / name
            rel = str(fp.relative_to(root)).replace("\\", "/")
            if _is_ignored(rel, ignore):
                continue
            if tps is not None:
                ext = fp.suffix.lower().lstrip(".")
                if ext not in tps:
                    continue
            try:
                st = fp.stat()
                items.append({
                    "path": rel,
                    "size": int(st.st_size),
                    "modified": getattr(st, "st_mtime", None),
                })
            except Exception:
                continue
            if len(items) >= max_items:
                break
        if len(items) >= max_items:
            break
    return {"files": items, "count": len(items), "base": str(root)}


@mcp.tool()
async def localfs_read(
    ctx: Context,
    base_path: str,
    rel_path: str,
    max_bytes: Optional[int] = None,
    decode_text: bool = True,
) -> Dict:
    root = Path(base_path).expanduser().resolve()
    target = (root / rel_path).resolve()
    if root not in target.parents and target != root:
        return {"error": "Path outside base"}
    try:
        data = target.read_bytes() if max_bytes is None else target.open("rb").read(max_bytes)
    except Exception as e:
        return {"error": str(e)}
    if decode_text:
        try:
            content = data.decode("utf-8")
            encoding = "utf-8"
        except Exception:
            content = base64.b64encode(data).decode("ascii")
            encoding = "base64"
    else:
        content = base64.b64encode(data).decode("ascii")
        encoding = "base64"
    return {"base": str(root), "path": rel_path, "content": content, "encoding": encoding}


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport in ("http", "streamable-http"):
        mcp.run(transport="streamable-http")
    else:
        mcp.run()
