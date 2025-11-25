from dataclasses import dataclass
from typing import AsyncIterator, Optional, Dict
from contextlib import asynccontextmanager
import os

from mcp.server.fastmcp import FastMCP, Context

from ..database import DatabaseTool
from ..models import DatabaseConnection, DatabaseType, DatabaseQuery


@dataclass
class AppContext:
    db_tool: DatabaseTool


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    tool = DatabaseTool()
    try:
        yield AppContext(db_tool=tool)
    finally:
        await tool.close_all_connections()


mcp = FastMCP(
    name="MCP Data Steward",
    lifespan=lifespan,
    dependencies=[
        "psycopg2-binary",
        "mysql-connector-python",
        "sqlparse",
    ],
)


def _build_connection(
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    ssl_mode: Optional[str] = None,
    connection_timeout: int = 30,
    pool_size: int = 5,
) -> DatabaseConnection:
    return DatabaseConnection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        database_type=DatabaseType(database_type),
        ssl_mode=ssl_mode,
        connection_timeout=connection_timeout,
        pool_size=pool_size,
    )


@mcp.tool()
async def db_test_connection(
    ctx: Context,
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    ssl_mode: Optional[str] = None,
) -> bool:
    conn = _build_connection(database_type, host, port, database, username, password, ssl_mode)
    return await ctx.request_context.lifespan_context.db_tool.test_connection(conn)


@mcp.tool()
async def db_validate(
    ctx: Context,
    query: str,
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    ssl_mode: Optional[str] = None,
) -> Dict:
    conn = _build_connection(database_type, host, port, database, username, password, ssl_mode)
    res = await ctx.request_context.lifespan_context.db_tool.validate_query(query, conn)
    return res.model_dump()


@mcp.tool()
async def db_schema(
    ctx: Context,
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    ssl_mode: Optional[str] = None,
) -> Dict:
    conn = _build_connection(database_type, host, port, database, username, password, ssl_mode)
    res = await ctx.request_context.lifespan_context.db_tool.get_database_schema(conn)
    return res.model_dump()


@mcp.tool()
async def db_list_tables(
    ctx: Context,
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: Optional[str] = None,
    ssl_mode: Optional[str] = None,
) -> Dict:
    conn = _build_connection(database_type, host, port, database, username, password, ssl_mode)
    names = await ctx.request_context.lifespan_context.db_tool.list_tables(conn, schema)
    return {"tables": names}


@mcp.tool()
async def db_query(
    ctx: Context,
    query: str,
    database_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    parameters: Optional[Dict[str, object]] = None,
    timeout: int = 30,
    max_rows: Optional[int] = None,
    ssl_mode: Optional[str] = None,
) -> Dict:
    conn = _build_connection(database_type, host, port, database, username, password, ssl_mode)
    dq = DatabaseQuery(query=query, parameters=parameters, database_connection=conn, timeout=timeout, max_rows=max_rows)
    res = await ctx.request_context.lifespan_context.db_tool.execute_query(dq)
    return res.model_dump()


if __name__ == "__main__":
    # Option 1: stdio (good for local MCP clients like Claude Desktop / IDEs)
    # mcp.run()

    # Option 2: Streamable HTTP transport (good for remote / web clients)
    #   - Then your MCP endpoint is exposed over HTTP.
    #   - Client connects to: http://host:port/mcp

    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport in ("http", "streamable-http"):
        mcp.run(transport="streamable-http")
    else:
        mcp.run()
