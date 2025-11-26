# HelloMCP

A Model Context Protocol (MCP) server and client that expose safe database tools for PostgreSQL/MySQL/Oracle. Includes:
- MCP server with tools: `db_test_connection`, `db_validate`, `db_schema`, `db_list_tables`, `db_query`
- MCP client harness for stdio and streamable HTTP transports
- Optional agent tool-mode for quick ad‑hoc queries
- Optional web UI for natural language → SQL display

## Requirements
- Python 3.12 (`py -3.12` on Windows)
- A running database with reachable credentials
- Recommended: PowerShell on Windows

Install dependencies:
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configure
Create a local client config at `config/mcp_client.json`:
```json
{
  "transport": "streamable-http",
  "url": "http://127.0.0.1:8000/mcp",
  "server": "src.core.server",
  "python_cmd": "py -3.12",
  "db": {
    "type": "postgresql",
    "host": "<< >>",
    "port": 5432,
    "name": "<<  >>",
    "user": "<< >>",
    "pass": "<< >>",
    "ssl_mode": null,
    "schema": null
  }
}
```
Or set an environment variable:
```powershell
$env:MCP_CLIENT_CONFIG = "config/mcp_client.json"
```

## Run MCP Server
- HTTP transport (recommended for browser/remote clients):
```powershell
$env:MCP_TRANSPORT = "streamable-http"
py -3.12 -m src.core.server
```
- Stdio transport (recommended for local IDE clients):
```powershell
py -3.12 -m src.core.server
```
Transport selection is controlled by `MCP_TRANSPORT`; see `src/core/server.py`.

## Run MCP Client
Use the client harness to initialize a session and exercise DB tools.
```powershell
$env:MCP_CLIENT_CONFIG = "config/mcp_client.json"
py -3.12 -m src.client.mcp_client
```
What it does:
- Initializes MCP session and lists tools
- Calls:
  - `db_test_connection`
  - `db_validate` with `SELECT 1`
  - `db_schema`
  - `db_list_tables`
  - `db_query` with `SELECT 1`

Client code references:
- Entry: `src/client/mcp_client.py:154–163`
- HTTP mode: `src/client/mcp_client.py:64–71`
- Stdio mode: `src/client/mcp_client.py:52–62`

## Quick Tests (Agent Tool‑Mode)
Run ad‑hoc queries without Bedrock by using the agent’s tool mode.
- List tables:
```powershell
py -3.12 src\ai\bedrock_agent.py --transport http --url http://127.0.0.1:8000/mcp --tool db_list_tables
```
- Query rows:
```powershell
py -3.12 src\ai\bedrock_agent.py --transport http --url http://127.0.0.1:8000/mcp --tool db_query --sql "SELECT id, name FROM apartments_apartment ORDER BY id LIMIT 10"
```
- Parameterized query (Postgres style):
```powershell
py -3.12 src\ai\bedrock_agent.py --transport http --url http://127.0.0.1:8000/mcp --tool db_query --sql "SELECT * FROM payments_paymentrecord WHERE payment_date >= %(start)s" --params "{\"start\":\"2024-01-01\"}"
```



## S3 MCP Server
- Configure client at `config/mc_S3.json`:
  ```json
  {
    "transport": "streamable-http",
    "url": "http://127.0.0.1:8002/mcp",
    "server": "src.s3.server",
    "bucket": "",
    "prefix": "",
    "key": "",
    "max_bytes": null,
    "decode_text": true
  }
  ```
- Start server on port 8002:
  - `$env:MCP_TRANSPORT="streamable-http"; $env:MCP_HTTP_PORT=8002; py -3.12 -m src.s3.server`
- Test with client:
  - `$env:MCP_CLIENT_CONFIG="config/mc_S3.json"; py -3.12 -m src.client.s3_client`
- Tools:
  - `s3_list_buckets` lists buckets
  - `s3_list_objects` lists objects in a bucket (use `bucket`/`prefix`)
  - `s3_read_object` reads object content (use `bucket`/`key`)
- Notes:
  - AWS credentials must be available (env or default profile)
  - `s3_list_buckets` requires `s3:ListAllMyBuckets` permission; set `bucket` and use `s3_list_objects` otherwise
  - Optional `AWS_REGION` and `AWS_S3_ENDPOINT` are supported by the server
  - Server code: `src/s3/server.py:38–108`, run: `src/s3/server.py:112–116`

## LocalFS MCP Server
- Configure client at `config/localfile_mcp.json`:
  ```json
  {
    "transport": "streamable-http",
    "url": "http://127.0.0.1:8003/mcp",
    "server": "src.localfs.server",
    "path": ".",
    "types": ["py", "txt"],
    "ignore": [".venv/**", "__pycache__/**", "node_modules/**"],
    "read": "README.md",
    "max_depth": 5,
    "max_items": 200
  }
  ```
- Start server on port 8003:
  - `$env:MCP_TRANSPORT="streamable-http"; $env:MCP_HTTP_PORT=8003; py -3.12 -m src.localfs.server`
- Test with client:
  - `$env:MCP_CLIENT_CONFIG="config/localfile_mcp.json"; py -3.12 -m src.client.localfs_client`
- Tools:
  - `localfs_list` lists files under `path` with optional `types` and `ignore`
-  - `localfs_read` reads a file relative to `path`
- Notes:
  - `ignore` uses glob patterns relative to `path`
  - `types` are extensions without dots (e.g., `"py"`)
  - Path containment is enforced (cannot read outside base)
  - Server code: `src/localfs/server.py:33–104`, run: `src/localfs/server.py:108–114`

## Troubleshooting
- 406 Not Acceptable when calling `/mcp` directly:
  - Use the client or agent tool‑mode, which performs the MCP handshake.
- Pydantic missing field error on `db_query`:
  - Ensure payload includes `query` and DB connection fields.
- Parameter binding:
  - PostgreSQL/MySQL: pass a dict of parameters; use `%(name)s` in SQL for Postgres.
  - Oracle: placeholders are converted to `:name` internally.

## Project Structure
- Server and tools: `src/core/server.py`
- Client harness: `src/client/mcp_client.py`
- Database connectors: `src/database/connectors/*.py`
- Safety checks: `src/database/safety/risk_checker.py`
- Agent tool‑mode: `src/ai/bedrock_agent.py`
- Web UI: `src/ui/chat_server.py`

## Security
- Secrets and local configs are ignored via `.gitignore` (e.g., `.env`, `config/mcp_client.json`, `src/creds`).
- Do not commit credentials; use env vars or local config files.
