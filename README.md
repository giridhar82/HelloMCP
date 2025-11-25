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
