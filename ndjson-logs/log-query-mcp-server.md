# Log Query MCP Server

A portable MCP server that gives Claude Code (or any MCP client) SQL access to your NDJSON structured logs via DuckDB. Drop-in companion to [ndjson-logging-with-duckdb.md](ndjson-logging-with-duckdb.md).

## What It Does

Three tools in ~130 lines of Python:

| Tool | Purpose |
|------|---------|
| `query_logs` | Search logs by time range, source, level, keyword. Handles epoch-ns timestamps and timezone display automatically. |
| `list_log_sources` | Discover what log sources exist and their entry counts. |
| `query_log_sql` | Run arbitrary SQL against the `logs` view for anything the structured tools don't cover. |

## Dependencies

```bash
pip install duckdb fastmcp
```

That's it. No project imports, no external services.

## Setup

### 1. Configure

Edit the three constants at the top of `log_query_mcp_server.py`:

```python
LOG_DIR = "/tmp/app_logs"       # Must match your logger's LOG_DIR
TIMEZONE = "America/Chicago"    # Display timezone for timestamps
PORT = 8001                     # Server port
```

### 2. Start the Server

```bash
# Foreground (for testing)
python log_query_mcp_server.py

# Background (for persistent use)
tmux new-session -d -s log-mcp 'python log_query_mcp_server.py'
```

### 3. Register with Claude Code

```bash
claude mcp add logs --transport http http://localhost:8001/mcp
```

Start a new Claude Code session after registering. The tools will appear automatically.

### 4. Restart After Changes

```bash
tmux kill-session -t log-mcp
tmux new-session -d -s log-mcp 'python log_query_mcp_server.py'
```

No Claude Code restart needed — HTTP transport means the server lifecycle is independent of client sessions.

## Usage Examples

Once registered, Claude Code can call these tools directly:

```
"Show me errors from the last 30 minutes"
→ query_logs(minutes_ago=30, level="ERROR")

"What log sources are available for today?"
→ list_log_sources(date="2026-02-12")

"How many warnings per source in the last hour?"
→ query_log_sql("SELECT source, count(*) FROM logs WHERE level = 'WARNING' AND ts::BIGINT >= epoch_ns((now() - INTERVAL '60 minutes')::TIMESTAMP AT TIME ZONE 'America/Chicago') GROUP BY source ORDER BY 2 DESC")
```

## How It Works

On startup, the server creates a DuckDB view:

```sql
CREATE OR REPLACE VIEW logs AS
SELECT * FROM read_ndjson('/tmp/app_logs/**/*.ndjson')
```

DuckDB auto-discovers the Hive partition columns (`date`, `source`) from the directory structure. The `query_logs` tool builds SQL with proper epoch-ns time filtering and timezone-aware display. `query_log_sql` passes SQL through directly for anything custom.

The server uses `stateless_http=True` so every request is independent — no server-side session state. This means you can restart the server anytime without breaking active Claude Code sessions.

## Key Design Decisions

**Why HTTP transport?** Decouples server lifecycle from Claude Code. You can restart the server after config changes without restarting Claude. Multiple Claude sessions share one server process.

**Why `stateless_http=True`?** Without it, FastMCP tracks sessions in memory. A server restart invalidates all existing sessions, causing "Session not found" errors. Since log queries are stateless, there's no reason for session tracking.

**Why a DuckDB view instead of lazy table expansion?** The portable logging doc shows a regex-based lazy expansion pattern (for projects with many tables). This server has exactly one table, so a DuckDB view is simpler and sufficient.

**Why a separate `query_log_sql` tool?** The `query_logs` tool covers 90% of use cases with a clean parameter interface. But log analysis often needs ad-hoc SQL (aggregations, joins, window functions) that no fixed parameter set can anticipate. Having both means the LLM uses structured parameters for common queries and falls back to raw SQL when needed.

## File Inventory

```
docs/portable/
├── ndjson-logging-with-duckdb.md    # The logging pattern (logger + DuckDB querying)
├── log-query-mcp-server.md          # This file (setup + usage guide)
└── log_query_mcp_server.py          # The server (drop into any project)
```
