"""Portable MCP server for querying NDJSON structured logs via DuckDB.

Pairs with the logging guide: ndjson-logging-with-duckdb.md
Drop this into any project using the NDJSON logging pattern and go.

Dependencies:
    pip install duckdb fastmcp

Usage:
    # Start the server (use tmux for persistence):
    tmux new-session -d -s log-mcp 'python log_query_mcp_server.py'

    # Register with Claude Code (one-time):
    claude mcp add logs --transport http http://localhost:8001/mcp

    # Restart after config changes:
    tmux kill-session -t log-mcp
    tmux new-session -d -s log-mcp 'python log_query_mcp_server.py'
"""

import duckdb
from fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration — change these to match your project
# ---------------------------------------------------------------------------
LOG_DIR = "/tmp/app_logs"       # Must match the logger's LOG_DIR
TIMEZONE = "America/Chicago"    # Display timezone for timestamps
PORT = 8001                     # Server port

# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------
_conn = duckdb.connect()
_conn.execute(f"""
    CREATE OR REPLACE VIEW logs AS
    SELECT * FROM read_ndjson('{LOG_DIR}/**/*.ndjson')
""")

mcp = FastMCP("logs", stateless_http=True)


# ---------------------------------------------------------------------------
# Tool: query_logs
# ---------------------------------------------------------------------------
@mcp.tool
def query_logs(
    minutes_ago: int = 60,
    source: str | None = None,
    level: str | None = None,
    keyword: str | None = None,
    date: str | None = None,
    limit: int = 100,
) -> str:
    """Query structured application logs with proper timestamp handling.

    Handles epoch nanosecond timestamps and timezone conversions automatically.
    Logs are Hive-partitioned by date and source.

    Log fields: ts (epoch ns), level, source, pid, file, line, func, msg.

    Args:
        minutes_ago: How many minutes back to search (default 60). Ignored if date is set.
        source: Filter by log source name (Hive partition, fast filter).
        level: Filter by log level (DEBUG, INFO, WARNING, ERROR).
        keyword: Text search in the msg field (case-insensitive).
        date: Specific date to query (YYYY-MM-DD). Uses Hive partition for speed.
              If not set, uses minutes_ago relative to now.
        limit: Maximum rows to return (default 100).

    Returns:
        Formatted log entries with local-time timestamps.

    Examples:
        query_logs(minutes_ago=30, source="web_server")
        query_logs(level="ERROR", minutes_ago=120)
        query_logs(date="2026-01-30", source="task_worker", keyword="timeout")
    """
    where = []

    if date:
        where.append(f"date = '{date}'")
    else:
        where.append(
            f"ts::BIGINT >= epoch_ns("
            f"(now() - INTERVAL '{minutes_ago} minutes')"
            f"::TIMESTAMP AT TIME ZONE '{TIMEZONE}')"
        )

    if source:
        where.append(f"source = '{source}'")
    if level:
        where.append(f"level = '{level.upper()}'")
    if keyword:
        safe = keyword.replace("'", "''")
        where.append(f"msg ILIKE '%{safe}%'")

    query = f"""
        SELECT
            make_timestamp(ts::BIGINT // 1000)
                AT TIME ZONE 'UTC'
                AT TIME ZONE '{TIMEZONE}' as ts_local,
            level, source, func, msg
        FROM logs
        WHERE {' AND '.join(where)}
        ORDER BY ts::BIGINT DESC
        LIMIT {limit}
    """

    df = _conn.execute(query).fetchdf()

    if df.empty:
        return "No log entries found matching the criteria."
    return f"Log entries: {len(df)}\n\n{df.to_string()}"


# ---------------------------------------------------------------------------
# Tool: list_log_sources
# ---------------------------------------------------------------------------
@mcp.tool
def list_log_sources(date: str | None = None) -> str:
    """List available log sources and their entry counts.

    Useful for discovering what log sources exist before querying.

    Args:
        date: Optional date filter (YYYY-MM-DD). If not set, shows all dates.

    Returns:
        Table of log sources with entry counts and time ranges.
    """
    date_filter = f"WHERE date = '{date}'" if date else ""

    query = f"""
        SELECT
            source, date,
            COUNT(*) as entries,
            MIN(make_timestamp(ts::BIGINT // 1000)
                AT TIME ZONE 'UTC' AT TIME ZONE '{TIMEZONE}') as earliest,
            MAX(make_timestamp(ts::BIGINT // 1000)
                AT TIME ZONE 'UTC' AT TIME ZONE '{TIMEZONE}') as latest
        FROM logs
        {date_filter}
        GROUP BY source, date
        ORDER BY date DESC, entries DESC
    """

    df = _conn.execute(query).fetchdf()

    if df.empty:
        return "No log data found."
    return f"Log sources:\n\n{df.to_string()}"


# ---------------------------------------------------------------------------
# Tool: query_log_sql
# ---------------------------------------------------------------------------
@mcp.tool
def query_log_sql(query: str) -> str:
    """Run arbitrary SQL against the logs.

    A `logs` view is pre-registered pointing at the NDJSON log directory.
    Use this for ad-hoc queries not covered by query_logs.

    The `ts` field is epoch nanoseconds (BIGINT). For filtering by time,
    convert the target to nanoseconds:
        WHERE ts::BIGINT >= epoch_ns('2026-01-30 15:00:00'::TIMESTAMP AT TIME ZONE 'America/Chicago')

    For display, convert to human-readable:
        SELECT make_timestamp(ts::BIGINT // 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago'

    Args:
        query: SQL query. The `logs` table/view is available.

    Returns:
        Query results as a formatted table.

    Examples:
        query_log_sql("SELECT source, level, count(*) FROM logs WHERE date = '2026-01-30' GROUP BY ALL")
        query_log_sql("SELECT * FROM logs WHERE func = 'handle_request' AND level = 'ERROR' LIMIT 20")
    """
    df = _conn.execute(query).fetchdf()

    if df.empty:
        return "No results returned."

    row_count = len(df)
    if row_count > 200:
        display = df.head(200).to_string()
        return f"Rows: {row_count} (showing first 200)\n\n{display}"

    return f"Rows: {row_count}\n\n{df.to_string()}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=PORT)
