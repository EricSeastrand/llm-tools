# SQL-Queryable Structured Logging with NDJSON + DuckDB

A pattern for writing application logs as structured NDJSON files that DuckDB can query directly — no log aggregation service, no Elasticsearch, no external infrastructure. Just files on disk and SQL.

## Why This Exists

Traditional text logs are easy to write and painful to analyze. When something goes wrong, you end up:

- Opening 6 log files to correlate events across components
- Writing throwaway grep pipelines to find state transitions
- Losing history to log rotation right when you need it
- Unable to aggregate ("how many errors per hour?") without custom scripts

This pattern fixes all of that by writing logs as **newline-delimited JSON** (NDJSON) into a **Hive-partitioned** directory structure that DuckDB reads natively. The result: full SQL access to your logs with zero infrastructure.

```sql
-- What was happening across ALL components during the outage?
SELECT ts_display, source, level, msg
FROM logs
WHERE date = '2026-01-30'
  AND ts >= epoch_ns('2026-01-30 15:05:00'::TIMESTAMP AT TIME ZONE 'America/Chicago')
  AND ts <  epoch_ns('2026-01-30 15:06:00'::TIMESTAMP AT TIME ZONE 'America/Chicago')
ORDER BY ts
```

## Architecture Overview

```
Application Code
    │
    ▼
Python logging module
    │
    ├── ConsoleHandler (INFO+, human-readable)
    │
    └── NdjsonFileHandler (DEBUG+, structured JSON)
            │
            ▼
        Hive-partitioned NDJSON files on disk
            │
            ▼
        DuckDB read_ndjson() ── zero-copy, SQL queryable
```

**Key insight**: DuckDB's `read_ndjson()` function reads NDJSON files directly — no import step, no ETL, no separate database process. You write a file; it's immediately queryable.

## Part 1: The Logger (Python)

### File Structure on Disk

```
{LOG_DIR}/
  date=2026-01-30/
    source=web_server/
      1738267200-691644-a7f2c3e1.ndjson
    source=task_worker/
      1738267200-691644-b2e4f6a8.ndjson
    source=scheduler/
      1738267200-691644-c9d1e3f5.ndjson
  date=2026-01-31/
    source=web_server/
      1738353600-712003-e1c3a5f7.ndjson
    ...
```

**Hive-style partitioning** (`key=value/` directory names) is a convention that DuckDB, Spark, and other engines recognize. DuckDB automatically extracts `date` and `source` as queryable columns from the directory names. This means:
- `WHERE date = '2026-01-30'` only scans files in that date directory (partition pruning)
- `WHERE source = 'web_server'` only scans that component's files
- No index needed — the filesystem IS the index

**File naming**: `{epoch}-{pid}-{uuid8}.ndjson`
- `epoch`: Unix timestamp when file was opened (chronological ordering)
- `pid`: Process ID (prevents concurrent process collisions)
- `uuid8`: First 8 chars of uuid4 (prevents PID-reuse collisions across restarts)
- Result: multiple processes/restarts can write to the same partition safely with zero coordination

### JSON Schema

Each line is a self-contained JSON object:

```json
{"ts": 1769841631866115981, "level": "WARNING", "source": "web_server", "pid": 691644, "file": "server.py", "line": 360, "func": "handle_request", "msg": "Request timeout after 30s"}
```

| Field    | Type   | Purpose |
|----------|--------|---------|
| `ts`     | int    | Epoch **nanoseconds** (UTC). Monotonically increasing — ties bumped by 1ns. |
| `level`  | string | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `source` | string | Logger/component name (matches partition directory) |
| `pid`    | int    | OS process ID |
| `file`   | string | Source filename (basename only — full paths waste ~50 bytes/line and are redundant) |
| `line`   | int    | Line number |
| `func`   | string | Function name |
| `msg`    | string | The log message |

**Why nanosecond epoch integers?**
1. **Monotonic ordering** — rapid-fire logs that land in the same microsecond get bumped by 1ns, so they sort correctly
2. **Fast filtering** — integer comparison in WHERE clauses, no per-row parsing
3. **No timezone ambiguity** — always UTC epoch; convert to local time only for display

### Implementation

This is the complete logger. It uses only the Python standard library — no external dependencies.

```python
"""Structured NDJSON logger with Hive-partitioned output for DuckDB querying.

Usage:
    from logger import get_logger
    logger = get_logger("my_component")
    logger.info("Server started on port 8080")

Logs go to:
    - Console: INFO+, human-readable
    - NDJSON file: DEBUG+, structured JSON, partitioned by date and source
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from io import TextIOWrapper
from zoneinfo import ZoneInfo

LOG_DIR = "/tmp/app_logs"  # Change to suit your project
LOCAL_TZ = ZoneInfo("America/Chicago")  # Change to your local timezone

CONSOLE_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


class NdjsonFileHandler(logging.Handler):
    """Writes structured NDJSON logs to date/source-partitioned files.

    File layout:
        {LOG_DIR}/date=YYYY-MM-DD/source={name}/{epoch}-{pid}-{uuid8}.ndjson

    Date rollover is detected on each emit(). Multiple processes can write
    concurrently without coordination (unique filenames per process).
    """

    def __init__(self, name: str):
        super().__init__(level=logging.DEBUG)
        self._name = name
        self._pid = os.getpid()
        self._current_date: str | None = None
        self._file: TextIOWrapper | None = None
        self._last_ts: int = 0
        self._open_file()

    def _make_path(self, date_str: str) -> str:
        uuid8 = uuid.uuid4().hex[:8]
        epoch = int(time.time())
        filename = f"{epoch}-{self._pid}-{uuid8}.ndjson"
        return os.path.join(LOG_DIR, f"date={date_str}", f"source={self._name}", filename)

    def _open_file(self) -> None:
        date_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
        path = self._make_path(date_str)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._file = open(path, "a")
        self._current_date = date_str

    def _close_file(self) -> None:
        if self._file:
            try:
                self._file.close()
            except IOError:
                pass
            self._file = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Monotonic guard: if clock returns same/earlier ns, bump by 1
            ts = time.time_ns()
            if ts <= self._last_ts:
                ts = self._last_ts + 1
            self._last_ts = ts

            # Date rollover check
            date_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
            if date_str != self._current_date:
                self._close_file()
                self._open_file()

            if not self._file:
                return

            line = json.dumps({
                "ts": ts,
                "level": record.levelname,
                "source": record.name,
                "pid": record.process,
                "file": record.filename,
                "line": record.lineno,
                "func": record.funcName,
                "msg": self.format(record) if self.formatter else record.getMessage(),
            })
            self._file.write(line + "\n")
            self._file.flush()
        except IOError:
            print(f"NdjsonFileHandler: failed to write log for {self._name}", file=sys.stderr)

    def close(self) -> None:
        self._close_file()
        super().close()


def get_logger(name: str = "app"):
    """Return a logger with console (INFO+) and NDJSON file (DEBUG+) handlers."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(CONSOLE_FORMAT))
        logger.addHandler(console)

        logger.addHandler(NdjsonFileHandler(name))

    return logger
```

### Usage in Application Code

```python
from logger import get_logger

logger = get_logger("web_server")

logger.debug("Detailed trace info")     # Goes to NDJSON file only
logger.info("Server started on :8080")  # Goes to both console and NDJSON
logger.warning("Slow query: 2.3s")      # Goes to both
logger.error("Connection refused")      # Goes to both
```

Zero changes to calling code compared to standard Python logging. Every module calls `get_logger("name")` and uses the normal logging API.

## Part 2: Querying with DuckDB

### Basic Setup

DuckDB reads NDJSON files directly with `read_ndjson()`. No import, no schema definition.

```python
import duckdb

LOG_DIR = "/tmp/app_logs"

conn = duckdb.connect()

# Query all logs (DuckDB auto-discovers the Hive partition columns)
df = conn.execute(f"""
    SELECT * FROM read_ndjson('{LOG_DIR}/**/*.ndjson')
    WHERE date = '2026-01-30'
    LIMIT 10
""").fetchdf()
```

### Making It Ergonomic: Table Alias

Typing `read_ndjson('{LOG_DIR}/**/*.ndjson')` everywhere is tedious. Create a view:

```python
conn.execute(f"""
    CREATE OR REPLACE VIEW logs AS
    SELECT * FROM read_ndjson('{LOG_DIR}/**/*.ndjson')
""")

# Now just use "logs" in all queries
df = conn.execute("SELECT * FROM logs WHERE date = '2026-01-30' LIMIT 10").fetchdf()
```

Or, if you have a query helper that preprocesses SQL, you can do string replacement — expand `logs` to the `read_ndjson(...)` call automatically. This is what we do in production (see the "Advanced: Lazy Table Expansion" section below).

### Timestamp Handling (Critical!)

The `ts` field is **epoch nanoseconds** (a BIGINT). This is intentional — integer comparisons are fast and unambiguous.

**Rule: Convert the TARGET time to nanoseconds. Never convert `ts` to a datetime per row.**

```sql
-- CORRECT: Convert target to nanoseconds ONCE, compare as integers
WHERE ts::BIGINT >= epoch_ns('2026-01-30 15:05:00'::TIMESTAMP AT TIME ZONE 'America/Chicago')
  AND ts::BIGINT <  epoch_ns('2026-01-30 15:06:00'::TIMESTAMP AT TIME ZONE 'America/Chicago')

-- WRONG: Converts ts to datetime on EVERY ROW (slow!)
WHERE make_timestamp(ts::BIGINT // 1000) > '2026-01-30 15:05:00'
```

**For display** (in SELECT, never in WHERE):
```sql
SELECT
    make_timestamp(ts::BIGINT // 1000)
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Chicago' as ts_display,
    level, source, msg
FROM logs
```

The `// 1000` converts nanoseconds to microseconds (what `make_timestamp` expects). The double `AT TIME ZONE` first declares the value as UTC, then converts to local.

### Query Cookbook

**All logs from a specific date and component:**
```sql
SELECT ts::BIGINT as ts, level, func, msg
FROM logs
WHERE date = '2026-01-30' AND source = 'web_server'
ORDER BY ts
```

**Cross-component correlation during an incident window:**
```sql
SELECT
    make_timestamp(ts::BIGINT // 1000)
        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' as ts_display,
    source, level, func, msg
FROM logs
WHERE date = '2026-01-30'
  AND ts::BIGINT >= epoch_ns('2026-01-30 15:05:20'::TIMESTAMP AT TIME ZONE 'America/Chicago')
  AND ts::BIGINT <= epoch_ns('2026-01-30 15:05:30'::TIMESTAMP AT TIME ZONE 'America/Chicago')
ORDER BY ts::BIGINT
```

**Error summary by component:**
```sql
SELECT source, func, count(*) as n
FROM logs
WHERE date = '2026-01-30' AND level = 'ERROR'
GROUP BY source, func
ORDER BY n DESC
```

**Find state transitions** (when did a value change?):
```sql
WITH stats AS (
    SELECT ts::BIGINT as ts,
           json_extract_string(msg, '$.total_records')::INT as records
    FROM logs
    WHERE date = '2026-01-30' AND source = 'task_worker' AND func = '_log_stats'
)
SELECT * FROM stats
WHERE records != LAG(records) OVER (ORDER BY ts)
ORDER BY ts DESC LIMIT 5
```

**Data rate over time** (5-minute buckets):
```sql
SELECT
    make_timestamp((ts::BIGINT // 300000000000) * 300000000000 // 1000)
        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' as bucket,
    count(*) as log_lines
FROM logs
WHERE date = '2026-01-30' AND source = 'web_server'
GROUP BY 1 ORDER BY 1
```

**Recent logs** (last N minutes):
```sql
SELECT
    make_timestamp(ts::BIGINT // 1000)
        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' as ts_display,
    level, source, func, msg
FROM logs
WHERE ts::BIGINT >= epoch_ns(
    (now() - INTERVAL '30 minutes')::TIMESTAMP AT TIME ZONE 'America/Chicago'
)
ORDER BY ts::BIGINT DESC
LIMIT 100
```

**Discover available log sources:**
```sql
SELECT
    source, date,
    COUNT(*) as entries,
    MIN(make_timestamp(ts::BIGINT // 1000)
        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as earliest,
    MAX(make_timestamp(ts::BIGINT // 1000)
        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as latest
FROM logs
GROUP BY source, date
ORDER BY date DESC, entries DESC
```

## Part 3: Log Cleanup

Date partitioning makes retention trivial — just delete old date directories.

```python
"""Delete log partitions older than a retention threshold.

Usage:
    python log_cleanup.py              # dry-run, 14-day default
    python log_cleanup.py --apply      # actually delete
    python log_cleanup.py --days 7     # custom retention
"""

import argparse
import shutil
from datetime import date, timedelta
from pathlib import Path

LOG_DIR = "/tmp/app_logs"  # Must match the logger's LOG_DIR
DEFAULT_RETENTION_DAYS = 14


def cleanup_old_logs(retention_days: int = DEFAULT_RETENTION_DAYS, dry_run: bool = True) -> list[str]:
    """Remove date-partitioned log directories older than retention_days."""
    cutoff = date.today() - timedelta(days=retention_days)
    log_root = Path(LOG_DIR)

    if not log_root.exists():
        return []

    deleted = []
    for entry in sorted(log_root.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("date="):
            continue

        date_str = entry.name.removeprefix("date=")
        try:
            partition_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        if partition_date >= cutoff:
            continue

        if dry_run:
            print(f"[DRY RUN] would delete: {entry}")
        else:
            shutil.rmtree(entry)
            print(f"deleted: {entry}")

        deleted.append(str(entry))

    return deleted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up old NDJSON log partitions")
    parser.add_argument("--days", type=int, default=DEFAULT_RETENTION_DAYS,
                        help=f"Keep logs from the last N days (default: {DEFAULT_RETENTION_DAYS})")
    parser.add_argument("--apply", action="store_true",
                        help="Actually delete (default is dry-run)")
    args = parser.parse_args()

    deleted = cleanup_old_logs(retention_days=args.days, dry_run=not args.apply)

    if not deleted:
        print(f"Nothing to clean up (retention: {args.days} days)")
    elif not args.apply:
        print(f"\n{len(deleted)} partition(s) would be deleted. Run with --apply to confirm.")
```

You can also call `cleanup_old_logs()` from a long-running daemon to self-clean periodically.

## Part 4: Advanced — Lazy Table Expansion

If you use DuckDB as your project's primary query engine, you can make `logs` (and other large datasets) act like normal table names by expanding them to `read_ndjson(...)` calls automatically at query time.

The idea: write a thin wrapper around `duckdb.execute()` that does regex replacement of known table names before the query reaches DuckDB.

```python
import re
import duckdb

# Registry of "lazy" tables — names that expand to reader functions
LAZY_TABLES = {
    "logs": {
        "path": "/tmp/app_logs/**/*.ndjson",
        "reader": "read_ndjson",
    },
    # You can add parquet tables too:
    # "events": {
    #     "path": "/data/events/**/*.parquet",
    #     "reader": "read_parquet",
    # },
}

def expand_lazy_tables(sql: str) -> str:
    """Replace lazy table names with reader function calls."""
    for name, config in LAZY_TABLES.items():
        pattern = r'\b' + re.escape(name) + r'\b'
        replacement = f"{config['reader']}('{config['path']}')"
        sql = re.sub(pattern, replacement, sql)
    return sql

def query(sql: str, params=None):
    """Execute SQL with automatic lazy table expansion."""
    expanded = expand_lazy_tables(sql)
    conn = duckdb.connect()
    return conn.execute(expanded, params).fetchdf()

# Now queries look clean:
df = query("SELECT * FROM logs WHERE date = '2026-01-30' AND level = 'ERROR'")
```

This keeps your SQL clean (`FROM logs`) while the expansion handles the verbose `read_ndjson(...)` call transparently.

## Design Decisions & Trade-offs

### Why NDJSON over structured Parquet logs?
Parquet requires a write-then-close cycle (columnar format can't be appended to). NDJSON is append-friendly — each line is independent. You get the queryability of structured data with the simplicity of appending to a file. DuckDB reads both formats natively.

### Why epoch nanoseconds over ISO timestamps?
- **Monotonic ordering**: `time.time_ns()` with collision bumping guarantees unique, ordered timestamps even for rapid-fire logs
- **Fast comparison**: Integer `>=` is the fastest WHERE predicate DuckDB can evaluate
- **No timezone bugs**: UTC epoch is unambiguous; local time conversion happens only at display time

### Why Hive partitioning over flat directories?
- **Partition pruning**: `WHERE date = '2026-01-30'` skips all other directories entirely
- **Auto-extracted columns**: DuckDB adds `date` and `source` columns from directory names for free
- **Trivial retention**: `rm -rf date=2026-01-15/` deletes exactly one day

### Why basename-only filenames?
Full paths (`/home/app/src/services/auth/handler.py`) are 50-70 bytes and identical across all log lines from the same module. The basename (`handler.py`) combined with `source` and `func` is unambiguous. At hundreds of thousands of lines per day, this saves megabytes.

### What you lose
- **Human-readable log files**: NDJSON isn't pleasant to `cat`. Use `tail -f *.ndjson | python -m json.tool` for quick tailing, or just query with DuckDB.
- **Size caps**: No `RotatingFileHandler` equivalent — files grow unbounded within a day. At ~300 bytes/line, even high-volume components produce manageable files (10s of MB/day). The cleanup script handles long-term growth.

## Dependencies

- **Python**: 3.9+ (for `ZoneInfo`; use `pytz` on 3.8)
- **DuckDB**: `pip install duckdb` (the only external dependency)
- **No log aggregation service** — that's the point

## Quick Start Checklist

1. Copy the logger module into your project
2. Set `LOG_DIR` and `LOCAL_TZ` to match your environment
3. Replace existing logging setup with `get_logger("component_name")`
4. Query logs with DuckDB: `SELECT * FROM read_ndjson('{LOG_DIR}/**/*.ndjson') WHERE date = '2026-02-12' LIMIT 10`
5. (Optional) Set up the cleanup script on a cron or call it from your daemon
6. (Optional) Create a DuckDB view or use lazy table expansion for cleaner SQL
