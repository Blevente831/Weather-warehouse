"""Load step: copy landed JSON files into the database's raw layer.

Tables created here:
  raw_weather_hourly  one row per city and hour, values exactly as delivered
                      (the timestamp stays text; cleaning and typing happen in staging)
  raw_weather_files   one row per loaded file: its content hash, row count and lineage
  pipeline_runs       one row per run of a step, with counts and a status

Properties:
  * Incremental: a file whose content hash is already recorded is skipped.
  * Idempotent: reloading a changed file replaces that file's rows, never duplicates them.
  * Atomic per file: a file's rows and its record are committed together or not at all.
  * Isolated failures: one bad file is reported, and the others still load.

The SQL is deliberately plain (portable between DuckDB and SQLite) and every function takes
the connection as an argument, so the logic is tested on SQLite while production uses DuckDB.

Run:  python -m weatherwh.load
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from weatherwh import config
from weatherwh.extract import validate_payload

log = logging.getLogger("weatherwh.load")

HOURLY_TABLE = "raw_weather_hourly"
FILES_TABLE = "raw_weather_files"
RUNS_TABLE = "pipeline_runs"

FILES_COLUMNS = ["source_file", "city_slug", "start_date", "end_date", "fetched_at",
                 "file_sha256", "row_count", "loaded_at", "run_id"]
RUNS_COLUMNS = ["run_id", "step", "started_at", "finished_at", "status",
                "files_loaded", "files_skipped", "files_failed", "rows_loaded", "message"]

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class LoadError(Exception):
    """A problem with the database or its setup (as opposed to one bad file)."""


# --------------------------------------------------------------------- schema
def hourly_columns() -> list[str]:
    """Column names of the hourly table, derived from the configured variables."""
    for name in config.HOURLY_VARIABLES:
        if not _IDENTIFIER.match(name):  # these names are put into SQL text, so they must be safe
            raise LoadError(f"unsafe column name in config: {name!r}")
    return ["city_slug", "observed_at", *config.HOURLY_VARIABLES,
            "source_file", "fetched_at", "loaded_at", "run_id"]


def ddl_statements() -> list[str]:
    value_columns = ",\n    ".join(f"{name} DOUBLE" for name in config.HOURLY_VARIABLES)
    return [
        f"""CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
    city_slug VARCHAR,
    observed_at VARCHAR,
    {value_columns},
    source_file VARCHAR,
    fetched_at VARCHAR,
    loaded_at VARCHAR,
    run_id VARCHAR
)""",
        f"""CREATE TABLE IF NOT EXISTS {FILES_TABLE} (
    source_file VARCHAR,
    city_slug VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    fetched_at VARCHAR,
    file_sha256 VARCHAR,
    row_count INTEGER,
    loaded_at VARCHAR,
    run_id VARCHAR
)""",
        f"""CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    run_id VARCHAR,
    step VARCHAR,
    started_at VARCHAR,
    finished_at VARCHAR,
    status VARCHAR,
    files_loaded INTEGER,
    files_skipped INTEGER,
    files_failed INTEGER,
    rows_loaded INTEGER,
    message VARCHAR
)""",
    ]


def table_columns(conn, table: str) -> list[str]:
    return [d[0] for d in conn.execute(f"SELECT * FROM {table} LIMIT 0").description]


def ensure_schema(conn) -> None:
    """Create the tables if needed, and refuse to continue if an existing table no longer matches."""
    for statement in ddl_statements():
        conn.execute(statement)
    expected = {HOURLY_TABLE: hourly_columns(), FILES_TABLE: FILES_COLUMNS, RUNS_TABLE: RUNS_COLUMNS}
    for table, columns in expected.items():
        found = table_columns(conn, table)
        if found != columns:
            raise LoadError(
                f"table {table} has columns {found}, expected {columns}. The configured variables changed; "
                "drop the table (the raw files are the source of truth) or add a migration."
            )


# ---------------------------------------------------------------- SQL helpers
@contextmanager
def transaction(conn):
    """BEGIN ... COMMIT, or ROLLBACK if anything inside raises."""
    conn.execute("BEGIN")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def insert_rows(conn, table: str, columns: list[str], rows: list[tuple], chunk: int = 200) -> None:
    """Insert rows with multi-row INSERT statements (much faster than one statement per row)."""
    if not rows:
        return
    one_row = "(" + ", ".join("?" * len(columns)) + ")"
    column_list = ", ".join(columns)
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        sql = f"INSERT INTO {table} ({column_list}) VALUES " + ", ".join([one_row] * len(part))
        conn.execute(sql, [value for row in part for value in row])


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------- run log
@dataclass
class LoadSummary:
    run_id: str
    loaded: list[str] = field(default_factory=list)
    skipped: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)
    rows: int = 0

    @property
    def status(self) -> str:
        if not self.failed:
            return "success"
        # "failed" only when nothing at all succeeded; otherwise some files are fine or already up to date
        return "partial" if (self.loaded or self.skipped) else "failed"

    @property
    def message(self) -> str:
        text = "; ".join(f"{name}: {reason}" for name, reason in self.failed)
        return text[:500]


def make_run_id(now: datetime) -> str:
    return f"{now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def start_run(conn, run_id: str, step: str, now: datetime) -> None:
    insert_rows(conn, RUNS_TABLE, RUNS_COLUMNS,
                [(run_id, step, iso(now), None, "running", None, None, None, None, None)])


def finish_run(conn, run_id: str, step: str, status: str, files_loaded=None, files_skipped=None,
               files_failed=None, rows_loaded=None, message=None) -> None:
    conn.execute(
        f"UPDATE {RUNS_TABLE} SET finished_at = ?, status = ?, files_loaded = ?, files_skipped = ?, "
        "files_failed = ?, rows_loaded = ?, message = ? WHERE run_id = ? AND step = ?",
        [iso(datetime.now(timezone.utc)), status, files_loaded, files_skipped, files_failed,
         rows_loaded, message, run_id, step],
    )


# -------------------------------------------------------------------- loading
def build_rows(envelope: dict, source_file: str, loaded_at: str, run_id: str) -> list[tuple]:
    """Turn the API's column-wise arrays into one row per hour, adding lineage columns."""
    meta, hourly = envelope["meta"], envelope["payload"]["hourly"]
    return [
        (meta["slug"], stamp, *[hourly[v][i] for v in config.HOURLY_VARIABLES],
         source_file, meta["fetched_at"], loaded_at, run_id)
        for i, stamp in enumerate(hourly["time"])
    ]


def load_one(conn, path: Path, source_file: str, known: dict, run_id: str, loaded_at: str) -> int | None:
    """Load one file. Returns the number of rows loaded, or None if it was already loaded unchanged."""
    raw_bytes = path.read_bytes()
    digest = hashlib.sha256(raw_bytes).hexdigest()
    if known.get(source_file) == digest:
        return None

    envelope = json.loads(raw_bytes.decode("utf-8"))
    meta = envelope["meta"]
    validate_payload(envelope["payload"], date.fromisoformat(meta["start_date"]),
                     date.fromisoformat(meta["end_date"]), config.HOURLY_VARIABLES)
    rows = build_rows(envelope, source_file, loaded_at, run_id)

    with transaction(conn):  # the old rows go and the new rows arrive together, or nothing changes
        conn.execute(f"DELETE FROM {HOURLY_TABLE} WHERE source_file = ?", [source_file])
        conn.execute(f"DELETE FROM {FILES_TABLE} WHERE source_file = ?", [source_file])
        insert_rows(conn, HOURLY_TABLE, hourly_columns(), rows)
        insert_rows(conn, FILES_TABLE, FILES_COLUMNS,
                    [(source_file, meta["slug"], meta["start_date"], meta["end_date"], meta["fetched_at"],
                      digest, len(rows), loaded_at, run_id)])
    known[source_file] = digest
    return len(rows)


def load_files(conn, raw_dir: Path = config.RAW_DIR, now: datetime | None = None) -> LoadSummary:
    """Load every new or changed file under raw_dir. One bad file does not stop the others."""
    now = now or datetime.now(timezone.utc)
    raw_dir = Path(raw_dir)
    summary = LoadSummary(run_id=make_run_id(now))

    ensure_schema(conn)
    start_run(conn, summary.run_id, "load", now)
    try:
        known = dict(conn.execute(f"SELECT source_file, file_sha256 FROM {FILES_TABLE}").fetchall())
        for path in sorted(raw_dir.glob("*/*.json")):
            name = path.relative_to(raw_dir).as_posix()  # same name on every operating system
            try:
                rows = load_one(conn, path, name, known, summary.run_id, iso(now))
            except Exception as exc:  # isolate the failure to this file and carry on
                summary.failed.append((name, f"{type(exc).__name__}: {exc}"))
                log.error("Failed %s: %s", name, exc)
                continue
            if rows is None:
                summary.skipped += 1
            else:
                summary.loaded.append(name)
                summary.rows += rows
                log.info("Loaded %s (%d rows)", name, rows)
    except BaseException as exc:
        finish_run(conn, summary.run_id, "load", "failed", len(summary.loaded), summary.skipped,
                   len(summary.failed), summary.rows, f"aborted: {type(exc).__name__}: {exc}"[:500])
        raise
    finish_run(conn, summary.run_id, "load", summary.status, len(summary.loaded), summary.skipped,
               len(summary.failed), summary.rows, summary.message)
    return summary


# ------------------------------------------------------------------------ CLI
def connect_warehouse(path: Path = config.WAREHOUSE_PATH):
    try:
        import duckdb
    except ImportError as exc:
        raise LoadError("DuckDB is not installed. Run: pip install -r requirements.txt") from exc
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Load landed weather files into the DuckDB raw layer.")
    parser.add_argument("--db", type=Path, default=config.WAREHOUSE_PATH, help="DuckDB file to write")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR, help="folder with the landed JSON files")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        conn = connect_warehouse(args.db)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        summary = load_files(conn, args.raw_dir)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    log.info("Run %s: %d files loaded (%d rows), %d unchanged, %d failed",
             summary.run_id, len(summary.loaded), summary.rows, summary.skipped, len(summary.failed))
    for name, reason in summary.failed:
        log.error("  %s: %s", name, reason)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())