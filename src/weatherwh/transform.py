"""Transform step: build the staging and marts layers from the raw layer, using the SQL in sql/.

Layers (table prefixes show which is which):
  stg_weather_hourly     one clean, typed row per city and hour
  dim_city               one row per configured city
  fct_weather_daily      one row per city and day
  fct_weather_monthly    one row per city and month

Every run rebuilds these tables completely from the raw layer. That is simple, always consistent
and fast at this size, and it means fixing a model is just editing its SQL and running again.
The whole rebuild is one transaction, so readers never see a half-built layer, and if any model
fails everything is rolled back and the previous tables stay as they were.

The SQL files are plain SQL with two placeholders for the only things that differ between DuckDB
(production) and SQLite (fast tests): how a text timestamp becomes a TIMESTAMP and a DATE.

Run:  python -m weatherwh.transform
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from string import Template

from weatherwh import config
from weatherwh.load import (
    LoadError,
    connect_warehouse,
    ensure_schema,
    finish_run,
    insert_rows,
    make_run_id,
    start_run,
    transaction,
)

log = logging.getLogger("weatherwh.transform")

# Build order matters: marts read from staging, and the monthly mart reads from the daily one.
MODEL_FILES = (
    "staging/stg_weather_hourly.sql",
    "marts/fct_weather_daily.sql",
    "marts/fct_weather_monthly.sql",
)
DIM_CITY = "dim_city"
BUILT_TABLES = ("stg_weather_hourly", DIM_CITY, "fct_weather_daily", "fct_weather_monthly")

# The raw timestamp looks like 2024-01-01T00:00. Normalised, it becomes 2024-01-01 00:00:00.
_NORMALISED = "REPLACE(observed_at_text, 'T', ' ') || ':00'"
DIALECTS = {
    "duckdb": {
        "observed_at_expr": f"CAST({_NORMALISED} AS TIMESTAMP)",
        "observed_date_expr": "CAST(SUBSTR(observed_at_text, 1, 10) AS DATE)",
    },
    "sqlite": {  # SQLite has no TIMESTAMP or DATE types, so these stay as sortable text
        "observed_at_expr": _NORMALISED,
        "observed_date_expr": "SUBSTR(observed_at_text, 1, 10)",
    },
}


def detect_dialect(conn) -> str:
    """Which SQL flavour the connection speaks. DuckDB's connection class lives in a module whose
    name varies between releases (for example "_duckdb"), so check both the module and the class name."""
    kind = type(conn)
    looks_like_duckdb = "duckdb" in kind.__module__.lower() or kind.__name__.lower().startswith("duckdb")
    return "duckdb" if looks_like_duckdb else "sqlite"


def render_sql(text: str, dialect: str) -> str:
    return Template(text).substitute(DIALECTS[dialect])


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements (comments removed, so they may not contain semicolons)."""
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [part.strip() for part in without_comments.split(";") if part.strip()]


def build_dim_city(conn) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {DIM_CITY}")
    conn.execute(f"CREATE TABLE {DIM_CITY} (city_slug VARCHAR, city_name VARCHAR, latitude DOUBLE, longitude DOUBLE)")
    insert_rows(conn, DIM_CITY, ["city_slug", "city_name", "latitude", "longitude"],
                [(c.slug, c.name, c.latitude, c.longitude) for c in config.CITIES])


@dataclass
class TransformSummary:
    run_id: str
    row_counts: dict = field(default_factory=dict)

    @property
    def message(self) -> str:
        return ", ".join(f"{table}={count}" for table, count in self.row_counts.items())


def run_transform(conn, now: datetime | None = None, sql_dir: Path = config.SQL_DIR,
                  dialect: str | None = None) -> TransformSummary:
    """Rebuild staging and marts from the raw layer, atomically, and record the run."""
    now = now or datetime.now(timezone.utc)
    dialect = dialect or detect_dialect(conn)
    summary = TransformSummary(run_id=make_run_id(now))
    log.info("Using the %s SQL dialect", dialect)

    ensure_schema(conn)
    start_run(conn, summary.run_id, "transform", now)
    try:
        with transaction(conn):
            for relative in MODEL_FILES:
                sql = render_sql((Path(sql_dir) / relative).read_text(encoding="utf-8"), dialect)
                for statement in split_statements(sql):
                    conn.execute(statement)
                log.info("Built %s", relative)
            build_dim_city(conn)
        for table in BUILT_TABLES:
            summary.row_counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except BaseException as exc:
        finish_run(conn, summary.run_id, "transform", "failed", message=f"{type(exc).__name__}: {exc}"[:500])
        raise
    finish_run(conn, summary.run_id, "transform", "success",
               rows_loaded=summary.row_counts["stg_weather_hourly"], message=summary.message)
    return summary


# ------------------------------------------------------------------------ CLI
def coverage(conn) -> list[tuple]:
    return conn.execute(
        "SELECT city_slug, MIN(observed_date), MAX(observed_date), COUNT(*), SUM(is_complete_day) "
        "FROM fct_weather_daily GROUP BY city_slug ORDER BY city_slug").fetchall()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build the staging and marts layers from the raw layer.")
    parser.add_argument("--db", type=Path, default=config.WAREHOUSE_PATH, help="DuckDB file to read and write")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        conn = connect_warehouse(args.db)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        summary = run_transform(conn)
        rows = coverage(conn)
    except (LoadError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    log.info("Run %s done: %s", summary.run_id, summary.message)
    print(f"\n{'city':<10} {'first day':<12} {'last day':<12} {'days':>5} {'complete':>9}")
    for city, first, last, days, complete in rows:
        print(f"{city:<10} {str(first):<12} {str(last):<12} {days:>5} {complete:>9}")
    return 0


if __name__ == "__main__":
    sys.exit(main())