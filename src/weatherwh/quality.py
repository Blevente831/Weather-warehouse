"""Data quality checks, run after the transform step.

Every check is a small named rule that counts violations (0 means it passes) and, when it
fails, shows a few example rows. A check has a severity:

  error  the data cannot be trusted (duplicates, impossible values, gaps, stale data, a failed
         earlier step). Any failing error check makes the whole run fail.
  warn   something looks off but the data is still usable. Reported, but does not fail the run.

A check that cannot even run (for example, its table is missing) is recorded as "error" and
also fails the run, because a rule that silently does not run protects nothing.

Results are stored in quality_results (one row per check per run, so history builds up), and the
run is logged in pipeline_runs. The command exits with a non-zero code when the run fails, so a
scheduler can stop or alert.

Run:  python -m weatherwh.quality
"""

import argparse
import logging
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from weatherwh import config
from weatherwh.load import (
    HOURLY_TABLE,
    RUNS_TABLE,
    LoadError,
    connect_warehouse,
    ensure_schema,
    finish_run,
    insert_rows,
    make_run_id,
    start_run,
    table_columns,
    transaction,
)

log = logging.getLogger("weatherwh.quality")

RESULTS_TABLE = "quality_results"
RESULT_COLUMNS = ["run_id", "checked_at", "check_name", "table_name", "severity", "status", "violations", "message"]
SAMPLE_SIZE = 5
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


# ----------------------------------------------------------------- structures
@dataclass(frozen=True)
class Context:
    today: date
    now: datetime


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    severity: str  # "error" or "warn"
    description: str
    run: Callable  # run(conn, context) -> (number of violations, detail text)


@dataclass
class CheckResult:
    name: str
    table: str
    severity: str
    description: str
    status: str  # "pass", "fail" or "error" (the check could not run)
    violations: int | None
    message: str


@dataclass
class QualitySummary:
    run_id: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failed_errors(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "fail" and r.severity == "error"]

    @property
    def crashed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "error"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "fail" and r.severity == "warn"]

    @property
    def passed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "pass"]

    @property
    def ok(self) -> bool:
        return not self.failed_errors and not self.crashed

    @property
    def message(self) -> str:
        text = (f"{len(self.results)} checks: {len(self.passed)} passed, {len(self.failed_errors)} failed, "
                f"{len(self.warnings)} warnings, {len(self.crashed)} could not run")
        names = [r.name for r in self.failed_errors + self.crashed]
        return text + (f"; failing: {', '.join(names)}" if names else "")


# -------------------------------------------------------------------- helpers
def _scalar(conn, sql: str, params=()):
    return conn.execute(sql, list(params)).fetchone()[0]


def to_datetime(value) -> datetime:
    """Timestamps arrive as datetime objects (DuckDB) or as text (SQLite)."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value))


def to_date(value) -> date:
    return to_datetime(value).date()


def format_rows(rows) -> str:
    text = "; ".join(str(tuple(row)) for row in rows)
    return text if len(text) <= 240 else text[:237] + "..."


def where_check(name, table, severity, description, where, params=(), columns="*") -> Check:
    """A check that counts the rows of one table matching a WHERE clause (the violations)."""
    def run(conn, ctx):
        count = _scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
        if not count:
            return 0, ""
        sample = conn.execute(f"SELECT {columns} FROM {table} WHERE {where} LIMIT {SAMPLE_SIZE}",
                              list(params)).fetchall()
        return count, f"e.g. {format_rows(sample)}"
    return Check(name, table, severity, description, run)


def compare_counts(label_a: str, count_a: int, label_b: str, count_b: int):
    """Two counts that must be equal. The violation count is how far apart they are."""
    gap = abs(count_a - count_b)
    return gap, (f"{label_a} has {count_a}, {label_b} has {count_b}" if gap else "")


# ------------------------------------------------------------- python checks
def check_raw_not_empty(conn, ctx):
    rows = _scalar(conn, f"SELECT COUNT(*) FROM {HOURLY_TABLE}")
    return (0, "") if rows else (1, "the raw layer has no rows; nothing has been loaded")


def check_staging_matches_raw(conn, ctx):
    keys = _scalar(conn, f"SELECT COUNT(*) FROM (SELECT city_slug, observed_at FROM {HOURLY_TABLE} "
                         "GROUP BY city_slug, observed_at) AS keys")
    return compare_counts("raw (distinct city-hours)", keys, "staging", _scalar(conn, "SELECT COUNT(*) FROM stg_weather_hourly"))


def check_daily_matches_staging(conn, ctx):
    keys = _scalar(conn, "SELECT COUNT(*) FROM (SELECT city_slug, observed_date FROM stg_weather_hourly "
                         "GROUP BY city_slug, observed_date) AS keys")
    return compare_counts("staging (distinct city-days)", keys, "fct_weather_daily",
                          _scalar(conn, "SELECT COUNT(*) FROM fct_weather_daily"))


def check_monthly_matches_daily(conn, ctx):
    keys = _scalar(conn, "SELECT COUNT(*) FROM (SELECT city_slug, observed_month FROM fct_weather_daily "
                         "GROUP BY city_slug, observed_month) AS keys")
    return compare_counts("fct_weather_daily (distinct city-months)", keys, "fct_weather_monthly",
                          _scalar(conn, "SELECT COUNT(*) FROM fct_weather_monthly"))


def check_hour_coverage(conn, ctx):
    """Between a city's first and last hour, every hour must be present (no missing months or days)."""
    rows = conn.execute("SELECT city_slug, MIN(observed_at), MAX(observed_at), COUNT(*) "
                        "FROM stg_weather_hourly GROUP BY city_slug ORDER BY city_slug").fetchall()
    problems, total = [], 0
    for city, first, last, count in rows:
        expected = int((to_datetime(last) - to_datetime(first)).total_seconds() // 3600) + 1
        gap = expected - count
        if gap:
            total += abs(gap)
            problems.append(f"{city}: {count} rows where {expected} were expected between {first} and {last}")
    return total, "; ".join(problems)


def check_freshness(conn, ctx):
    """Each configured city must have data up to the date the archive can currently provide."""
    latest = dict(conn.execute("SELECT city_slug, MAX(observed_date) FROM stg_weather_hourly "
                               "GROUP BY city_slug").fetchall())
    oldest_acceptable = ctx.today - timedelta(days=config.ARCHIVE_LAG_DAYS + config.FRESHNESS_SLACK_DAYS)
    problems = []
    for city in config.CITIES:
        seen = latest.get(city.slug)
        if seen is None:
            problems.append(f"{city.slug}: no data at all")
        elif to_date(seen) < oldest_acceptable:
            problems.append(f"{city.slug}: latest day is {to_date(seen)}, expected {oldest_acceptable} or later")
    return len(problems), "; ".join(problems)


def check_null_temperature_share(conn, ctx):
    rows = conn.execute("SELECT city_slug, SUM(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END), COUNT(*) "
                        "FROM stg_weather_hourly GROUP BY city_slug ORDER BY city_slug").fetchall()
    problems = []
    for city, nulls, total in rows:
        share = int(nulls) / total if total else 0.0
        if share > config.MAX_NULL_SHARE:
            problems.append(f"{city}: {share:.1%} of temperatures are missing")
    return len(problems), "; ".join(problems)


def _latest_runs(conn) -> dict:
    """The most recent run of the extract, load and transform steps: step -> (status, started_at)."""
    rows = conn.execute(f"SELECT step, status, started_at, finished_at FROM {RUNS_TABLE} "
                        "WHERE step IN ('extract', 'load', 'transform')").fetchall()
    latest = {}
    for step, status, started, finished in rows:
        key = (started or "", finished or "~")  # a run with no finish time is the newest
        if step not in latest or key >= latest[step][0]:  # on an exact tie the later-inserted row wins
            latest[step] = (key, status, started)
    return {step: (status, started) for step, (_, status, started) in latest.items()}


def check_latest_runs_not_failed(conn, ctx):
    problems = []
    for step, (status, started) in sorted(_latest_runs(conn).items()):
        if status == "failed":
            problems.append(f"the latest {step} run failed (started {started})")
        elif status == "running":
            begun = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if ctx.now - begun > timedelta(hours=config.STALE_RUN_HOURS):
                problems.append(f"the latest {step} run (started {started}) never finished")
    return len(problems), "; ".join(problems)


def check_latest_runs_not_partial(conn, ctx):
    problems = [f"the latest {step} run was partial (some files failed)"
                for step, (status, _) in sorted(_latest_runs(conn).items()) if status == "partial"]
    return len(problems), "; ".join(problems)


def check_duplicate_city_hours(conn, ctx):
    sql = ("FROM stg_weather_hourly GROUP BY city_slug, observed_at HAVING COUNT(*) > 1")
    count = _scalar(conn, f"SELECT COUNT(*) FROM (SELECT city_slug, observed_at {sql}) AS duplicates")
    if not count:
        return 0, ""
    sample = conn.execute(f"SELECT city_slug, observed_at, COUNT(*) {sql} LIMIT {SAMPLE_SIZE}").fetchall()
    return count, f"e.g. (city, hour, copies): {format_rows(sample)}"


# ------------------------------------------------------------------ registry
def all_checks() -> list[Check]:
    checks = [
        Check("raw_not_empty", HOURLY_TABLE, "error", "The raw layer contains data", check_raw_not_empty),
        Check("staging_unique_city_hour", "stg_weather_hourly", "error",
              "At most one row per city and hour", check_duplicate_city_hours),
        where_check("staging_no_null_keys", "stg_weather_hourly", "error", "City and timestamp are never missing",
                    "city_slug IS NULL OR observed_at IS NULL OR observed_date IS NULL",
                    columns="city_slug, observed_at, observed_date"),
        where_check("staging_known_cities", "stg_weather_hourly", "error", "Every city is a configured city",
                    "city_slug NOT IN (SELECT city_slug FROM dim_city)", columns="city_slug, observed_at"),
        where_check("staging_hour_valid", "stg_weather_hourly", "error", "The hour of day is between 0 and 23",
                    "observed_hour < 0 OR observed_hour > 23", columns="city_slug, observed_at, observed_hour"),
    ]
    for column, (low, high) in config.QUALITY_RANGES.items():
        if not _IDENTIFIER.match(column):
            raise LoadError(f"unsafe column name in config: {column!r}")
        checks.append(where_check(
            f"range_{column}", "stg_weather_hourly", "error", f"{column} stays between {low} and {high}",
            f"{column} < ? OR {column} > ?", params=(low, high), columns=f"city_slug, observed_at, {column}"))
    checks += [
        Check("staging_matches_raw", "stg_weather_hourly", "error",
              "Staging has exactly one row for every distinct city-hour in raw", check_staging_matches_raw),
        Check("hour_coverage", "stg_weather_hourly", "error",
              "No missing hours between a city's first and last hour", check_hour_coverage),
        Check("freshness", "stg_weather_hourly", "error",
              "Every city has data up to the latest day the archive provides", check_freshness),
        Check("null_temperature_share", "stg_weather_hourly", "warn",
              "Few temperatures are missing", check_null_temperature_share),
        Check("daily_matches_staging", "fct_weather_daily", "error",
              "One daily row per city-day in staging", check_daily_matches_staging),
        where_check("daily_hours_at_most_24", "fct_weather_daily", "error", "No day has more than 24 hours of data",
                    "hours_with_data > 24", columns="city_slug, observed_date, hours_with_data"),
        where_check("daily_min_not_above_max", "fct_weather_daily", "error", "A day's minimum is never above its maximum",
                    "temp_min_c > temp_max_c", columns="city_slug, observed_date, temp_min_c, temp_max_c"),
        where_check("daily_complete_flag_consistent", "fct_weather_daily", "error",
                    "A day is flagged complete exactly when it has 24 hours of data",
                    "(is_complete_day = 1 AND hours_with_data <> 24) OR (is_complete_day = 0 AND hours_with_data = 24)",
                    columns="city_slug, observed_date, is_complete_day, hours_with_data"),
        where_check("daily_temperature_swing", "fct_weather_daily", "warn",
                    f"No day swings by more than {config.MAX_DAILY_TEMPERATURE_SWING_C} degrees",
                    "(temp_max_c - temp_min_c) > ?", params=(config.MAX_DAILY_TEMPERATURE_SWING_C,),
                    columns="city_slug, observed_date, temp_min_c, temp_max_c"),
        Check("monthly_matches_daily", "fct_weather_monthly", "error",
              "One monthly row per city-month in the daily table", check_monthly_matches_daily),
        where_check("monthly_days_consistent", "fct_weather_monthly", "error",
                    "Complete days never exceed total days, and a month has at most 31 days",
                    "days_complete > days_total OR days_total > 31",
                    columns="city_slug, observed_month, days_total, days_complete"),
        Check("latest_runs_not_failed", RUNS_TABLE, "error",
              "The latest extract, load and transform runs did not fail or hang", check_latest_runs_not_failed),
        Check("latest_runs_not_partial", RUNS_TABLE, "warn",
              "The latest extract, load and transform runs had no failed files", check_latest_runs_not_partial),
    ]
    return checks


# ------------------------------------------------------------------- running
def ensure_quality_schema(conn) -> None:
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
    run_id VARCHAR,
    checked_at VARCHAR,
    check_name VARCHAR,
    table_name VARCHAR,
    severity VARCHAR,
    status VARCHAR,
    violations INTEGER,
    message VARCHAR
)""")
    found = table_columns(conn, RESULTS_TABLE)
    if found != RESULT_COLUMNS:
        raise LoadError(f"table {RESULTS_TABLE} has columns {found}, expected {RESULT_COLUMNS}")


def execute_check(check: Check, conn, ctx: Context) -> CheckResult:
    try:
        violations, detail = check.run(conn, ctx)
    except Exception as exc:  # a broken check must be visible, and must not hide the other checks
        return CheckResult(check.name, check.table, check.severity, check.description, "error", None,
                           f"could not run: {type(exc).__name__}: {exc}"[:300])
    status = "fail" if violations else "pass"
    return CheckResult(check.name, check.table, check.severity, check.description, status, int(violations), detail)


def run_checks(conn, today: date | None = None, now: datetime | None = None, checks=None) -> QualitySummary:
    """Run every check, store the results, log the run. The summary says whether the run is ok."""
    now = now or datetime.now(timezone.utc)
    ctx = Context(today=today or now.date(), now=now)
    summary = QualitySummary(run_id=make_run_id(now))

    ensure_schema(conn)
    ensure_quality_schema(conn)
    start_run(conn, summary.run_id, "quality", now)
    try:
        for check in checks if checks is not None else all_checks():
            summary.results.append(execute_check(check, conn, ctx))
        stamp = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with transaction(conn):
            insert_rows(conn, RESULTS_TABLE, RESULT_COLUMNS,
                        [(summary.run_id, stamp, r.name, r.table, r.severity, r.status, r.violations, r.message)
                         for r in summary.results])
    except BaseException as exc:
        finish_run(conn, summary.run_id, "quality", "failed", message=f"{type(exc).__name__}: {exc}"[:500])
        raise
    finish_run(conn, summary.run_id, "quality", "success" if summary.ok else "failed", message=summary.message[:500])
    return summary


# ------------------------------------------------------------------------ CLI
def format_report(summary: QualitySummary, verbose: bool = False) -> str:
    lines = []
    for result in summary.results:
        if result.status == "pass" and not verbose:
            continue
        label = {"pass": "PASS ", "fail": "FAIL " if result.severity == "error" else "WARN ", "error": "ERROR"}[result.status]
        count = "" if result.violations is None else f" ({result.violations})"
        lines.append(f"{label} {result.name}{count}: {result.message or result.description}")
    verdict = "OK" if summary.ok else "FAILED"
    lines.append(f"\n{verdict}: {summary.message}")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run the data quality checks against the warehouse.")
    parser.add_argument("--db", type=Path, default=config.WAREHOUSE_PATH, help="DuckDB file to check")
    parser.add_argument("--verbose", action="store_true", help="also list the checks that passed")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        conn = connect_warehouse(args.db)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        summary = run_checks(conn)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    print(format_report(summary, args.verbose))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())