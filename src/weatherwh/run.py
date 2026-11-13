"""Run the whole pipeline in order: extract, load, transform, quality checks.

Policy for failures:
  * Extract problems (a city or month that could not be fetched) do not stop the run. The data
    that did arrive is still good, and the quality checks decide whether what we have is
    acceptable (for example, a city that has gone stale fails the freshness check).
  * If load crashes outright, transform is skipped, because there is nothing trustworthy to build from.
  * The quality checks always run last, as long as the database can be opened.
  * Exit code 0 means: load and transform did not crash and the data passed the quality gate.
    Warnings are reported but do not change the exit code.

Every step records itself in pipeline_runs, so the history shows what happened on each run.

Run:  python -m weatherwh.run
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from weatherwh import config
from weatherwh.extract import ExtractError, extract_all, fetch_json, select_cities
from weatherwh.load import (
    LoadError,
    connect_warehouse,
    ensure_schema,
    finish_run,
    load_files,
    make_run_id,
    start_run,
)
from weatherwh.quality import QualitySummary, format_report, run_checks
from weatherwh.transform import run_transform

log = logging.getLogger("weatherwh.run")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StepResult:
    name: str
    status: str  # success, partial, failed or skipped
    detail: str = ""


@dataclass
class PipelineReport:
    steps: list[StepResult] = field(default_factory=list)
    quality: QualitySummary | None = None

    @property
    def ok(self) -> bool:
        crashed = any(s.name in ("load", "transform") and s.status == "failed" for s in self.steps)
        return not crashed and self.quality is not None and self.quality.ok


def extract_status(fetched: int, failed: int) -> str:
    """success if nothing failed; partial if some new data arrived; failed if we tried and got none.

    Files that were already up to date are not counted: the question is whether the API delivered
    the new data we asked for, so a completely unreachable API is "failed", not "partial".
    """
    if not failed:
        return "success"
    return "partial" if fetched else "failed"


# --------------------------------------------------------------------- steps
def step_extract(conn, cities, today, start, raw_dir, fetch, delay, sleep, now) -> StepResult:
    run_id = make_run_id(now)
    start_run(conn, run_id, "extract", now)
    try:
        summary = extract_all(cities, today=today, start=start, raw_dir=raw_dir, fetch=fetch, delay=delay, sleep=sleep)
    except Exception as exc:  # an unexpected crash is recorded; the rest of the pipeline can still run
        reason = f"{type(exc).__name__}: {exc}"
        finish_run(conn, run_id, "extract", "failed", message=reason[:500])
        log.error("Extract crashed: %s", reason)
        return StepResult("extract", "failed", reason)

    status = extract_status(len(summary.fetched), len(summary.failed))
    message = "; ".join(f"{label}: {reason}" for label, reason in summary.failed)
    finish_run(conn, run_id, "extract", status, len(summary.fetched), summary.skipped, len(summary.failed),
               None, message[:500])
    return StepResult("extract", status,
                      f"{len(summary.fetched)} fetched, {summary.skipped} up to date, {len(summary.failed)} failed")


def step_load(conn, raw_dir, now) -> StepResult:
    try:
        summary = load_files(conn, raw_dir, now=now)
    except Exception as exc:
        return StepResult("load", "failed", f"{type(exc).__name__}: {exc}")
    return StepResult("load", summary.status,
                      f"{len(summary.loaded)} files loaded ({summary.rows} rows), {summary.skipped} unchanged, "
                      f"{len(summary.failed)} failed")


def step_transform(conn, now) -> StepResult:
    try:
        summary = run_transform(conn, now=now)
    except Exception as exc:
        return StepResult("transform", "failed", f"{type(exc).__name__}: {exc}")
    return StepResult("transform", "success", summary.message)


def run_pipeline(conn, cities=None, start: date = config.START_DATE, raw_dir: Path = config.RAW_DIR,
                 today: date | None = None, clock=utcnow, fetch=fetch_json,
                 delay: float = config.REQUEST_DELAY_SECONDS, sleep=time.sleep,
                 skip_extract: bool = False) -> PipelineReport:
    """Run every step in order and return what happened."""
    cities = list(cities if cities is not None else config.CITIES)
    today = today or clock().date()
    ensure_schema(conn)
    report = PipelineReport()

    if skip_extract:
        report.steps.append(StepResult("extract", "skipped", "--skip-extract"))
    else:
        report.steps.append(step_extract(conn, cities, today, start, raw_dir, fetch, delay, sleep, clock()))

    loaded = step_load(conn, raw_dir, clock())
    report.steps.append(loaded)
    if loaded.status == "failed":
        report.steps.append(StepResult("transform", "skipped", "load failed"))
    else:
        report.steps.append(step_transform(conn, clock()))

    try:
        report.quality = run_checks(conn, today=today, now=clock())
        report.steps.append(StepResult("quality", "success" if report.quality.ok else "failed", report.quality.message))
    except Exception as exc:
        report.steps.append(StepResult("quality", "failed", f"{type(exc).__name__}: {exc}"))
    return report


# ----------------------------------------------------------------- reporting
def format_pipeline_report(report: PipelineReport, verbose: bool = False) -> str:
    lines = [f"{'step':<10} {'status':<8} detail"]
    lines += [f"{s.name:<10} {s.status:<8} {s.detail}" for s in report.steps]
    if report.quality is not None:
        lines += ["", format_report(report.quality, verbose)]
    lines.append(f"\nPipeline: {'OK' if report.ok else 'FAILED'}")
    return "\n".join(lines)


def markdown_summary(report: PipelineReport) -> str:
    lines = ["## Weather pipeline: " + ("OK" if report.ok else "FAILED"), "",
             "| Step | Status | Detail |", "|---|---|---|"]
    lines += [f"| {s.name} | {s.status} | {s.detail} |" for s in report.steps]
    if report.quality is not None:
        problems = report.quality.failed_errors + report.quality.crashed + report.quality.warnings
        if problems:
            lines += ["", "### Quality checks needing attention", ""]
            for r in problems:
                kind = "error" if r.status == "error" or r.severity == "error" else "warning"
                lines.append(f"- **{r.name}** ({kind}): {r.message or r.description}")
    return "\n".join(lines) + "\n"


def write_github_summary(report: PipelineReport, path: str | None) -> None:
    """Add the report to the GitHub Actions run page (a no-op outside GitHub)."""
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(markdown_summary(report))


# ------------------------------------------------------------------------ CLI
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run extract, load, transform and the quality checks, in order.")
    parser.add_argument("--start", type=date.fromisoformat, default=config.START_DATE,
                        help="first date to load (YYYY-MM-DD); default from config")
    parser.add_argument("--cities", help="comma-separated city slugs; default: all configured cities")
    parser.add_argument("--db", type=Path, default=config.WAREHOUSE_PATH, help="DuckDB file")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR, help="folder for the landed JSON files")
    parser.add_argument("--skip-extract", action="store_true", help="do not call the API; use the files already landed")
    parser.add_argument("--verbose", action="store_true", help="also list the quality checks that passed")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        cities = select_cities(args.cities)
        conn = connect_warehouse(args.db)
    except (ExtractError, LoadError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        report = run_pipeline(conn, cities, start=args.start, raw_dir=args.raw_dir, skip_extract=args.skip_extract)
    except LoadError as exc:  # for example, a table that no longer matches the configuration
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    print("\n" + format_pipeline_report(report, args.verbose))
    write_github_summary(report, os.environ.get("GITHUB_STEP_SUMMARY"))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
