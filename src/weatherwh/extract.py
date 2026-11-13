"""Extract step: land raw Open-Meteo archive responses as JSON files.

Design:
  * One file per city and calendar month: data/raw/<city>/<YYYY-MM>.json
  * Incremental: a month that is already complete is never fetched again; the current
    (still growing) month is refreshed on each run.
  * Idempotent: running twice makes the same files, with no duplicates.
  * Each file wraps the untouched API response in a small envelope recording where and
    when it was fetched, so later steps can trace every row back to its source.
  * Files are written atomically, so a crash never leaves a half-written file behind.

Run:  python -m weatherwh.extract --dry-run     (show the plan, no network)
      python -m weatherwh.extract
"""

import argparse
import calendar
import json
import logging
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from weatherwh import config
from weatherwh.config import City

log = logging.getLogger("weatherwh.extract")

USER_AGENT = "weather-warehouse/0.1 (learning project)"


class ExtractError(Exception):
    """Anything that stops one piece of the extract from completing."""


# ------------------------------------------------------------------ planning
def latest_available_date(today: date) -> date:
    """The last date we ask the archive for (it trails real time by a few days)."""
    return today - timedelta(days=config.ARCHIVE_LAG_DAYS)


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month windows (first and last may be partial)."""
    windows = []
    cursor = start
    while cursor <= end:
        month_end = date(cursor.year, cursor.month, calendar.monthrange(cursor.year, cursor.month)[1])
        windows.append((cursor, min(month_end, end)))
        cursor = month_end + timedelta(days=1)
    return windows


@dataclass(frozen=True)
class Task:
    city: City
    start: date
    end: date
    path: Path

    @property
    def label(self) -> str:
        return f"{self.city.slug} {self.start:%Y-%m} ({self.start} to {self.end})"


def raw_path(raw_dir: Path, city: City, window_start: date) -> Path:
    return Path(raw_dir) / city.slug / f"{window_start:%Y-%m}.json"


def read_envelope(path: Path) -> dict | None:
    """Load a landed file, or None if it is missing or unreadable."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        log.warning("Ignoring unreadable file %s; it will be fetched again", path)
        return None


def needs_fetch(path: Path, window: tuple[date, date]) -> bool:
    """True if the file is missing, unreadable, or does not yet cover the whole window."""
    envelope = read_envelope(path)
    if envelope is None:
        return True
    meta = envelope.get("meta", {})
    try:
        have_start = date.fromisoformat(meta["start_date"])
        have_end = date.fromisoformat(meta["end_date"])
    except (KeyError, ValueError, TypeError):
        return True
    return have_start > window[0] or have_end < window[1]


def plan_extraction(cities, start: date, end: date, raw_dir: Path) -> tuple[list[Task], int]:
    """Work out which city-month files must be fetched. Returns (tasks, total windows)."""
    windows = month_windows(start, end)
    tasks = []
    for city in cities:
        for w_start, w_end in windows:
            path = raw_path(raw_dir, city, w_start)
            if needs_fetch(path, (w_start, w_end)):
                tasks.append(Task(city, w_start, w_end, path))
    return tasks, len(windows) * len(list(cities))


# ------------------------------------------------------------------- fetching
def build_params(city: City, start: date, end: date) -> dict:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(config.HOURLY_VARIABLES),
        "timezone": config.TIMEZONE,
        **config.UNITS,
    }


def _error_reason(exc: urllib.error.HTTPError) -> str:
    """Open-Meteo explains errors in a JSON body such as {"error": true, "reason": "..."}."""
    try:
        body = json.loads(exc.read().decode("utf-8"))
        return str(body.get("reason", body))
    except Exception:  # an unreadable body should never hide the original error
        return exc.reason if isinstance(exc.reason, str) else "no details"


def fetch_json(
    url: str,
    params: dict,
    *,
    opener=urllib.request.urlopen,
    sleep=time.sleep,
    timeout: float = config.REQUEST_TIMEOUT_SECONDS,
    retries: int = config.MAX_RETRIES,
    backoff: float = config.RETRY_BACKOFF_SECONDS,
) -> dict:
    """GET a JSON document. Retries temporary failures with exponential backoff.

    Retried: timeouts, connection errors, HTTP 429 and 5xx, unreadable responses.
    Not retried: other 4xx errors (a bad request will not fix itself).
    """
    request = urllib.request.Request(
        f"{url}?{urllib.parse.urlencode(params)}", headers={"User-Agent": USER_AGENT}
    )
    last_error = "unknown error"
    for attempt in range(retries + 1):
        try:
            with opener(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:  # must come before URLError, its parent class
            reason = _error_reason(exc)
            if exc.code != 429 and exc.code < 500:
                raise ExtractError(f"HTTP {exc.code}: {reason}") from exc
            last_error = f"HTTP {exc.code}: {reason}"
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < retries:
            wait = backoff * 2**attempt
            log.warning("Attempt %d failed (%s); retrying in %.1fs", attempt + 1, last_error, wait)
            sleep(wait)
    raise ExtractError(f"giving up after {retries + 1} attempts: {last_error}")


def validate_payload(payload: dict, start: date, end: date, variables) -> None:
    """Fail loudly if the response is not the shape and size we asked for."""
    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    if not isinstance(hourly, dict):
        raise ExtractError("response has no 'hourly' block")
    times = hourly.get("time")
    if not times:
        raise ExtractError("response has no hourly timestamps")
    for variable in variables:
        if variable not in hourly:
            raise ExtractError(f"response is missing variable {variable!r}")
        if len(hourly[variable]) != len(times):
            raise ExtractError(f"variable {variable!r} has {len(hourly[variable])} values for {len(times)} timestamps")

    expected = ((end - start).days + 1) * 24
    if len(times) != expected:
        raise ExtractError(f"expected {expected} hourly rows for {start} to {end}, got {len(times)}")
    try:
        first, last = datetime.fromisoformat(times[0]), datetime.fromisoformat(times[-1])
    except ValueError as exc:
        raise ExtractError(f"unreadable timestamp: {exc}") from exc
    if first != datetime(start.year, start.month, start.day, 0) or last != datetime(end.year, end.month, end.day, 23):
        raise ExtractError(f"timestamps run {times[0]} to {times[-1]}, expected {start} 00:00 to {end} 23:00")


# --------------------------------------------------------------------- landing
def write_envelope(path: Path, task: Task, params: dict, payload: dict, fetched_at: datetime) -> None:
    """Save the response with its provenance. Atomic: write to a temp file, then rename."""
    envelope = {
        "meta": {
            "city": task.city.name,
            "slug": task.city.slug,
            "start_date": task.start.isoformat(),
            "end_date": task.end.isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "source": config.ARCHIVE_URL,
            "params": params,
        },
        "payload": payload,
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(envelope, handle)
        os.replace(tmp_name, path)  # atomic on Windows, macOS and Linux
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


@dataclass
class ExtractSummary:
    fetched: list[str] = field(default_factory=list)
    skipped: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


def extract_all(
    cities,
    today: date | None = None,
    start: date = config.START_DATE,
    raw_dir: Path = config.RAW_DIR,
    fetch=fetch_json,
    delay: float = config.REQUEST_DELAY_SECONDS,
    sleep=time.sleep,
) -> ExtractSummary:
    """Fetch every city-month that is missing or incomplete. One failure does not stop the rest."""
    cities = list(cities)
    today = today or datetime.now(timezone.utc).date()
    end = latest_available_date(today)

    tasks, total = plan_extraction(cities, start, end, raw_dir)
    summary = ExtractSummary(skipped=total - len(tasks))
    log.info("%d files to fetch, %d already up to date (data through %s)", len(tasks), summary.skipped, end)

    for index, task in enumerate(tasks):
        params = build_params(task.city, task.start, task.end)
        try:
            payload = fetch(config.ARCHIVE_URL, params)
            validate_payload(payload, task.start, task.end, config.HOURLY_VARIABLES)
            write_envelope(task.path, task, params, payload, datetime.now(timezone.utc))
            summary.fetched.append(task.label)
            log.info("Landed %s", task.label)
        except ExtractError as exc:
            summary.failed.append((task.label, str(exc)))
            log.error("Failed %s: %s", task.label, exc)
        if delay and index < len(tasks) - 1:
            sleep(delay)
    return summary


# ------------------------------------------------------------------------ CLI
def select_cities(slugs: str | None) -> list[City]:
    if not slugs:
        return list(config.CITIES)
    by_slug = {city.slug: city for city in config.CITIES}
    wanted = [s.strip().lower() for s in slugs.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in by_slug]
    if unknown:
        raise ExtractError(f"unknown city {', '.join(unknown)}; choose from {', '.join(by_slug)}")
    return [by_slug[s] for s in wanted]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Land raw Open-Meteo archive data as JSON files.")
    parser.add_argument("--start", type=date.fromisoformat, default=config.START_DATE,
                        help="first date to load (YYYY-MM-DD); default from config")
    parser.add_argument("--cities", help="comma-separated city slugs; default: all configured cities")
    parser.add_argument("--dry-run", action="store_true", help="show what would be fetched, without any network calls")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        cities = select_cities(args.cities)
    except ExtractError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    today = datetime.now(timezone.utc).date()
    if args.dry_run:
        end = latest_available_date(today)
        tasks, total = plan_extraction(cities, args.start, end, config.RAW_DIR)
        print(f"Data through {end}. {len(tasks)} of {total} city-month files need fetching:")
        for city in cities:
            mine = [t for t in tasks if t.city is city]
            span = f"{mine[0].start:%Y-%m} to {mine[-1].start:%Y-%m}" if mine else "nothing"
            print(f"  {city.slug:<10} {len(mine):>3} files  ({span})")
        return 0

    summary = extract_all(cities, today=today, start=args.start)
    log.info("Done: %d fetched, %d already up to date, %d failed",
             len(summary.fetched), summary.skipped, len(summary.failed))
    for label, reason in summary.failed:
        log.error("  %s: %s", label, reason)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())