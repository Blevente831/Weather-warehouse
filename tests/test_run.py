from datetime import date, datetime, timedelta, timezone

import pytest

from tests.helpers import FakeApi, land, make_conn, scalar
from weatherwh import config, run
from weatherwh.extract import ExtractError

NOW = datetime(2024, 3, 21, 6, 0, tzinfo=timezone.utc)
TODAY = date(2024, 3, 20)  # the archive can provide data through 15 March
NEXT_DAY = date(2024, 3, 21)  # ... and through 16 March
LONDON = config.CITIES[1]


_TICKS = [0]


def tick():
    """A clock that moves forward on every call, as a real one does between runs."""
    _TICKS[0] += 1
    return NOW + timedelta(seconds=_TICKS[0])


class ApiDown:
    """An API that never answers."""

    def __init__(self):
        self.calls = 0

    def __call__(self, url, params):
        self.calls += 1
        raise ExtractError("service unavailable")


class ForbiddenApi:
    def __call__(self, url, params):
        raise AssertionError("the API must not be called")


def go(conn, raw_dir, api=None, today=TODAY, **kwargs):
    return run.run_pipeline(conn, cities=config.CITIES, start=date(2024, 1, 15), raw_dir=raw_dir, today=today,
                            clock=tick, fetch=api or FakeApi(), delay=0, sleep=lambda s: None, **kwargs)


def statuses(report):
    return {s.name: s.status for s in report.steps}


def log_rows(conn, step):
    return conn.execute("SELECT status, files_loaded, files_skipped, files_failed, message FROM pipeline_runs "
                        "WHERE step = ? ORDER BY started_at, finished_at", [step]).fetchall()


# ------------------------------------------------------------------- happy path
def test_a_clean_first_run_succeeds_end_to_end(tmp_path):
    conn = make_conn()
    report = go(conn, tmp_path)

    assert report.ok and report.quality.ok
    assert statuses(report) == {"extract": "success", "load": "success", "transform": "success", "quality": "success"}
    assert scalar(conn, "SELECT COUNT(*) FROM stg_weather_hourly") == len(config.CITIES) * 61 * 24
    assert all(r.status == "pass" for r in report.quality.results)


def test_every_step_is_recorded_in_the_run_log(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    rows = dict(conn.execute("SELECT step, status FROM pipeline_runs").fetchall())
    assert rows == {"extract": "success", "load": "success", "transform": "success", "quality": "success"}
    assert log_rows(conn, "extract")[0][:4] == ("success", 15, 0, 0)  # 5 cities x 3 monthly files fetched


def test_a_second_run_changes_nothing_and_stays_green(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    api = FakeApi()
    report = go(conn, tmp_path, api=api)

    assert report.ok and api.calls == []  # nothing new to fetch
    assert "0 fetched, 15 up to date" in report.steps[0].detail
    assert "0 files loaded" in report.steps[1].detail and "15 unchanged" in report.steps[1].detail
    assert scalar(conn, "SELECT COUNT(*) FROM pipeline_runs") == 8  # four steps, twice


def test_the_next_days_run_fetches_only_the_new_data(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    api = FakeApi()
    report = go(conn, tmp_path, api=api, today=NEXT_DAY)
    assert report.ok and len(api.calls) == 5  # one growing monthly file per city
    assert "5 files loaded" in report.steps[1].detail


# ---------------------------------------------------------------- extract trouble
def test_one_city_failing_for_a_day_is_a_warning_not_a_failure(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    report = go(conn, tmp_path, api=FakeApi(fail_for_latitude=LONDON.latitude), today=NEXT_DAY)

    assert statuses(report)["extract"] == "partial" and report.ok  # London is a day behind, within tolerance
    assert [r.name for r in report.quality.warnings] == ["latest_runs_not_partial"]
    assert scalar(conn, "SELECT MAX(observed_date) FROM stg_weather_hourly WHERE city_slug = 'london'") == "2024-03-15"
    assert scalar(conn, "SELECT MAX(observed_date) FROM stg_weather_hourly WHERE city_slug = 'paris'") == "2024-03-16"


def test_the_pipeline_still_loads_and_builds_what_did_arrive(tmp_path):
    conn = make_conn()
    report = go(conn, tmp_path, api=FakeApi(fail_for_latitude=LONDON.latitude))  # London never arrives
    assert statuses(report)["extract"] == "partial"
    assert statuses(report)["load"] == "success" and statuses(report)["transform"] == "success"
    assert scalar(conn, "SELECT COUNT(DISTINCT city_slug) FROM stg_weather_hourly") == 4
    assert not report.ok  # but with no London data at all, the freshness check fails the run
    assert "london: no data at all" in next(r for r in report.quality.results if r.name == "freshness").message


def test_an_unreachable_api_is_an_error_even_while_the_data_is_still_fresh(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    api = ApiDown()
    report = go(conn, tmp_path, api=api, today=NEXT_DAY)

    assert statuses(report)["extract"] == "failed" and api.calls == 5
    assert [r.name for r in report.quality.failed_errors] == ["latest_runs_not_failed"]
    assert not report.ok


def test_data_goes_stale_if_the_api_stays_down(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    report = go(conn, tmp_path, api=ApiDown(), today=date(2024, 3, 30))
    assert "freshness" in [r.name for r in report.quality.failed_errors]


def test_an_unexpected_crash_in_extract_is_recorded_and_the_rest_still_runs(tmp_path):
    def explode(url, params):
        raise RuntimeError("DNS meltdown")

    conn = make_conn()
    report = go(conn, tmp_path, api=explode)
    assert statuses(report)["extract"] == "failed" and "RuntimeError: DNS meltdown" in report.steps[0].detail
    assert statuses(report)["load"] == "success" and report.quality is not None
    assert log_rows(conn, "extract")[0][0] == "failed" and "DNS meltdown" in log_rows(conn, "extract")[0][4]


def test_extract_status_rule():
    assert run.extract_status(5, 0) == "success" and run.extract_status(0, 0) == "success"
    assert run.extract_status(3, 2) == "partial" and run.extract_status(0, 2) == "failed"


# ------------------------------------------------------------------ skipping
def test_skip_extract_uses_the_files_already_landed(tmp_path):
    land(tmp_path, today=TODAY, cities=config.CITIES)
    conn = make_conn()
    report = go(conn, tmp_path, api=ForbiddenApi(), skip_extract=True)
    assert statuses(report)["extract"] == "skipped" and report.ok
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE step = 'extract'").fetchone()[0] == 0


# ------------------------------------------------------------ crashes in later steps
def test_a_load_crash_skips_transform_but_still_runs_the_quality_checks(tmp_path, monkeypatch):
    def boom(conn, raw_dir, now=None):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(run, "load_files", boom)
    report = go(make_conn(), tmp_path)
    assert statuses(report) == {"extract": "success", "load": "failed", "transform": "skipped", "quality": "failed"}
    assert not report.ok and report.quality is not None


def test_a_transform_crash_fails_the_run_and_the_checks_report_the_missing_tables(tmp_path, monkeypatch):
    def boom(conn, now=None):
        raise RuntimeError("bad sql")

    monkeypatch.setattr(run, "run_transform", boom)
    report = go(make_conn(), tmp_path)
    assert statuses(report)["transform"] == "failed" and not report.ok
    assert report.quality is not None and report.quality.crashed  # checks on the missing tables could not run


def test_a_crash_in_the_quality_step_fails_the_run(tmp_path, monkeypatch):
    def boom(conn, today=None, now=None):
        raise RuntimeError("checks broke")

    monkeypatch.setattr(run, "run_checks", boom)
    report = go(make_conn(), tmp_path)
    assert report.quality is None and statuses(report)["quality"] == "failed" and not report.ok


# ---------------------------------------------------------------------- reporting
def test_the_text_report_shows_each_step_and_a_verdict(tmp_path):
    report = go(make_conn(), tmp_path)
    text = run.format_pipeline_report(report)
    assert "extract    success" in text and "quality    success" in text and text.strip().endswith("Pipeline: OK")


def test_the_text_report_names_the_problems_when_it_fails(tmp_path):
    report = go(make_conn(), tmp_path, api=ApiDown())
    text = run.format_pipeline_report(report)
    assert "Pipeline: FAILED" in text and "FAIL  freshness" in text


def test_markdown_summary_for_the_github_run_page(tmp_path):
    conn = make_conn()
    go(conn, tmp_path)
    report = go(conn, tmp_path, api=FakeApi(fail_for_latitude=LONDON.latitude), today=NEXT_DAY)
    markdown = run.markdown_summary(report)
    assert markdown.startswith("## Weather pipeline: OK") and "| extract | partial |" in markdown
    assert "**latest_runs_not_partial** (warning)" in markdown

    target = tmp_path / "summary.md"
    run.write_github_summary(report, str(target))
    run.write_github_summary(report, str(target))  # appends; it never overwrites other steps' summaries
    assert target.read_text().count("## Weather pipeline") == 2
    run.write_github_summary(report, None)  # outside GitHub this does nothing


def test_an_unknown_city_is_rejected_before_anything_runs(capsys):
    assert run.main(["--cities", "atlantis"]) == 2
    assert "unknown city" in capsys.readouterr().err


# ------------------------------------------------------------ the real database
def test_whole_pipeline_on_a_real_duckdb_database(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    report = go(conn, tmp_path / "raw")
    assert report.ok, run.format_pipeline_report(report, verbose=True)

    again = go(conn, tmp_path / "raw", api=FakeApi(), today=NEXT_DAY)
    assert again.ok
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 8
    assert conn.execute("SELECT MAX(observed_date) FROM stg_weather_hourly").fetchone()[0] == date(2024, 3, 16)
    conn.close()
