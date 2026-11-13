import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from tests.helpers import land, make_conn, prepared, scalar
from weatherwh import config, load, quality, transform
from weatherwh.load import LoadError

NOW = datetime(2024, 3, 21, 6, 0, tzinfo=timezone.utc)
TODAY = date(2024, 3, 20)  # the archive can provide data through 15 March

_TEMPLATE = None  # a healthy warehouse, built once and copied for every test


def healthy():
    """A fully built, healthy warehouse: 5 cities, 61 days each, extracted, loaded and transformed."""
    global _TEMPLATE
    if _TEMPLATE is None:
        conn = make_conn()
        with tempfile.TemporaryDirectory() as raw_dir:
            land(Path(raw_dir), today=TODAY, cities=config.CITIES)
            assert not load.load_files(conn, Path(raw_dir), now=NOW).failed
        transform.run_transform(conn, now=NOW)
        _TEMPLATE = conn
    copy = make_conn()
    _TEMPLATE.backup(copy)
    return copy


def run(conn, today=TODAY, now=NOW):
    return quality.run_checks(conn, today=today, now=now)


def result(summary, name):
    return next(r for r in summary.results if r.name == name)


def names(results):
    return sorted(r.name for r in results)


def add_run(conn, step, status, started, finished=None):
    load.insert_rows(conn, load.RUNS_TABLE, load.RUNS_COLUMNS,
                     [("manual-run", step, started, finished, status, None, None, None, None, None)])


# ------------------------------------------------------------------ healthy case
def test_a_healthy_warehouse_passes_every_check():
    summary = run(healthy())
    assert summary.ok and len(summary.results) == len(quality.all_checks())
    assert all(r.status == "pass" for r in summary.results), [(r.name, r.message) for r in summary.results if r.status != "pass"]
    assert summary.failed_errors == [] and summary.warnings == [] and summary.crashed == []


def test_check_names_are_unique_and_severities_valid():
    checks = quality.all_checks()
    assert len({c.name for c in checks}) == len(checks)
    assert {c.severity for c in checks} <= {"error", "warn"}
    assert all(c.description for c in checks)


# -------------------------------------------------- impossible or invalid values
def test_an_impossible_temperature_is_caught_with_an_example():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET temperature_c = 99 "
                 "WHERE city_slug = 'paris' AND observed_date = '2024-02-01' AND observed_hour = 3")
    summary = run(conn)
    found = result(summary, "range_temperature_c")
    assert found.status == "fail" and found.violations == 1 and "99" in found.message and "paris" in found.message
    assert names(summary.failed_errors) == ["range_temperature_c"] and not summary.ok


def test_other_value_ranges_are_checked():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET relative_humidity_pct = 120 WHERE city_slug = 'rome' AND observed_hour = 1 "
                 "AND observed_date = '2024-02-01'")
    conn.execute("UPDATE stg_weather_hourly SET precipitation_mm = -1 WHERE city_slug = 'rome' AND observed_hour = 2 "
                 "AND observed_date = '2024-02-01'")
    summary = run(conn)
    assert names(summary.failed_errors) == ["range_precipitation_mm", "range_relative_humidity_pct"]


def test_missing_values_are_not_range_violations():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET wind_speed_kmh = NULL WHERE city_slug = 'berlin' AND observed_hour = 4")
    assert result(run(conn), "range_wind_speed_kmh").status == "pass"


def test_a_city_that_is_not_configured_is_caught():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET city_slug = 'atlantis' WHERE city_slug = 'rome' AND observed_date = '2024-02-01'")
    found = result(run(conn), "staging_known_cities")
    assert found.status == "fail" and found.violations == 24 and "atlantis" in found.message


def test_null_keys_are_caught():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET city_slug = NULL WHERE city_slug = 'rome' AND observed_date = '2024-02-01' "
                 "AND observed_hour = 0")
    assert result(run(conn), "staging_no_null_keys").violations == 1


# ----------------------------------------------------------- duplicates and gaps
def test_duplicate_city_hours_are_caught():
    conn = healthy()
    conn.execute("INSERT INTO stg_weather_hourly SELECT * FROM stg_weather_hourly WHERE city_slug = 'paris' LIMIT 1")
    summary = run(conn)
    found = result(summary, "staging_unique_city_hour")
    assert found.status == "fail" and found.violations == 1 and "paris" in found.message
    assert {"staging_unique_city_hour", "staging_matches_raw"} <= set(names(summary.failed_errors))


def test_a_missing_day_is_caught_by_coverage_and_reconciliation():
    conn = healthy()
    conn.execute("DELETE FROM stg_weather_hourly WHERE city_slug = 'london' AND observed_date = '2024-02-10'")
    summary = run(conn)
    coverage = result(summary, "hour_coverage")
    assert coverage.status == "fail" and coverage.violations == 24 and "london" in coverage.message
    assert result(summary, "staging_matches_raw").violations == 24
    assert result(summary, "daily_matches_staging").violations == 1  # the daily table still has the day


# ------------------------------------------------------------------- freshness
def test_stale_data_is_caught_for_every_city():
    summary = run(healthy(), today=date(2024, 6, 1))
    found = result(summary, "freshness")
    assert found.status == "fail" and found.violations == len(config.CITIES)
    assert names(summary.failed_errors) == ["freshness"]


def test_a_city_with_no_data_at_all_is_caught():
    conn = healthy()
    conn.execute("DELETE FROM stg_weather_hourly WHERE city_slug = 'rome'")
    found = result(run(conn), "freshness")
    assert found.status == "fail" and "rome: no data at all" in found.message


def test_data_just_inside_the_allowed_delay_passes():
    # the latest day is 15 March; with 5 days of archive delay and 2 of slack, 22 March is still acceptable
    assert result(run(healthy(), today=date(2024, 3, 22)), "freshness").status == "pass"
    assert result(run(healthy(), today=date(2024, 3, 23)), "freshness").status == "fail"


# ----------------------------------------------------------------- earlier steps
def test_a_failed_earlier_run_is_an_error():
    conn = healthy()
    add_run(conn, "load", "failed", "2024-03-22T00:00:00Z", "2024-03-22T00:01:00Z")
    found = result(run(conn), "latest_runs_not_failed")
    assert found.status == "fail" and "load run failed" in found.message


def test_the_extract_step_is_watched_too_partial_is_a_warning_and_failed_is_an_error():
    conn = healthy()
    add_run(conn, "extract", "partial", "2024-03-22T00:00:00Z", "2024-03-22T00:01:00Z")
    summary = run(conn)
    assert names(summary.warnings) == ["latest_runs_not_partial"] and summary.ok
    assert "extract run was partial" in result(summary, "latest_runs_not_partial").message

    add_run(conn, "extract", "failed", "2024-03-23T00:00:00Z", "2024-03-23T00:01:00Z")
    failed = run(conn)
    assert "latest_runs_not_failed" in names(failed.failed_errors) and not failed.ok
    assert "extract run failed" in result(failed, "latest_runs_not_failed").message


def test_an_older_failure_is_not_held_against_a_later_success():
    conn = healthy()
    add_run(conn, "load", "failed", "2024-03-01T00:00:00Z", "2024-03-01T00:01:00Z")  # before the healthy load
    assert result(run(conn), "latest_runs_not_failed").status == "pass"


def test_a_partial_run_is_only_a_warning():
    conn = healthy()
    add_run(conn, "load", "partial", "2024-03-22T00:00:00Z", "2024-03-22T00:01:00Z")
    summary = run(conn)
    assert names(summary.warnings) == ["latest_runs_not_partial"] and summary.ok


def test_a_run_stuck_in_running_is_an_error_only_after_the_stale_threshold():
    conn = healthy()
    add_run(conn, "transform", "running", "2024-03-21T07:00:00Z")
    fresh = run(conn, now=datetime(2024, 3, 21, 8, 0, tzinfo=timezone.utc))  # one hour old: still plausibly running
    assert result(fresh, "latest_runs_not_failed").status == "pass"
    stale = run(conn, now=datetime(2024, 3, 22, 12, 0, tzinfo=timezone.utc))  # 29 hours old: it crashed
    assert result(stale, "latest_runs_not_failed").status == "fail" and "never finished" in result(stale, "latest_runs_not_failed").message


# -------------------------------------------------------------------- warnings
def test_many_missing_temperatures_warn_without_failing_the_run():
    conn = healthy()
    conn.execute("UPDATE stg_weather_hourly SET temperature_c = NULL WHERE city_slug = 'paris' AND observed_date < '2024-02-01'")
    summary = run(conn)
    assert names(summary.warnings) == ["null_temperature_share"] and "paris" in result(summary, "null_temperature_share").message
    assert summary.ok and summary.failed_errors == []


def test_a_huge_daily_temperature_swing_warns():
    conn = healthy()
    conn.execute("UPDATE fct_weather_daily SET temp_max_c = 80 WHERE city_slug = 'paris' AND observed_date = '2024-02-01'")
    summary = run(conn)
    assert names(summary.warnings) == ["daily_temperature_swing"] and summary.ok


# ------------------------------------------------------------------------ marts
def test_inconsistent_daily_rows_are_caught():
    conn = healthy()
    conn.execute("UPDATE fct_weather_daily SET hours_with_data = 25 WHERE city_slug = 'paris' AND observed_date = '2024-02-01'")
    conn.execute("UPDATE fct_weather_daily SET temp_min_c = 99 WHERE city_slug = 'rome' AND observed_date = '2024-02-01'")
    summary = run(conn)
    failing = set(names(summary.failed_errors))
    assert {"daily_hours_at_most_24", "daily_complete_flag_consistent", "daily_min_not_above_max"} <= failing


def test_inconsistent_monthly_rows_are_caught():
    conn = healthy()
    conn.execute("UPDATE fct_weather_monthly SET days_complete = days_total + 1 "
                 "WHERE city_slug = 'paris' AND observed_month = '2024-02'")
    assert result(run(conn), "monthly_days_consistent").violations == 1


def test_monthly_rows_must_match_the_daily_table():
    conn = healthy()
    conn.execute("DELETE FROM fct_weather_monthly WHERE city_slug = 'paris' AND observed_month = '2024-03'")
    assert result(run(conn), "monthly_matches_daily").violations == 1


# ----------------------------------------------------------- checks that cannot run
def test_a_check_that_cannot_run_is_reported_and_fails_the_run():
    conn = healthy()
    conn.execute("DROP TABLE fct_weather_monthly")
    summary = run(conn)
    assert names(summary.crashed) == ["monthly_days_consistent", "monthly_matches_daily"]
    assert all(r.status == "error" and "could not run" in r.message and r.violations is None for r in summary.crashed)
    assert not summary.ok
    assert result(summary, "range_temperature_c").status == "pass"  # the other checks still ran


def test_execute_check_isolates_a_crashing_check():
    def boom(conn, ctx):
        raise RuntimeError("kaput")

    outcome = quality.execute_check(quality.Check("x", "t", "warn", "d", boom), make_conn(), quality.Context(TODAY, NOW))
    assert outcome.status == "error" and "RuntimeError: kaput" in outcome.message


def test_an_empty_warehouse_fails_clearly():
    conn = prepared()
    transform.run_transform(conn, now=NOW)
    summary = run(conn)
    assert not summary.ok and {"raw_not_empty", "freshness"} <= set(names(summary.failed_errors))


def test_unsafe_column_names_in_the_config_are_rejected(monkeypatch):
    monkeypatch.setattr(config, "QUALITY_RANGES", {"temperature_c; DROP TABLE x": (0, 1)})
    with pytest.raises(LoadError, match="unsafe column name"):
        quality.all_checks()


# ------------------------------------------------------------ results and the log
def test_results_are_stored_and_history_builds_up():
    conn = healthy()
    first = run(conn)
    conn.execute("UPDATE stg_weather_hourly SET temperature_c = 99 WHERE city_slug = 'paris' AND observed_hour = 3 "
                 "AND observed_date = '2024-02-01'")
    second = run(conn, now=datetime(2024, 3, 21, 7, 0, tzinfo=timezone.utc))

    total = len(quality.all_checks())
    assert scalar(conn, f"SELECT COUNT(*) FROM {quality.RESULTS_TABLE}") == 2 * total
    assert scalar(conn, f"SELECT COUNT(*) FROM {quality.RESULTS_TABLE} WHERE run_id = ?", [first.run_id]) == total
    stored = conn.execute(f"SELECT status, severity, violations, checked_at FROM {quality.RESULTS_TABLE} "
                          "WHERE run_id = ? AND check_name = 'range_temperature_c'", [second.run_id]).fetchone()
    assert stored == ("fail", "error", 1, "2024-03-21T07:00:00Z")


def test_the_run_is_logged_with_a_status_and_summary():
    conn = healthy()
    good = run(conn)
    conn.execute("UPDATE stg_weather_hourly SET temperature_c = 99 WHERE city_slug = 'paris' AND observed_hour = 3 "
                 "AND observed_date = '2024-02-01'")
    bad = run(conn)
    rows = dict(conn.execute("SELECT run_id, status FROM pipeline_runs WHERE step = 'quality'").fetchall())
    assert rows == {good.run_id: "success", bad.run_id: "failed"}
    message = scalar(conn, "SELECT message FROM pipeline_runs WHERE run_id = ?", [bad.run_id])
    assert "1 failed" in message and "range_temperature_c" in message


def test_results_table_with_the_wrong_columns_is_refused():
    conn = healthy()
    conn.execute(f"CREATE TABLE {quality.RESULTS_TABLE} (just_one_column VARCHAR)")
    with pytest.raises(LoadError, match="quality_results"):
        run(conn)


def test_report_lists_failures_and_a_verdict():
    conn = healthy()
    ok_report = quality.format_report(run(conn))
    assert ok_report.strip().startswith("OK:") and "FAIL" not in ok_report

    conn.execute("UPDATE stg_weather_hourly SET temperature_c = 99 WHERE city_slug = 'paris' AND observed_hour = 3 "
                 "AND observed_date = '2024-02-01'")
    summary = run(conn)
    report = quality.format_report(summary)
    assert "FAIL  range_temperature_c (1)" in report and "FAILED:" in report and "PASS" not in report
    assert "PASS  freshness" in quality.format_report(summary, verbose=True)


def test_small_helpers():
    assert quality.to_datetime("2024-01-15 03:00:00") == datetime(2024, 1, 15, 3)
    assert quality.to_datetime(date(2024, 1, 15)) == datetime(2024, 1, 15)
    assert quality.to_date(datetime(2024, 1, 15, 22)) == date(2024, 1, 15)
    assert quality.compare_counts("a", 5, "b", 5) == (0, "")
    assert quality.compare_counts("a", 5, "b", 3) == (2, "a has 5, b has 3")


# ------------------------------------------------------------ the real database
def test_checks_on_a_real_duckdb_database(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    land(tmp_path / "raw", today=TODAY, cities=config.CITIES)
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    load.load_files(conn, tmp_path / "raw", now=NOW)
    transform.run_transform(conn, now=NOW)

    summary = quality.run_checks(conn, today=TODAY, now=NOW)
    assert summary.ok and all(r.status == "pass" for r in summary.results), [(r.name, r.message) for r in summary.results if r.status != "pass"]
    assert conn.execute(f"SELECT COUNT(*) FROM {quality.RESULTS_TABLE}").fetchone()[0] == len(quality.all_checks())

    conn.execute("UPDATE stg_weather_hourly SET temperature_c = 99 "
                 "WHERE city_slug = 'paris' AND observed_hour = 3 AND observed_date = DATE '2024-02-01'")
    broken = quality.run_checks(conn, today=TODAY, now=NOW)
    found = next(r for r in broken.results if r.name == "range_temperature_c")
    assert found.status == "fail" and found.violations == 1 and not broken.ok
    conn.close()