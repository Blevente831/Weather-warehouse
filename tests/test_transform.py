import shutil
from datetime import date, datetime, timezone

import pytest

from tests.helpers import land, make_conn, prepared, raw_day, scalar
from weatherwh import config, load, transform

NOW = datetime(2024, 3, 21, 6, 0, tzinfo=timezone.utc)
DAYS_LANDED = 17 + 29 + 15  # the windows that land() fetches for each of its two cities


def daily(conn, city, day):
    return conn.execute(
        "SELECT hours_with_data, is_complete_day, temp_min_c, temp_max_c, temp_avg_c, precipitation_total_mm, "
        "wind_max_kmh, wind_avg_kmh, humidity_avg_pct, cloud_cover_avg_pct FROM fct_weather_daily "
        "WHERE city_slug = ? AND observed_date = ?", [city, day]).fetchone()


# --------------------------------------------------------------------- helpers
def test_split_statements_drops_comments_and_empty_parts():
    sql = "-- header; with nothing\nDROP TABLE IF EXISTS t;\n\nCREATE TABLE t AS SELECT 1 -- trailing\n;\n"
    assert transform.split_statements(sql) == ["DROP TABLE IF EXISTS t", "CREATE TABLE t AS SELECT 1"]


def test_dialect_detection_and_rendering():
    class FakeDuckConnection:
        pass

    FakeDuckConnection.__module__ = "duckdb"
    assert transform.detect_dialect(FakeDuckConnection()) == "duckdb"

    class DuckDBPyConnection:  # how recent DuckDB releases name the real class, in an internal module
        pass

    DuckDBPyConnection.__module__ = "_duckdb"
    assert transform.detect_dialect(DuckDBPyConnection()) == "duckdb"
    assert transform.detect_dialect(make_conn()) == "sqlite"

    template = (config.SQL_DIR / "staging" / "stg_weather_hourly.sql").read_text()
    assert "AS TIMESTAMP" in transform.render_sql(template, "duckdb")
    assert "AS DATE" in transform.render_sql(template, "duckdb")
    assert "TIMESTAMP" not in transform.render_sql(template, "sqlite")
    assert "$" not in transform.render_sql(template, "duckdb")  # every placeholder was filled


def test_every_model_file_exists_and_splits_into_statements():
    for relative in transform.MODEL_FILES:
        text = (config.SQL_DIR / relative).read_text()
        statements = transform.split_statements(transform.render_sql(text, "sqlite"))
        assert len(statements) == 2 and statements[0].startswith("DROP TABLE IF EXISTS")


# --------------------------------------------------------------------- staging
def test_staging_types_renames_and_derives_columns():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [float(h) for h in range(24)], precips=[0.1] * 24)
    transform.run_transform(conn, now=NOW)

    assert conn.execute(
        "SELECT observed_at, observed_date, observed_month, observed_hour FROM stg_weather_hourly "
        "WHERE observed_hour = 5").fetchone() == ("2024-03-01 05:00:00", "2024-03-01", "2024-03", 5)
    row = conn.execute("SELECT temperature_c, relative_humidity_pct, precipitation_mm, wind_speed_kmh, "
                       "cloud_cover_pct FROM stg_weather_hourly WHERE observed_hour = 7").fetchone()
    assert row == (7.0, 80.0, 0.1, 10.0, 50.0)
    columns = [c[0] for c in conn.execute("SELECT * FROM stg_weather_hourly LIMIT 0").description]
    assert columns[:5] == ["city_slug", "observed_at", "observed_date", "observed_month", "observed_hour"]
    assert {"source_file", "fetched_at", "loaded_at", "run_id"} <= set(columns)  # lineage is kept


def test_staging_keeps_the_most_recently_fetched_copy_of_an_hour():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [10.0] * 24, source="old.json", fetched_at="2024-03-02T00:00:00Z")
    raw_day(conn, "paris", "2024-03-01", [20.0] * 24, source="new.json", fetched_at="2024-03-09T00:00:00Z")
    raw_day(conn, "paris", "2024-03-02", [5.0] * 24, source="old.json", fetched_at="2024-03-02T00:00:00Z")
    summary = transform.run_transform(conn, now=NOW)

    assert summary.row_counts["stg_weather_hourly"] == 48  # 2 days x 24 hours, not 72
    assert scalar(conn, "SELECT COUNT(DISTINCT temperature_c) FROM stg_weather_hourly WHERE observed_date = '2024-03-01'") == 1
    assert scalar(conn, "SELECT MAX(temperature_c) FROM stg_weather_hourly WHERE observed_date = '2024-03-01'") == 20.0
    assert scalar(conn, "SELECT source_file FROM stg_weather_hourly WHERE observed_date = '2024-03-01' LIMIT 1") == "new.json"
    assert scalar(conn, "SELECT temperature_c FROM stg_weather_hourly WHERE observed_date = '2024-03-02' LIMIT 1") == 5.0


def test_staging_keeps_missing_values_as_null():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [1.0, None, 3.0])
    transform.run_transform(conn, now=NOW)
    assert scalar(conn, "SELECT COUNT(*) FROM stg_weather_hourly WHERE temperature_c IS NULL") == 1


# ----------------------------------------------------------------------- daily
def test_daily_mart_aggregates_a_complete_day():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [float(h) for h in range(24)], precips=[0.5, 1.0] + [0.0] * 22, wind=10.0)
    transform.run_transform(conn, now=NOW)

    # hours, complete, min, max, avg, rain, wind max, wind avg, humidity, cloud
    assert daily(conn, "paris", "2024-03-01") == (24, 1, 0.0, 23.0, 11.5, 1.5, 10.0, 10.0, 80.0, 50.0)


def test_daily_mart_flags_incomplete_days_and_ignores_missing_values():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [4.0, 6.0, None, 8.0], precips=[1.0, 2.0, 3.0, None])
    transform.run_transform(conn, now=NOW)

    hours, complete, t_min, t_max, t_avg, rain = daily(conn, "paris", "2024-03-01")[:6]
    assert (hours, complete) == (3, 0)  # one hour has no temperature, and the day is short anyway
    assert (t_min, t_max, t_avg, rain) == (4.0, 8.0, 6.0, 6.0)  # nulls are skipped, never treated as zero


def test_daily_mart_keeps_cities_and_days_apart():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [10.0] * 24)
    raw_day(conn, "paris", "2024-03-02", [12.0] * 24)
    raw_day(conn, "london", "2024-03-01", [2.0] * 24)
    transform.run_transform(conn, now=NOW)

    assert scalar(conn, "SELECT COUNT(*) FROM fct_weather_daily") == 3
    assert daily(conn, "paris", "2024-03-02")[4] == 12.0 and daily(conn, "london", "2024-03-01")[4] == 2.0


# --------------------------------------------------------------------- monthly
def test_monthly_mart_uses_complete_days_only():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [float(h) for h in range(24)], precips=[0.5, 1.0] + [0.0] * 22)  # rainy
    raw_day(conn, "paris", "2024-03-02", [5.0] * 24)  # dry
    raw_day(conn, "paris", "2024-03-03", [30.0] * 6, precips=[5.0] * 6)  # incomplete: must not count
    raw_day(conn, "paris", "2024-04-01", [9.0] * 24)  # a different month
    transform.run_transform(conn, now=NOW)

    march = conn.execute(
        "SELECT days_total, days_complete, temp_avg_c, temp_min_c, temp_max_c, precipitation_total_mm, rainy_days "
        "FROM fct_weather_monthly WHERE city_slug = 'paris' AND observed_month = '2024-03'").fetchone()
    assert march == (3, 2, 8.25, 0.0, 23.0, 1.5, 1)  # the 30-degree short day and its 30 mm of rain are excluded
    assert scalar(conn, "SELECT COUNT(*) FROM fct_weather_monthly") == 2


def test_a_month_with_no_complete_days_has_empty_statistics():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [5.0] * 10)
    transform.run_transform(conn, now=NOW)
    row = conn.execute("SELECT days_total, days_complete, temp_avg_c, precipitation_total_mm, rainy_days "
                       "FROM fct_weather_monthly").fetchone()
    assert row == (1, 0, None, None, 0)


# ------------------------------------------------------------------- dimensions
def test_dim_city_comes_from_the_configuration():
    conn = prepared()
    transform.run_transform(conn, now=NOW)
    rows = conn.execute("SELECT city_slug, city_name, latitude, longitude FROM dim_city ORDER BY city_slug").fetchall()
    assert rows == sorted((c.slug, c.name, c.latitude, c.longitude) for c in config.CITIES)


# -------------------------------------------------------------- rebuild + run log
def test_rebuilding_is_idempotent_and_picks_up_new_raw_data():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [10.0] * 24)
    first = transform.run_transform(conn, now=NOW)
    second = transform.run_transform(conn, now=NOW)
    assert first.row_counts == second.row_counts and second.row_counts["stg_weather_hourly"] == 24

    raw_day(conn, "paris", "2024-03-02", [11.0] * 24)
    third = transform.run_transform(conn, now=NOW)
    assert third.row_counts["fct_weather_daily"] == 2 and third.row_counts["stg_weather_hourly"] == 48


def test_an_empty_raw_layer_builds_empty_tables_without_error():
    conn = prepared()
    summary = transform.run_transform(conn, now=NOW)
    assert summary.row_counts["stg_weather_hourly"] == 0 and summary.row_counts["fct_weather_monthly"] == 0
    assert summary.row_counts["dim_city"] == len(config.CITIES)


def test_the_run_is_logged():
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [10.0] * 24)
    summary = transform.run_transform(conn, now=NOW)
    row = conn.execute("SELECT step, status, rows_loaded, started_at, finished_at, message FROM pipeline_runs "
                       "WHERE run_id = ?", [summary.run_id]).fetchone()
    assert row[:3] == ("transform", "success", 24)
    assert row[3] == "2024-03-21T06:00:00Z" and row[4] is not None
    assert "fct_weather_daily=1" in row[5]


def test_a_failing_model_rolls_everything_back_and_is_logged(tmp_path):
    conn = prepared()
    raw_day(conn, "paris", "2024-03-01", [10.0] * 24)
    transform.run_transform(conn, now=NOW)
    before = scalar(conn, "SELECT COUNT(*) FROM fct_weather_daily")

    broken = tmp_path / "sql"
    shutil.copytree(config.SQL_DIR, broken)
    (broken / "marts" / "fct_weather_monthly.sql").write_text(
        "DROP TABLE IF EXISTS fct_weather_monthly;\nCREATE TABLE fct_weather_monthly AS SELECT nope FROM missing_table;\n")
    raw_day(conn, "paris", "2024-03-02", [11.0] * 24)  # new data that the failed run must not half-apply

    with pytest.raises(Exception):
        transform.run_transform(conn, now=NOW, sql_dir=broken)

    assert scalar(conn, "SELECT COUNT(*) FROM fct_weather_daily") == before  # staging and daily were rolled back
    assert scalar(conn, "SELECT COUNT(*) FROM stg_weather_hourly") == 24
    assert scalar(conn, "SELECT COUNT(*) FROM fct_weather_monthly") == 1  # the old monthly table survived
    status = conn.execute("SELECT status, message FROM pipeline_runs WHERE step = 'transform' "
                          "ORDER BY started_at DESC, finished_at DESC").fetchall()
    assert "failed" in [s for s, _ in status] and any("missing_table" in (m or "") for _, m in status)


# --------------------------------------------------- the whole pipeline, end to end
def test_extract_load_transform_end_to_end(tmp_path):
    land(tmp_path)  # fake API -> JSON files for 2 cities
    conn = make_conn()
    assert not load.load_files(conn, tmp_path, now=NOW).failed
    summary = transform.run_transform(conn, now=NOW)

    assert summary.row_counts["stg_weather_hourly"] == 2 * DAYS_LANDED * 24
    assert summary.row_counts["fct_weather_daily"] == 2 * DAYS_LANDED
    assert summary.row_counts["fct_weather_monthly"] == 2 * 3
    assert scalar(conn, "SELECT SUM(is_complete_day) FROM fct_weather_daily") == 2 * DAYS_LANDED
    assert conn.execute("SELECT days_total, days_complete FROM fct_weather_monthly WHERE city_slug = 'paris' "
                        "AND observed_month = '2024-02'").fetchone() == (29, 29)  # 2024 is a leap year
    assert [r[0] for r in transform.coverage(conn)] == ["london", "paris"]


# ------------------------------------------------------- the real database
def test_full_pipeline_on_a_real_duckdb_database(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    land(tmp_path / "raw")
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    load.load_files(conn, tmp_path / "raw", now=NOW)

    assert transform.detect_dialect(conn) == "duckdb"  # checked first, so a wrong guess is obvious
    first = transform.run_transform(conn, now=NOW)
    assert first.row_counts == {"stg_weather_hourly": 2 * DAYS_LANDED * 24, "dim_city": len(config.CITIES),
                                "fct_weather_daily": 2 * DAYS_LANDED, "fct_weather_monthly": 6}
    types = conn.execute("SELECT typeof(observed_at), typeof(observed_date) FROM stg_weather_hourly LIMIT 1").fetchone()
    assert types == ("TIMESTAMP", "DATE")
    assert conn.execute("SELECT MIN(observed_date), MAX(observed_date) FROM fct_weather_daily").fetchone() == (
        date(2024, 1, 15), date(2024, 3, 15))

    assert transform.run_transform(conn, now=NOW).row_counts == first.row_counts  # rebuilding changes nothing
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE step = 'transform' AND status = 'success'").fetchone()[0] == 2
    conn.close()