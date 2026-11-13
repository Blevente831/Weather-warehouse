import json
from datetime import date, datetime, timezone

import pytest

from tests.helpers import FakeApi, land, make_conn, scalar
from weatherwh import config, load
from weatherwh.load import LoadError

NOW = datetime(2024, 3, 21, 6, 0, tzinfo=timezone.utc)
HOURS_PER_FILE = {"2024-01": 17 * 24, "2024-02": 29 * 24, "2024-03": 15 * 24}  # windows landed by land()
ROWS_FOR_ONE_CITY = sum(HOURS_PER_FILE.values())


# ----------------------------------------------------------------------- schema
def test_ensure_schema_creates_the_three_tables_and_is_repeatable():
    conn = make_conn()
    load.ensure_schema(conn)
    load.ensure_schema(conn)  # a second call changes nothing and does not fail
    assert load.table_columns(conn, load.HOURLY_TABLE) == load.hourly_columns()
    assert load.table_columns(conn, load.FILES_TABLE) == load.FILES_COLUMNS
    assert load.table_columns(conn, load.RUNS_TABLE) == load.RUNS_COLUMNS
    assert load.hourly_columns()[:2] == ["city_slug", "observed_at"]
    assert set(config.HOURLY_VARIABLES) <= set(load.hourly_columns())


def test_ensure_schema_refuses_a_table_that_no_longer_matches_the_config():
    conn = make_conn()
    conn.execute(f"CREATE TABLE {load.HOURLY_TABLE} (city_slug VARCHAR, observed_at VARCHAR)")
    with pytest.raises(LoadError, match="raw_weather_hourly"):
        load.ensure_schema(conn)


def test_unsafe_variable_names_are_rejected_before_they_reach_sql(monkeypatch):
    monkeypatch.setattr(config, "HOURLY_VARIABLES", ("temperature_2m", "x; DROP TABLE y"))
    with pytest.raises(LoadError, match="unsafe column name"):
        load.hourly_columns()


# ------------------------------------------------------------------ SQL helpers
def test_insert_rows_splits_large_inserts_into_chunks():
    conn = make_conn()
    conn.execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
    rows = [(i, f"row{i}") for i in range(1203)]
    load.insert_rows(conn, "t", ["a", "b"], rows, chunk=500)
    assert scalar(conn, "SELECT COUNT(*) FROM t") == 1203
    assert scalar(conn, "SELECT b FROM t WHERE a = 1202") == "row1202"
    load.insert_rows(conn, "t", ["a", "b"], [])  # nothing to insert is fine


def test_transaction_commits_on_success_and_rolls_back_on_error():
    conn = make_conn()
    conn.execute("CREATE TABLE t (a INTEGER)")
    with load.transaction(conn):
        conn.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(RuntimeError):
        with load.transaction(conn):
            conn.execute("INSERT INTO t VALUES (2)")
            raise RuntimeError("boom")
    assert [r[0] for r in conn.execute("SELECT a FROM t").fetchall()] == [1]


# ------------------------------------------------------------------ loading
def test_first_load_copies_every_hour_with_lineage(tmp_path):
    land(tmp_path)  # 2 cities x 3 monthly files
    conn = make_conn()
    summary = load.load_files(conn, tmp_path, now=NOW)

    assert len(summary.loaded) == 6 and summary.skipped == 0 and not summary.failed
    assert summary.rows == 2 * ROWS_FOR_ONE_CITY and summary.status == "success"
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}") == 2 * ROWS_FOR_ONE_CITY
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.FILES_TABLE}") == 6

    row = conn.execute(
        f"SELECT city_slug, observed_at, temperature_2m, source_file, run_id FROM {load.HOURLY_TABLE} "
        "WHERE source_file = 'paris/2024-01.json' ORDER BY observed_at LIMIT 1").fetchone()
    assert row == ("paris", "2024-01-15T00:00", 10.0, "paris/2024-01.json", summary.run_id)
    assert scalar(conn, f"SELECT row_count FROM {load.FILES_TABLE} WHERE source_file = 'paris/2024-02.json'") == 29 * 24


def test_values_are_loaded_as_delivered_including_missing_ones(tmp_path):
    land(tmp_path, cities=config.CITIES[:1], start=date(2024, 3, 1))
    path = tmp_path / "paris" / "2024-03.json"
    envelope = json.loads(path.read_text())
    envelope["payload"]["hourly"]["precipitation"][3] = None  # the API sends null for a missing value
    path.write_text(json.dumps(envelope))

    conn = make_conn()
    assert not load.load_files(conn, tmp_path, now=NOW).failed
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE} WHERE precipitation IS NULL") == 1
    assert scalar(conn, f"SELECT observed_at FROM {load.HOURLY_TABLE} WHERE precipitation IS NULL") == "2024-03-01T03:00"


def test_second_load_changes_nothing(tmp_path):
    land(tmp_path)
    conn = make_conn()
    load.load_files(conn, tmp_path, now=NOW)
    summary = load.load_files(conn, tmp_path, now=NOW)

    assert summary.loaded == [] and summary.skipped == 6 and summary.rows == 0
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}") == 2 * ROWS_FOR_ONE_CITY
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.FILES_TABLE}") == 6


def test_a_refreshed_file_replaces_its_rows_without_duplicates(tmp_path):
    land(tmp_path, today=date(2024, 3, 20))
    conn = make_conn()
    load.load_files(conn, tmp_path, now=NOW)

    land(tmp_path, today=date(2024, 3, 22))  # the growing March file now runs to the 17th
    summary = load.load_files(conn, tmp_path, now=NOW)

    assert sorted(summary.loaded) == ["london/2024-03.json", "paris/2024-03.json"] and summary.skipped == 4
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE} WHERE source_file = 'paris/2024-03.json'") == 17 * 24
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}") == 2 * (ROWS_FOR_ONE_CITY + 2 * 24)
    assert scalar(conn, f"SELECT COUNT(*) FROM (SELECT city_slug, observed_at FROM {load.HOURLY_TABLE} "
                        "GROUP BY city_slug, observed_at HAVING COUNT(*) > 1)") == 0
    assert scalar(conn, f"SELECT row_count FROM {load.FILES_TABLE} WHERE source_file = 'paris/2024-03.json'") == 17 * 24


def test_change_is_detected_by_content_not_by_timestamp(tmp_path):
    land(tmp_path, cities=config.CITIES[:1], start=date(2024, 3, 1))
    conn = make_conn()
    load.load_files(conn, tmp_path, now=NOW)
    path = tmp_path / "paris" / "2024-03.json"
    path.write_bytes(path.read_bytes())  # rewritten with identical content, so its modified time changes
    assert load.load_files(conn, tmp_path, now=NOW).skipped == 1


# ------------------------------------------------------------ failures
def test_a_corrupt_file_is_reported_and_the_others_still_load(tmp_path):
    land(tmp_path)
    (tmp_path / "london" / "2024-02.json").write_text("{ not json")
    conn = make_conn()
    summary = load.load_files(conn, tmp_path, now=NOW)

    assert len(summary.loaded) == 5 and len(summary.failed) == 1 and summary.status == "partial"
    assert summary.failed[0][0] == "london/2024-02.json"
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE} WHERE source_file = 'london/2024-02.json'") == 0

    land(tmp_path, today=date(2024, 3, 20))  # extract notices the unreadable file and fetches it again
    again = load.load_files(conn, tmp_path, now=NOW)
    assert again.failed == [] and again.loaded == ["london/2024-02.json"]


def test_a_file_that_fails_validation_is_not_loaded(tmp_path):
    land(tmp_path, cities=config.CITIES[:1], start=date(2024, 3, 1))
    path = tmp_path / "paris" / "2024-03.json"
    envelope = json.loads(path.read_text())
    del envelope["payload"]["hourly"]["cloud_cover"]
    path.write_text(json.dumps(envelope))

    conn = make_conn()
    summary = load.load_files(conn, tmp_path, now=NOW)
    assert summary.status == "failed" and "cloud_cover" in summary.failed[0][1]
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}") == 0


def test_a_crash_while_replacing_a_file_keeps_the_previous_rows(tmp_path, monkeypatch):
    land(tmp_path, today=date(2024, 3, 20), cities=config.CITIES[:1])
    conn = make_conn()
    load.load_files(conn, tmp_path, now=NOW)
    before = scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}")

    land(tmp_path, today=date(2024, 3, 22), cities=config.CITIES[:1])  # March changed on disk
    real_insert = load.insert_rows

    def crashing_insert(conn_, table, columns, rows, chunk=200):
        if table == load.HOURLY_TABLE:
            raise RuntimeError("disk full")
        return real_insert(conn_, table, columns, rows, chunk)

    monkeypatch.setattr(load, "insert_rows", crashing_insert)
    summary = load.load_files(conn, tmp_path, now=NOW)

    assert summary.failed and "disk full" in summary.failed[0][1]
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}") == before  # nothing was lost
    assert scalar(conn, f"SELECT COUNT(*) FROM {load.FILES_TABLE} WHERE source_file = 'paris/2024-03.json'") == 1


def test_an_empty_raw_folder_is_not_an_error(tmp_path):
    conn = make_conn()
    summary = load.load_files(conn, tmp_path, now=NOW)
    assert summary.status == "success" and summary.loaded == [] and summary.skipped == 0


# ------------------------------------------------------------------- run log
def test_each_run_is_recorded_with_its_counts(tmp_path):
    land(tmp_path)
    (tmp_path / "paris" / "2024-01.json").write_text("garbage")
    conn = make_conn()
    first = load.load_files(conn, tmp_path, now=NOW)
    second = load.load_files(conn, tmp_path, now=NOW)

    runs = conn.execute(
        "SELECT run_id, step, status, files_loaded, files_skipped, files_failed, rows_loaded, started_at, "
        f"finished_at FROM {load.RUNS_TABLE} ORDER BY finished_at, run_id").fetchall()
    by_id = {r[0]: r for r in runs}
    assert len(runs) == 2 and first.run_id != second.run_id
    assert by_id[first.run_id][1:7] == ("load", "partial", 5, 0, 1, first.rows)
    assert by_id[second.run_id][1:7] == ("load", "partial", 0, 5, 1, 0)  # the garbage file fails again
    assert by_id[first.run_id][7] == "2024-03-21T06:00:00Z" and by_id[first.run_id][8] is not None


def test_status_logic():
    summary = load.LoadSummary(run_id="x")
    assert summary.status == "success"
    summary.failed.append(("a", "bad"))
    assert summary.status == "failed"
    summary.loaded.append("b")
    assert summary.status == "partial" and summary.message == "a: bad"
    summary.loaded.clear()
    summary.skipped = 4  # everything else was already up to date
    assert summary.status == "partial"


# ------------------------------------------------------- the real database
def test_full_load_on_a_real_duckdb_database(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    land(tmp_path / "raw")
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))

    first = load.load_files(conn, tmp_path / "raw", now=NOW)
    assert len(first.loaded) == 6 and not first.failed
    assert conn.execute(f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}").fetchone()[0] == 2 * ROWS_FOR_ONE_CITY
    assert conn.execute(
        f"SELECT temperature_2m FROM {load.HOURLY_TABLE} WHERE observed_at = '2024-01-15T00:00' LIMIT 1"
    ).fetchone()[0] == 10.0

    second = load.load_files(conn, tmp_path / "raw", now=NOW)
    assert second.skipped == 6 and second.loaded == []

    land(tmp_path / "raw", today=date(2024, 3, 22), api=FakeApi())
    third = load.load_files(conn, tmp_path / "raw", now=NOW)
    assert len(third.loaded) == 2
    assert conn.execute(f"SELECT COUNT(*) FROM {load.HOURLY_TABLE}").fetchone()[0] == 2 * (ROWS_FOR_ONE_CITY + 2 * 24)
    assert conn.execute(f"SELECT COUNT(*) FROM {load.RUNS_TABLE}").fetchone()[0] == 3
    conn.close()