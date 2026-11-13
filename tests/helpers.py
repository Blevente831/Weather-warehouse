"""Shared test doubles: a fake weather API and a helper that lands files with it."""

import sqlite3
from datetime import date, datetime, timedelta

from weatherwh import config, extract, load
from weatherwh.extract import ExtractError


def make_payload(params: dict) -> dict:
    """What the API returns for a request: one row per hour, every requested variable."""
    start = date.fromisoformat(params["start_date"])
    end = date.fromisoformat(params["end_date"])
    hours = ((end - start).days + 1) * 24
    first = datetime(start.year, start.month, start.day)
    times = [(first + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(hours)]
    hourly = {"time": times}
    for variable in params["hourly"].split(","):
        hourly[variable] = [10.0] * hours
    return {"latitude": params["latitude"], "longitude": params["longitude"], "hourly": hourly}


class FakeApi:
    """Stands in for fetch_json: records every call and never touches the network."""

    def __init__(self, fail_for_latitude=None):
        self.calls = []
        self.fail_for_latitude = fail_for_latitude

    def __call__(self, url, params):
        self.calls.append(params)
        if params["latitude"] == self.fail_for_latitude:
            raise ExtractError("simulated outage")
        return make_payload(params)


def land(raw_dir, today=date(2024, 3, 20), start=date(2024, 1, 15), cities=None, api=None):
    """Run the real extract step against the fake API, writing files under raw_dir."""
    api = api or FakeApi()
    extract.extract_all(cities or config.CITIES[:2], today=today, start=start, raw_dir=raw_dir,
                        fetch=api, delay=0, sleep=lambda s: None)
    return api


def make_conn():
    """An in-memory SQLite connection standing in for DuckDB (autocommit, like DuckDB)."""
    return sqlite3.connect(":memory:", isolation_level=None)


def scalar(conn, sql, params=()):
    return conn.execute(sql, list(params)).fetchone()[0]


def prepared():
    """An empty in-memory warehouse with the raw tables created."""
    conn = make_conn()
    load.ensure_schema(conn)
    return conn


def raw_day(conn, city, day, temps, precips=None, humidity=80.0, wind=10.0, cloud=50.0,
            source="manual/a.json", fetched_at="2024-03-01T00:00:00Z"):
    """Insert one day of hand-made raw rows (one per entry in temps, starting at hour 0)."""
    precips = precips if precips is not None else [0.0] * len(temps)
    rows = [
        (city, f"{day}T{hour:02d}:00", temp, humidity, precips[hour], wind, cloud,
         source, fetched_at, "2024-03-21T06:00:00Z", "test-run")
        for hour, temp in enumerate(temps)
    ]
    load.insert_rows(conn, load.HOURLY_TABLE, load.hourly_columns(), rows)