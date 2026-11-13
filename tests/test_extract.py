import io
import json
import urllib.error
from datetime import date, datetime, timedelta

import pytest

from weatherwh import config, extract
from weatherwh.config import City
from tests.helpers import FakeApi, make_payload
from weatherwh.extract import ExtractError

PARIS = config.CITIES[0]
LONDON = config.CITIES[1]


# ------------------------------------------------------------------ test doubles
class FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code: int, body: bytes):
    return urllib.error.HTTPError("http://x", code, "error", {}, io.BytesIO(body))


# ------------------------------------------------------------------------ config
def test_config_is_sane():
    slugs = [c.slug for c in config.CITIES]
    assert len(slugs) == len(set(slugs)), "city slugs must be unique"
    assert all(-90 <= c.latitude <= 90 and -180 <= c.longitude <= 180 for c in config.CITIES)
    assert config.HOURLY_VARIABLES and config.ARCHIVE_LAG_DAYS >= 0
    assert config.ARCHIVE_URL.startswith("https://")


def test_requests_explicit_units_and_utc():
    params = extract.build_params(PARIS, date(2024, 1, 1), date(2024, 1, 31))
    assert params["temperature_unit"] == "celsius" and params["wind_speed_unit"] == "kmh"
    assert params["precipitation_unit"] == "mm" and params["timezone"] == "GMT"


# ---------------------------------------------------------------------- planning
def test_month_windows_split_on_calendar_months():
    windows = extract.month_windows(date(2024, 1, 15), date(2024, 3, 10))
    assert windows == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),  # 2024 is a leap year
        (date(2024, 3, 1), date(2024, 3, 10)),
    ]


def test_month_windows_single_day_and_empty_range():
    assert extract.month_windows(date(2024, 5, 5), date(2024, 5, 5)) == [(date(2024, 5, 5), date(2024, 5, 5))]
    assert extract.month_windows(date(2024, 5, 6), date(2024, 5, 5)) == []


def test_month_windows_cross_a_year_boundary():
    windows = extract.month_windows(date(2023, 12, 20), date(2024, 1, 5))
    assert windows == [(date(2023, 12, 20), date(2023, 12, 31)), (date(2024, 1, 1), date(2024, 1, 5))]


def test_latest_available_date_applies_the_lag():
    assert extract.latest_available_date(date(2024, 3, 20)) == date(2024, 3, 20) - timedelta(days=config.ARCHIVE_LAG_DAYS)


def test_raw_path_layout(tmp_path):
    assert extract.raw_path(tmp_path, PARIS, date(2024, 2, 1)) == tmp_path / "paris" / "2024-02.json"


def test_select_cities():
    assert extract.select_cities(None) == list(config.CITIES)
    assert [c.slug for c in extract.select_cities("Rome, paris")] == ["rome", "paris"]
    with pytest.raises(ExtractError):
        extract.select_cities("paris,atlantis")


# ----------------------------------------------------------------------- fetching
def test_fetch_json_retries_temporary_failures_with_backoff():
    attempts, sleeps = [], []

    def opener(request, timeout):
        attempts.append(request.full_url)
        if len(attempts) < 3:
            raise urllib.error.URLError("connection reset")
        return FakeResponse(b'{"ok": true}')

    result = extract.fetch_json("http://api", {"a": 1}, opener=opener, sleep=sleeps.append, retries=3, backoff=1.0)
    assert result == {"ok": True}
    assert len(attempts) == 3 and sleeps == [1.0, 2.0]
    assert attempts[0] == "http://api?a=1"


def test_fetch_json_gives_up_after_the_retry_budget():
    calls = []

    def opener(request, timeout):
        calls.append(1)
        raise http_error(503, b"{}")

    with pytest.raises(ExtractError, match="giving up after 3 attempts"):
        extract.fetch_json("http://api", {}, opener=opener, sleep=lambda s: None, retries=2, backoff=0.0)
    assert len(calls) == 3


def test_fetch_json_retries_rate_limiting():
    calls = []

    def opener(request, timeout):
        calls.append(1)
        if len(calls) == 1:
            raise http_error(429, b'{"error": true, "reason": "too many requests"}')
        return FakeResponse(b"{}")

    assert extract.fetch_json("http://api", {}, opener=opener, sleep=lambda s: None) == {}
    assert len(calls) == 2


def test_fetch_json_does_not_retry_a_bad_request_and_reports_the_reason():
    calls = []

    def opener(request, timeout):
        calls.append(1)
        raise http_error(400, b'{"error": true, "reason": "Parameter start_date is out of range"}')

    with pytest.raises(ExtractError, match="HTTP 400: Parameter start_date is out of range"):
        extract.fetch_json("http://api", {}, opener=opener, sleep=lambda s: None)
    assert len(calls) == 1


def test_fetch_json_retries_unreadable_responses():
    bodies = [b"<html>gateway error</html>", b'{"fine": 1}']

    def opener(request, timeout):
        return FakeResponse(bodies.pop(0))

    assert extract.fetch_json("http://api", {}, opener=opener, sleep=lambda s: None) == {"fine": 1}


# --------------------------------------------------------------------- validation
def good_payload():
    return make_payload(extract.build_params(PARIS, date(2024, 1, 1), date(2024, 1, 3)))


def test_validate_accepts_a_correct_payload():
    extract.validate_payload(good_payload(), date(2024, 1, 1), date(2024, 1, 3), config.HOURLY_VARIABLES)


def test_validate_rejects_bad_payloads():
    start, end, variables = date(2024, 1, 1), date(2024, 1, 3), config.HOURLY_VARIABLES

    no_hourly = {"error": True}
    with pytest.raises(ExtractError, match="no 'hourly'"):
        extract.validate_payload(no_hourly, start, end, variables)

    missing_variable = good_payload()
    del missing_variable["hourly"]["precipitation"]
    with pytest.raises(ExtractError, match="precipitation"):
        extract.validate_payload(missing_variable, start, end, variables)

    ragged = good_payload()
    ragged["hourly"]["temperature_2m"].pop()
    with pytest.raises(ExtractError, match="temperature_2m"):
        extract.validate_payload(ragged, start, end, variables)

    truncated = good_payload()
    for values in truncated["hourly"].values():
        values.pop()
    with pytest.raises(ExtractError, match="expected 72 hourly rows"):
        extract.validate_payload(truncated, start, end, variables)

    shifted = good_payload()
    shifted["hourly"]["time"] = [(datetime.fromisoformat(t) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
                                 for t in shifted["hourly"]["time"]]
    with pytest.raises(ExtractError, match="timestamps run"):
        extract.validate_payload(shifted, start, end, variables)


# ----------------------------------------------------------------------- landing
def test_write_envelope_keeps_the_payload_and_records_provenance(tmp_path):
    task = extract.Task(PARIS, date(2024, 1, 1), date(2024, 1, 3), extract.raw_path(tmp_path, PARIS, date(2024, 1, 1)))
    params = extract.build_params(PARIS, task.start, task.end)
    payload = make_payload(params)
    extract.write_envelope(task.path, task, params, payload, datetime(2024, 2, 1, 6, 0))

    saved = json.loads(task.path.read_text())
    assert saved["payload"] == payload  # the API response is stored untouched
    assert saved["meta"]["slug"] == "paris" and saved["meta"]["end_date"] == "2024-01-03"
    assert saved["meta"]["fetched_at"] == "2024-02-01T06:00:00"
    assert saved["meta"]["params"]["hourly"] == ",".join(config.HOURLY_VARIABLES)


def test_write_envelope_leaves_no_temp_files_even_when_it_fails(tmp_path):
    task = extract.Task(PARIS, date(2024, 1, 1), date(2024, 1, 1), tmp_path / "paris" / "2024-01.json")
    with pytest.raises(TypeError):
        extract.write_envelope(task.path, task, {}, {"bad": object()}, datetime(2024, 1, 1))  # not JSON serializable
    assert not task.path.exists()
    assert list((tmp_path / "paris").glob("*.tmp")) == []


def test_unreadable_file_is_treated_as_missing(tmp_path):
    path = tmp_path / "paris" / "2024-01.json"
    path.parent.mkdir()
    path.write_text("{ this is not json")
    assert extract.read_envelope(path) is None
    assert extract.needs_fetch(path, (date(2024, 1, 1), date(2024, 1, 31)))


# ------------------------------------------------------------ the whole extract
def run(api, tmp_path, today, start=date(2024, 1, 15), cities=(PARIS, LONDON)):
    return extract.extract_all(cities, today=today, start=start, raw_dir=tmp_path, fetch=api,
                               delay=0, sleep=lambda s: None)


def test_first_run_lands_one_file_per_city_and_month(tmp_path):
    api = FakeApi()
    summary = run(api, tmp_path, today=date(2024, 3, 20))  # data available through 15 March
    assert len(api.calls) == 6 and len(summary.fetched) == 6 and not summary.failed
    for slug in ("paris", "london"):
        assert sorted(p.name for p in (tmp_path / slug).iterdir()) == ["2024-01.json", "2024-02.json", "2024-03.json"]
    assert api.calls[0]["start_date"] == "2024-01-15"  # the first window starts at --start, not on the 1st
    assert api.calls[2]["end_date"] == "2024-03-15"  # the last window ends at the latest available date


def test_second_run_is_a_no_op(tmp_path):
    run(FakeApi(), tmp_path, today=date(2024, 3, 20))
    api = FakeApi()
    summary = run(api, tmp_path, today=date(2024, 3, 20))
    assert api.calls == [] and summary.fetched == [] and summary.skipped == 6


def test_a_later_run_refreshes_only_the_growing_month(tmp_path):
    run(FakeApi(), tmp_path, today=date(2024, 3, 20))
    api = FakeApi()
    summary = run(api, tmp_path, today=date(2024, 3, 22))  # two more days of data now exist
    assert len(api.calls) == 2 and {c["latitude"] for c in api.calls} == {PARIS.latitude, LONDON.latitude}
    assert all(c["start_date"] == "2024-03-01" and c["end_date"] == "2024-03-17" for c in api.calls)
    assert summary.skipped == 4
    saved = json.loads((tmp_path / "paris" / "2024-03.json").read_text())
    assert saved["meta"]["end_date"] == "2024-03-17" and len(saved["payload"]["hourly"]["time"]) == 17 * 24


def test_a_month_boundary_completes_the_previous_month_then_starts_the_next(tmp_path):
    run(FakeApi(), tmp_path, today=date(2024, 3, 20))
    api = FakeApi()
    run(api, tmp_path, today=date(2024, 4, 3), cities=(PARIS,))  # available through 29 March, then 5 April
    assert [(c["start_date"], c["end_date"]) for c in api.calls] == [("2024-03-01", "2024-03-29")]
    api = FakeApi()
    run(api, tmp_path, today=date(2024, 4, 10), cities=(PARIS,))  # March is complete; April begins
    assert [(c["start_date"], c["end_date"]) for c in api.calls] == [("2024-03-01", "2024-03-31"), ("2024-04-01", "2024-04-05")]


def test_widening_the_start_date_backfills_only_the_new_months(tmp_path):
    run(FakeApi(), tmp_path, today=date(2024, 3, 20), start=date(2024, 2, 1))
    api = FakeApi()
    run(api, tmp_path, today=date(2024, 3, 20), start=date(2024, 1, 1), cities=(PARIS,))
    assert [(c["start_date"], c["end_date"]) for c in api.calls] == [("2024-01-01", "2024-01-31")]


def test_partial_first_month_is_refetched_when_start_moves_earlier_within_it(tmp_path):
    run(FakeApi(), tmp_path, today=date(2024, 3, 20), start=date(2024, 1, 15), cities=(PARIS,))
    api = FakeApi()
    run(api, tmp_path, today=date(2024, 3, 20), start=date(2024, 1, 1), cities=(PARIS,))
    assert [(c["start_date"], c["end_date"]) for c in api.calls] == [("2024-01-01", "2024-01-31")]


def test_one_failing_city_does_not_stop_the_others(tmp_path):
    api = FakeApi(fail_for_latitude=LONDON.latitude)
    summary = run(api, tmp_path, today=date(2024, 3, 20))
    assert len(summary.fetched) == 3 and len(summary.failed) == 3
    assert all("london" in label and "simulated outage" in reason for label, reason in summary.failed)
    assert (tmp_path / "paris" / "2024-03.json").exists() and not (tmp_path / "london").exists()


def test_a_failed_city_is_retried_on_the_next_run(tmp_path):
    run(FakeApi(fail_for_latitude=LONDON.latitude), tmp_path, today=date(2024, 3, 20))
    api = FakeApi()
    summary = run(api, tmp_path, today=date(2024, 3, 20))
    assert len(api.calls) == 3 and not summary.failed
    assert {c["latitude"] for c in api.calls} == {LONDON.latitude}


def test_invalid_response_is_not_saved(tmp_path):
    def broken_api(url, params):
        payload = make_payload(params)
        payload["hourly"]["temperature_2m"].pop()
        return payload

    summary = extract.extract_all([PARIS], today=date(2024, 3, 20), start=date(2024, 3, 1), raw_dir=tmp_path,
                                  fetch=broken_api, delay=0, sleep=lambda s: None)
    assert summary.failed and not (tmp_path / "paris").exists()


def test_nothing_to_do_when_start_is_after_the_latest_available_date(tmp_path):
    api = FakeApi()
    summary = run(api, tmp_path, today=date(2024, 3, 20), start=date(2024, 3, 19))
    assert api.calls == [] and summary.fetched == [] and summary.skipped == 0


def test_the_pause_between_requests_is_applied(tmp_path):
    pauses = []
    extract.extract_all([PARIS], today=date(2024, 3, 20), start=date(2024, 1, 15), raw_dir=tmp_path,
                        fetch=FakeApi(), delay=0.5, sleep=pauses.append)
    assert pauses == [0.5, 0.5]  # between three requests, not after the last


def test_custom_city_objects_work(tmp_path):
    oslo = City("Oslo", "oslo", 59.9139, 10.7522)
    api = FakeApi()
    run(api, tmp_path, today=date(2024, 3, 20), start=date(2024, 3, 1), cities=(oslo,))
    assert (tmp_path / "oslo" / "2024-03.json").exists()