"""Project paths and settings. Everything is resolved relative to the repository root."""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Landing zone for raw API responses, and the database the load step will write to.
RAW_DIR = ROOT / "data" / "raw"
WAREHOUSE_PATH = ROOT / "data" / "warehouse" / "weather.duckdb"
SQL_DIR = ROOT / "sql"  # the staging and marts models


@dataclass(frozen=True)
class City:
    name: str
    slug: str  # used in file and folder names
    latitude: float
    longitude: float


CITIES = (
    City("Paris", "paris", 48.8566, 2.3522),
    City("London", "london", 51.5072, -0.1276),
    City("Berlin", "berlin", 52.5200, 13.4050),
    City("Madrid", "madrid", 40.4168, -3.7038),
    City("Rome", "rome", 41.9028, 12.4964),
)

# Hourly variables to request from Open-Meteo.
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
)

# Units are requested explicitly, so staging column names (_c, _mm, _kmh, _pct) are always true.
UNITS = {"temperature_unit": "celsius", "wind_speed_unit": "kmh", "precipitation_unit": "mm"}

# Open-Meteo historical archive API.
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = "GMT"  # timestamps are returned in UTC, so there are no daylight-saving surprises

START_DATE = date(2020, 1, 1)  # how far back the first run loads
ARCHIVE_LAG_DAYS = 5  # the archive trails real time; only request dates up to today minus this

# HTTP behaviour
REQUEST_TIMEOUT_SECONDS = 15  # normal responses take about a second; do not wait long on a stalled one
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0  # doubles after each failed attempt
REQUEST_DELAY_SECONDS = 0.25  # pause between requests, to stay well inside the free limits

# Data quality thresholds (used by the quality step)
QUALITY_RANGES = {  # physically plausible limits for hourly values; anything outside is an error
    "temperature_c": (-50.0, 55.0),
    "relative_humidity_pct": (0.0, 100.0),
    "precipitation_mm": (0.0, 150.0),
    "wind_speed_kmh": (0.0, 250.0),
    "cloud_cover_pct": (0.0, 100.0),
}
MAX_DAILY_TEMPERATURE_SWING_C = 40.0  # a bigger day-and-night gap than this is suspicious (warning)
MAX_NULL_SHARE = 0.02  # more than 2% missing temperatures for a city is worth a warning
FRESHNESS_SLACK_DAYS = 2  # allowed lateness beyond the archive's own delay (ARCHIVE_LAG_DAYS)
STALE_RUN_HOURS = 6  # a run still marked "running" after this long is treated as crashed