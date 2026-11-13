-- stg_weather_hourly
-- Grain: one row per city and hour.
-- Built from raw_weather_hourly:
--   * de-duplicates: if the same city and hour appears in more than one file,
--     the most recently fetched copy wins (ties broken by file name)
--   * types the text timestamp (the expressions are filled in per database)
--   * adds date, month and hour columns for easy grouping
--   * renames the measurements with their units (the extract step requests these units explicitly)
-- Values are not altered. Plausibility checks belong to the quality-check step.

DROP TABLE IF EXISTS stg_weather_hourly;

CREATE TABLE stg_weather_hourly AS
WITH ranked AS (
    SELECT
        city_slug,
        observed_at AS observed_at_text,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        cloud_cover,
        source_file,
        fetched_at,
        loaded_at,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY city_slug, observed_at
            ORDER BY fetched_at DESC, source_file DESC
        ) AS row_rank
    FROM raw_weather_hourly
)
SELECT
    city_slug,
    $observed_at_expr AS observed_at,
    $observed_date_expr AS observed_date,
    SUBSTR(observed_at_text, 1, 7) AS observed_month,
    CAST(SUBSTR(observed_at_text, 12, 2) AS INTEGER) AS observed_hour,
    temperature_2m AS temperature_c,
    relative_humidity_2m AS relative_humidity_pct,
    precipitation AS precipitation_mm,
    wind_speed_10m AS wind_speed_kmh,
    cloud_cover AS cloud_cover_pct,
    source_file,
    fetched_at,
    loaded_at,
    run_id
FROM ranked
WHERE row_rank = 1;
