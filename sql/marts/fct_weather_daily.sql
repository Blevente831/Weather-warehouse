-- fct_weather_daily
-- Grain: one row per city and calendar day (UTC).
-- hours_with_data counts hours that have a temperature reading. A day is "complete"
-- when all 24 hours are present, and only complete days feed the monthly summaries.

DROP TABLE IF EXISTS fct_weather_daily;

CREATE TABLE fct_weather_daily AS
SELECT
    city_slug,
    observed_date,
    observed_month,
    COUNT(temperature_c) AS hours_with_data,
    CASE WHEN COUNT(temperature_c) = 24 THEN 1 ELSE 0 END AS is_complete_day,
    ROUND(MIN(temperature_c), 1) AS temp_min_c,
    ROUND(MAX(temperature_c), 1) AS temp_max_c,
    ROUND(AVG(temperature_c), 2) AS temp_avg_c,
    ROUND(SUM(precipitation_mm), 1) AS precipitation_total_mm,
    ROUND(MAX(wind_speed_kmh), 1) AS wind_max_kmh,
    ROUND(AVG(wind_speed_kmh), 1) AS wind_avg_kmh,
    ROUND(AVG(relative_humidity_pct), 0) AS humidity_avg_pct,
    ROUND(AVG(cloud_cover_pct), 0) AS cloud_cover_avg_pct
FROM stg_weather_hourly
GROUP BY city_slug, observed_date, observed_month;
