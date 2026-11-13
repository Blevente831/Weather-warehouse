-- fct_weather_monthly
-- Grain: one row per city and month.
-- Statistics use complete days only, so a month with missing hours is not biased.
-- days_total counts every day that has any data; days_complete counts the usable ones.
-- temp_avg_c is the average of the daily averages. A rainy day has at least 1.0 mm.

DROP TABLE IF EXISTS fct_weather_monthly;

CREATE TABLE fct_weather_monthly AS
SELECT
    city_slug,
    observed_month,
    COUNT(*) AS days_total,
    SUM(is_complete_day) AS days_complete,
    ROUND(AVG(CASE WHEN is_complete_day = 1 THEN temp_avg_c END), 2) AS temp_avg_c,
    MIN(CASE WHEN is_complete_day = 1 THEN temp_min_c END) AS temp_min_c,
    MAX(CASE WHEN is_complete_day = 1 THEN temp_max_c END) AS temp_max_c,
    ROUND(SUM(CASE WHEN is_complete_day = 1 THEN precipitation_total_mm END), 1) AS precipitation_total_mm,
    SUM(CASE WHEN is_complete_day = 1 AND precipitation_total_mm >= 1.0 THEN 1 ELSE 0 END) AS rainy_days
FROM fct_weather_daily
GROUP BY city_slug, observed_month;
