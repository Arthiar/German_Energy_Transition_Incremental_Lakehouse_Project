-- Databricks notebook source
-- MAGIC %md
-- MAGIC # German electricity analytics
-- MAGIC
-- MAGIC This notebook contains simple business-focused SQL analysis using the
-- MAGIC Gold tables. Run the complete data pipeline before running these queries.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Preview daily electricity load
-- MAGIC
-- MAGIC Shows the most recent daily actual load, forecast load and forecast error.

-- COMMAND ----------

SELECT
    event_date,
    de_load_actual_mwh,
    de_load_forecast_mwh,
    de_forecast_error_mwh
FROM german_energy_transition_lakehouse.gold.daily_load_summary
ORDER BY event_date DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Monthly load trend
-- MAGIC
-- MAGIC Groups daily values into months to show how electricity demand changes
-- MAGIC over time.

-- COMMAND ----------

SELECT
    DATE_TRUNC('month', event_date) AS month,
    ROUND(SUM(de_load_actual_mwh), 2) AS monthly_load_mwh,
    ROUND(AVG(de_load_actual_mwh), 2) AS average_daily_load_mwh
FROM german_energy_transition_lakehouse.gold.daily_load_summary
GROUP BY DATE_TRUNC('month', event_date)
ORDER BY month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Monthly forecast accuracy
-- MAGIC
-- MAGIC Mean absolute error uses the absolute difference between actual and
-- MAGIC forecast load. A smaller value means the forecast was closer to reality.

-- COMMAND ----------

SELECT
    DATE_TRUNC('month', event_date) AS month,
    ROUND(AVG(ABS(de_forecast_error_mwh)), 2) AS mean_absolute_error_mwh,
    ROUND(
        AVG(ABS(de_forecast_error_mwh) / NULLIF(de_load_actual_mwh, 0)) * 100,
        2
    ) AS mean_absolute_percentage_error
FROM german_energy_transition_lakehouse.gold.daily_load_summary
GROUP BY DATE_TRUNC('month', event_date)
ORDER BY month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Daily wind and solar share trend
-- MAGIC
-- MAGIC This result works well as a line chart in Databricks.

-- COMMAND ----------

SELECT
    event_date,
    wind_solar_share_percent
FROM german_energy_transition_lakehouse.gold.daily_wind_solar_share
ORDER BY event_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Top 10 wind and solar days
-- MAGIC
-- MAGIC Finds the days when wind and solar supplied the largest percentage
-- MAGIC of Germany's actual electricity load.

-- COMMAND ----------

SELECT
    event_date,
    de_wind_solar_generation_mwh,
    de_load_actual_mwh,
    wind_solar_share_percent
FROM german_energy_transition_lakehouse.gold.daily_wind_solar_share
WHERE wind_solar_share_percent IS NOT NULL
ORDER BY wind_solar_share_percent DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Monthly solar and wind generation
-- MAGIC
-- MAGIC Compares the two renewable sources at a monthly level.

-- COMMAND ----------

SELECT
    DATE_TRUNC('month', event_date) AS month,
    ROUND(SUM(de_solar_generation_mwh), 2) AS solar_generation_mwh,
    ROUND(SUM(de_wind_generation_mwh), 2) AS wind_generation_mwh,
    ROUND(
        SUM(de_solar_generation_mwh) + SUM(de_wind_generation_mwh),
        2
    ) AS total_renewable_generation_mwh
FROM german_energy_transition_lakehouse.gold.daily_generation_summary
GROUP BY DATE_TRUNC('month', event_date)
ORDER BY month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Solar versus wind contribution
-- MAGIC
-- MAGIC Calculates each source's percentage of combined solar and wind generation.

-- COMMAND ----------

WITH renewable_totals AS (
    SELECT
        SUM(de_solar_generation_mwh) AS solar_mwh,
        SUM(de_wind_generation_mwh) AS wind_mwh
    FROM german_energy_transition_lakehouse.gold.daily_generation_summary
)
SELECT
    ROUND(solar_mwh, 2) AS solar_generation_mwh,
    ROUND(wind_mwh, 2) AS wind_generation_mwh,
    ROUND(solar_mwh / NULLIF(solar_mwh + wind_mwh, 0) * 100, 2) AS solar_percent,
    ROUND(wind_mwh / NULLIF(solar_mwh + wind_mwh, 0) * 100, 2) AS wind_percent
FROM renewable_totals;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Yearly wind and solar progress
-- MAGIC
-- MAGIC Summarizes wind and solar share by year. The average shows a typical day,
-- MAGIC while the maximum identifies the strongest renewable day in that year.

-- COMMAND ----------

SELECT
    YEAR(event_date) AS year,
    ROUND(AVG(wind_solar_share_percent), 2) AS average_wind_solar_share_percent,
    ROUND(MAX(wind_solar_share_percent), 2) AS maximum_wind_solar_share_percent,
    ROUND(SUM(de_wind_solar_generation_mwh), 2) AS wind_solar_generation_mwh
FROM german_energy_transition_lakehouse.gold.daily_wind_solar_share
GROUP BY YEAR(event_date)
ORDER BY year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Seasonal electricity load
-- MAGIC
-- MAGIC Assigns each month to a season and compares average daily demand.

-- COMMAND ----------

WITH seasonal_load AS (
    SELECT
        CASE
            WHEN MONTH(event_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(event_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(event_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
        END AS season,
        de_load_actual_mwh
    FROM german_energy_transition_lakehouse.gold.daily_load_summary
)
SELECT
    season,
    ROUND(AVG(de_load_actual_mwh), 2) AS average_daily_load_mwh
FROM seasonal_load
GROUP BY season
ORDER BY
    CASE season
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        WHEN 'Autumn' THEN 4
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Combined monthly business summary
-- MAGIC
-- MAGIC Joins all three Gold subjects into one result that can support a dashboard.

-- COMMAND ----------

WITH monthly_load AS (
    SELECT
        DATE_TRUNC('month', event_date) AS month,
        SUM(de_load_actual_mwh) AS load_mwh,
        AVG(ABS(de_forecast_error_mwh)) AS forecast_error_mwh
    FROM german_energy_transition_lakehouse.gold.daily_load_summary
    GROUP BY DATE_TRUNC('month', event_date)
),
monthly_generation AS (
    SELECT
        DATE_TRUNC('month', event_date) AS month,
        SUM(de_solar_generation_mwh) AS solar_mwh,
        SUM(de_wind_generation_mwh) AS wind_mwh
    FROM german_energy_transition_lakehouse.gold.daily_generation_summary
    GROUP BY DATE_TRUNC('month', event_date)
),
monthly_share AS (
    SELECT
        DATE_TRUNC('month', event_date) AS month,
        AVG(wind_solar_share_percent) AS wind_solar_share_percent
    FROM german_energy_transition_lakehouse.gold.daily_wind_solar_share
    GROUP BY DATE_TRUNC('month', event_date)
)
SELECT
    load.month,
    ROUND(load.load_mwh, 2) AS load_mwh,
    ROUND(generation.solar_mwh, 2) AS solar_mwh,
    ROUND(generation.wind_mwh, 2) AS wind_mwh,
    ROUND(share.wind_solar_share_percent, 2) AS wind_solar_share_percent,
    ROUND(load.forecast_error_mwh, 2) AS mean_absolute_forecast_error_mwh
FROM monthly_load AS load
INNER JOIN monthly_generation AS generation USING (month)
INNER JOIN monthly_share AS share USING (month)
ORDER BY load.month;

