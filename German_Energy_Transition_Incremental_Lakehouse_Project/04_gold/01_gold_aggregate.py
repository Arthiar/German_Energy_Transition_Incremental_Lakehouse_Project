# Databricks notebook source
# MAGIC %run ../01_config/03_gold_config

# COMMAND ----------

# Requirements - imports needed before the class below
from pyspark.sql import functions as F

# COMMAND ----------

class GoldAggregation:
    """Recomputes all three gold tables from the current state of silver.
    A full recompute is fine at this data size - no incremental gold logic."""

    def __init__(self, spark, silver_table, load_table, generation_table, share_table, tso_zones):
        self.spark = spark
        self.silver_table = silver_table
        self.load_table = load_table
        self.generation_table = generation_table
        self.share_table = share_table
        self.tso_zones = tso_zones

    def refresh_gold(self):
        silver_df = self.spark.sql(f"SELECT * FROM {self.silver_table}")
        daily_df = silver_df.withColumn("event_date", F.to_date(F.col("utc_timestamp")))

        # Daily load: DE and DE_LU actual/forecast, hourly MW summed into daily MWh
        load_summary_df = (
            daily_df
            .groupBy("event_date")
            .agg(
                F.round(F.sum("de_load_actual_entsoe_transparency"), 2).alias("de_load_actual_mwh"),
                F.round(F.sum("de_load_forecast_entsoe_transparency"), 2).alias("de_load_forecast_mwh"),
                F.round(F.sum("de_lu_load_actual_entsoe_transparency"), 2).alias("de_lu_load_actual_mwh"),
                F.round(F.sum("de_lu_load_forecast_entsoe_transparency"), 2).alias("de_lu_load_forecast_mwh"),
            )
            .withColumn("de_forecast_error_mwh", F.round(F.col("de_load_actual_mwh") - F.col("de_load_forecast_mwh"), 2))
            .withColumn("de_lu_forecast_error_mwh", F.round(F.col("de_lu_load_actual_mwh") - F.col("de_lu_load_forecast_mwh"), 2))
            .orderBy("event_date")
        )

        # Daily generation: national totals plus a per-TSO-zone breakdown
        generation_summary_df = self._build_generation_summary(daily_df)

        # Daily renewable share: national renewable generation as % of national load
        renewable_share_df = (
            generation_summary_df
            .withColumn("de_renewable_generation_mwh", F.round(F.col("de_solar_generation_mwh") + F.col("de_wind_generation_mwh"), 2))
            .select("event_date", "de_renewable_generation_mwh")
            .join(load_summary_df.select("event_date", "de_load_actual_mwh"), on="event_date", how="inner")
            .withColumn("renewable_share_percent", F.round((F.col("de_renewable_generation_mwh") / F.col("de_load_actual_mwh")) * 100, 1))
            .orderBy("event_date")
        )

        load_summary_df.write.format("delta").mode("overwrite").saveAsTable(self.load_table)
        generation_summary_df.write.format("delta").mode("overwrite").saveAsTable(self.generation_table)
        renewable_share_df.write.format("delta").mode("overwrite").saveAsTable(self.share_table)

        rows_load = load_summary_df.count()
        rows_generation = generation_summary_df.count()
        rows_share = renewable_share_df.count()

        print("Gold tables refreshed:")
        print(f"  {self.load_table}: {rows_load} rows")
        print(f"  {self.generation_table}: {rows_generation} rows")
        print(f"  {self.share_table}: {rows_share} rows")

        return {
            "daily_load_summary_rows": rows_load,
            "daily_generation_summary_rows": rows_generation,
            "daily_renewable_share_rows": rows_share,
        }

    def _build_generation_summary(self, daily_df):
        agg_exprs = [
            F.round(F.sum("de_solar_generation_actual"), 2).alias("de_solar_generation_mwh"),
            F.round(
                F.sum("de_wind_onshore_generation_actual") + F.sum("de_wind_offshore_generation_actual"), 2
            ).alias("de_wind_generation_mwh"),
        ]

        for zone in self.tso_zones:
            solar_col = f"de_{zone}_solar_generation_actual"
            onshore_col = f"de_{zone}_wind_onshore_generation_actual"
            offshore_col = f"de_{zone}_wind_offshore_generation_actual"

            if solar_col in daily_df.columns:
                agg_exprs.append(F.round(F.sum(solar_col), 2).alias(f"{zone}_solar_generation_mwh"))

            if onshore_col in daily_df.columns and offshore_col in daily_df.columns:
                wind_expr = F.sum(onshore_col) + F.sum(offshore_col)
            elif onshore_col in daily_df.columns:
                wind_expr = F.sum(onshore_col)
            elif offshore_col in daily_df.columns:
                wind_expr = F.sum(offshore_col)
            else:
                wind_expr = None

            if wind_expr is not None:
                agg_exprs.append(F.round(wind_expr, 2).alias(f"{zone}_wind_generation_mwh"))

        return daily_df.groupBy("event_date").agg(*agg_exprs).orderBy("event_date")
