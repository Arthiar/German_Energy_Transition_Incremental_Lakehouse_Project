# Databricks notebook source
# MAGIC %run ../01_config/03_gold_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

class GoldAggregation:
    """Builds daily Gold results for one monthly batch."""

    def __init__(self, spark, silver_table, load_table, generation_table, share_table, tso_zones):
        self.spark = spark
        self.silver_table = silver_table
        self.load_table = load_table
        self.generation_table = generation_table
        self.share_table = share_table
        self.tso_zones = tso_zones

    def process_batch(self, batch_id):
        """Calculate and merge the Gold rows belonging to one batch."""
        daily_df = (
            self.spark.table(self.silver_table)
            .filter(F.col("batch_id") == batch_id)
            .withColumn("event_date", F.to_date("utc_timestamp"))
        )

        if daily_df.limit(1).count() == 0:
            raise ValueError(f"No Silver data found for batch {batch_id}")

        load_df = (
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
        )

        generation_df = self._build_generation_summary(daily_df)

        share_df = (
            generation_df
            .withColumn(
                "de_wind_solar_generation_mwh",
                F.round(
                    F.coalesce(F.col("de_solar_generation_mwh"), F.lit(0.0))
                    + F.coalesce(F.col("de_wind_generation_mwh"), F.lit(0.0)),
                    2,
                ),
            )
            .select("event_date", "de_wind_solar_generation_mwh")
            .join(load_df.select("event_date", "de_load_actual_mwh"), "event_date", "inner")
            .withColumn(
                "wind_solar_share_percent",
                F.when(
                    F.col("de_load_actual_mwh") > 0,
                    F.round(F.col("de_wind_solar_generation_mwh") / F.col("de_load_actual_mwh") * 100, 2),
                ),
            )
        )

        self._merge_table(load_df, self.load_table)
        self._merge_table(generation_df, self.generation_table)
        self._merge_table(share_df, self.share_table)

        print(f"Gold processing completed for batch {batch_id}")

    def _build_generation_summary(self, daily_df):
        """Create national and available TSO-zone generation totals."""
        zero = F.lit(0.0)
        agg_exprs = [
            F.round(F.sum("de_solar_generation_actual"), 2).alias("de_solar_generation_mwh"),
            F.round(
                F.coalesce(F.sum("de_wind_onshore_generation_actual"), zero)
                + F.coalesce(F.sum("de_wind_offshore_generation_actual"), zero),
                2,
            ).alias("de_wind_generation_mwh"),
        ]

        for zone in self.tso_zones:
            solar_col = f"de_{zone}_solar_generation_actual"
            wind_columns = [
                column_name
                for column_name in (
                    f"de_{zone}_wind_onshore_generation_actual",
                    f"de_{zone}_wind_offshore_generation_actual",
                )
                if column_name in daily_df.columns
            ]

            if solar_col in daily_df.columns:
                agg_exprs.append(F.round(F.sum(solar_col), 2).alias(f"{zone}_solar_generation_mwh"))

            if wind_columns:
                wind_total = sum(
                    (F.coalesce(F.sum(column_name), zero) for column_name in wind_columns),
                    zero,
                )
                agg_exprs.append(F.round(wind_total, 2).alias(f"{zone}_wind_generation_mwh"))

        return daily_df.groupBy("event_date").agg(*agg_exprs)

    def _merge_table(self, dataframe, table_name):
        """Insert new dates and update dates already present in Gold."""
        if not self.spark.catalog.tableExists(table_name):
            dataframe.write.format("delta").saveAsTable(table_name)
            return

        (
            DeltaTable.forName(self.spark, table_name).alias("target")
            .merge(dataframe.alias("source"), "target.event_date = source.event_date")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
