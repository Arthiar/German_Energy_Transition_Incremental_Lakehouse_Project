# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_config/01_bronze_config

# COMMAND ----------

# Requirements - imports needed before the class below
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

class BronzeIngestion:
    """Reads one (year, month) batch from the landing volume, scopes it down
    to the in-scope columns, adds bronze metadata, and MERGEs it into bronze.
    No row filtering beyond selecting this batch, no quality logic - bronze
    is a raw, replayable copy of the in-scope columns."""

    def __init__(self, spark, table_name, volume_path, column_prefix, timestamp_cols, merge_key):
        self.spark = spark
        self.table_name = table_name
        self.volume_path = volume_path
        self.column_prefix = column_prefix
        self.timestamp_cols = timestamp_cols
        self.merge_key = merge_key

    def ingest_batch(self, year, month):
        batch_id = f"{year}-{month:02d}"
        year_path = f"{self.volume_path}/year={year}"

        raw_df = (
            self.spark.read
            .option("header", True)
            .csv(year_path)
            .withColumn("source_file", F.col("_metadata.file_path"))
            .filter(
                (F.year(F.to_timestamp(F.col("utc_timestamp"))) == year)
                & (F.month(F.to_timestamp(F.col("utc_timestamp"))) == month)
            )
        )

        de_columns = [c for c in raw_df.columns if c.startswith(self.column_prefix)]
        columns_to_keep = self.timestamp_cols + de_columns + ["source_file"]

        bronze_df = (
            raw_df
            .select(*columns_to_keep)
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        rows_written = bronze_df.count()

        if rows_written == 0:
            print(f"No rows found for batch {batch_id}, nothing to ingest")
            return {"rows_written": 0, "status": "success"}

        self._merge_into_bronze(bronze_df)

        print(f"Bronze ingest complete for batch {batch_id}. Rows written: {rows_written}")
        return {"rows_written": rows_written, "status": "success"}

    def _merge_into_bronze(self, df):
        if not self.spark.catalog.tableExists(self.table_name):
            df.write.format("delta").saveAsTable(self.table_name)
            return

        target_table = DeltaTable.forName(self.spark, self.table_name)

        (
            target_table.alias("t")
            .merge(df.alias("s"), f"t.{self.merge_key} = s.{self.merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
