# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_config/01_bronze_config

# COMMAND ----------

from pyspark.sql import functions as F

control_table = f"{catalog_name}.orchestration.batch_control"

# COMMAND ----------

class NextBatchFinder:
    """Finds the earliest source month that has not been completed."""

    def find(self):
        source_batches = (
            spark.read.option("header", True).csv(f"{landing_volume_path}/year=*")
            .select(F.to_timestamp("utc_timestamp").alias("utc_timestamp"))
            .filter(F.col("utc_timestamp").isNotNull())
            .select(
                F.date_format("utc_timestamp", "yyyy-MM").alias("batch_id"),
                F.year("utc_timestamp").alias("year"),
                F.month("utc_timestamp").alias("month"),
            )
            .distinct()
        )

        completed_batches = (
            spark.table(control_table)
            .filter(F.col("status") == "completed")
            .select("batch_id")
        )

        rows = (
            source_batches.join(completed_batches, "batch_id", "left_anti")
            .orderBy("year", "month")
            .limit(1)
            .collect()
        )
        return rows[0] if rows else None

# COMMAND ----------

next_batch = NextBatchFinder().find()

if next_batch:
    batch_id = next_batch["batch_id"]
    print(f"Next batch: {batch_id}")
    dbutils.jobs.taskValues.set(key="p_batch_id", value=batch_id)
    dbutils.jobs.taskValues.set(key="has_batch", value="true")
else:
    print("No unprocessed batches were found")
    dbutils.jobs.taskValues.set(key="p_batch_id", value="")
    dbutils.jobs.taskValues.set(key="has_batch", value="false")
