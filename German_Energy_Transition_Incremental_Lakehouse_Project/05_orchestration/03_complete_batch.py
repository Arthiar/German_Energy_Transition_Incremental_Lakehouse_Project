# Databricks notebook source
# MAGIC %run ../04_gold/01_gold_aggregate

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

control_table = f"{catalog_name}.orchestration.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

class BatchCompleter:
    """Processes the batch in Gold and marks it complete."""

    def complete(self, batch_id):
        if not batch_id:
            raise ValueError("p_batch_id is required")

        gold = GoldAggregation(
            spark, silver_table, daily_load_summary_table,
            daily_generation_summary_table, daily_wind_solar_share_table, tso_zones,
        )
        gold.process_batch(batch_id)

        updates = (
            spark.createDataFrame([(batch_id,)], ["batch_id"])
            .withColumn("status", F.lit("completed"))
            .withColumn("updated_at", F.current_timestamp())
        )

        (
            DeltaTable.forName(spark, control_table).alias("target")
            .merge(updates.alias("source"), "target.batch_id = source.batch_id")
            .whenMatchedUpdate(
                condition="target.status = 'in_progress'",
                set={
                    "status": "source.status",
                    "updated_at": "source.updated_at",
                    "error_message": "NULL",
                },
            )
            .execute()
        )
        print(f"Completed batch {batch_id}")

# COMMAND ----------

BatchCompleter().complete(batch_id)

# COMMAND ----------

display(spark.table(control_table).orderBy("year", "month"))

