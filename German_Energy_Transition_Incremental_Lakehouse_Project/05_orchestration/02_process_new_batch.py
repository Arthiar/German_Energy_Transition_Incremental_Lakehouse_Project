# Databricks notebook source
# MAGIC %run ../02_bronze/01_bronze_ingest

# COMMAND ----------

# MAGIC %run ../03_silver/01_silver_transform

# COMMAND ----------

from pyspark.sql import functions as F

control_table = f"{catalog_name}.orchestration.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

class BatchProcessor:
    """Marks one batch in progress and runs Bronze followed by Silver."""

    def __init__(self):
        self.bronze = BronzeIngestion(
            spark, bronze_table, landing_volume_path,
            germany_column_prefix, timestamp_columns, bronze_merge_key,
        )
        self.silver = SilverTransformation(
            spark, silver_table, bronze_table,
            germany_column_prefix, silver_merge_key, silver_partition_columns,
        )

    def process(self, batch_id):
        if not batch_id:
            raise ValueError("p_batch_id is required")

        year, month = [int(value) for value in batch_id.split("-")]

        spark.sql(f"DELETE FROM {control_table} WHERE batch_id = '{batch_id}'")
        spark.createDataFrame(
            [(batch_id, year, month, "in_progress")],
            ["batch_id", "year", "month", "status"],
        ).select(
            "batch_id", "year", "month", "status",
            F.lit(None).cast("int").alias("rows_bronze"),
            F.lit(None).cast("int").alias("rows_silver"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            F.lit(None).cast("string").alias("error_message"),
        ).write.format("delta").mode("append").saveAsTable(control_table)

        try:
            bronze_result = self.bronze.ingest_batch(year, month)
            silver_result = self.silver.transform_batch(year, month)

            spark.sql(f"""
                UPDATE {control_table}
                SET rows_bronze = {bronze_result['rows_written']},
                    rows_silver = {silver_result['rows_written']},
                    updated_at = current_timestamp()
                WHERE batch_id = '{batch_id}'
            """)
            print(f"Processed {batch_id} through Bronze and Silver")
        except Exception as error:
            safe_error = str(error).replace("'", "")[:500]
            spark.sql(f"""
                UPDATE {control_table}
                SET status = 'failed',
                    updated_at = current_timestamp(),
                    error_message = '{safe_error}'
                WHERE batch_id = '{batch_id}'
            """)
            raise

# COMMAND ----------

BatchProcessor().process(batch_id)

