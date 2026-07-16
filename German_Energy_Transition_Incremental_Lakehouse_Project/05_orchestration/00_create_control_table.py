# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_config/01_bronze_config

# COMMAND ----------

control_table = f"{catalog_name}.orchestration.batch_control"

# COMMAND ----------

class ControlTableSetup:
    """Creates the Delta table used to track monthly batches."""

    def create(self):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {control_table} (
                batch_id STRING,
                year INT,
                month INT,
                status STRING,
                rows_bronze INT,
                rows_silver INT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                error_message STRING
            )
            USING DELTA
        """)
        print(f"Control table is ready: {control_table}")

# COMMAND ----------

ControlTableSetup().create()

# COMMAND ----------

display(spark.table(control_table).orderBy("year", "month"))
