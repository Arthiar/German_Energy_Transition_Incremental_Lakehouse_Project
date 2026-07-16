# Databricks notebook source
# MAGIC %run ../01_config/02_silver_config

# COMMAND ----------

# Requirements - imports needed before the class below
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

class SilverTransformation:
    """Cleans one bronze batch and MERGEs it into silver. All row-level
    decisions (parsing, casting, dedup, nulls) live here - bronze itself
    never filters or casts anything."""

    def __init__(self, spark, table_name, source_table, column_prefix, merge_key, partition_columns):
        self.spark = spark
        self.table_name = table_name
        self.source_table = source_table
        self.column_prefix = column_prefix
        self.merge_key = merge_key
        self.partition_columns = partition_columns

    def transform_batch(self, year, month):
        batch_id = f"{year}-{month:02d}"

        bronze_df = self.spark.sql(f"""
            SELECT * FROM {self.source_table}
            WHERE batch_id = '{batch_id}'
        """)
        rows_read = bronze_df.count()

        if rows_read == 0:
            print(f"No bronze rows found for batch {batch_id}, nothing to transform")
            return {"rows_written": 0, "status": "success"}

        # Parse timestamps: utc_timestamp ends in a literal 'Z' (e.g. ...T23:00:00Z),
        # cet_cest_timestamp carries a numeric offset instead (e.g. +0100)
        silver_df = (
            bronze_df
            .withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withColumn("cet_cest_timestamp", F.to_timestamp(F.col("cet_cest_timestamp"), "yyyy-MM-dd'T'HH:mm:ssZ"))
        )

        # Rename to snake_case: OPSD columns already use underscores, so
        # lowercasing every column name is enough - no 40-entry rename map to maintain
        for column_name in silver_df.columns:
            silver_df = silver_df.withColumnRenamed(column_name, column_name.lower())

        # Cast every DE_ column from string to double
        de_prefix_lower = self.column_prefix.lower()
        de_columns = [c for c in silver_df.columns if c.startswith(de_prefix_lower)]
        for column_name in de_columns:
            silver_df = silver_df.withColumn(column_name, F.col(column_name).cast("double"))

        # Add partition columns, then dedupe on utc_timestamp keeping the
        # most recently ingested row per timestamp
        dedupe_window = Window.partitionBy(self.merge_key).orderBy(F.col("ingestion_timestamp").desc())

        silver_df = (
            silver_df
            .withColumn("year", F.year(F.col("utc_timestamp")))
            .withColumn("month", F.month(F.col("utc_timestamp")))
            .withColumn("row_rank", F.row_number().over(dedupe_window))
            .filter(F.col("row_rank") == 1)
            .drop("row_rank")
        )

        rows_written = silver_df.count()

        # Null handling: nulls are kept, not dropped - several TSO zone columns
        # only start reporting partway through 2015. One aggregation pass computes
        # null % for every DE_ column, instead of one .count() call per column.
        null_counts = silver_df.agg(
            *[F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in de_columns]
        ).collect()[0]

        print("Null percentage by column:")
        for column_name in de_columns:
            null_percent = round((null_counts[column_name] / rows_written) * 100, 1) if rows_written > 0 else 0.0
            print(f"  {column_name}: {null_percent}% null")

        self._merge_into_silver(silver_df)

        print(f"Silver transform complete for batch {batch_id}. Rows read: {rows_read}, rows written: {rows_written}")
        return {"rows_written": rows_written, "status": "success"}

    def _merge_into_silver(self, df):
        if not self.spark.catalog.tableExists(self.table_name):
            df.write.format("delta").partitionBy(*self.partition_columns).saveAsTable(self.table_name)
            return

        target_table = DeltaTable.forName(self.spark, self.table_name)

        (
            target_table.alias("t")
            .merge(df.alias("s"), f"t.{self.merge_key} = s.{self.merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
