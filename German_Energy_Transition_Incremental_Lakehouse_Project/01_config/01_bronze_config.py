# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze config
# MAGIC Catalog names, the landing volume path, and the column scope used by the bronze layer.

# COMMAND ----------

catalog_name = "german_energy_transition_lakehouse"

bronze_schema = "bronze"
bronze_table = f"{catalog_name}.{bronze_schema}.germany_energy_raw"

# Landing volume where the raw OPSD CSV files are staged, one folder per year:
# /Volumes/<catalog>/landing/raw_files/raw/energy/year=YYYY/
landing_volume_path = f"/Volumes/{catalog_name}/landing/raw_files/raw/energy"

# In-scope columns: the two timestamp columns, plus every column that starts
# with "DE_". That single prefix already covers Germany national (DE_),
# the DE_LU joint bidding zone, and all four German TSO zones
# (DE_50hertz_, DE_amprion_, DE_tennet_, DE_transnetbw_), since they all
# start with "DE_". Everything else is a different country and out of scope.
timestamp_columns = ["utc_timestamp", "cet_cest_timestamp"]
germany_column_prefix = "DE_"

# Row key used to MERGE into bronze, so re-running a batch never creates duplicates
bronze_merge_key = "utc_timestamp"
