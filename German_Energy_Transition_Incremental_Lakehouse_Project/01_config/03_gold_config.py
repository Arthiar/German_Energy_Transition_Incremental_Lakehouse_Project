# Databricks notebook source
# MAGIC %run ./02_silver_config

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold config
# MAGIC Names of the three gold output tables, and the list of German TSO
# MAGIC (transmission system operator) zones used for the per-zone generation rollup.

# COMMAND ----------

gold_schema = "gold"

daily_load_summary_table = f"{catalog_name}.{gold_schema}.daily_load_summary"
daily_generation_summary_table = f"{catalog_name}.{gold_schema}.daily_generation_summary"
daily_renewable_share_table = f"{catalog_name}.{gold_schema}.daily_renewable_share"

# The four German TSO zones. Not every zone reports every generation type
# (for example, not all of them have offshore wind), so the gold build
# checks which columns actually exist for each zone before summing them.
tso_zones = ["50hertz", "amprion", "tennet", "transnetbw"]
