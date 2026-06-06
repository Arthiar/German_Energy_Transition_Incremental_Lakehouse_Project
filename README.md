# German Energy Transition Analytics Lakehouse

## What This Project Is

This is an end-to-end data engineering project I built to analyze Germany's renewable energy transition. The idea is to take hourly electricity data from 2015 to 2020 and process it through a proper data pipeline - landing, Bronze, Silver, and Gold - using PySpark, Delta Lake, and incremental batch processing with a control table.

The dataset comes from Open Power System Data (OPSD), which is a public European electricity time series. I filtered it down to Germany-specific columns only.

**Author:** Arthisree Saraswathi Rajamanickam
**Status:** Active - Phase 2 in progress
**Dataset:** Open Power System Data - Time Series (2015-2020, hourly, 50,401 rows)

---

## Where Things Stand Right Now

### What Is Actually Built (Done)

I started this project on Microsoft Azure using Azure Databricks, ADLS Gen2, and Unity Catalog. What I completed covers the infrastructure setup and the full data preparation layer - from raw source file all the way to initialized batch control tables ready for ingestion.

To be direct: Bronze, Silver, and Gold layers have not been built yet. The pipeline stops at the point where the data is prepared, split into monthly batches, and the batch control table is populated and waiting. The reason is cost. A NAT gateway that gets provisioned as part of an Azure Databricks workspace charges around 1.50 EUR per day just to exist, even when nothing is running. Over a month that added up to roughly 74 EUR before I caught it. At that point I made the decision to delete the Azure resources rather than continue spending money on infrastructure for a portfolio project.

All the code that was written is preserved in this repository. Nothing is lost - it just stops where the money ran out.

What is actually done and in this repo:
- Resource Group, Storage Account (ADLS Gen2), Databricks Workspace, Unity Catalog all set up and configured
- Storage credential and external location created with Managed Identity authentication
- Source file validated - 130 MB, 50,401 rows, 300 columns across Europe
- 11 Germany-specific columns extracted from the multi-country dataset
- 72 monthly batch files created covering 2015-01 to 2020-12, stored in the landing layer
- Batch control table initialized with all 72 batches in pending status, ready for Bronze ingestion
- Pipeline run log table created
- Centralized config notebook built for path and catalog management

### What Comes Next - Databricks Community Edition (Not Started Yet)

Bronze, Silver, and Gold layers are planned to be built on Databricks Community Edition, which is free and has no ongoing infrastructure costs. The core logic will stay the same - same medallion architecture, same Delta MERGE pattern, same batch control table. The only technical difference is that Community Edition does not support Unity Catalog or scheduled Workflows, so the catalog switches to hive_metastore and notebooks will be run manually in sequence.

---

## Architecture

### How the Stack Changed Between Phases

| Component | Phase 1 - Azure (Done) | Phase 2+ - Community Edition (Active) |
|---|---|---|
| Platform | Microsoft Azure | Databricks Community Edition |
| Storage | ADLS Gen2 | DBFS |
| Catalog | Unity Catalog | hive_metastore |
| Compute | Serverless Databricks | Single-node cluster (auto-terminates) |
| Orchestration | Databricks Workflows | Manual notebook execution |
| Data Format | Delta Lake | Delta Lake |

### Medallion Architecture

```
Landing (CSV Files)
       |
       v
Bronze (Raw Delta + Metadata)
       |
       v
Silver (Cleaned + Typed + Validated + MERGE)
       |
       v
Gold (Aggregated Business KPIs)
```

### Database Structure on Community Edition

```
hive_metastore
├── german_energy_bronze
│   └── energy_raw
├── german_energy_silver
│   └── energy_hourly_clean
├── german_energy_gold
│   ├── daily_energy_kpis
│   ├── monthly_energy_transition_kpis
│   └── hourly_pattern_kpis
└── german_energy_audit
    ├── batch_control
    └── pipeline_run_log
```

---

## Project Structure

```
German_Energy_Transition_Lakehouse/
|
├── 00_setup/
│   ├── 01_create_storage_credential_external_location  (done)
│   ├── 02_catalog_and_schema_setup                     (done)
│   └── 03_project_config                               (done)
|
├── 01_data_preparation/
│   ├── 01_validate_landing_source_file                 (done)
│   ├── 02_create_germany_selected_file                 (done)
│   ├── 03_split_source_into_monthly_batches            (done)
│   ├── 04_create_audit_control_tables                  (done)
│   └── 05_initialize_batch_control                     (done)
|
├── 02_bronze_ingestion/
│   └── (not started - Azure deleted due to cost)
|
├── 03_silver_transformation/
│   └── (not started)
|
├── 04_gold_aggregation/
│   └── (not started)
|
└── README.md
```

Everything from 02_bronze_ingestion onwards does not exist yet. The repository honestly reflects what was built before the Azure resources were deleted.

---

## The Dataset

**Source:** Open Power System Data (open-power-system-data.org)
**License:** Creative Commons Attribution 4.0 International

The original file covers all of Europe with around 300 columns. I selected only the Germany columns using the DE_ prefix. One thing worth noting - there is no standalone DE_price_day_ahead column. The price column is actually DE_LU_price_day_ahead because Germany and Luxembourg share a combined day-ahead price zone. I found this during data validation and renamed it clearly in the Silver layer.

**Columns I selected and what I renamed them to:**

| Original Column | Renamed To | What It Means |
|---|---|---|
| utc_timestamp | timestamp_utc | Hourly timestamp in UTC |
| cet_cest_timestamp | timestamp_local | Central European Time |
| DE_load_actual_entsoe_transparency | load_actual_mw | Actual electricity consumption (MW) |
| DE_load_forecast_entsoe_transparency | load_forecast_mw | Forecasted consumption (MW) |
| DE_solar_generation_actual | solar_generation_mw | Solar power produced (MW) |
| DE_solar_capacity | solar_capacity_mw | Total installed solar capacity (MW) |
| DE_wind_generation_actual | wind_generation_mw | Total wind power produced (MW) |
| DE_wind_capacity | wind_capacity_mw | Total installed wind capacity (MW) |
| DE_wind_onshore_generation_actual | wind_onshore_generation_mw | Onshore wind (MW) |
| DE_wind_offshore_generation_actual | wind_offshore_generation_mw | Offshore wind (MW) |
| DE_LU_price_day_ahead | price_day_ahead_eur_mwh | Day-ahead electricity price (EUR/MWh) |

---

## How the Pipeline Works

### Why Monthly Batches Instead of One Big File

The dataset is one historical CSV. If I just loaded the whole thing at once, there is nothing incremental about it. To simulate how a real pipeline actually works - where data arrives in regular drops from a provider - I split the file into 72 monthly CSVs covering 2015 to 2020. Each monthly file gets processed one at a time and tracked in the batch control table.

This matters in interviews because incremental loading is a core concept and you cannot demonstrate it with a single file load.

Each batch moves through these states:

```
pending -> in_progress -> completed
                      -> failed (retriable)
```

### Bronze

The Bronze layer reads one pending monthly batch at a time. It appends the raw data to a Delta table and adds metadata columns - batch_id, source_file_name, ingestion_timestamp, and a record_hash. After writing, it updates the batch_control table to completed. If something goes wrong, the batch is marked as failed and can be retried. Completed batches are never reprocessed.

### Silver

Silver reads from Bronze by batch_id. It renames all columns to readable names, casts everything to the correct data types, removes duplicate records based on timestamp_utc, and adds calculated columns like renewable_share_percent, residual_load_mw, and forecast error. Instead of deleting rows with missing values, I add data quality flags so downstream users can see what the data quality looks like rather than having records silently dropped. Updates use Delta MERGE with timestamp_utc as the natural key.

### Gold

Three Gold tables built for analytics:
- daily_energy_kpis - one row per date with load, generation, price, and forecast error summaries
- monthly_energy_transition_kpis - one row per year-month showing renewable share trends over time
- hourly_pattern_kpis - one row per season and hour showing average generation patterns

---

## Technical Decisions and Why I Made Them

**Delta Lake over plain Parquet:** MERGE operations need ACID guarantees. Without Delta Lake, doing an upsert-style incremental update in Spark requires reading the whole table, filtering, unioning, and rewriting. Delta makes this a single MERGE statement with proper transaction guarantees.

**Batch control table:** Without it, there is no way to know which files have been processed and which haven't. Every pipeline run would need to process everything from scratch or risk processing the same data twice. The control table makes the pipeline idempotent - you can rerun it safely at any point.

**Monthly batch simulation:** Explained above - it is the only way to demonstrate incremental loading logic with a static historical dataset.

**Medallion architecture:** Each layer has a specific job. Bronze keeps raw data untouched so you always have something to fall back on. Silver enforces business rules and data quality. Gold is built for analytics consumers and should never contain transformation logic. Separating concerns this way makes debugging much easier - if a Gold number looks wrong, you check Silver first, then Bronze.

---

## What I Learned (Including the Hard Lessons)

**Always inspect the actual column names before writing code.** I assumed the price column would be DE_price_day_ahead. It was DE_LU_price_day_ahead. That assumption would have caused a silent null column in the pipeline if I had not validated the schema first.

**Spark does not write single files.** When you write a CSV with Spark, you get a folder of part files, not one named file. I used coalesce(1) combined with dbutils.fs.mv() to get single named monthly batch files. It is not elegant but it works for this use case.

**Design for idempotency from day one.** My first version of the batch control initialization used INSERT. When I re-ran the notebook, I got duplicate records for every batch. Switched to Delta MERGE on batch_id immediately. If you do not design for replayability from the start, you will fix it under pressure later.

**Cloud infrastructure costs money when you are not using it.** A NAT gateway in Azure charges approximately 1.50 EUR per day regardless of activity. I learned this after it had already cost me around 74 EUR over a month. The lesson is to understand what every resource you provision actually costs per hour before you create it, not after.

---

## Business Questions This Project Answers

- How has Germany's renewable energy share grown from 2015 to 2020?
- Is there a correlation between high renewable generation and low or negative electricity prices?
- How accurate are electricity load forecasts compared to actual demand?
- How do solar and wind generation patterns differ by season and time of day?
- How many hours per year does Germany experience negative electricity prices, and what drives them?

---

## How to Run This Project

The runnable part of this project currently covers setup and data preparation only. Bronze, Silver, and Gold are not built yet.

To run what exists:
1. Set up an Azure Databricks workspace with Unity Catalog and ADLS Gen2 storage
2. Create a storage credential and external location as shown in 00_setup/01
3. Run the catalog and schema setup from 00_setup/02
4. Upload the OPSD CSV to the landing volume
5. Run data preparation notebooks 01 through 05 in order

After 05_initialize_batch_control runs successfully, the batch control table will show 72 rows in pending status. That is where the project currently stops. Bronze ingestion picking up those pending batches is the next thing to be built.

---

## What Comes Next

The immediate priority is building the Bronze ingestion layer on Databricks Community Edition. Once that is working, Silver transformation and Gold aggregation follow. These are planned but not started.

- Bronze - incremental batch ingestion from landing, metadata columns, batch control updates
- Silver - column renaming, type casting, deduplication, derived columns, Delta MERGE
- Gold - daily KPIs, monthly renewable share trends, hourly pattern analysis
- Databricks SQL dashboard connected to the Gold tables
- Simple load forecasting baseline model using Spark MLlib

---

## Citation

> Open Power System Data. 2020. Data Package Time series. Version 2020-10-06. https://data.open-power-system-data.org/time_series/2020-10-06/

---

*This is a portfolio project using public data and simulated monthly batch arrivals. It is not connected to any production system.*
