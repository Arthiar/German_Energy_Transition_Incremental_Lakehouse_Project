### German Energy Transition Lakehouse

#### Project Overview

A production-grade data lakehouse built on Azure Databricks and Unity Catalog to analyze Germany's renewable energy transition. This project demonstrates end-to-end data engineering capabilities including cloud infrastructure provisioning, secure data storage, incremental batch processing, and medallion architecture implementation.

**Data Source:** Open Power System Data (OPSD) - European energy time series dataset covering 2015-2020

**Business Goal:** Enable analysis of Germany's renewable energy generation, consumption patterns, and price dynamics to support energy transition insights.

---

#### Architecture

##### Technology Stack

* **Cloud Platform:** Microsoft Azure
* **Data Processing:** Azure Databricks (Serverless Compute)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Catalog:** Unity Catalog with External Location
* **Authentication:** Databricks Access Connector with Managed Identity
* **Data Format:** Delta Lake (Bronze/Silver/Gold), CSV (Landing)

##### Medallion Architecture

```
Landing (Raw Files)  →  Bronze (Raw Delta)  →  Silver (Cleaned)  →  Gold (Aggregated)
       ↓                       ↓                      ↓                    ↓
   UC Volume            Delta Tables          Delta Tables         Delta Tables
   CSV Files            + Metadata            + Quality Rules      + Business Logic
```

##### Infrastructure Components

**Azure Resources:**
* Resource Group: `rg-german-energy-lakehouse`
* Storage Account: `stgerenergyarthidev` (ADLS Gen2 enabled)
* Databricks Workspace: Production workspace with Unity Catalog
* Access Connector: Secure authentication using Managed Identity

**Storage Hierarchy:**
```
abfss://german1energy@stgerenergyarthidev.dfs.core.windows.net/
├── landing/         # Raw source files
├── bronze/          # Initial Delta ingestion
├── silver/          # Cleaned and validated data
├── gold/            # Business-ready aggregations
├── checkpoints/     # Streaming checkpoints
└── archive/         # Historical backups
```

**Unity Catalog Structure:**
```
german_energy_transition_lakehouse (Catalog)
├── landing (Schema)
│   └── raw_files (Volume) → ADLS Gen2 external location
├── bronze (Schema)
├── silver (Schema)
├── gold (Schema)
├── audit (Schema)
│   ├── batch_control
│   └── pipeline_run_log
└── ml (Schema)
```

---

#### Key Features

##### 1. Secure Cloud Infrastructure

* **Managed Identity Authentication:** Databricks Access Connector eliminates credential management
* **Role-Based Access Control (RBAC):** Storage Blob Data Contributor role for scoped access
* **Unity Catalog Governance:** Centralized metadata, lineage tracking, and access policies

##### 2. Incremental Batch Processing

* **Monthly Batch Strategy:** Historical data split into monthly files for realistic incremental loads
* **Batch Control Table:** Tracks processing status (pending/running/completed/failed) and retry logic
* **Idempotent Design:** MERGE-based operations prevent duplicate processing
* **Audit Trail:** Complete pipeline run logs with timestamps, row counts, and error tracking

##### 3. Data Quality & Validation

* **Schema Validation:** Landing file validation before Bronze ingestion
* **Column Selection:** Focused Germany dataset (DE_* columns) extracted from multi-country source
* **Metadata Enrichment:** Batch ID, processing timestamp, source file tracking added at Bronze
* **Error Handling:** Retry logic and error message capture in control tables

##### 4. Scalable Design

* **Serverless Compute:** Auto-scaling Databricks clusters for cost efficiency
* **Delta Lake Format:** ACID transactions, time travel, and optimized queries
* **Partitioned Storage:** Year/month folder structure for partition pruning
* **Reusable Configuration:** Centralized config notebook (`03-project-config`) for environment portability

---

#### Project Structure

```
German_Energy_Transition_Lakehouse/
│
├── 00_setup/
│   ├── 01_create_catalog_schemas.sql          # Unity Catalog setup
│   ├── 02_create_audit_tables.sql             # Control tables DDL
│   └── 03-project-config                       # Shared configuration notebook
│
├── 01_data_preparation/
│   ├── 01_validate_landing_source_file         # Source file validation
│   ├── 02_create_germany_selected_file         # Germany column extraction
│   ├── 03_split_source_into_monthly_batches    # Monthly batch file creation
|   ├── 04-04_create_audit_control_tables
│   └── 05_initialize_batch_control             # Batch control table population
│
├── 02_bronze_ingestion/
│   └── (Bronze layer ingestion notebooks - in progress)
│
├── 03_silver_transformation/
│   └── (Silver layer transformations - planned)
│
├── 04_gold_aggregation/
│   └── (Gold layer business aggregations - planned)
│
└── README.md
```

---

#### Setup Instructions

##### Prerequisites

* Azure subscription with Contributor access
* Azure Databricks workspace (Premium or Enterprise tier for Unity Catalog)
* Basic knowledge of PySpark, SQL, and Azure services

##### Step 1: Azure Infrastructure

1. **Create Resource Group**
   ```bash
   az group create --name rg-german-energy-lakehouse --location westeurope
   ```

2. **Create Storage Account (ADLS Gen2)**
   ```bash
   az storage account create \
     --name stgerenergyarthidev \
     --resource-group rg-german-energy-lakehouse \
     --location westeurope \
     --sku Standard_LRS \
     --kind StorageV2 \
     --hierarchical-namespace true
   ```

3. **Create Container and Folders**
   ```bash
   az storage container create --name german1energy --account-name stgerenergyarthidev
   # Create subfolders: landing, bronze, silver, gold, checkpoints, archive
   ```

4. **Create Databricks Access Connector**
   * In Azure Portal → Create Resource → Databricks Access Connector
   * Enable Managed Identity

5. **Assign Storage Permissions**
   * Navigate to Storage Account → Access Control (IAM)
   * Add role assignment: Storage Blob Data Contributor
   * Assign to: Databricks Access Connector managed identity

##### Step 2: Unity Catalog Setup

1. **Create External Location**
   * In Databricks → Catalog → External Locations → Create
   * URL: `abfss://german1energy@stgerenergyarthidev.dfs.core.windows.net/`
   * Credential: Select Access Connector

2. **Run Setup Notebooks**
   ```python
   # Execute in order:
   %run ./00_setup/01_create_catalog_schemas
   %run ./00_setup/02_create_audit_tables
   ```

##### Step 3: Data Ingestion

1. **Upload Source Data**
   * Download OPSD dataset: [time_series_60min_singleindex.csv](https://data.open-power-system-data.org/)
   * Upload to Unity Catalog Volume: `german_energy_transition_lakehouse.landing.raw_files`

2. **Run Data Preparation Notebooks**
   ```python
   %run ./01_data_preparation/01_validate_landing_source_file
   %run ./01_data_preparation/02_create_germany_selected_file
   %run ./01_data_preparation/03_split_source_into_monthly_batches
   %run ./01_data_preparation/05_initialize_batch_control
   ```

##### Step 4: Verify Setup

```sql
-- Check batch control table
SELECT * FROM german_energy_transition_lakehouse.audit.batch_control;

-- Verify monthly batches
SELECT batch_year, batch_month, status, source_file_name
FROM german_energy_transition_lakehouse.audit.batch_control
ORDER BY batch_year, batch_month;
```

---

#### Data Pipeline

##### Current Implementation (Phase 1)

**Landing Layer:**
* Original OPSD CSV uploaded to Unity Catalog Volume
* Germany-only columns extracted and saved
* Monthly batch files created (2015-2020, ~72 batches)
* Batch control table initialized with "pending" status

**Key Metrics:**
* Source file: 130 MB, 50,401 rows, 300 columns
* Germany dataset: 11 selected columns
* Batches: 72 monthly files (2015-01 to 2020-12)
* Storage format: CSV (Landing) → Delta (Bronze/Silver/Gold)

##### Planned Implementation (Phase 2-4)

**Bronze Layer (In Progress):**
* Incremental batch ingestion from Landing to Bronze
* Add metadata columns: `batch_id`, `source_file_name`, `ingestion_timestamp`
* Update batch control table status after processing
* Implement retry logic for failed batches

**Silver Layer (Planned):**
* Data type casting (string → numeric/timestamp)
* Handle missing values and outliers
* Add derived columns: `renewable_percentage`, `load_gap`
* Data quality checks and rejection handling

**Gold Layer (Planned):**
* Daily/monthly aggregations by energy type
* Year-over-year growth calculations
* Price-generation correlation analysis
* Business-ready tables for dashboards

---

#### Dataset Details

##### Source: Open Power System Data (OPSD)

**Coverage:** 2015-2020, hourly granularity

**Selected Germany Columns:**

| Column Name | Description |
|------------|-------------|
| `utc_timestamp` | UTC timestamp (hourly) |
| `cet_cest_timestamp` | Central European Time (CET/CEST) |
| `DE_load_actual_entsoe_transparency` | Actual electricity load (MW) |
| `DE_load_forecast_entsoe_transparency` | Forecasted electricity load (MW) |
| `DE_solar_generation_actual` | Actual solar generation (MW) |
| `DE_solar_capacity` | Installed solar capacity (MW) |
| `DE_wind_generation_actual` | Actual wind generation (MW) |
| `DE_wind_capacity` | Installed wind capacity (MW) |
| `DE_wind_onshore_generation_actual` | Onshore wind generation (MW) |
| `DE_wind_offshore_generation_actual` | Offshore wind generation (MW) |
| `DE_LU_price_day_ahead` | Day-ahead electricity price (EUR/MWh) |

**Business Questions to Answer:**
* How has renewable energy capacity grown over time?
* What is the correlation between renewable generation and electricity prices?
* How accurate are load forecasts compared to actual demand?
* What is the contribution of solar vs wind to total renewable generation?
* How do renewable generation patterns vary by season?

---

#### Technical Highlights

##### Delta Lake Implementation

* **ACID Transactions:** Ensures data consistency during concurrent writes
* **Time Travel:** Enables auditing and rollback capabilities
* **Schema Evolution:** Supports adding new columns without breaking downstream
* **Partition Pruning:** Year/month partitions optimize query performance

##### Batch Control Pattern

```python
# Control table tracks each batch lifecycle:
batch_control (
    batch_id: "2015_01",
    status: "pending" | "running" | "completed" | "failed",
    start_timestamp: timestamp,
    end_timestamp: timestamp,
    rows_read: bigint,
    rows_written: bigint,
    error_message: string,
    retry_count: int
)
```

**Benefits:**
* Idempotent processing: MERGE on `batch_id` prevents duplicates
* Retry logic: Failed batches can be reprocessed
* Observability: Full audit trail of pipeline execution
* Incremental loads: Only process new/pending batches

##### Configuration Management

```python
# Centralized config notebook (03-project-config)
catalog_name = "german_energy_transition_lakehouse"
landing_schema = "landing"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
audit_schema = "audit"

# Reusable across notebooks via %run
%run ./00_setup/03-project-config
```

---

#### Skills Demonstrated

##### Cloud & Infrastructure
* Azure Resource Group and Storage Account provisioning
* ADLS Gen2 hierarchical namespace configuration
* Databricks Access Connector with Managed Identity
* Unity Catalog external location setup
* RBAC and least-privilege access patterns

##### Data Engineering
* Medallion architecture (Bronze/Silver/Gold) design
* Incremental batch processing with control tables
* Delta Lake ACID transactions and MERGE operations
* PySpark transformations and DataFrame operations
* Idempotent pipeline design for reliability

---

#### Lessons Learned

##### Challenges & Solutions

**Challenge 1: Column Name Discovery**
* **Issue:** Day-ahead price column was named `DE_LU_price_day_ahead` (Luxembourg region), not `DE_price_day_ahead`
* **Solution:** Validated actual schema using `df.columns` before hardcoding column list
* **Lesson:** Always inspect raw data schema before assumptions

**Challenge 2: Spark CSV Write Output**
* **Issue:** Spark writes CSV to folder with `part-*.csv` files, not a single named file
* **Solution:** Used `.coalesce(1)` + `dbutils.fs.mv()` to rename and cleanup
* **Lesson:** Understand Spark's distributed write behavior and plan for cleanup

**Challenge 3: Batch Idempotency**
* **Issue:** Re-running initialization would create duplicate batch records
* **Solution:** Used Delta `MERGE` on `batch_id` instead of `INSERT`
* **Lesson:** Design for replayability from day one

##### Future Improvements

* **Data Quality Framework:** Implement Great Expectations or Delta Live Tables expectations
* **Orchestration:** Add Azure Data Factory or Databricks Workflows for scheduling
* **Monitoring:** Integrate with Azure Monitor or Databricks SQL alerts
* **Performance:** Add Z-ordering on frequently filtered columns
* **Testing:** Unit tests for transformation logic using pytest + chispa
* **CI/CD:** Databricks Asset Bundles (DABs) for deployment automation

---

#### Results & Metrics

##### Infrastructure Provisioned
* ✅ 1 Azure Resource Group
* ✅ 1 ADLS Gen2 Storage Account with 6 containers
* ✅ 1 Databricks Workspace with Unity Catalog
* ✅ 1 Unity Catalog with 6 schemas
* ✅ 1 External Location with Managed Identity

##### Data Prepared
* ✅ 130 MB source CSV validated and uploaded
* ✅ 11 Germany-specific columns extracted
* ✅ 72 monthly batch files created (2015-2020)
* ✅ 50,401 rows prepared for Bronze ingestion

##### Notebooks Created
* ✅ 7 production notebooks with documentation
* ✅ 2 SQL setup scripts for DDL
* ✅ 1 centralized configuration notebook
* ✅ Full markdown explanations in each notebook

---

#### Next Steps

##### Phase 2: Bronze Layer (Current Priority)
1. Implement incremental batch reader from Landing → Bronze
2. Add batch metadata columns to Bronze tables
3. Update batch control status after ingestion
4. Add retry logic for failed batches

##### Phase 3: Silver Layer
1. Type casting and data cleansing
2. Missing value imputation strategies
3. Outlier detection and handling
4. Data quality validation rules

##### Phase 4: Gold Layer
1. Daily/monthly aggregations
2. Renewable energy KPIs
3. Price-generation correlation analysis
4. Forecasting model features

##### Phase 5: Analytics & ML
1. Power BI dashboard integration
2. Time-series forecasting models
3. Anomaly detection for generation/load
4. Renewable energy optimization insights

---

#### Contact & Portfolio

**Author:** Arthisree


---

#### License

This project uses Open Power System Data, which is published under the [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

**Citation:**
> Open Power System Data. 2020. Data Package Time series. Version 2020-10-06. https://data.open-power-system-data.org/time_series/2020-10-06/. (Primary data from various sources, for a complete list see URL).

---

#### Acknowledgments

* **Data Source:** [Open Power System Data (OPSD)](https://open-power-system-data.org/)
* **Platform:** Azure Databricks and Unity Catalog
* **Community:** Databricks documentation and community forums

---

*This is a portfolio project demonstrating end-to-end data engineering skills on Azure. The project is actively maintained and expanded with new features.*
