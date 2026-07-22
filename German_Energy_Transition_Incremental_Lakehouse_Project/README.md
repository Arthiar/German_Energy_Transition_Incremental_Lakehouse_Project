# German Energy Transition Lakehouse

## 1. Project introduction

This project builds a simple data pipeline for German electricity data using
Databricks, PySpark, Delta Lake and Unity Catalog. It follows the medallion
architecture:

```text
Raw CSV files -> Bronze -> Silver -> Gold
                     ^        ^        ^
                     |        |        |
                 raw copy   cleaned   business summaries
```

The pipeline processes the source data one calendar month at a time. A month
such as January 2019 is called a **batch** and has the batch ID `2019-01`.
Processing smaller monthly batches makes the pipeline easier to restart when
one month fails. The orchestration control table records which months have
already completed.

This README intentionally explains the project from the beginning. You do not
need to know advanced Databricks concepts before reading it.

## 2. Technologies used

- **Databricks** runs the notebooks and Spark jobs.
- **PySpark** reads, transforms and aggregates the data.
- **Delta Lake** provides reliable tables and supports `MERGE` operations.
- **Unity Catalog** organizes the schemas, tables and source volume.
- **Databricks widgets** pass the selected batch ID between job tasks.
- **Databricks task values** allow the identify task to send values to later
  tasks in a Databricks Workflow.

## 3. Source data

The source is the Open Power System Data hourly time-series CSV. It contains
electricity information for several European countries. This project only
keeps German columns.

The raw files are expected in this Unity Catalog volume path:

```text
/Volumes/german_energy_transition_lakehouse/landing/raw_files/raw/energy/
```

Files must be organized by year:

```text
energy/
|-- year=2014/
|   `-- time_series_60min_singleindex.csv
|-- year=2015/
|   `-- time_series_60min_singleindex.csv
|-- year=2016/
|   `-- time_series_60min_singleindex.csv
`-- ...
```

The catalog, `landing` schema and source volume must already exist. The setup
notebook creates the project schemas but does not upload the source file.

## 4. Folder structure

The folders are numbered in dependency and processing order:

```text
German_Energy_Transition_Lakehouse_v2/
|-- 00_setup/
|   `-- 01_create_catalog_schema.py
|-- 01_config/
|   |-- 01_bronze_config.py
|   |-- 02_silver_config.py
|   `-- 03_gold_config.py
|-- 02_bronze/
|   `-- 01_bronze_ingest.py
|-- 03_silver/
|   `-- 01_silver_transform.py
|-- 04_gold/
|   `-- 01_gold_aggregate.py
|-- 05_orchestration/
|   |-- 00_create_control_table.py
|   |-- 01_identify_next_batch.py
|   |-- 02_process_new_batch.py
|   `-- 03_complete_batch.py
|-- 06_analytics/
|   `-- 01_energy_analytics.sql
`-- README.md
```

There are exactly four orchestration notebooks. They match the control-table
pattern used in the Formula 1 example, but use monthly German energy batches.

## 5. Unity Catalog objects

The catalog name is:

```text
german_energy_transition_lakehouse
```

The project uses these schemas:

| Schema | Purpose |
|---|---|
| `landing` | Contains the volume with source CSV files. |
| `bronze` | Stores the raw German columns and ingestion metadata. |
| `silver` | Stores cleaned and typed hourly data. |
| `gold` | Stores daily tables used for analysis and reporting. |
| `orchestration` | Stores the batch control table. |

## 6. Configuration notebooks

Configuration is separated from processing code so names and paths are
defined once.

### `01_config/01_bronze_config.py`

This notebook defines:

- the catalog name;
- the Bronze table name;
- the landing volume path;
- the two timestamp columns;
- the German column prefix `DE_`;
- the Bronze merge key `utc_timestamp`.

Only the timestamp columns and columns beginning with `DE_` are in scope.
This includes national Germany values, the `DE_LU` bidding zone and German
transmission-system-operator zones. Columns for other countries are outside
the purpose of this project.

### `01_config/02_silver_config.py`

This notebook runs the Bronze configuration first and then defines the Silver
table, merge key, and `year`/`month` partition columns.

### `01_config/03_gold_config.py`

This notebook runs the Silver configuration and defines the three Gold table
names and the four German transmission zones: 50Hertz, Amprion, TenneT and
TransnetBW.

## 7. Setup notebook

Run `00_setup/01_create_catalog_schema.py` once before running the pipeline.
It creates the catalog and the Bronze, Silver, Gold and orchestration schemas
with `IF NOT EXISTS`. Therefore, running it again does not delete existing
tables or data.

The setup notebook does not create the control table. Control-table creation
belongs to the first orchestration step, which keeps each responsibility in a
clear location.

## 8. Bronze layer

`02_bronze/01_bronze_ingest.py` contains the `BronzeIngestion` class.

For one selected year and month, it:

1. Reads the CSV data from the matching year folder.
2. Selects rows belonging to the requested month.
3. Keeps the two timestamps and all columns starting with `DE_`.
4. Adds `source_file`, `batch_id` and `ingestion_timestamp` metadata.
5. Writes the data to the Bronze Delta table.

Bronze deliberately avoids business cleaning. Its purpose is to preserve a
replayable copy of the source columns used by this project.

The Bronze write uses a Delta `MERGE` on `utc_timestamp`. If the same batch is
run again, matching timestamps are updated instead of appended as duplicates.
This property is called **idempotency**: repeating the same operation produces
the same final table state.

Bronze table:

```text
german_energy_transition_lakehouse.bronze.germany_energy_raw
```

## 9. Silver layer

`03_silver/01_silver_transform.py` contains the `SilverTransformation` class.

For the selected batch, it:

1. Reads only that `batch_id` from Bronze.
2. Converts the timestamp strings to Spark timestamps.
3. Changes column names to lowercase.
4. Casts German measurement columns from strings to doubles.
5. Adds `year` and `month` columns.
6. Removes duplicate timestamps by keeping the most recently ingested row.
7. Reports null percentages for visibility.
8. Merges the result into the Silver Delta table.

Null values are reported but not automatically deleted. Some transmission
zones did not report every measurement during the complete historical period.
A null can therefore describe missing source coverage rather than a corrupt
row. Removing every row containing a null would discard valid measurements.

Silver table:

```text
german_energy_transition_lakehouse.silver.germany_energy_clean
```

Silver is partitioned by `year` and `month`. This small dataset does not need
partitioning for speed, but the design demonstrates how a larger historical
dataset can avoid scanning unrelated periods.

## 10. Gold layer

`04_gold/01_gold_aggregate.py` contains the `GoldAggregation` class. Gold data
is rebuilt from the complete Silver table after a batch successfully reaches
Silver.

It creates three daily tables:

### `daily_load_summary`

Contains daily actual load, forecast load and forecast error for Germany and
the Germany-Luxembourg bidding zone.

### `daily_generation_summary`

Contains daily solar and wind generation. It includes national values and
available values for the four German transmission zones.

### `daily_renewable_share`

Calculates renewable generation as a percentage of actual load:

```text
renewable share % = (solar generation + wind generation) / actual load * 100
```

Gold uses overwrite rather than an incremental merge. This is a deliberate
simple choice: the source contains roughly fifty thousand hourly records, so
rebuilding the small daily summaries is easy to understand and inexpensive.

## 11. The batch control table

The table is:

```text
german_energy_transition_lakehouse.orchestration.batch_control
```

It contains one row for every attempted monthly batch:

| Column | Meaning |
|---|---|
| `batch_id` | Month in `YYYY-MM` format, for example `2019-01`. |
| `year` | Numeric year used by Bronze and Silver. |
| `month` | Numeric month used by Bronze and Silver. |
| `status` | Current batch state. |
| `rows_bronze` | Number of Bronze rows processed. |
| `rows_silver` | Number of Silver rows processed. |
| `created_at` | Time processing started. |
| `updated_at` | Time the row was last updated. |
| `error_message` | Short error message when processing fails. |

The status flow is:

```text
not tracked -> in_progress -> completed
                    |
                    `--------> failed
```

A completed batch is excluded when the next batch is selected. A failed or
unfinished batch remains eligible, so a later workflow run can retry it.
Bronze and Silver `MERGE` operations make that retry safe.

## 12. The four orchestration notebooks

### Step 1: `00_create_control_table.py`

This creates the empty Delta control table with `CREATE TABLE IF NOT EXISTS`.
It is safe to run more than once. It also displays the existing control rows
so you can inspect pipeline history.

### Step 2: `01_identify_next_batch.py`

This reads timestamps from the source folders and builds the available list of
monthly batch IDs. It reads completed IDs from the control table and selects
the earliest source month that is not completed.

It publishes two Databricks task values:

- `p_batch_id`: the selected month, such as `2019-01`;
- `has_batch`: `true` when work exists, otherwise `false`.

This notebook only identifies work. It does not write Bronze, Silver or Gold.

### Step 3: `02_process_new_batch.py`

This notebook has a `p_batch_id` widget. The Databricks job passes the value
created by the previous task into this widget.

It replaces any previous attempt for that batch with an `in_progress` control
row, then calls Bronze followed by Silver. After both layers succeed, it saves
the row counts. If either layer raises an error, it changes the status to
`failed`, saves a short error message, and raises the error again so the
Databricks task is visibly marked as failed.

### Step 4: `03_complete_batch.py`

This notebook also receives `p_batch_id`. It runs only after the processing
task succeeds. It refreshes the Gold tables and then changes the batch status
from `in_progress` to `completed`.

The batch is deliberately completed after Gold succeeds. If Gold fails, the
batch is not incorrectly recorded as complete and can be retried.

## 13. Databricks Workflow configuration

Create a Databricks Job with these four notebook tasks:

```text
create_control_table
        |
        v
identify_next_batch
        |
        v
process_new_batch
        |
        v
complete_batch
```

Use these notebook paths:

| Task key | Notebook |
|---|---|
| `create_control_table` | `05_orchestration/00_create_control_table.py` |
| `identify_next_batch` | `05_orchestration/01_identify_next_batch.py` |
| `process_new_batch` | `05_orchestration/02_process_new_batch.py` |
| `complete_batch` | `05_orchestration/03_complete_batch.py` |

Set each task to depend on the task above it. For `process_new_batch`, add the
notebook parameter:

```text
p_batch_id = {{tasks.identify_next_batch.values.p_batch_id}}
```

Add the same parameter to `complete_batch`:

```text
p_batch_id = {{tasks.identify_next_batch.values.p_batch_id}}
```

Configure `process_new_batch` and `complete_batch` to run only when
`has_batch` is `true`. In the Databricks Workflows interface this can be done
with an If/else condition task between identification and processing, using:

```text
{{tasks.identify_next_batch.values.has_batch}} == true
```

One job run processes one monthly batch. Run the job again to process the next
month. This mirrors the supplied control-table example and makes each run easy
to inspect. A schedule can run the job repeatedly until no batch remains.

## 14. Manual testing order

For an initial test in the Databricks workspace:

1. Confirm that the source volume and year folders exist.
2. Run `00_setup/01_create_catalog_schema.py`.
3. Run `05_orchestration/00_create_control_table.py`.
4. Run `05_orchestration/01_identify_next_batch.py` and note its batch ID.
5. Open `02_process_new_batch.py`, enter that ID in `p_batch_id`, and run it.
6. Open `03_complete_batch.py`, enter the same ID, and run it.
7. Inspect the control, Bronze, Silver and Gold tables.

Widgets are placed before the classes in notebooks that accept parameters, so
the input is visible before the processing logic begins.

## 15. Rerun and failure examples

### Processing fails in Silver

Suppose `2019-01` reaches Bronze but Silver fails. The control row becomes
`failed`. Fix the cause and run the workflow again. The identify notebook sees
that `2019-01` is not completed and selects it again. Bronze safely merges the
same timestamps and Silver runs again.

### Gold fails

If Bronze and Silver succeed but Gold fails, the row stays `in_progress`.
Because it is not completed, the next job run selects it again. This may rerun
Bronze and Silver, but their merges prevent duplicates.

### No batches remain

The identify notebook sets `has_batch` to `false` and `p_batch_id` to an empty
string. The workflow condition should skip processing and completion.

## 16. Useful validation queries

Run these in a Databricks SQL cell after processing:

```sql
SELECT *
FROM german_energy_transition_lakehouse.orchestration.batch_control
ORDER BY year, month;
```

```sql
SELECT batch_id, COUNT(*) AS row_count
FROM german_energy_transition_lakehouse.bronze.germany_energy_raw
GROUP BY batch_id
ORDER BY batch_id;
```

```sql
SELECT year, month, COUNT(*) AS row_count
FROM german_energy_transition_lakehouse.silver.germany_energy_clean
GROUP BY year, month
ORDER BY year, month;
```

```sql
SELECT *
FROM german_energy_transition_lakehouse.gold.daily_renewable_share
ORDER BY event_date
LIMIT 20;
```