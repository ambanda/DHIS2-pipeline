# DHIS2 Data Engineering Pipeline

## Overview

This project implements a scalable PySpark-based DHIS2 analytics pipeline using a modular medallion-style architecture and dimensional modeling principles.

The pipeline ingests DHIS2 metadata and transactional datasets, performs metadata reconciliation, hierarchy enrichment, data quality processing, dimensional modeling, and analytics generation.

The solution is containerized using Docker and designed for reproducibility, scalability, modularity, and analytical consumption.

---

# Pipeline Flow

```text
Ingestion
    ↓
Metadata Drift Detection
    ↓
Incremental Filtering
    ↓
Metadata Resolution
    ↓
Hierarchy Enrichment
    ↓
DQ Processing
    ↓
Dimensional Modeling
    ↓
Contract Validation
    ↓
Analytics
```

---

# Project Structure

```text
project-root/
│
├── data/
│   ├── metadata.json
│   ├── org_units.json
│   ├── programs.json
│   └── data_values.json
│
├── metadata_snapshots/
│   └── metadata_reference.json
│
├── models/
│   ├── orchestration.py
│   ├── ingest.py
│   ├── metadata_drift.py
│   ├── incremental_processing.py
│   ├── metadata_resolution.py
│   ├── org_hierarchy.py
│   ├── dq_processing.py
│   ├── dimensional_modeling.py
│   ├── contract_validation.py
│   ├── analytics.py
│   │
│   └── utils/
│       ├── spark_utils.py
│       ├── io_utils.py
│       ├── logger.py
│       └── dedup_utils.py
│
├── output/
│
├── Dockerfile
├── requirements.txt
└── README.md
```

---

# Architecture Design

## Design Principles

The pipeline was designed around the following engineering principles:

* Modular architecture
* Scalability
* Incremental processing
* Broadcast join optimization
* Separation of concerns
* Metadata-driven processing
* Dimensional modeling
* Data quality enforcement
* Reproducibility using Docker
* Analytics-ready outputs

---

# End-to-End Pipeline Stages

## 1. Ingestion

The ingestion layer reads DHIS2 source datasets from JSON files.

### Datasets

| Dataset                 | Purpose                    |
| ----------------------- | -------------------------- |
| metadata.json           | Data element metadata      |
| org_units.json          | Organizational hierarchy   |
| programs.json           | Program metadata           |
| data_values.json        | Fact transaction dataset   |
| metadata_reference.json | Baseline metadata snapshot |

### Output

Raw Spark DataFrames.

---

## 2. Metadata Drift Detection

This stage compares the current metadata snapshot against a historical reference snapshot.

### Drift Types Detected

* Added data elements
* Removed data elements
* Renamed data elements

### Outputs

```text
output/metadata_drift/
```

Includes:

* Added data elements parquet
* Removed data elements parquet
* Renamed data elements parquet
* JSON drift summary

---

## 3. Incremental Filtering

The pipeline supports incremental loading.

### Logic

* Detect existing partitions
* Identify new periods
* Filter only unseen data
* Run full load if no partitions exist

### Benefits

* Reduced processing cost
* Faster execution
* Scalable historical processing

---

## 4. Metadata Resolution

Fact records are enriched using metadata reference datasets.

### Resolution Includes

#### Data Elements

* Data element name
* Short name
* Value type
* Aggregation type
* Domain type

#### Category Option Combos

* Category option combo names

### Optimization

Small metadata tables are broadcast joined for performance.

---

## 5. Hierarchy Enrichment

Organizational hierarchy paths are exploded into analytical dimensions.

### Hierarchy Levels

* Country
* Region
* District
* Facility

### Example

```text
/UID1/UID2/UID3/UID4
```

Becomes:

| country | region  | district  | facility   |
| ------- | ------- | --------- | ---------- |
| Kenya   | Nairobi | Westlands | Hospital A |

---

## 6. DQ Processing

The data quality layer handles:

### Exact Duplicate Detection

Detects duplicate fact rows.

### Near Duplicate Resolution

Uses business keys:

```text
(dataelement, period, orgunit, categoryoptioncombo)
```

Retains latest records based on:

```text
lastupdated
```

### Additional DQ Rules

* Null validation
* Referential integrity
* Metadata resolution checks
* Hierarchy resolution checks

---

## 7. Dimensional Modeling

The pipeline generates a star schema optimized for analytics.

# Star Schema Diagram

```text
                        +-------------------+
                        |   dim_org_unit    |
                        +-------------------+
                        | orgunit_key       |
                        | orgunit_uid       |
                        | country_name      |
                        | region_name       |
                        | district_name     |
                        | facility_name     |
                        +-------------------+
                                  |
                                  |
                                  |
+-------------------+     +-------------------+     +----------------------+
| dim_data_element  |     | fact_data_values  |     | dim_category_option  |
+-------------------+     +-------------------+     +----------------------+
| dataelement_key   |----<| dataelement_key   |>----| categoryoption_key   |
| dataelement_uid   |     | orgunit_key       |     | categoryoption_uid   |
| data_element_name |     | categoryoption_key|     | categoryoption_name  |
| value_type        |     | period            |     +----------------------+
| aggregationtype   |     | value             |
+-------------------+     | lastupdated       |
                            +-------------------+
```

---

## Fact Table

### fact_data_values

Contains transactional DHIS2 measures.

### Measures

* value

### Grain

```text
One row per:
(dataelement, period, orgunit, categoryoptioncombo)
```

---

## Dimension Tables

### dim_data_element

Stores data element attributes.

### dim_org_unit

Stores organizational hierarchy dimensions.

### dim_category_option

Stores category option combo attributes.

---

## 8. Contract Validation

Schema validation ensures:

* Required columns exist
* Data types are valid
* Nullability rules are respected
* Output schema consistency is maintained

This prevents downstream analytics failures.

---

## 9. Analytics Layer

The analytics layer produces curated outputs for reporting.

### Example Outputs

* Period trends
* Facility reporting
* Regional aggregations
* Metadata coverage metrics
* Data quality metrics

Outputs are stored as parquet datasets.

---

# Setup Instructions

## Prerequisites

Install:

*py4j==0.10.9.7
pyspark==3.4.0
PyYAML==6.0.1
pandas==2.2.2
numpy==1.26.4
pyarrow==14.0.2
Docker Desktop


---

# Local Python Setup

## Create Virtual Environment

### PowerShell

```powershell
python -m venv venv
```

## Activate Environment

```powershell
.\venv\Scripts\Activate.ps1
```

## Install Dependencies

```powershell
pip install -r requirements.txt
```

---

# Validation and CI Commands

## Run Unit and Smoke Tests

```powershell
python -m pytest -q
```

## Run Tiny Smoke Pipeline

The smoke test builds a temporary one-row DHIS2 fixture and runs the full
pipeline against it:

```powershell
python -m pytest tests/test_pipeline_smoke.py -q
```

## CI Command Sequence

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
```

---

# Docker Setup

## Build Docker Image

```powershell
docker build -t dhis2-pipeline .
```

## Run Docker Container

```powershell
docker run dhis2-pipeline
```

For larger local runs, give Docker enough memory and tune Spark explicitly:

```powershell
docker run --rm --memory=6g `
  -e SPARK_MASTER=local[2] `
  -e SPARK_DRIVER_MEMORY=2g `
  -e SPARK_EXECUTOR_MEMORY=2g `
  -e SPARK_SQL_SHUFFLE_PARTITIONS=4 `
  -e DQ_MAX_QUARANTINE_RATE=0.25 `
  dhis2-pipeline
```

---

# Running the Pipeline Locally

## Execute Main Pipeline

```powershell
python pipeline.py
```

Useful environment variables:

```powershell
$env:PIPELINE_LOG_TO_FILE="false"
$env:ENABLE_INCREMENTAL_LOADING="true"
$env:SPARK_DRIVER_MEMORY="4g"
$env:SPARK_EXECUTOR_MEMORY="4g"
python pipeline.py
```

---

# Output Structure

```text
output/
│
├── metadata_drift/
├── dimensions/
├── facts/
├── dq/
├── analytics/
└── validation/
```

Additional runtime outputs:

```text
output/quarantine/
output/_checkpoints/pipeline_state.json
```

---

# Performance Optimizations

## Broadcast Joins

Small lookup datasets are broadcast joined.

### Examples

* Data elements
* Category option combos
* Hierarchy lookups

---

## Incremental Processing

Only new partitions are processed.

---

## Column Pruning

Only required columns are selected.

---

## Partitioning

Fact datasets are partitioned by:

```text
period
```

Fact outputs are written by `year_month` using dynamic partition overwrite.
Reruns replace touched partitions instead of appending duplicate rows.

---

# Design Decisions

## Why PySpark?

PySpark was selected because it:

* Handles large-scale distributed processing
* Supports scalable joins
* Supports partitioned workloads
* Integrates well with cloud lakehouses
* Supports advanced analytical transformations

---

## Why Star Schema?

The star schema provides:

* Faster analytical queries
* Simplified BI modeling
* Optimized aggregations
* Better semantic modeling
* Reduced duplication

---

## Why Metadata Drift Detection?

DHIS2 metadata changes frequently.

Drift detection allows:

* Schema evolution tracking
* Change monitoring
* Analytical consistency
* Governance support

---

## Why Incremental Loading?

Incremental loading improves:

* Scalability
* Processing efficiency
* Cost optimization
* Runtime performance

---

# Assumptions

The pipeline assumes:

1. Input JSON files are valid.
2. Metadata IDs are globally unique.
3. Organizational hierarchy paths are valid.
4. Period values are consistently formatted.
5. Metadata reference snapshots exist.
6. Spark runtime is available.
7. Docker is installed for container execution.

---

# Known Limitations

## 1. Local File-Based Storage

The current implementation uses local file storage.

Future improvement:

* Cloud object storage
* Delta Lake
* BigQuery
* Snowflake

---

## 2. No Streaming Support

The pipeline currently supports batch ingestion only.

Future improvement:

* Kafka
* Structured Streaming
* CDC ingestion

---

## 3. Limited Metadata Drift Scope

Current drift detection covers:

* Added metadata
* Removed metadata
* Renamed metadata

Future enhancement:

* Data type drift
* Hierarchy drift
* Aggregation rule drift

---

## 4. No Workflow Orchestration Yet

The pipeline currently runs manually.

Future enhancement:

* Apache Airflow
* Dagster
* Prefect

---

## 5. No Lakehouse Transaction Layer

Current parquet outputs are append-based.

Future enhancement:

* Delta Lake
* Apache Iceberg
* Hudi

---

# Future Enhancements

## Planned Improvements

* CDC ingestion using Debezium
* Delta Lake support
* Data contracts with Great Expectations
* Airflow orchestration
* Data lineage tracking
* Metrics observability
* Real-time streaming ingestion
* Cloud-native deployment
* Power BI semantic model integration
* ML-based anomaly detection

---

# Example Execution Logs

```text
STARTING DHIS2 PIPELINE
Starting ingestion
Starting metadata drift detection
Incremental loading enabled
Resolving data element UIDs
Building organization hierarchy
Starting DQ pipeline
Generating dimensional model
Contract validation completed
Analytics generation completed
PIPELINE COMPLETED SUCCESSFULLY
```

---

# Technology Stack

| Component         | Technology                 |
| ----------------- | -------------------------- |
| Processing Engine | PySpark                    |
| Language          | Python                     |
| Containerization  | Docker                     |
| Storage           | Parquet                    |
| Logging           | Python Logging             |
| Validation        | Custom Contract Validation |
| Data Model        | Star Schema                |

---

# Author

DHIS2 PySpark Data Engineering Pipeline

Designed for scalable healthcare analytics engineering and dimensional modeling.
