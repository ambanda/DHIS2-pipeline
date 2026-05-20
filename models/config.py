

from typing import Dict
from pathlib import Path
from dataclasses import dataclass
import os
import sys

# =========================================================
# Python Executable
# =========================================================

PYTHON_EXECUTABLE = sys.executable

# =========================================================
# Base Directory
# =========================================================

BASE_DIR = Path(__file__).resolve().parent.parent


# =========================================================
# Data Directories
# =========================================================

DATA_DIR = BASE_DIR / "data"

OUTPUT_DIR = BASE_DIR / "output"

LOG_DIR = BASE_DIR / "logs"

CONTRACTS_DIR = BASE_DIR / "contracts"


METADATA_SNAPSHOT_DIR = (
    BASE_DIR / "metadata_snapshots"
)

# =========================================================
# Output Subdirectories
# =========================================================

DIMENSIONS_DIR = (
    OUTPUT_DIR / "dimensions"
)

FACTS_DIR = (
    OUTPUT_DIR / "facts"
)

ANALYTICS_DIR = (
    OUTPUT_DIR / "analytics"
)

QUARANTINE_DIR = (
    OUTPUT_DIR / "quarantine"
)

DQ_DIR = (
    OUTPUT_DIR / "dq"
)

METADATA_DRIFT_DIR = (
    OUTPUT_DIR / "metadata_drift"
)

# =========================================================
# Logging Configuration
# =========================================================

LOG_FILE = LOG_DIR / "pipeline.log"

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# =========================================================
# Incremental Loading
# =========================================================

ENABLE_INCREMENTAL_LOADING = True


# =========================================================
# Spark Configuration
# =========================================================

SPARK_CONFIG = {

    "app_name": "DHIS2HealthPipeline",

    "master": "local[*]",

    "spark_conf": {

        "spark.sql.shuffle.partitions": "8",

        "spark.sql.adaptive.enabled": "true",

        "spark.sql.adaptive.coalescePartitions.enabled":
            "true",

        "spark.sql.session.timeZone":
            "UTC",

        "spark.serializer":
            "org.apache.spark.serializer.KryoSerializer",

        "spark.driver.memory":
            "4g",

        "spark.executor.memory":
            "4g",

        "spark.sql.execution.arrow.pyspark.enabled":
            "true",

        "spark.sql.parquet.compression.codec":
            "snappy"
    }
}

# =========================================================
# DQ Thresholds
# =========================================================

DQ_THRESHOLDS = {
    "max_quarantine_rate": 0.10,
    "max_unresolved_uid_rate": 0.15,
    "min_fact_rows": 1
}


# =========================================================
# Required Directories
# =========================================================

REQUIRED_DIRECTORIES = [

    OUTPUT_DIR,

    LOG_DIR,

    CONTRACTS_DIR,

    DIMENSIONS_DIR,

    FACTS_DIR,

    ANALYTICS_DIR,

    QUARANTINE_DIR,

    METADATA_SNAPSHOT_DIR,

    METADATA_DRIFT_DIR,

    DQ_DIR
]



for directory in REQUIRED_DIRECTORIES:

    directory.mkdir(
        parents=True,
        exist_ok=True
    )
