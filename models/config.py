

from pathlib import Path
import os
import sys

# =========================================================
# Python Executable
# =========================================================

PYTHON_EXECUTABLE = sys.executable

# =========================================================
# Base Directory
# =========================================================

BASE_DIR = Path(
    os.getenv(
        "PIPELINE_BASE_DIR",
        Path(__file__).resolve().parent.parent
    )
)


# =========================================================
# Data Directories
# =========================================================

DATA_DIR = Path(
    os.getenv("PIPELINE_DATA_DIR", BASE_DIR / "data")
)

OUTPUT_DIR = Path(
    os.getenv("PIPELINE_OUTPUT_DIR", BASE_DIR / "output")
)

LOG_DIR = Path(
    os.getenv("PIPELINE_LOG_DIR", BASE_DIR / "logs")
)

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

STAGING_DIR = (
    OUTPUT_DIR / "_staging"
)

# =========================================================
# Logging Configuration
# =========================================================

LOG_FILE = LOG_DIR / "pipeline.log"

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

LOG_TO_FILE = (
    os.getenv("PIPELINE_LOG_TO_FILE", "true")
    .strip()
    .lower()
    not in {"0", "false", "no"}
)

# =========================================================
# Incremental Loading
# =========================================================

ENABLE_INCREMENTAL_LOADING = (
    os.getenv("ENABLE_INCREMENTAL_LOADING", "true")
    .strip()
    .lower()
    not in {"0", "false", "no"}
)


# =========================================================
# Spark Configuration
# =========================================================

SPARK_CONFIG = {

    "app_name": "DHIS2HealthPipeline",

    "master": os.getenv("SPARK_MASTER", "local[2]"),

    "spark_conf": {

        "spark.sql.shuffle.partitions":
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),

        "spark.sql.adaptive.enabled": "true",

        "spark.sql.adaptive.coalescePartitions.enabled":
            "true",

        "spark.sql.session.timeZone":
            "UTC",

        "spark.serializer":
            "org.apache.spark.serializer.KryoSerializer",

        "spark.driver.memory":
            os.getenv("SPARK_DRIVER_MEMORY", "2g"),

        "spark.executor.memory":
            os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),

        "spark.sql.execution.arrow.pyspark.enabled":
            "true",

        "spark.sql.parquet.compression.codec":
            "snappy",

        "spark.sql.sources.partitionOverwriteMode":
            "dynamic",

        "spark.sql.sources.partitionColumnTypeInference.enabled":
            "false",

        "spark.io.encryption.enabled":
            os.getenv("SPARK_IO_ENCRYPTION_ENABLED", "false"),

        "spark.sql.files.maxRecordsPerFile":
            os.getenv("SPARK_SQL_MAX_RECORDS_PER_FILE", "250000")
    }
}

# =========================================================
# DQ Thresholds
# =========================================================

DQ_THRESHOLDS = {
    "max_quarantine_rate": float(
        os.getenv("DQ_MAX_QUARANTINE_RATE", "0.25")
    ),
    "max_unresolved_uid_rate": float(
        os.getenv("DQ_MAX_UNRESOLVED_UID_RATE", "0.15")
    ),
    "min_fact_rows": int(
        os.getenv("DQ_MIN_FACT_ROWS", "1")
    )
}


# =========================================================
# Checkpointing
# =========================================================

CHECKPOINT_DIR = (
    OUTPUT_DIR / "_checkpoints"
)

PIPELINE_STATE_FILE = (
    CHECKPOINT_DIR / "pipeline_state.json"
)


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

    DQ_DIR,

    STAGING_DIR,

    CHECKPOINT_DIR
]



for directory in REQUIRED_DIRECTORIES:

    directory.mkdir(
        parents=True,
        exist_ok=True
    )
