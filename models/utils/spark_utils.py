import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from models.config import SPARK_CONFIG
from models.utils.logger import get_logger

# =========================================================
# Force Spark to Use Current Virtual Environment Python
# =========================================================

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE

os.environ["HADOOP_HOME"] = "C:\\hadoop"

logger = get_logger(__name__)


# =========================================================
# Spark Session
# =========================================================

def create_spark_session() -> SparkSession:
    """
    Create Spark session for local execution.
    """

    logger.info("Creating Spark session")

    python_executable = sys.executable

    logger.info(
        f"Using Python executable: {python_executable}"
    )

    # =====================================================
    # Critical for Windows + Virtual Environment
    # =====================================================

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    builder = (
        SparkSession.builder
        .appName(
            SPARK_CONFIG["app_name"]
        )
        .master(
            SPARK_CONFIG["master"]
        )
    )

    # =====================================================
    # Apply Spark Configurations
    # =====================================================

    for config_key, config_value in SPARK_CONFIG[
        "spark_conf"
    ].items():

        builder = builder.config(
            config_key,
            config_value
        )

    # =====================================================
    # Extra Stability Configs for Windows
    # =====================================================

    builder = (
        builder
        .config(
            "spark.pyspark.python",
            python_executable
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable
        )
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.driver.host",
            "127.0.0.1"
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )
        .config(
    "spark.hadoop.io.native.lib.available",
    "true"
)
         .config(
        "spark.sql.warehouse.dir",
        "file:///C:/tmp/spark-warehouse"
    )

    .config(
        "spark.driver.extraJavaOptions",
        "-Djava.io.tmpdir=C:/tmp"
    )

    .config(
        "spark.executor.extraJavaOptions",
        "-Djava.io.tmpdir=C:/tmp"
    )
    )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark session created successfully: "
        f"{spark.version}"
    )

    return spark


# =========================================================
# DataFrame Helpers
# =========================================================

def standardize_column_names(
    dataframe: DataFrame
) -> DataFrame:
    """
    Convert column names to lowercase snake_case.
    """

    renamed_df = dataframe

    for column_name in dataframe.columns:

        cleaned_name = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        renamed_df = renamed_df.withColumnRenamed(
            column_name,
            cleaned_name
        )

    return renamed_df


def validate_dataframe_not_empty(
    dataframe: DataFrame,
    dataframe_name: str
) -> None:
    """
    Raise exception if dataframe is empty.
    """

    if dataframe.limit(1).count() == 0:

        raise ValueError(
            f"{dataframe_name} is empty"
        )


def select_existing_columns(
    dataframe: DataFrame,
    columns: list[str]
) -> DataFrame:
    """
    Select only existing columns.
    """

    existing_columns = [
        column_name
        for column_name in columns
        if column_name in dataframe.columns
    ]

    return dataframe.select(
        *[
            col(column_name)
            for column_name in existing_columns
        ]
    )


def safe_cache(
    dataframe: DataFrame,
    dataframe_name: str
) -> DataFrame:
    """
    Cache dataframe safely.
    """

    logger.info(
        f"Caching dataframe: {dataframe_name}"
    )

    dataframe.cache()

    dataframe.count()

    logger.info(
        f"Successfully cached: {dataframe_name}"
    )

    return dataframe


def safe_unpersist(
    dataframe: DataFrame,
    dataframe_name: str
) -> None:
    """
    Unpersist dataframe safely.
    """

    logger.info(
        f"Unpersisting dataframe: {dataframe_name}"
    )

    dataframe.unpersist()

    logger.info(
        f"Successfully unpersisted: "
        f"{dataframe_name}"
    )

