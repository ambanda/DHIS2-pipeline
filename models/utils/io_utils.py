from pathlib import Path
import json
import shutil

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from models.utils.logger import get_logger

logger = get_logger(__name__)


# =========================================================
# Directory Creation
# =========================================================

def ensure_directory_exists(
    path: str
) -> None:
    """
    Create directory if it does not exist.
    """

    Path(path).mkdir(
        parents=True,
        exist_ok=True
    )


# =========================================================
# Parquet Writer
# =========================================================


def write_parquet(
    dataframe: DataFrame,
    output_path: str,
    partition_columns: list[str] | None = None,
    mode: str = "overwrite",
    max_records_per_file: int | None = None
) -> None:
    """
    Write dataframe as parquet.
    """

    ensure_directory_exists(output_path)

    writer = (
        dataframe.write
        .mode(mode)
    )

    if max_records_per_file:

        writer = writer.option(
            "maxRecordsPerFile",
            max_records_per_file
        )

    if partition_columns:

        writer = (
            writer
            .option(
                "partitionOverwriteMode",
                "dynamic"
            )
            .partitionBy(
                *partition_columns
            )
        )

    writer.parquet(output_path)

    logger.info(
        f"Parquet written successfully: "
        f"{output_path}"
    )


def materialize_parquet(
    dataframe: DataFrame,
    output_path: str,
    dataframe_name: str,
    partition_columns: list[str] | None = None,
    max_records_per_file: int | None = 250000
) -> DataFrame:
    """
    Persist and reload a dataframe to break Spark lineage.
    """

    logger.info(
        f"Materializing dataframe: {dataframe_name}"
    )

    write_parquet(
        dataframe=dataframe,
        output_path=output_path,
        partition_columns=partition_columns,
        mode="overwrite",
        max_records_per_file=max_records_per_file
    )

    materialized_df = (
        dataframe
        .sparkSession
        .read
        .parquet(output_path)
    )

    if partition_columns:

        for partition_column in partition_columns:

            if partition_column in materialized_df.columns:

                materialized_df = materialized_df.withColumn(
                    partition_column,
                    col(partition_column).cast("string")
                )

    logger.info(
        f"Materialized dataframe loaded: {dataframe_name}"
    )

    return materialized_df

# =========================================================
# CSV Writer
# =========================================================

def write_csv(
    dataframe: DataFrame,
    output_path: str,
    mode: str = "overwrite"
) -> None:
    """
    Write dataframe as CSV.
    """

    ensure_directory_exists(output_path)

    (
        dataframe.write
        .mode(mode)
        .option("header", True)
        .csv(output_path)
    )

    logger.info(
        f"CSV written successfully: "
        f"{output_path}"
    )


# =========================================================
# JSON Writer (for Python dicts ONLY)
# =========================================================

def write_json(
    data: dict,
    output_path: str
) -> None:
    """
    Write Python dictionary as JSON file.
    """

    output_path = Path(output_path)

    # Remove directory if same name exists
    if (
        output_path.exists()
        and output_path.is_dir()
    ):
        shutil.rmtree(output_path)

    # Create parent directories only
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            data,
            f,
            indent=2
        )

    logger.info(
        f"JSON written successfully: "
        f"{output_path}"
    )

