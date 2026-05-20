from pathlib import Path

from pyspark.sql import DataFrame

from models.utils.logger import get_logger
import json
import os
import shutil

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
    partition_columns: list[str] | None = None
) -> None:
    """
    Write dataframe as parquet.
    """

    ensure_directory_exists(output_path)

    writer = (
        dataframe.write
        .mode("overwrite")
    )

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    writer.parquet(output_path)

    logger.info(
        f"Parquet written successfully: {output_path}"
    )


# =========================================================
# CSV Writer
# =========================================================

def write_csv(
    dataframe: DataFrame,
    output_path: str
) -> None:
    """
    Write dataframe as CSV.
    """

    ensure_directory_exists(output_path)

    (
        dataframe.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    logger.info(
        f"CSV written successfully: {output_path}"
    )


# =========================================================
# JSON Writer (for Python dicts ONLY)
# =========================================================

def write_json(data: dict, output_path: str) -> None:
    output_path = Path(output_path)

    # 🧨 If a folder exists with same name → remove it
    if output_path.exists() and output_path.is_dir():
        shutil.rmtree(output_path)

    # ✅ only create parent directory
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"JSON written successfully: {output_path}")
