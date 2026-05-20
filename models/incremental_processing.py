from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col
)

from models.config import (
    FACTS_DIR
)

from models.utils.logger import (
    get_logger,
    log_row_count
)


logger = get_logger(__name__)


# =========================================================
# Detect Existing Partitions
# =========================================================

def get_existing_year_month_partitions() -> list:
    """
    Detect existing fact table partitions.

    Example:
        year_month=202401
    """

    logger.info(
        "Detecting existing fact partitions"
    )

    fact_path = (
        FACTS_DIR
        / "fact_service_delivery"
    )

    if not fact_path.exists():

        logger.info(
            "Fact table path does not exist"
        )

        return []

    existing_periods = []

    for item in fact_path.iterdir():

        if (
            item.is_dir()
            and item.name.startswith(
                "year_month="
            )
        ):

            period = (
                item.name.split("=")[1]
            )

            existing_periods.append(
                period
            )

    logger.info(
        f"Detected "
        f"{len(existing_periods)} "
        f"existing partitions"
    )

    return existing_periods


# =========================================================
# Filter Incremental Data
# =========================================================

def filter_incremental_periods(
    dataframe: DataFrame,
    existing_periods: list
) -> DataFrame:
    """
    Keep only periods not already loaded.
    """

    logger.info(
        "Filtering incremental periods"
    )

    if not existing_periods:

        logger.info(
            "No existing partitions found. "
            "Running full load."
        )

        return dataframe

    incremental_df = dataframe.filter(
        ~col("year_month").isin(
            existing_periods
        )
    )

    log_row_count(
        logger,
        incremental_df,
        "incremental_df"
    )

    return incremental_df


# =========================================================
# Incremental Availability Check
# =========================================================

def is_incremental_load_required(
    dataframe: DataFrame
) -> bool:
    """
    Determine if new incremental data exists.
    """

    row_count = dataframe.count()

    if row_count == 0:

        logger.warning(
            "No new incremental periods found"
        )

        return False

    logger.info(
        f"Incremental dataset contains "
        f"{row_count:,} rows"
    )

    return True
