import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col
)

from models.config import (
    METADATA_DRIFT_DIR
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

from models.utils.io_utils import (
    write_parquet,
    write_json
)


logger = get_logger(__name__)


# =========================================================
# Detect Added Data Elements
# =========================================================

def detect_added_data_elements(
    current_df: DataFrame,
    reference_df: DataFrame
) -> DataFrame:
    """
    Detect newly added data elements.
    """

    logger.info(
        "Detecting added data elements"
    )

    added_df = (
        current_df.alias("current")
        .join(
            reference_df.alias("reference"),
            on="id",
            how="left_anti"
        )
    )

    log_row_count(
        logger,
        added_df,
        "added_data_elements"
    )

    return added_df


# =========================================================
# Detect Removed Data Elements
# =========================================================

def detect_removed_data_elements(
    current_df: DataFrame,
    reference_df: DataFrame
) -> DataFrame:
    """
    Detect removed data elements.
    """

    logger.info(
        "Detecting removed data elements"
    )

    removed_df = (
        reference_df.alias("reference")
        .join(
            current_df.alias("current"),
            on="id",
            how="left_anti"
        )
    )

    log_row_count(
        logger,
        removed_df,
        "removed_data_elements"
    )

    return removed_df


# =========================================================
# Detect Renamed Data Elements
# =========================================================

def detect_renamed_data_elements(
    current_df: DataFrame,
    reference_df: DataFrame
) -> DataFrame:
    """
    Detect renamed data elements.
    """

    logger.info(
        "Detecting renamed data elements"
    )

    renamed_df = (

        current_df.alias("current")

        .join(
            reference_df.alias("reference"),
            on="id",
            how="inner"
        )

        .filter(
            col("current.name")
            != col("reference.name")
        )

        .select(

            col("id"),

            col("reference.name")
            .alias("old_name"),

            col("current.name")
            .alias("new_name")
        )
    )

    log_row_count(
        logger,
        renamed_df,
        "renamed_data_elements"
    )

    return renamed_df


# =========================================================
# Write Drift Outputs
# =========================================================

def write_metadata_drift_outputs(
    added_df: DataFrame,
    removed_df: DataFrame,
    renamed_df: DataFrame
) -> None:
    """
    Persist metadata drift outputs.
    """

    logger.info(
        "Writing metadata drift outputs"
    )

    write_parquet(
        added_df,
        str(
            METADATA_DRIFT_DIR
            / "added_data_elements"
        )
    )

    write_parquet(
        removed_df,
        str(
            METADATA_DRIFT_DIR
            / "removed_data_elements"
        )
    )

    write_parquet(
        renamed_df,
        str(
            METADATA_DRIFT_DIR
            / "renamed_data_elements"
        )
    )

    summary = {

        "added_data_elements":
            added_df.count(),

        "removed_data_elements":
            removed_df.count(),

        "renamed_data_elements":
            renamed_df.count()
    }

    write_json(
        summary,
        str(
            METADATA_DRIFT_DIR
            / "metadata_drift_summary.json"
        )
    )

    logger.info(
        "Metadata drift outputs written"
    )


# =========================================================
# Full Metadata Drift Pipeline
# =========================================================

def run_metadata_drift_detection(
    current_metadata_df: DataFrame,
    reference_metadata_df: DataFrame
) -> dict:
    """
    Execute full metadata drift detection.
    """

    logger.info("=" * 60)
    logger.info(
        "STARTING METADATA DRIFT DETECTION"
    )
    logger.info("=" * 60)

    added_df = (
        detect_added_data_elements(
            current_metadata_df,
            reference_metadata_df
        )
    )

    removed_df = (
        detect_removed_data_elements(
            current_metadata_df,
            reference_metadata_df
        )
    )

    renamed_df = (
        detect_renamed_data_elements(
            current_metadata_df,
            reference_metadata_df
        )
    )

    write_metadata_drift_outputs(
        added_df,
        removed_df,
        renamed_df
    )

    logger.info("=" * 60)
    logger.info(
        "METADATA DRIFT DETECTION COMPLETED"
    )
    logger.info("=" * 60)

    return {

        "added_df": added_df,

        "removed_df": removed_df,

        "renamed_df": renamed_df
    }
