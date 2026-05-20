from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    row_number,
    desc
)

from models.constants import DEDUP_KEYS


# =========================================================
# Column Resolver Utilities
# =========================================================

def resolve_column_name(
    dataframe: DataFrame,
    target_column: str
) -> str:
    """
    Resolve dataframe column using case-insensitive matching.
    """

    column_mapping = {
        c.lower(): c
        for c in dataframe.columns
    }

    resolved_column = column_mapping.get(target_column.lower())

    if not resolved_column:
        raise ValueError(
            f"Column '{target_column}' not found in dataframe"
        )

    return resolved_column


def resolve_dedup_keys(
    dataframe: DataFrame
) -> list[str]:
    """
    Resolve deduplication keys dynamically
    using case-insensitive matching.
    """

    resolved_keys = []

    missing_keys = []

    for key in DEDUP_KEYS:

        try:
            resolved_key = resolve_column_name(
                dataframe,
                key
            )

            resolved_keys.append(resolved_key)

        except ValueError:
            missing_keys.append(key)

    if missing_keys:
        raise ValueError(
            f"Missing deduplication keys: {missing_keys}"
        )

    return resolved_keys


# =========================================================
# Exact Duplicate Removal
# =========================================================

def remove_exact_duplicates(
    dataframe: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Remove fully identical rows.
    """

    deduped_df = dataframe.dropDuplicates()

    duplicate_df = dataframe.subtract(deduped_df)

    return deduped_df, duplicate_df


# =========================================================
# Near Duplicate Resolution
# =========================================================

def resolve_near_duplicates(
    dataframe: DataFrame,
    timestamp_column: str = "lastUpdated"
) -> tuple[DataFrame, DataFrame]:
    """
    Keep latest record per business key.

    Uses:
    - dynamic key resolution
    - case-insensitive column matching
    - timestamp ordering
    """

    # -----------------------------------------------------
    # Resolve timestamp column safely
    # -----------------------------------------------------

    resolved_timestamp_col = resolve_column_name(
        dataframe,
        timestamp_column
    )

    # -----------------------------------------------------
    # Resolve deduplication keys safely
    # -----------------------------------------------------

    resolved_keys = resolve_dedup_keys(
        dataframe
    )

    # -----------------------------------------------------
    # Window specification
    # -----------------------------------------------------

    window_spec = (
        Window
        .partitionBy(*resolved_keys)
        .orderBy(
            desc(col(resolved_timestamp_col))
        )
    )

    # -----------------------------------------------------
    # Rank records
    # -----------------------------------------------------

    ranked_df = dataframe.withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    # -----------------------------------------------------
    # Latest records
    # -----------------------------------------------------

    latest_df = (
        ranked_df
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # -----------------------------------------------------
    # Superseded records
    # -----------------------------------------------------

    superseded_df = (
        ranked_df
        .filter(col("row_num") > 1)
        .drop("row_num")
    )

    return latest_df, superseded_df
