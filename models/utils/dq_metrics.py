from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when
)


# =========================================================
# Row Count
# =========================================================

def get_row_count(
    dataframe: DataFrame
) -> int:
    """
    Return dataframe row count.
    """

    return dataframe.count()


# =========================================================
# Null Count
# =========================================================

def get_null_count(
    dataframe: DataFrame,
    column_name: str
) -> int:
    """
    Count null values for a column.
    """

    return dataframe.filter(
        col(column_name).isNull()
    ).count()


# =========================================================
# Null Rate
# =========================================================

def get_null_rate(
    dataframe: DataFrame,
    column_name: str
) -> float:
    """
    Calculate null percentage.
    """

    total_rows = dataframe.count()

    if total_rows == 0:
        return 0.0

    null_rows = get_null_count(
        dataframe,
        column_name
    )

    return round(
        null_rows / total_rows,
        4
    )


# =========================================================
# Quarantine Rate
# =========================================================

def get_quarantine_rate(
    total_rows: int,
    quarantine_rows: int
) -> float:
    """
    Calculate quarantine percentage.
    """

    if total_rows == 0:
        return 0.0

    return round(
        quarantine_rows / total_rows,
        4
    )


# =========================================================
# Duplicate Rate
# =========================================================

def get_duplicate_rate(
    total_rows: int,
    duplicate_rows: int
) -> float:
    """
    Calculate duplicate percentage.
    """

    if total_rows == 0:
        return 0.0

    return round(
        duplicate_rows / total_rows,
        4
    )


# =========================================================
# Completeness Score
# =========================================================

def calculate_completeness_score(
    reported_count: int,
    expected_count: int
) -> float:
    """
    Calculate completeness score.
    """

    if expected_count == 0:
        return 0.0

    return round(
        reported_count / expected_count,
        4
    )
