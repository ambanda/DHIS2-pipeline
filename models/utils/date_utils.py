from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    to_date,
    to_timestamp,
    last_day,
    year,
    month,
    quarter,
    date_add
)


# =========================================================
# Period Parsing
# =========================================================

def add_period_dates(
    dataframe: DataFrame,
    period_column: str = "period"
) -> DataFrame:
    """
    Convert yyyyMM period into usable date columns.
    """

    transformed_df = (
        dataframe
        .withColumn(
            "year_month",
            col(period_column)
        )
        .withColumn(
            "period_start_date",
            to_date(
                concat(
                    col("year_month"),
                    lit("01")
                ),
                "yyyyMMdd"
            )
        )
        .withColumn(
            "period_end_date",
            last_day(col("period_start_date"))
        )
        .withColumn(
            "year",
            year(col("period_start_date"))
        )
        .withColumn(
            "month",
            month(col("period_start_date"))
        )
        .withColumn(
            "quarter",
            quarter(col("period_start_date"))
        )
    )

    return transformed_df


# =========================================================
# Timestamp Standardization
# =========================================================

def standardize_timestamps(
    dataframe: DataFrame,
    timestamp_columns: list[str]
) -> DataFrame:
    """
    Convert timestamp columns into Spark timestamps.
    """

    transformed_df = dataframe

    for column_name in timestamp_columns:

        transformed_df = transformed_df.withColumn(
            column_name,
            to_timestamp(col(column_name))
        )

    return transformed_df


# =========================================================
# Late Reporting Logic
# =========================================================

def add_late_reporting_deadline(
    dataframe: DataFrame,
    days: int = 60
) -> DataFrame:
    """
    Add reporting deadline column.
    """

    return dataframe.withColumn(
        "reporting_deadline",
        date_add(
            col("period_end_date"),
            days
        )
    )
