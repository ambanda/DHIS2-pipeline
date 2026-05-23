from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit
)

from models.constants import (
    INTEGER_TYPES,
    FLOAT_TYPES,
    BOOLEAN_TYPES,
    LATE_REPORTING_DAYS
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

from models.utils.date_utils import (
    add_period_dates,
    add_late_reporting_deadline
)

from models.utils.dedup_utils import (
    remove_exact_duplicates,
    resolve_near_duplicates
)

from models.utils.dq_metrics import (
    get_duplicate_rate
)

logger = get_logger(__name__)


DQ_FACT_COLUMNS = [
    "dataelement",
    "period",
    "orgunit",
    "categoryoptioncombo",
    "attributeoptioncombo",
    "value",
    "storedby",
    "created",
    "lastupdated",
    "followup",
    "year_month",
    "period_start_date",
    "period_end_date",
    "year",
    "month",
    "quarter",
    "data_element_name",
    "category_option_combo_name",
    "country_name",
    "region_name",
    "district_name",
    "facility_name",
    "value_type"
]


def prune_fact_columns(
    dataframe: DataFrame
) -> DataFrame:
    """
    Keep only fact-stream columns required downstream.
    """

    existing_columns = [
        column_name
        for column_name in DQ_FACT_COLUMNS
        if column_name in dataframe.columns
    ]

    missing_columns = [
        column_name
        for column_name in DQ_FACT_COLUMNS
        if column_name not in dataframe.columns
    ]

    if missing_columns:

        logger.warning(
            f"DQ input missing optional columns: {missing_columns}"
        )

    return dataframe.select(
        *existing_columns
    )


# =========================================================
# Exact Duplicate Processing
# =========================================================

def process_exact_duplicates(
    dataframe: DataFrame
) -> tuple[DataFrame, DataFrame]:

    logger.info("Processing exact duplicates")

    # materialize once to avoid recomputation
    dataframe = dataframe.cache()
    total_count = dataframe.count()

    clean_df, duplicate_df = remove_exact_duplicates(dataframe)

    duplicate_df = duplicate_df.cache()
    duplicate_count = duplicate_df.count()

    duplicate_rate = get_duplicate_rate(
        total_count,
        duplicate_count
    )

    logger.info(f"Exact duplicate rate: {duplicate_rate:.4f}")

    dataframe.unpersist()

    return clean_df, duplicate_df


# =========================================================
# Near Duplicate Processing
# =========================================================

def process_near_duplicates(
    dataframe: DataFrame
) -> tuple[DataFrame, DataFrame]:

    logger.info(f"DQ dataframe columns: {dataframe.columns}")

    latest_df, superseded_df = resolve_near_duplicates(dataframe)

    return latest_df, superseded_df


# =========================================================
# Typed Value Casting
# =========================================================

def cast_typed_values(dataframe: DataFrame) -> DataFrame:

    logger.info("Casting typed values")

    return (
        dataframe
        .withColumn(
            "numeric_value",
            when(
                col("value_type").isin(list(INTEGER_TYPES)),
                col("value").cast("int")
            ).when(
                col("value_type").isin(list(FLOAT_TYPES)),
                col("value").cast("double")
            )
        )
        .withColumn(
            "boolean_value",
            when(
                col("value_type").isin(list(BOOLEAN_TYPES)),
                col("value").cast("boolean")
            )
        )
    )


# =========================================================
# Missing Value Flags
# =========================================================

def add_missing_value_flags(dataframe: DataFrame) -> DataFrame:

    logger.info("Adding missing value flags")

    return (
        dataframe
        .withColumn("is_null_value", col("value").isNull())
        .withColumn("is_blank_value", col("value") == "")
        .withColumn("is_zero_value", col("value") == "0")
    )


# =========================================================
# Late Reporting Flags
# =========================================================

def add_late_reporting_flags(dataframe: DataFrame) -> DataFrame:

    logger.info("Adding late reporting flags")

    reporting_df = add_period_dates(dataframe)
    reporting_df = add_late_reporting_deadline(reporting_df, LATE_REPORTING_DAYS)

    return reporting_df.withColumn(
        "is_late_submission",
        col("lastupdated") > col("reporting_deadline")
    )


# =========================================================
# Completeness Scoring
# =========================================================

def add_completeness_flags(dataframe: DataFrame) -> DataFrame:

    logger.info("Adding completeness indicators")

    return (
        dataframe
        .withColumn("has_reported_value", col("value").isNotNull())
        .withColumn("has_valid_numeric_value", col("numeric_value").isNotNull())
    )


# =========================================================
# Master DQ Pipeline
# =========================================================

def run_dq_pipeline(dataframe: DataFrame) -> dict:

    logger.info("Starting DQ pipeline")

    dataframe = prune_fact_columns(
        dataframe
    )

    # ---------------------------
    # Step 1: exact duplicates
    # ---------------------------
    clean_df, exact_duplicates_df = process_exact_duplicates(dataframe)

    # ---------------------------
    # Step 2: near duplicates
    # ---------------------------
    latest_df, superseded_df = process_near_duplicates(clean_df)

    # ---------------------------
    # Step 3: transformations
    # ---------------------------
    typed_df = cast_typed_values(latest_df)
    flagged_df = add_missing_value_flags(typed_df)
    reporting_df = add_late_reporting_flags(flagged_df)
    final_df = add_completeness_flags(reporting_df)

    logger.info("DQ pipeline completed")

    return {
        "clean_fact_df": final_df,
        "exact_duplicates_df": exact_duplicates_df,
        "superseded_records_df": superseded_df
    }
