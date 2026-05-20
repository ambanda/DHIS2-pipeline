from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    avg,
    lag,
    sum,
    countDistinct,
    round,
    when
)

from models.config import (
    ANALYTICS_DIR
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

from models.utils.io_utils import (
    write_parquet
)


logger = get_logger(__name__)


# =========================================================
# Month-over-Month Change
# =========================================================

def calculate_monthly_mom_change(
    fact_df: DataFrame
) -> DataFrame:
    """
    Calculate MoM metric change.
    """

    logger.info(
        "Calculating MoM changes"
    )

    aggregated_df = (
        fact_df
        .groupBy(
            "country_name",
            "data_element_name",
            "year_month"
        )
        .agg(
            sum("numeric_value")
            .alias("monthly_total")
        )
    )

    window_spec = (
        Window
        .partitionBy(
            "country_name",
            "data_element_name"
        )
        .orderBy("year_month")
    )

    result_df = (
        aggregated_df

        .withColumn(
            "previous_month_total",
            lag("monthly_total")
            .over(window_spec)
        )

        .withColumn(
            "mom_change",
            col("monthly_total")
            - col("previous_month_total")
        )
    )

    log_row_count(
        logger,
        result_df,
        "monthly_mom_change"
    )

    return result_df


# =========================================================
# Rolling 3-Month Average
# =========================================================

def calculate_rolling_3month_average(
    fact_df: DataFrame
) -> DataFrame:
    """
    Calculate rolling 3-month averages.
    """

    logger.info(
        "Calculating rolling averages"
    )

    aggregated_df = (
        fact_df
        .groupBy(
            "country_name",
            "data_element_name",
            "year_month"
        )
        .agg(
            sum("numeric_value")
            .alias("monthly_total")
        )
    )

    window_spec = (
        Window
        .partitionBy(
            "country_name",
            "data_element_name"
        )
        .orderBy("year_month")
        .rowsBetween(-2, 0)
    )

    result_df = (
        aggregated_df

        .withColumn(
            "rolling_3month_avg",
            avg("monthly_total")
            .over(window_spec)
        )
    )

    log_row_count(
        logger,
        result_df,
        "rolling_3month_avg"
    )

    return result_df


# =========================================================
# Country Reporting Rate
# =========================================================

def calculate_country_reporting_rate(
    fact_df: DataFrame
) -> DataFrame:
    """
    Calculate reporting completeness by country.
    """

    logger.info(
        "Calculating country reporting rates"
    )

    result_df = (
        fact_df
        .groupBy(
            "country_name",
            "year_month"
        )
        .agg(
            countDistinct(
                when(
                    col("has_reported_value")
                    == True,
                    col("facility_name")
                )
            ).alias("reporting_facilities"),

            countDistinct(
                "facility_name"
            ).alias("expected_facilities")
        )

        .withColumn(
            "reporting_rate",
            round(
                col("reporting_facilities")
                / col("expected_facilities"),
                4
            )
        )
    )

    return result_df


# =========================================================
# Underreporting Facilities
# =========================================================

def identify_underreporting_facilities(
    fact_df: DataFrame,
    threshold: float = 0.50
) -> DataFrame:
    """
    Identify facilities with poor reporting.
    """

    logger.info(
        "Identifying underreporting facilities"
    )

    facility_df = (
        fact_df
        .groupBy(
            "country_name",
            "facility_name"
        )
        .agg(
            round(
                avg(
                    when(
                        col("has_reported_value")
                        == True,
                        1
                    ).otherwise(0)
                ),
                4
            ).alias("reporting_score")
        )
    )

    result_df = facility_df.filter(
        col("reporting_score")
        < threshold
    )

    return result_df


# =========================================================
# Quarterly Global Totals
# =========================================================

def calculate_quarterly_global_totals(
    fact_df: DataFrame
) -> DataFrame:
    """
    Calculate quarterly global totals.
    """

    logger.info(
        "Calculating quarterly totals"
    )

    result_df = (
        fact_df
        .groupBy(
            "year",
            "quarter",
            "data_element_name"
        )
        .agg(
            sum("numeric_value")
            .alias("quarterly_total")
        )
    )

    return result_df


# =========================================================
# Completeness Comparison
# =========================================================

def calculate_completeness_comparison(
    fact_df: DataFrame
) -> DataFrame:
    """
    Compare completeness across countries.
    """

    logger.info(
        "Calculating completeness comparison"
    )

    result_df = (
        fact_df
        .groupBy(
            "country_name"
        )
        .agg(
            round(
                avg(
                    when(
                        col("has_reported_value")
                        == True,
                        1
                    ).otherwise(0)
                ),
                4
            ).alias("completeness_score")
        )
    )

    return result_df


# =========================================================
# Indicator Coverage Matrix
# =========================================================

def build_indicator_coverage_matrix(
    fact_df: DataFrame
) -> DataFrame:
    """
    Build country-indicator coverage matrix.
    """

    logger.info(
        "Building indicator coverage matrix"
    )

    result_df = (
        fact_df
        .groupBy(
            "country_name",
            "data_element_name"
        )
        .agg(
            round(
                avg(
                    when(
                        col("has_reported_value")
                        == True,
                        1
                    ).otherwise(0)
                ),
                4
            ).alias("coverage_score")
        )
    )

    return result_df


# =========================================================
# Low Completeness Countries
# =========================================================

def identify_low_completeness_countries(
    fact_df: DataFrame,
    threshold: float = 0.70
) -> DataFrame:
    """
    Identify low completeness countries.
    """

    logger.info(
        "Identifying low completeness countries"
    )

    completeness_df = (
        calculate_completeness_comparison(
            fact_df
        )
    )

    result_df = completeness_df.filter(
        col("completeness_score")
        < threshold
    )

    return result_df


# =========================================================
# Persist Analytics Outputs
# =========================================================

def write_analytics_outputs(
    analytics_outputs: dict
) -> None:
    """
    Persist all analytics outputs.
    """

    logger.info(
        "Writing analytics outputs"
    )

    for output_name, dataframe in analytics_outputs.items():

        write_parquet(
            dataframe,
            str(
                ANALYTICS_DIR
                / output_name
            )
        )

    logger.info(
        "Analytics outputs written successfully"
    )

