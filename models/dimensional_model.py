from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp
)

from models.constants import (
    FACT_PARTITIONS
)

from models.config import (
    DIMENSIONS_DIR,
    FACTS_DIR
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

from models.utils.io_utils import (
    write_parquet
)


logger = get_logger(__name__)


def validate_required_columns(
    dataframe: DataFrame,
    required_columns: list[str],
    dataframe_name: str
) -> None:

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:

        raise ValueError(
            f"{dataframe_name} is missing required columns: "
            f"{missing_columns}"
        )


# =========================================================
# Build Data Element Dimension
# =========================================================

def build_dim_data_element(
    data_elements_df: DataFrame
) -> DataFrame:

    logger.info(
        "Building dim_data_element"
    )

    dimension_df = (
        data_elements_df
        .select(
            col("id").alias(
                "data_element_id"
            ),
            col("name").alias(
                "data_element_name"
            ),
            "shortname",
            "code",
            "valuetype",
            "aggregationtype",
            "domaintype",
            "created",
            "lastupdated"
        )
        .dropDuplicates(
            ["data_element_id"]
        )
    )

    log_row_count(
        logger,
        dimension_df,
        "dim_data_element"
    )

    return dimension_df


# =========================================================
# Build Org Unit Dimension
# =========================================================

def build_dim_org_unit(
    org_hierarchy_df: DataFrame
) -> DataFrame:

    logger.info(
        "Building dim_org_unit"
    )

    dimension_df = (
        org_hierarchy_df
        .select(
            col("id").alias(
                "org_unit_id"
            ),
            col("name").alias(
                "org_unit_name"
            ),
            "shortname",
            "code",
            "level",
            "country_name",
            "region_name",
            "district_name",
            "facility_name",
            "created",
            "lastupdated"
        )
        .dropDuplicates(
            ["org_unit_id"]
        )
    )

    log_row_count(
        logger,
        dimension_df,
        "dim_org_unit"
    )

    return dimension_df


# =========================================================
# Build Program Dimension
# =========================================================

def build_dim_program(
    programs_df: DataFrame
) -> DataFrame:

    logger.info(
        "Building dim_program"
    )

    dimension_df = (
        programs_df
        .select(
            col("id").alias(
                "program_id"
            ),
            col("name").alias(
                "program_name"
            ),
            "shortname",
            "healtharea",
            "country",
            "reportingfrequency",
            "created",
            "lastupdated"
        )
        .dropDuplicates(
            ["program_id"]
        )
    )

    log_row_count(
        logger,
        dimension_df,
        "dim_program"
    )

    return dimension_df


# =========================================================
# Build Period Dimension
# =========================================================

def build_dim_period(
    fact_df: DataFrame
) -> DataFrame:

    logger.info(
        "Building dim_period"
    )

    validate_required_columns(
        fact_df,
        [
            "year_month",
            "period_start_date",
            "period_end_date",
            "year",
            "month",
            "quarter"
        ],
        "clean_fact_df"
    )

    dimension_df = (
        fact_df
        .select(
            "year_month",
            "period_start_date",
            "period_end_date",
            "year",
            "month",
            "quarter"
        )
        .dropDuplicates(
            ["year_month"]
        )
    )

    log_row_count(
        logger,
        dimension_df,
        "dim_period"
    )

    return dimension_df


# =========================================================
# Build Fact Table
# =========================================================

# =========================================================
# Build Fact Table
# =========================================================

def build_fact_service_delivery(
    clean_fact_df: DataFrame
) -> DataFrame:

    logger.info(
        "Building fact_service_delivery"
    )

    validate_required_columns(
        clean_fact_df,
        [
            "dataelement",
            "orgunit",
            "categoryoptioncombo",
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
            "value",
            "numeric_value",
            "boolean_value",
            "value_type",
            "is_null_value",
            "is_blank_value",
            "is_zero_value",
            "has_reported_value",
            "has_valid_numeric_value",
            "is_late_submission",
            "created",
            "lastupdated"
        ],
        "clean_fact_df"
    )

    fact_df = (
        clean_fact_df
        .select(

            # =================================================
            # Dimension Keys
            # =================================================

            col("dataelement")
            .alias("data_element_id"),

            col("orgunit")
            .alias("org_unit_id"),

            col("categoryoptioncombo")
            .alias(
                "category_option_combo_id"
            ),

            # =================================================
            # Time Dimensions
            # =================================================

            "year_month",
            "period_start_date",
            "period_end_date",
            "year",
            "month",
            "quarter",

            # =================================================
            # Descriptive Attributes
            # =================================================

            "data_element_name",
            "category_option_combo_name",

            "country_name",
            "region_name",
            "district_name",
            "facility_name",

            # =================================================
            # Measures
            # =================================================

            "value",
            "numeric_value",
            "boolean_value",

            # =================================================
            # DQ Attributes
            # =================================================

            "value_type",
            "is_null_value",
            "is_blank_value",
            "is_zero_value",
            "has_reported_value",
            "has_valid_numeric_value",
            "is_late_submission",

            # =================================================
            # Audit Columns
            # =================================================

            "created",
            "lastupdated"
        )

        .withColumn(
            "load_timestamp",
            current_timestamp()
        )
    )

    log_row_count(
        logger,
        fact_df,
        "fact_service_delivery"
    )

    return fact_df

# =========================================================
# Write Dimensions
# =========================================================

def write_dimensions(
    dim_data_element_df: DataFrame,
    dim_org_unit_df: DataFrame,
    dim_program_df: DataFrame,
    dim_period_df: DataFrame
) -> None:

    logger.info(
        "Writing dimension tables"
    )

    write_parquet(
        dim_data_element_df,
        str(
            DIMENSIONS_DIR
            / "dim_data_element"
        )
    )

    write_parquet(
        dim_org_unit_df,
        str(
            DIMENSIONS_DIR
            / "dim_org_unit"
        )
    )

    write_parquet(
        dim_program_df,
        str(
            DIMENSIONS_DIR
            / "dim_program"
        )
    )

    write_parquet(
        dim_period_df,
        str(
            DIMENSIONS_DIR
            / "dim_period"
        )
    )

    logger.info(
        "Dimension writes completed"
    )


# =========================================================
# Write Fact Table
# =========================================================

# =========================================================
# Write Fact Table
# =========================================================

def write_fact_table(
    fact_df: DataFrame
) -> None:

    logger.info(
        "Writing fact table"
    )

    logger.info(
        f"Fact partitions: {FACT_PARTITIONS}"
    )

    missing_partitions = [
        column
        for column in FACT_PARTITIONS
        if column not in fact_df.columns
    ]

    if missing_partitions:

        raise ValueError(
            "Missing partition columns in fact dataframe: "
            f"{missing_partitions}"
        )

    partition_count = (
        fact_df
        .select(*FACT_PARTITIONS)
        .distinct()
        .count()
    )

    target_partitions = max(
        1,
        min(
            partition_count,
            24
        )
    )

    logger.info(
        f"Repartitioning fact table to "
        f"{target_partitions} writer partitions"
    )

    write_df = (
        fact_df
        .repartition(
            target_partitions,
            *[
                col(column_name)
                for column_name in FACT_PARTITIONS
            ]
        )
    )

    write_parquet(
        dataframe=write_df,
        output_path=str(
            FACTS_DIR
            / "fact_service_delivery"
        ),
        partition_columns=FACT_PARTITIONS,
        mode="overwrite",
        max_records_per_file=250000
    )

    logger.info(
        "Fact table write completed"
    )
    
