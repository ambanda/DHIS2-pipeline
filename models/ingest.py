from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode

from models.schemas import (
    METADATA_SCHEMA,
    ORG_UNITS_SCHEMA,
    PROGRAMS_SCHEMA,
    DATA_VALUES_SCHEMA
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

from models.utils.spark_utils import (
    standardize_column_names,
    validate_dataframe_not_empty
)

from models.utils.date_utils import (
    standardize_timestamps
)


logger = get_logger(__name__)


# =========================================================
# Generic JSON Reader
# =========================================================

def read_json_file(
    spark: SparkSession,
    file_path: str,
    schema
) -> DataFrame:
    """
    Read JSON file using explicit schema.
    """

    logger.info(
        f"Reading JSON file: {file_path}"
    )

    dataframe = (

        spark.read

        .schema(schema)

        .option(
            "multiLine",
            True
        )

        .json(file_path)
    )

    validate_dataframe_not_empty(
        dataframe,
        file_path
    )

    return dataframe


# =========================================================
# Metadata Standardization
# =========================================================

def standardize_metadata_dataframe(
    dataframe: DataFrame
) -> DataFrame:
    """
    Standardize metadata dataframe.

    Applies:
    - standardized column names
    - standardized timestamps
    """

    dataframe = standardize_column_names(
        dataframe
    )

    dataframe = standardize_timestamps(
        dataframe,
        [
            "created",
            "lastupdated"
        ]
    )

    return dataframe


# =========================================================
# Metadata Ingestion
# =========================================================

def ingest_metadata(
    spark: SparkSession,
    metadata_path: str
) -> tuple[DataFrame, DataFrame]:
    """
    Ingest metadata.json.

    Returns:
        (
            data_elements_df,
            category_option_combos_df
        )
    """

    logger.info(
        "Starting metadata ingestion"
    )

    metadata_df = read_json_file(
        spark,
        metadata_path,
        METADATA_SCHEMA
    )

    # =====================================================
    # Data Elements
    # =====================================================

    data_elements_df = (

        metadata_df

        .select(
            explode(
                "dataElements"
            ).alias(
                "data_element"
            )
        )

        .select(
            "data_element.*"
        )
    )

    data_elements_df = (
        standardize_metadata_dataframe(
            data_elements_df
        )
    )

    # =====================================================
    # Category Option Combos
    # =====================================================

    category_option_combos_df = (

        metadata_df

        .select(
            explode(
                "categoryOptionCombos"
            ).alias(
                "category_option_combo"
            )
        )

        .select(
            "category_option_combo.*"
        )
    )

    category_option_combos_df = (
        standardize_metadata_dataframe(
            category_option_combos_df
        )
    )

    log_row_count(
        logger,
        data_elements_df,
        "data_elements_df"
    )

    log_row_count(
        logger,
        category_option_combos_df,
        "category_option_combos_df"
    )

    logger.info(
        "Metadata ingestion completed"
    )

    return (
        data_elements_df,
        category_option_combos_df
    )


# =========================================================
# Reference Metadata Ingestion
# =========================================================

def ingest_reference_metadata(
    spark: SparkSession,
    metadata_path: str
) -> DataFrame:
    """
    Ingest historical metadata snapshot.

    Used for:
    - metadata drift detection
    - rename detection
    - added/removed metadata analysis
    """

    logger.info(
        "Starting reference metadata ingestion"
    )

    metadata_df = read_json_file(
        spark,
        metadata_path,
        METADATA_SCHEMA
    )

    reference_metadata_df = (

        metadata_df

        .select(
            explode(
                "dataElements"
            ).alias(
                "data_element"
            )
        )

        .select(
            "data_element.*"
        )
    )

    reference_metadata_df = (
        standardize_metadata_dataframe(
            reference_metadata_df
        )
    )

    log_row_count(
        logger,
        reference_metadata_df,
        "reference_metadata_df"
    )

    logger.info(
        "Reference metadata ingestion completed"
    )

    return reference_metadata_df


# =========================================================
# Org Unit Ingestion
# =========================================================

def ingest_org_units(
    spark: SparkSession,
    org_units_path: str
) -> DataFrame:
    """
    Ingest organization units.
    """

    logger.info(
        "Starting org unit ingestion"
    )

    org_units_df = read_json_file(
        spark,
        org_units_path,
        ORG_UNITS_SCHEMA
    )

    org_units_df = (

        org_units_df

        .select(
            explode(
                "organisationUnits"
            ).alias(
                "org_unit"
            )
        )

        .select(
            "org_unit.*"
        )
    )

    org_units_df = standardize_column_names(
        org_units_df
    )

    org_units_df = standardize_timestamps(
        org_units_df,
        [
            "created",
            "lastupdated"
        ]
    )

    log_row_count(
        logger,
        org_units_df,
        "org_units_df"
    )

    logger.info(
        "Org unit ingestion completed"
    )

    return org_units_df


# =========================================================
# Program Ingestion
# =========================================================

def ingest_programs(
    spark: SparkSession,
    programs_path: str
) -> DataFrame:
    """
    Ingest programs.json.
    """

    logger.info(
        "Starting program ingestion"
    )

    programs_df = read_json_file(
        spark,
        programs_path,
        PROGRAMS_SCHEMA
    )

    programs_df = (

        programs_df

        .select(
            explode(
                "programs"
            ).alias(
                "program"
            )
        )

        .select(
            "program.*"
        )
    )

    programs_df = standardize_column_names(
        programs_df
    )

    programs_df = standardize_timestamps(
        programs_df,
        [
            "created",
            "lastupdated"
        ]
    )

    log_row_count(
        logger,
        programs_df,
        "programs_df"
    )

    logger.info(
        "Program ingestion completed"
    )

    return programs_df


# =========================================================
# Data Values Ingestion
# =========================================================

def ingest_data_values(
    spark: SparkSession,
    data_values_path: str
) -> DataFrame:
    """
    Ingest and flatten data values.
    """

    logger.info(
        "Starting data values ingestion"
    )

    data_values_df = read_json_file(
        spark,
        data_values_path,
        DATA_VALUES_SCHEMA
    )

    data_values_df = (

        data_values_df

        .select(
            explode(
                "dataValues"
            ).alias(
                "data_value"
            )
        )

        .select(
            "data_value.*"
        )
    )

    data_values_df = standardize_column_names(
        data_values_df
    )

    data_values_df = standardize_timestamps(
        data_values_df,
        [
            "created",
            "lastupdated"
        ]
    )

    log_row_count(
        logger,
        data_values_df,
        "data_values_df"
    )

    logger.info(
        "Data values ingestion completed"
    )

    return data_values_df

