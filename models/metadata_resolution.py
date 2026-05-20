from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

logger = get_logger(__name__)


# =========================================================
# Resolve Data Element Metadata
# =========================================================

def resolve_data_elements(
    data_values_df: DataFrame,
    data_elements_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Resolve data element UIDs.

    Returns:
        resolved_df,
        unresolved_df
    """

    logger.info("Resolving data element UIDs")

    data_element_lookup = (
        data_elements_df
        .select(
            col("id").alias("data_element_uid"),
            col("name").alias("data_element_name"),
            "shortname",
            "code",
            col("valuetype").alias("value_type"),
            "domaintype",
            "aggregationtype",
            "zeroissignificant",
            "categorycombo",
            "dataelementgroups"
        )
    )

    resolved_df = (
        data_values_df.alias("dv")
        .join(
            broadcast(data_element_lookup).alias("de"),
            col("dv.dataelement")
            == col("de.data_element_uid"),
            "left"
        )
        .drop("data_element_uid")
    )

    unresolved_df = resolved_df.filter(
        col("data_element_name").isNull()
    )

    resolved_df = resolved_df.filter(
        col("data_element_name").isNotNull()
    )

    log_row_count(
        logger,
        resolved_df,
        "resolved_data_elements_df"
    )

    log_row_count(
        logger,
        unresolved_df,
        "unresolved_data_elements_df"
    )

    logger.info("Data element resolution completed")

    return resolved_df, unresolved_df


# =========================================================
# Resolve Category Option Combos
# =========================================================

def resolve_category_option_combos(
    dataframe: DataFrame,
    category_option_combos_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Resolve category option combo UIDs.
    """

    logger.info(
        "Resolving category option combo UIDs"
    )

    coc_lookup = (
        category_option_combos_df
        .select(
            col("id").alias("category_option_combo_uid"),
            col("name").alias(
                "category_option_combo_name"
            )
        )
    )

    resolved_df = (
        dataframe.alias("dv")
        .join(
            broadcast(coc_lookup).alias("coc"),
            col("dv.categoryoptioncombo")
            == col("coc.category_option_combo_uid"),
            "left"
        )
        .drop("category_option_combo_uid")
    )

    unresolved_df = resolved_df.filter(
        col("category_option_combo_name").isNull()
    )

    resolved_df = resolved_df.filter(
        col("category_option_combo_name").isNotNull()
    )

    log_row_count(
        logger,
        resolved_df,
        "resolved_category_option_combos_df"
    )

    log_row_count(
        logger,
        unresolved_df,
        "unresolved_category_option_combos_df"
    )

    logger.info(
        "Category option combo resolution completed"
    )

    return resolved_df, unresolved_df
