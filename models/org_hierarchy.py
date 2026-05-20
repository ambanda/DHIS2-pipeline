from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    split,
    size,
    when
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

logger = get_logger(__name__)


# =========================================================
# Build Org Hierarchy Columns
# =========================================================

def build_org_hierarchy(
    org_units_df: DataFrame
) -> DataFrame:
    """
    Build dynamic hierarchy columns from path.
    """

    logger.info("Building organization hierarchy")

    hierarchy_df = (
        org_units_df
        .select(
            "id",
            "name",
            "path"
        )
        .withColumn(
            "path_array",
            split(col("path"), "/")
        )
    )

    hierarchy_df = (
        hierarchy_df
        .withColumn(
            "country_uid",
            when(
                size(col("path_array")) > 1,
                col("path_array")[1]
            )
        )
        .withColumn(
            "region_uid",
            when(
                size(col("path_array")) > 2,
                col("path_array")[2]
            )
        )
        .withColumn(
            "district_uid",
            when(
                size(col("path_array")) > 3,
                col("path_array")[3]
            )
        )
        .withColumn(
            "facility_uid",
            when(
                size(col("path_array")) > 4,
                col("path_array")[4]
            ).otherwise(col("id"))
        )
    )

    logger.info(
        "Organization hierarchy successfully built"
    )

    return hierarchy_df


# =========================================================
# Resolve Hierarchy Names
# =========================================================

def resolve_hierarchy_names(
    hierarchy_df: DataFrame
) -> DataFrame:
    """
    Resolve hierarchy UID names using self joins.
    """

    logger.info("Resolving hierarchy names")

    country_lookup = (
        hierarchy_df
        .select(
            col("id").alias("country_uid_lookup"),
            col("name").alias("country_name")
        )
    )

    region_lookup = (
        hierarchy_df
        .select(
            col("id").alias("region_uid_lookup"),
            col("name").alias("region_name")
        )
    )

    district_lookup = (
        hierarchy_df
        .select(
            col("id").alias("district_uid_lookup"),
            col("name").alias("district_name")
        )
    )

    facility_lookup = (
        hierarchy_df
        .select(
            col("id").alias("facility_uid_lookup"),
            col("name").alias("facility_name")
        )
    )

    resolved_df = (
        hierarchy_df

        .join(
            broadcast(country_lookup),
            col("country_uid")
            == col("country_uid_lookup"),
            "left"
        )
        .drop("country_uid_lookup")

        .join(
            broadcast(region_lookup),
            col("region_uid")
            == col("region_uid_lookup"),
            "left"
        )
        .drop("region_uid_lookup")

        .join(
            broadcast(district_lookup),
            col("district_uid")
            == col("district_uid_lookup"),
            "left"
        )
        .drop("district_uid_lookup")

        .join(
            broadcast(facility_lookup),
            col("facility_uid")
            == col("facility_uid_lookup"),
            "left"
        )
        .drop("facility_uid_lookup")
    )

    logger.info(
        "Hierarchy name resolution completed"
    )

    return resolved_df


# =========================================================
# Enrich Fact Records
# =========================================================

def enrich_fact_with_org_hierarchy(
    fact_df: DataFrame,
    hierarchy_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Enrich fact records with hierarchy dimensions.

    Returns:
        enriched_df,
        unresolved_org_units_df
    """

    logger.info(
        "Enriching fact records with hierarchy"
    )

    hierarchy_lookup = (
        hierarchy_df
        .select(
            col("id").alias("orgunit_uid"),
            "country_name",
            "region_name",
            "district_name",
            "facility_name"
        )
    )

    enriched_df = (
        fact_df.alias("fact")
        .join(
            broadcast(hierarchy_lookup).alias("org"),
            col("fact.orgunit")
            == col("org.orgunit_uid"),
            "left"
        )
        .drop("orgunit_uid")
    )

    unresolved_org_units_df = enriched_df.filter(
        col("facility_name").isNull()
    )

    enriched_df = enriched_df.filter(
        col("facility_name").isNotNull()
    )

    log_row_count(
        logger,
        enriched_df,
        "enriched_fact_df"
    )

    log_row_count(
        logger,
        unresolved_org_units_df,
        "unresolved_org_units_df"
    )

    logger.info(
        "Fact enrichment completed"
    )

    return enriched_df, unresolved_org_units_df
