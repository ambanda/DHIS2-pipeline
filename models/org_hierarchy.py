from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    element_at,
    size,
    split,
    when
)

from models.utils.logger import (
    get_logger,
    log_row_count
)

logger = get_logger(__name__)


# =========================================================
# Build Organization Hierarchy
# =========================================================

def build_org_hierarchy(
    org_units_df: DataFrame
) -> DataFrame:
    """
    Build organization hierarchy columns from
    DHIS2 path structure.

    Example DHIS2 path:
        /UID_COUNTRY/UID_REGION/UID_DISTRICT/UID_FACILITY

    Notes:
    - DHIS2 paths begin with "/"
    - split() therefore creates an empty first element
    - element_at() uses 1-based indexing
    """

    logger.info(
        "Building organization hierarchy"
    )

    hierarchy_df = (

        org_units_df

        .select(
            "id",
            "name",
            "shortname",
            "code",
            "level",
            "path",
            "created",
            "lastupdated"
        )

        .withColumn(
            "path_array",
            split(col("path"), "/")
        )

        # =================================================
        # Hierarchy UID Columns
        # =================================================

        .withColumn(
            "country_uid",
            when(
                size(col("path_array")) > 1,
                element_at(col("path_array"), 2)
            )
        )

        .withColumn(
            "region_uid",
            when(
                size(col("path_array")) > 2,
                element_at(col("path_array"), 3)
            )
        )

        .withColumn(
            "district_uid",
            when(
                size(col("path_array")) > 3,
                element_at(col("path_array"), 4)
            )
        )

        .withColumn(
            "facility_uid",
            when(
                size(col("path_array")) > 4,
                element_at(col("path_array"), 5)
            ).otherwise(col("id"))
        )

        .drop("path_array")
    )

    log_row_count(
        logger,
        hierarchy_df,
        "hierarchy_df"
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
    Resolve hierarchy names using optimized
    reusable lookup dataframe.

    Optimizations:
    - Single reusable lookup dataframe
    - Broadcast joins
    - Reduced Spark lineage complexity
    - Cleaner execution plans
    """

    logger.info(
        "Resolving hierarchy names"
    )

    # =====================================================
    # Reusable Lookup DataFrame
    # =====================================================

    lookup_df = (

        hierarchy_df

        .select(
            col("id").alias("lookup_uid"),
            col("name").alias("lookup_name")
        )

        .dropDuplicates(
            ["lookup_uid"]
        )

        .cache()
    )

    # =====================================================
    # Resolve Hierarchy Names
    # =====================================================

    resolved_df = (

        hierarchy_df

        # =================================================
        # Country
        # =================================================

        .join(
            broadcast(
                lookup_df.alias("country")
            ),
            col("country_uid")
            == col("country.lookup_uid"),
            "left"
        )

        .withColumn(
            "country_name",
            col("country.lookup_name")
        )

        .drop(
            "country.lookup_uid",
            "country.lookup_name"
        )

        # =================================================
        # Region
        # =================================================

        .join(
            broadcast(
                lookup_df.alias("region")
            ),
            col("region_uid")
            == col("region.lookup_uid"),
            "left"
        )

        .withColumn(
            "region_name",
            col("region.lookup_name")
        )

        .drop(
            "region.lookup_uid",
            "region.lookup_name"
        )

        # =================================================
        # District
        # =================================================

        .join(
            broadcast(
                lookup_df.alias("district")
            ),
            col("district_uid")
            == col("district.lookup_uid"),
            "left"
        )

        .withColumn(
            "district_name",
            col("district.lookup_name")
        )

        .drop(
            "district.lookup_uid",
            "district.lookup_name"
        )

        # =================================================
        # Facility
        # =================================================

        .join(
            broadcast(
                lookup_df.alias("facility")
            ),
            col("facility_uid")
            == col("facility.lookup_uid"),
            "left"
        )

        .withColumn(
            "facility_name",
            col("facility.lookup_name")
        )

        .drop(
            "facility.lookup_uid",
            "facility.lookup_name"
        )
    )

    # =====================================================
    # Cleanup
    # =====================================================

    lookup_df.unpersist()

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
    Enrich fact records with organization hierarchy.

    Returns:
        (
            enriched_fact_df,
            unresolved_org_units_df
        )
    """

    logger.info(
        "Enriching fact records with hierarchy"
    )

    # =====================================================
    # Hierarchy Lookup
    # =====================================================

    hierarchy_lookup_df = (

        hierarchy_df

        .select(
            col("id").alias("orgunit_uid"),

            "country_name",
            "region_name",
            "district_name",
            "facility_name"
        )

        .dropDuplicates(
            ["orgunit_uid"]
        )
    )

    # =====================================================
    # Enrich Fact Data
    # =====================================================

    enriched_df = (

        fact_df.alias("fact")

        .join(
            broadcast(
                hierarchy_lookup_df
            ).alias("org"),
            col("fact.orgunit")
            == col("org.orgunit_uid"),
            "left"
        )

        .drop("orgunit_uid")
    )

    # =====================================================
    # Quarantine Unresolved Org Units
    # =====================================================

    unresolved_org_units_df = (

        enriched_df

        .filter(
            col("facility_name").isNull()
        )
    )

    enriched_df = (

        enriched_df

        .filter(
            col("facility_name").isNotNull()
        )
    )

    # =====================================================
    # Logging
    # =====================================================

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

    return (
        enriched_df,
        unresolved_org_units_df
    )
