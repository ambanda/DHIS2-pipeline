import sys

from models.config import (
    DATA_DIR,
    CONTRACTS_DIR,
    DQ_THRESHOLDS,
    ENABLE_INCREMENTAL_LOADING,
    METADATA_SNAPSHOT_DIR
)

from models.utils.logger import (
    get_logger
)

from models.utils.spark_utils import (
    create_spark_session,
    safe_cache,
    safe_unpersist
)

from models.utils.dq_metrics import (
    get_quarantine_rate
)

from models.ingest import (
    ingest_metadata,
    ingest_reference_metadata,
    ingest_org_units,
    ingest_programs,
    ingest_data_values 
    
)

from models.incremental_processing import (
    get_existing_year_month_partitions,
    filter_incremental_periods,
    is_incremental_load_required
)

from models.metadata_drift import (
    run_metadata_drift_detection
)

from models.metadata_resolution import (
    resolve_data_elements,
    resolve_category_option_combos
)

from models.org_hierarchy import (
    build_org_hierarchy,
    resolve_hierarchy_names,
    enrich_fact_with_org_hierarchy
)

from models.dq_processing import (
    run_dq_pipeline
)

from models.dimensional_model import (
    build_dim_data_element,
    build_dim_org_unit,
    build_dim_program,
    build_dim_period,
    build_fact_service_delivery,
    write_dimensions,
    write_fact_table
)

from models.analytics import (
    calculate_monthly_mom_change,
    calculate_rolling_3month_average,
    calculate_country_reporting_rate,
    identify_underreporting_facilities,
    calculate_quarterly_global_totals,
    calculate_completeness_comparison,
    build_indicator_coverage_matrix,
    identify_low_completeness_countries,
    write_analytics_outputs
)

from models.contract_validation import (
    validate_dataframe_contract
)


logger = get_logger(__name__)


# =========================================================
# Critical DQ Validation
# =========================================================

def validate_critical_dq_checks(
    total_input_rows: int,
    total_quarantine_rows: int,
    fact_row_count: int
) -> None:

    logger.info(
        "Running critical DQ checks"
    )

    quarantine_rate = (
        get_quarantine_rate(
            total_input_rows,
            total_quarantine_rows
        )
    )

    logger.info(
        f"Quarantine rate: "
        f"{quarantine_rate:.2%}"
    )

    logger.info(
        f"Fact row count: "
        f"{fact_row_count:,}"
    )

    if (
        quarantine_rate
        > DQ_THRESHOLDS[
            "max_quarantine_rate"
        ]
    ):

        logger.error(
            "Quarantine threshold exceeded"
        )

        sys.exit(1)

    if (
        fact_row_count
        < DQ_THRESHOLDS[
            "min_fact_rows"
        ]
    ):

        logger.error(
            "Fact table is empty"
        )

        sys.exit(1)

    logger.info(
        "Critical DQ checks passed"
    )


# =========================================================
# Main Pipeline
# =========================================================

def run_pipeline() -> None:

    logger.info("=" * 60)
    logger.info(
        "STARTING DHIS2 PIPELINE"
    )
    logger.info("=" * 60)

    spark = create_spark_session()

    try:

        # =================================================
        # INGESTION
        # =================================================

        logger.info(
            "Starting ingestion"
        )

        (
            data_elements_df,
            category_option_combos_df
        ) = ingest_metadata(
            spark,
            str(DATA_DIR / "metadata.json")
        )

        org_units_df = ingest_org_units(
            spark,
            str(DATA_DIR / "org_units.json")
        )

        programs_df = ingest_programs(
            spark,
            str(DATA_DIR / "programs.json")
        )

        data_values_df = ingest_data_values(
            spark,
            str(DATA_DIR / "data_values.json")
        )

        total_input_rows = (
            data_values_df.count()
        )
        # =================================================
        # REFERENCE METADATA INGESTION
        # =================================================

        reference_metadata_df = (
            ingest_reference_metadata(
                spark,
                str(
                    METADATA_SNAPSHOT_DIR
                    / "metadata_reference.json"
                )
            )
        )

        # =================================================
        # METADATA DRIFT DETECTION
        # =================================================

        logger.info(
            "Starting metadata drift detection"
        )

        run_metadata_drift_detection(
            data_elements_df,
            reference_metadata_df
        )

        logger.info(
            "Metadata drift detection completed"
        )
        logger.info(
            f"Input rows: "
            f"{total_input_rows:,}"
        )

        # =================================================
        # INCREMENTAL FILTERING
        # =================================================

        if ENABLE_INCREMENTAL_LOADING:

            logger.info(
                "Incremental loading enabled"
            )

            existing_periods = (
                get_existing_year_month_partitions()
            )

            data_values_df = (
                filter_incremental_periods(
                    data_values_df,
                    existing_periods
                )
            )

            incremental_required = (
                is_incremental_load_required(
                    data_values_df
                )
            )

            if not incremental_required:

                logger.info(
                    "No new periods detected"
                )

                spark.stop()

                return

        # =================================================
        # METADATA RESOLUTION
        # =================================================

        (
            resolved_data_elements_df,
            unresolved_data_elements_df
        ) = resolve_data_elements(
            data_values_df,
            data_elements_df
        )

        (
            resolved_metadata_df,
            unresolved_cocs_df
        ) = resolve_category_option_combos(
            resolved_data_elements_df,
            category_option_combos_df
        )

        # =================================================
        # ORG HIERARCHY
        # =================================================

        hierarchy_df = build_org_hierarchy(
            org_units_df
        )

        hierarchy_df = resolve_hierarchy_names(
            hierarchy_df
        )

        (
            enriched_fact_df,
            unresolved_org_units_df
        ) = enrich_fact_with_org_hierarchy(
            resolved_metadata_df,
            hierarchy_df
        )

        # =================================================
        # DQ PROCESSING
        # =================================================

        dq_results = run_dq_pipeline(
            enriched_fact_df
        )

        clean_fact_df = dq_results[
            "clean_fact_df"
        ]

        exact_duplicates_df = dq_results[
            "exact_duplicates_df"
        ]

        superseded_records_df = dq_results[
            "superseded_records_df"
        ]

        clean_fact_df = safe_cache(
            clean_fact_df,
            "clean_fact_df"
        )

        # =================================================
        # DIMENSIONAL MODELING
        # =================================================

        dim_data_element_df = (
            build_dim_data_element(
                data_elements_df
            )
        )

        dim_org_unit_df = (
            build_dim_org_unit(
                hierarchy_df
            )
        )

        dim_program_df = (
            build_dim_program(
                programs_df
            )
        )

        dim_period_df = (
            build_dim_period(
                clean_fact_df
            )
        )

        fact_df = (
            build_fact_service_delivery(
                clean_fact_df
            )
        )

        fact_row_count = (
            fact_df.count()
        )

        # =================================================
        # CONTRACT VALIDATION
        # =================================================

        validate_dataframe_contract(
            fact_df,
            str(
                CONTRACTS_DIR
                / "fact_service_delivery.yml"
            )
        )

        # =================================================
        # WRITE STAR SCHEMA
        # =================================================

        write_dimensions(
            dim_data_element_df,
            dim_org_unit_df,
            dim_program_df,
            dim_period_df
        )

        write_fact_table(
            fact_df
        )

        # =================================================
        # ANALYTICS
        # =================================================

        analytics_outputs = {

            "monthly_mom_change":
                calculate_monthly_mom_change(
                    fact_df
                ),

            "rolling_3month_avg":
                calculate_rolling_3month_average(
                    fact_df
                ),

            "country_reporting_rate":
                calculate_country_reporting_rate(
                    fact_df
                ),

            "underreporting_facilities":
                identify_underreporting_facilities(
                    fact_df
                ),

            "quarterly_global_totals":
                calculate_quarterly_global_totals(
                    fact_df
                ),

            "completeness_comparison":
                calculate_completeness_comparison(
                    fact_df
                ),

            "indicator_coverage_matrix":
                build_indicator_coverage_matrix(
                    fact_df
                ),

            "low_completeness_countries":
                identify_low_completeness_countries(
                    fact_df
                )
        }

        write_analytics_outputs(
            analytics_outputs
        )

        # =================================================
        # QUARANTINE METRICS
        # =================================================

        total_quarantine_rows = (

            unresolved_data_elements_df.count()

            + unresolved_cocs_df.count()

            + unresolved_org_units_df.count()

            + exact_duplicates_df.count()

            + superseded_records_df.count()
        )

        logger.info(
            f"Total quarantine rows: "
            f"{total_quarantine_rows:,}"
        )

        # =================================================
        # FINAL DQ VALIDATION
        # =================================================

        validate_critical_dq_checks(
            total_input_rows,
            total_quarantine_rows,
            fact_row_count
        )

        # =================================================
        # CLEANUP
        # =================================================

        safe_unpersist(
            clean_fact_df,
            "clean_fact_df"
        )

        logger.info("=" * 60)
        logger.info(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )
        logger.info("=" * 60)

    except Exception as error:

        logger.exception(
            f"Pipeline failed: {error}"
        )

        sys.exit(1)

    finally:

        logger.info(
            "Stopping Spark session"
        )

        spark.stop()

