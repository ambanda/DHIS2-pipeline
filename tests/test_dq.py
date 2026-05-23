from datetime import datetime

from models.dq_processing import run_dq_pipeline
from models.utils.dedup_utils import remove_exact_duplicates


def test_remove_exact_duplicates_preserves_duplicate_count(spark):
    dataframe = spark.createDataFrame(
        [
            {"id": "A", "value": "1"},
            {"id": "A", "value": "1"},
            {"id": "A", "value": "1"},
            {"id": "B", "value": "2"}
        ]
    )

    clean_df, duplicate_df = remove_exact_duplicates(dataframe)

    assert clean_df.count() == 2
    assert duplicate_df.count() == 2


def test_dq_pipeline_keeps_latest_near_duplicate(spark):
    rows = [
        {
            "dataelement": "DE1",
            "period": "202401",
            "year_month": "202401",
            "orgunit": "OU1",
            "categoryoptioncombo": "COC1",
            "value": "1",
            "value_type": "NUMBER",
            "lastupdated": datetime(2024, 2, 1),
            "created": datetime(2024, 1, 1),
            "data_element_name": "Cases",
            "category_option_combo_name": "default",
            "country_name": "Kenya",
            "region_name": "Nairobi",
            "district_name": "Westlands",
            "facility_name": "Clinic"
        },
        {
            "dataelement": "DE1",
            "period": "202401",
            "year_month": "202401",
            "orgunit": "OU1",
            "categoryoptioncombo": "COC1",
            "value": "5",
            "value_type": "NUMBER",
            "lastupdated": datetime(2024, 2, 5),
            "created": datetime(2024, 1, 1),
            "data_element_name": "Cases",
            "category_option_combo_name": "default",
            "country_name": "Kenya",
            "region_name": "Nairobi",
            "district_name": "Westlands",
            "facility_name": "Clinic"
        }
    ]

    results = run_dq_pipeline(
        spark.createDataFrame(rows)
    )

    clean_rows = results["clean_fact_df"].collect()

    assert len(clean_rows) == 1
    assert clean_rows[0]["value"] == "5"
    assert results["superseded_records_df"].count() == 1
