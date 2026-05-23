from datetime import datetime

import pytest

from models.contract_validation import validate_dataframe_contract


def write_contract(tmp_path):
    contract_path = tmp_path / "fact_service_delivery.yml"
    contract_path.write_text(
        """
table_name: fact_service_delivery
primary_grain:
  - data_element_id
  - org_unit_id
  - category_option_combo_id
  - year_month
columns:
  - name: data_element_id
    type: string
    nullable: false
  - name: org_unit_id
    type: string
    nullable: false
  - name: category_option_combo_id
    type: string
    nullable: false
  - name: year_month
    type: string
    nullable: false
  - name: numeric_value
    type: double
    nullable: true
    min_value: 0
    max_value: 100
  - name: load_timestamp
    type: timestamp
    nullable: false
        """,
        encoding="utf-8"
    )

    return contract_path


def base_row():
    return {
        "data_element_id": "DE1",
        "org_unit_id": "OU1",
        "category_option_combo_id": "COC1",
        "year_month": "202401",
        "numeric_value": 10.0,
        "load_timestamp": datetime(2024, 2, 1)
    }


def test_contract_validation_accepts_valid_dataframe(spark, tmp_path):
    contract_path = write_contract(tmp_path)
    dataframe = spark.createDataFrame([base_row()])

    validate_dataframe_contract(
        dataframe,
        str(contract_path)
    )


def test_contract_validation_detects_grain_violation(spark, tmp_path):
    contract_path = write_contract(tmp_path)
    dataframe = spark.createDataFrame(
        [
            base_row(),
            base_row()
        ]
    )

    with pytest.raises(ValueError, match="grain violation"):
        validate_dataframe_contract(
            dataframe,
            str(contract_path)
        )
