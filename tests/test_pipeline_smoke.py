import json
import os
import subprocess
import sys


def write_json(path, payload):
    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    path.write_text(
        json.dumps(payload),
        encoding="utf-8"
    )


def write_tiny_fixture(base_dir):
    data_dir = base_dir / "data"
    snapshot_dir = base_dir / "metadata_snapshots"
    contracts_dir = base_dir / "contracts"

    metadata = {
        "date": "2024-02-01",
        "version": "1",
        "dataElements": [
            {
                "id": "DE1",
                "name": "Cases",
                "shortName": "Cases",
                "code": "CASES",
                "valueType": "NUMBER",
                "domainType": "AGGREGATE",
                "aggregationType": "SUM",
                "zeroIsSignificant": True,
                "categoryCombo": {
                    "id": "CC1",
                    "name": "default"
                },
                "dataElementGroups": [],
                "created": "2024-01-01T00:00:00",
                "lastUpdated": "2024-01-01T00:00:00"
            }
        ],
        "categoryOptionCombos": [
            {
                "id": "COC1",
                "name": "default",
                "created": "2024-01-01T00:00:00",
                "lastUpdated": "2024-01-01T00:00:00"
            }
        ]
    }

    org_units = {
        "date": "2024-02-01",
        "version": "1",
        "organisationUnits": [
            {
                "id": "OU1",
                "name": "Kenya",
                "shortName": "Kenya",
                "code": "KE",
                "level": 1,
                "path": "/OU1",
                "groups": [],
                "created": "2024-01-01T00:00:00",
                "lastUpdated": "2024-01-01T00:00:00"
            }
        ]
    }

    programs = {
        "date": "2024-02-01",
        "version": "1",
        "programs": [
            {
                "id": "PR1",
                "name": "Malaria",
                "shortName": "Malaria",
                "healthArea": "Malaria",
                "country": "Kenya",
                "reportingFrequency": "MONTHLY",
                "dataElements": ["DE1"],
                "created": "2024-01-01T00:00:00",
                "lastUpdated": "2024-01-01T00:00:00"
            }
        ]
    }

    data_values = {
        "responseType": "dataValueSet",
        "version": "1",
        "exportDate": "2024-02-01T00:00:00",
        "dataValues": [
            {
                "dataElement": "DE1",
                "period": "202401",
                "orgUnit": "OU1",
                "categoryOptionCombo": "COC1",
                "attributeOptionCombo": "AOC1",
                "value": "10",
                "storedBy": "tester",
                "created": "2024-02-01T00:00:00",
                "lastUpdated": "2024-02-02T00:00:00",
                "followup": "false"
            }
        ]
    }

    write_json(data_dir / "metadata.json", metadata)
    write_json(snapshot_dir / "metadata_reference.json", metadata)
    write_json(data_dir / "org_units.json", org_units)
    write_json(data_dir / "programs.json", programs)
    write_json(data_dir / "data_values.json", data_values)

    contracts_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    contracts_dir.joinpath("fact_service_delivery.yml").write_text(
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
  - name: data_element_name
    type: string
    nullable: false
  - name: country_name
    type: string
    nullable: false
  - name: facility_name
    type: string
    nullable: false
  - name: numeric_value
    type: double
    nullable: true
    min_value: 0
    max_value: 1000000
  - name: boolean_value
    type: boolean
    nullable: true
  - name: value_type
    type: string
    nullable: false
  - name: is_late_submission
    type: boolean
    nullable: false
  - name: has_reported_value
    type: boolean
    nullable: false
  - name: load_timestamp
    type: timestamp
    nullable: false
        """,
        encoding="utf-8"
    )


def test_tiny_pipeline_smoke_run(tmp_path):
    write_tiny_fixture(tmp_path)

    env = {
        **os.environ,
        "PIPELINE_BASE_DIR": str(tmp_path),
        "PIPELINE_LOG_TO_FILE": "false",
        "SPARK_DRIVER_MEMORY": "1g",
        "SPARK_EXECUTOR_MEMORY": "1g",
        "SPARK_MASTER": "local[1]",
        "SPARK_SQL_SHUFFLE_PARTITIONS": "1",
        "SPARK_IO_ENCRYPTION_ENABLED": "true",
        "SPARK_LOCAL_TEMP_DIR": (
            tmp_path / "spark-temp"
        ).as_posix()
    }

    result = subprocess.run(
        [sys.executable, "pipeline.py"],
        cwd=os.getcwd(),
        env=env,
        text=True,
        capture_output=True,
        timeout=120
    )

    assert result.returncode == 0, result.stderr
    assert (
        tmp_path
        / "output"
        / "facts"
        / "fact_service_delivery"
        / "year_month=202401"
    ).exists()
    assert (
        tmp_path
        / "output"
        / "_checkpoints"
        / "pipeline_state.json"
    ).exists()
