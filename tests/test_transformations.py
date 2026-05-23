from datetime import date

from models.ingest import ingest_data_values
from models.utils.date_utils import add_period_dates


def test_add_period_dates_keeps_period_and_adds_year_month(spark):
    dataframe = spark.createDataFrame(
        [
            {
                "period": "202401",
                "value": "12"
            }
        ]
    )

    result = add_period_dates(dataframe).collect()[0]

    assert result["period"] == "202401"
    assert result["year_month"] == "202401"
    assert result["period_start_date"] == date(2024, 1, 1)
    assert result["period_end_date"] == date(2024, 1, 31)
    assert result["year"] == 2024
    assert result["month"] == 1
    assert result["quarter"] == 1


def test_ingest_data_values_adds_incremental_partition_column(
    spark,
    tmp_path
):
    data_file = tmp_path / "data_values.json"
    data_file.write_text(
        """
        {
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
        """,
        encoding="utf-8"
    )

    dataframe = ingest_data_values(
        spark,
        str(data_file)
    )

    row = dataframe.collect()[0]

    assert "year_month" in dataframe.columns
    assert row["year_month"] == "202401"
