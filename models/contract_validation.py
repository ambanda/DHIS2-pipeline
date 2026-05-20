from pathlib import Path
import yaml

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    min,
    max
)

from models.utils.logger import (
    get_logger
)


logger = get_logger(__name__)


# =========================================================
# Load YAML Contract
# =========================================================

def load_contract(
    contract_path: str
) -> dict:
    """
    Load YAML contract definition.
    """

    logger.info(
        f"Loading contract: {contract_path}"
    )

    with open(
        contract_path,
        "r",
        encoding="utf-8"
    ) as file:

        contract = yaml.safe_load(file)

    return contract


# =========================================================
# Validate Required Columns
# =========================================================

def validate_required_columns(
    dataframe: DataFrame,
    contract: dict
) -> None:
    """
    Validate all required columns exist.
    """

    logger.info(
        "Validating required columns"
    )

    dataframe_columns = set(
        dataframe.columns
    )

    contract_columns = set(
        column["name"]
        for column in contract["columns"]
    )

    missing_columns = (
        contract_columns
        - dataframe_columns
    )

    if missing_columns:

        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}"
        )


# =========================================================
# Validate Column Types
# =========================================================

def validate_column_types(
    dataframe: DataFrame,
    contract: dict
) -> None:
    """
    Validate dataframe column types.
    """

    logger.info(
        "Validating column data types"
    )

    dataframe_schema = {
        field.name: field.dataType.simpleString()
        for field in dataframe.schema.fields
    }

    type_mapping = {
        "string": "string",
        "double": "double",
        "boolean": "boolean",
        "timestamp": "timestamp"
    }

    for column in contract["columns"]:

        expected_type = (
            type_mapping[
                column["type"]
            ]
        )

        actual_type = dataframe_schema.get(
            column["name"]
        )

        if actual_type != expected_type:

            raise TypeError(
                f"Column "
                f"{column['name']} "
                f"expected type "
                f"{expected_type} "
                f"but found "
                f"{actual_type}"
            )


# =========================================================
# Validate Nullability
# =========================================================

def validate_nullability(
    dataframe: DataFrame,
    contract: dict
) -> None:
    """
    Validate nullable constraints.
    """

    logger.info(
        "Validating nullability"
    )

    for column in contract["columns"]:

        if not column["nullable"]:

            null_count = (
                dataframe
                .filter(
                    col(column["name"]).isNull()
                )
                .count()
            )

            if null_count > 0:

                raise ValueError(
                    f"Column "
                    f"{column['name']} "
                    f"contains "
                    f"{null_count} null values"
                )


# =========================================================
# Validate Numeric Ranges
# =========================================================

def validate_numeric_ranges(
    dataframe: DataFrame,
    contract: dict
) -> None:
    """
    Validate numeric ranges.
    """

    logger.info(
        "Validating numeric ranges"
    )

    for column in contract["columns"]:

        if (
            "min_value" in column
            or "max_value" in column
        ):

            column_name = column["name"]

            stats = (
                dataframe
                .select(
                    min(
                        col(column_name)
                    ).alias("min_value"),

                    max(
                        col(column_name)
                    ).alias("max_value")
                )
                .collect()[0]
            )

            actual_min = stats["min_value"]
            actual_max = stats["max_value"]

            if (
                "min_value" in column
                and actual_min is not None
                and actual_min < column["min_value"]
            ):

                raise ValueError(
                    f"{column_name} "
                    f"contains values below "
                    f"minimum allowed value"
                )

            if (
                "max_value" in column
                and actual_max is not None
                and actual_max > column["max_value"]
            ):

                raise ValueError(
                    f"{column_name} "
                    f"contains values above "
                    f"maximum allowed value"
                )


# =========================================================
# Validate Primary Grain
# =========================================================

def validate_primary_grain(
    dataframe: DataFrame,
    contract: dict
) -> None:
    """
    Validate fact grain uniqueness.
    """

    logger.info(
        "Validating primary grain"
    )

    grain_columns = contract[
        "primary_grain"
    ]

    total_rows = dataframe.count()

    unique_rows = (
        dataframe
        .dropDuplicates(grain_columns)
        .count()
    )

    if total_rows != unique_rows:

        duplicate_count = (
            total_rows - unique_rows
        )

        raise ValueError(
            f"Fact table grain violation. "
            f"Found "
            f"{duplicate_count} duplicate rows."
        )


# =========================================================
# Master Contract Validation
# =========================================================

def validate_dataframe_contract(
    dataframe: DataFrame,
    contract_path: str
) -> None:
    """
    Run all contract validations.
    """

    logger.info("=" * 60)
    logger.info(
        "STARTING CONTRACT VALIDATION"
    )
    logger.info("=" * 60)

    contract = load_contract(
        contract_path
    )

    validate_required_columns(
        dataframe,
        contract
    )

    validate_column_types(
        dataframe,
        contract
    )

    validate_nullability(
        dataframe,
        contract
    )

    validate_numeric_ranges(
        dataframe,
        contract
    )

    validate_primary_grain(
        dataframe,
        contract
    )

    logger.info("=" * 60)
    logger.info(
        "CONTRACT VALIDATION PASSED"
    )
    logger.info("=" * 60)

    