from pathlib import Path
import yaml

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    min,
    max,
    count,
    sum as spark_sum,
    when
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

    required_columns = [
        column["name"]
        for column in contract["columns"]
        if not column["nullable"]
    ]

    if not required_columns:

        return

    null_count_expressions = [
        spark_sum(
            when(
                col(column_name).isNull(),
                1
            ).otherwise(0)
        ).alias(column_name)
        for column_name in required_columns
    ]

    null_counts = (
        dataframe
        .select(*null_count_expressions)
        .collect()[0]
        .asDict()
    )

    violations = {
        column_name: null_count
        for column_name, null_count in null_counts.items()
        if null_count and null_count > 0
    }

    if violations:

        raise ValueError(
            f"Non-nullable columns contain nulls: "
            f"{violations}"
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

    range_columns = [
        column
        for column in contract["columns"]
        if (
            "min_value" in column
            or "max_value" in column
        )
    ]

    if not range_columns:

        return

    aggregate_expressions = []

    for column in range_columns:

        column_name = column["name"]

        aggregate_expressions.extend(
            [
                min(
                    col(column_name)
                ).alias(f"{column_name}__min"),

                max(
                    col(column_name)
                ).alias(f"{column_name}__max")
            ]
        )

    stats = (
        dataframe
        .select(*aggregate_expressions)
        .collect()[0]
        .asDict()
    )

    for column in range_columns:

        column_name = column["name"]
        actual_min = stats[f"{column_name}__min"]
        actual_max = stats[f"{column_name}__max"]

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

    total_rows = (
        dataframe
        .select(
            count("*").alias("row_count")
        )
        .collect()[0]["row_count"]
    )

    duplicate_rows = (
        dataframe
        .groupBy(*grain_columns)
        .agg(
            count("*").alias("grain_count")
        )
        .filter(
            col("grain_count") > 1
        )
        .select(
            spark_sum(
                col("grain_count") - 1
            ).alias("duplicate_rows")
        )
        .collect()[0]["duplicate_rows"]
    )

    duplicate_rows = duplicate_rows or 0

    if duplicate_rows > 0:

        raise ValueError(
            f"Fact table grain violation. "
            f"Found "
            f"{duplicate_rows} duplicate rows "
            f"out of {total_rows} rows."
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

    
