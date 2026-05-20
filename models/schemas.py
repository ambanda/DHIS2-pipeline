from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType
)


# =========================================================
# Common Nested Schemas
# =========================================================

PARENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

CATEGORY_COMBO_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

GROUP_SCHEMA = ArrayType(
    StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
)


# =========================================================
# metadata.json Schema
# =========================================================

DATA_ELEMENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("code", StringType(), True),
    StructField("valueType", StringType(), True),
    StructField("domainType", StringType(), True),
    StructField("aggregationType", StringType(), True),
    StructField("zeroIsSignificant", BooleanType(), True),
    StructField("categoryCombo", CATEGORY_COMBO_SCHEMA, True),
    StructField("dataElementGroups", GROUP_SCHEMA, True),
    StructField("created", StringType(), True),
    StructField("lastUpdated", StringType(), True)
])

CATEGORY_OPTION_COMBO_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("created", StringType(), True),
    StructField("lastUpdated", StringType(), True)
])

METADATA_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField(
        "dataElements",
        ArrayType(DATA_ELEMENT_SCHEMA),
        True
    ),
    StructField(
        "categoryOptionCombos",
        ArrayType(CATEGORY_OPTION_COMBO_SCHEMA),
        True
    )
])


# =========================================================
# org_units.json Schema
# =========================================================

ORG_UNIT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("code", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("path", StringType(), True),
    StructField("parent", PARENT_SCHEMA, True),
    StructField("groups", GROUP_SCHEMA, True),
    StructField("created", StringType(), True),
    StructField("lastUpdated", StringType(), True)
])

ORG_UNITS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField(
        "organisationUnits",
        ArrayType(ORG_UNIT_SCHEMA),
        True
    )
])


# =========================================================
# programs.json Schema
# =========================================================

PROGRAM_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("healthArea", StringType(), True),
    StructField("country", StringType(), True),
    StructField("reportingFrequency", StringType(), True),
    StructField(
        "dataElements",
        ArrayType(StringType()),
        True
    ),
    StructField("created", StringType(), True),
    StructField("lastUpdated", StringType(), True)
])

PROGRAMS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField(
        "programs",
        ArrayType(PROGRAM_SCHEMA),
        True
    )
])


# =========================================================
# data_values.json Schema
# =========================================================

DATA_VALUE_SCHEMA = StructType([
    StructField("dataElement", StringType(), True),
    StructField("period", StringType(), True),
    StructField("orgUnit", StringType(), True),
    StructField("categoryOptionCombo", StringType(), True),
    StructField("attributeOptionCombo", StringType(), True),
    StructField("value", StringType(), True),
    StructField("storedBy", StringType(), True),
    StructField("created", StringType(), True),
    StructField("lastUpdated", StringType(), True),
    StructField("followup", StringType(), True)
])

DATA_VALUES_SCHEMA = StructType([
    StructField("responseType", StringType(), True),
    StructField("version", StringType(), True),
    StructField("exportDate", StringType(), True),
    StructField(
        "dataValues",
        ArrayType(DATA_VALUE_SCHEMA),
        True
    )
])
