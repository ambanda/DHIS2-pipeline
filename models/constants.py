# =========================================================
# DHIS2 Value Types
# =========================================================

INTEGER_TYPES = {
    "INTEGER",
    "INTEGER_POSITIVE",
    "INTEGER_ZERO_OR_POSITIVE"
}

FLOAT_TYPES = {
    "NUMBER",
    "PERCENTAGE"
}

BOOLEAN_TYPES = {
    "BOOLEAN",
    "TRUE_ONLY"
}


# =========================================================
# Org Unit Levels
# =========================================================

ORG_UNIT_LEVELS = {
    1: "Country",
    2: "Region",
    3: "District",
    4: "Facility"
}


# =========================================================
# Health Areas
# =========================================================

HEALTH_AREAS = [
    "Malaria",
    "HIV",
    "Tuberculosis",
    "Reproductive Health",
    "Child Health"
]


# =========================================================
# Reporting Constants
# =========================================================

REPORTING_FREQUENCY = "MONTHLY"

LATE_REPORTING_DAYS = 60


# =========================================================
# Output Names
# =========================================================

FACT_TABLE_NAME = "fact_service_delivery"

DIM_DATA_ELEMENT = "dim_data_element"
DIM_ORG_UNIT = "dim_org_unit"
DIM_PROGRAM = "dim_program"
DIM_PERIOD = "dim_period"


# =========================================================
# Partition Columns
# =========================================================

FACT_PARTITIONS = [
    
    "year_month"
]


# =========================================================
# Composite Business Key
# =========================================================
DEDUP_KEYS = [
    "dataelement",
    "period",
    "orgunit",
    "categoryoptioncombo"
]
