import os

import pytest


os.environ.setdefault("PIPELINE_LOG_TO_FILE", "false")
os.environ.setdefault(
    "PIPELINE_BASE_DIR",
    "D:/PSI-Project/dhis2-health-pipeline/.test_runtime"
)
os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")
os.environ.setdefault("SPARK_EXECUTOR_MEMORY", "1g")
os.environ.setdefault("SPARK_MASTER", "local[1]")
os.environ.setdefault("SPARK_SQL_SHUFFLE_PARTITIONS", "1")
os.environ.setdefault("SPARK_IO_ENCRYPTION_ENABLED", "true")
os.environ.setdefault(
    "SPARK_LOCAL_TEMP_DIR",
    "D:/PSI-Project/dhis2-health-pipeline/spark_temp"
)

from models.utils.spark_utils import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session()

    yield session

    session.stop()
