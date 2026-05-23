from pyspark.sql import DataFrame

from models.config import QUARANTINE_DIR
from models.utils.io_utils import write_parquet
from models.utils.logger import get_logger


logger = get_logger(__name__)


def write_quarantine_outputs(
    quarantine_outputs: dict[str, DataFrame]
) -> None:
    """
    Persist quarantined records for audit and replay.
    """

    logger.info(
        "Writing quarantine outputs"
    )

    for output_name, dataframe in quarantine_outputs.items():

        write_parquet(
            dataframe,
            str(
                QUARANTINE_DIR
                / output_name
            )
        )

    logger.info(
        "Quarantine outputs written"
    )
