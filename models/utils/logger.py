import logging
from pathlib import Path

from models.config import (
    LOG_FILE,
    LOG_FORMAT,
    LOG_LEVEL
)


def get_logger(name: str) -> logging.Logger:
    """
    Create and return configured logger.

    Prevents duplicate handlers when imported multiple times.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)

    # Ensure log directory exists
    Path(LOG_FILE).parent.mkdir(
        parents=True,
        exist_ok=True
    )

    formatter = logging.Formatter(LOG_FORMAT)

    # =====================================================
    # File Handler
    # =====================================================

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)

    # =====================================================
    # Console Handler
    # =====================================================

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger


def log_row_count(
    logger: logging.Logger,
    dataframe,
    dataframe_name: str
) -> None:
    """
    Safely log dataframe row count.
    """

    try:
        row_count = dataframe.count()

        logger.info(
            f"{dataframe_name} row count: {row_count:,}"
        )

    except Exception as error:
        logger.exception(
            f"Failed counting rows for {dataframe_name}: {error}"
        )
    