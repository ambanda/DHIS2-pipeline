import logging
from datetime import datetime
from pathlib import Path
import sys

from models.config import (
    LOG_FILE,
    LOG_FORMAT,
    LOG_LEVEL,
    LOG_TO_FILE
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

    handlers = []

    if LOG_TO_FILE:

        try:

            file_handler = logging.FileHandler(
                LOG_FILE,
                encoding="utf-8"
            )

            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        except OSError as error:

            fallback_file = (
                Path(LOG_FILE).parent
                / (
                    "pipeline_"
                    f"{datetime.utcnow():%Y%m%dT%H%M%SZ}.log"
                )
            )

            try:

                fallback_handler = logging.FileHandler(
                    fallback_file,
                    encoding="utf-8"
                )

                fallback_handler.setFormatter(formatter)
                handlers.append(fallback_handler)

            except OSError:

                sys.stderr.write(
                    "File logging disabled. "
                    f"Could not open {LOG_FILE}: {error}\n"
                )

    # =====================================================
    # Console Handler
    # =====================================================

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    for handler in handlers:

        logger.addHandler(handler)

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
    
