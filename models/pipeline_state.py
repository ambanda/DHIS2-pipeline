from datetime import datetime

from models.config import PIPELINE_STATE_FILE
from models.utils.io_utils import write_json
from models.utils.logger import get_logger


logger = get_logger(__name__)


def write_success_checkpoint(
    payload: dict
) -> None:
    """
    Persist the latest successful run state.
    """

    checkpoint = {
        "status": "success",
        "completed_at_utc": (
            datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z"
        ),
        **payload
    }

    write_json(
        checkpoint,
        str(PIPELINE_STATE_FILE)
    )

    logger.info(
        f"Pipeline checkpoint written: {PIPELINE_STATE_FILE}"
    )
