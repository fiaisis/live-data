import logging
from http import HTTPStatus

import requests

logger = logging.getLogger(__name__)

def get_script(instrument: str) -> str:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    """
    logger.info("Getting latest sha")
    response = requests.get("https://api.github.com/repos/fiaisis/autoreduction-scripts/branches/main")
    script_sha = response.json()["commit"]["sha"]
    logger.info("Attempting to get latest %s script...", instrument)
    response = requests.get(
        f"https://raw.githubusercontent.com/fiaisis/autoreduction-scripts/{script_sha}/{instrument}/live_data.py?v=1",
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        raise RuntimeError("Failed to obtain script from remote, recieved status code: %s", response.status_code)
    logger.info("Successfully obtained %s script", instrument)
    return response.text
