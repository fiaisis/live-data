import logging
import os

import requests

logger = logging.getLogger(__name__)
FIA_API_URL = os.environ.get("FIA_API_URL", "https://dev.reduce.isis.cclrc.ac.uk/api")


def get_script(instrument: str) -> str | None:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    """

    logger.info("Getting latest script")
    response = requests.get(f"{FIA_API_URL}/live-data/{instrument.lower()}/script")
    print(response)
    return response.text if response.status_code == 200 else None
    # return """from mantid.simpleapi import *; SaveNexusProcessed(Filename="/output/output-lives.nxs", InputWorkspace="lives")"""  # noqa: E501


print(get_script("MERLIN"))
