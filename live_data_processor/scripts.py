import logging

logger = logging.getLogger(__name__)


def get_script(instrument: str) -> str:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    """
    # logger.info("Getting latest sha")  # noqa: ERA001
    # response = requests.get("https://api.github.com/repos/fiaisis/autoreduction-scripts/branches/main")  # noqa: E501, ERA001
    # script_sha = response.json()["commit"]["sha"]  # noqa: ERA001
    # logger.info("Attempting to get latest %s script...", instrument)  # noqa: ERA001
    # response = requests.get(  # noqa: ERA001, RUF100
    #     f"https://raw.githubusercontent.com/fiaisis/autoreduction-scripts/{script_sha}/{instrument}/live_data.py?v=1",  # noqa: E501, ERA001
    #     timeout=30,  # noqa: ERA001
    # )  # noqa: ERA001, RUF100
    # if response.status_code != HTTPStatus.OK:
    #     raise RuntimeError("Failed to obtain script from remote, recieved status code: %s", response.status_code)  # noqa: E501, ERA001
    # logger.info("Successfully obtained %s script", instrument)  # noqa: ERA001
    # return response.text  # noqa: ERA001
    return """from mantid.simpleapi import *; SaveNexusProcessed(Filename="/output/output-lives.nxs", InputWorkspace="lives")"""  # noqa: E501
