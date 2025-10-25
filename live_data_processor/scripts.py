import importlib
import inspect
import logging
import os
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)
FIA_API_URL = os.environ.get("FIA_API_URL", "https://dev.reduce.isis.cclrc.ac.uk/api")
REDUCTION_SCRIPT_PATH = Path("reduction_script.py")


def get_script(instrument: str) -> str | None:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    """

    # logger.info("Getting latest script")
    with open("foo.py") as fle:
        return fle.read()

    # response = requests.get(f"{FIA_API_URL}/live-data/{instrument.lower()}/script")
    # return response.json() if response.status_code == 200 else None
    return """from mantid.simpleapi import *
# def execute():
#     SaveNexusProcessed(Filename="~/work/live-data/output-lives.nxs", InputWorkspace="lives")"""


def write_reduction_script(script: str) -> None:
    """
    Given a script, write it to the file

    :param script: The script to write
    :return: None
    """
    with REDUCTION_SCRIPT_PATH.open("w") as fle:
        fle.write(script)


def get_reduction_function(instrument: str) -> Callable[[], None]:
    """
    Given an instrument, return the reduction function for that instrument

    :param instrument: The instrument name
    :return: The reduction function
    """

    try:
        import reduction_script  # noqa: PLC0415
    except ImportError:
        initialize_script = get_script(os.environ["INSTRUMENT"].lower())
        write_reduction_script(initialize_script)
        import reduction_script  # noqa: PLC0415
    latest_script = get_script(instrument)
    write_reduction_script(latest_script)
    importlib.reload(reduction_script)
    return reduction_script.execute


def refresh_reduction_function(reduction_function, instrument: str) -> Callable[[], None]:
    """
    Given the existing reduction function, check if there is a new script and return the new function if there is.

    :param reduction_function: The existing reduction function
    :param instrument: The instrument name
    :return: The new reduction function if there is one, otherwise the existing one
    """
    logger.info("Checking for updated script...")
    try:
        new_reduction_function = get_reduction_function(instrument)
        if inspect.getsource(new_reduction_function) != inspect.getsource(reduction_function):
            logger.info("New Script detected, Continuing with new script")
            reduction_function = new_reduction_function
        else:
            logger.info("No updates continuing with current script")
    except RuntimeError as exc:
        logger.warning("Could not get latest script, continuing with current script", exc_info=exc)
    return reduction_function
