import importlib
import inspect
import logging
import os
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path

import requests

logger = logging.getLogger(__name__)
FIA_API_URL = os.environ.get("FIA_API_URL", "https://dev.reduce.isis.cclrc.ac.uk/api")
FIA_API_API_KEY = os.environ.get("FIA_API_API_KEY")
REDUCTION_SCRIPT_PATH = Path("reduction_script.py")


def report_traceback(instrument: str, traceback_str: str) -> None:
    """
    Report a traceback to the FIA API for a specific instrument.

    :param instrument: The instrument name
    :param traceback_str: The traceback content
    :return: None
    """
    if not FIA_API_API_KEY:
        logger.warning("FIA_API_API_KEY not set, cannot report traceback")
        return

    logger.info("Reporting traceback for %s", instrument)
    try:
        response = requests.post(
            f"{FIA_API_URL}/live-data/{instrument.lower()}/traceback",
            json={"value": traceback_str},
            headers={"Authorization": f"Bearer {FIA_API_API_KEY}"},
            timeout=10,
        )
        if response.status_code != 200:
            logger.warning("Failed to report traceback: %s", response.text)
    except Exception as exc:
        logger.warning("Error reporting traceback", exc_info=exc)


def get_script(instrument: str) -> str | None:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    """

    response = requests.get(f"{FIA_API_URL}/live-data/{instrument.lower()}/script", timeout=5)
    return response.json() if response.status_code == HTTPStatus.OK else None


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
        import reduction_script
    except ImportError:
        initialize_script = get_script(os.environ["INSTRUMENT"].lower())
        write_reduction_script(initialize_script)
        import reduction_script
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
