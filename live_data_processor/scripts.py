import importlib
import logging
import os
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path

import requests

INSTRUMENT = os.environ.get("INSTRUMENT", "MERLIN").upper()

FIA_API_URL = os.environ.get("FIA_API_URL", "https://dev.reduce.isis.cclrc.ac.uk/api")
REDUCTION_SCRIPT_PATH = Path("reduction_script.py")


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


def get_initial_reduction_function(instrument: str) -> Callable[[], None]:
    """
    Given an instrument, fetch the initial reduction script and return the execution function.
    Called once at startup.

    :param instrument: The instrument name
    :return: The reduction function
    """
    latest_script = get_script(instrument)

    # Write the script if we successfully fetched it
    if latest_script:
        write_reduction_script(latest_script)

    import reduction_script

    return reduction_script.execute


def refresh_reduction_function(reduction_function: Callable[[], None], instrument: str) -> Callable[[], None]:
    """
    Given the existing reduction function, check if there is a new script and return the new function if there is.

    :param reduction_function: The existing reduction function
    :param instrument: The instrument name
    :return: The new reduction function if there is one, otherwise the existing one
    """
    # Grab the loggers dynamically to avoid import-order and environment variable bugs
    external_logger = logging.getLogger(f"external_{instrument}")
    internal_logger = logging.getLogger(f"internal_{instrument}")

    external_logger.info("Checking for updated script...")
    try:
        latest_script = get_script(instrument)
        if not latest_script:
            external_logger.warning("Failed to fetch script from API. Keeping current script.")
            return reduction_function

        # Read the current script from disk as a string
        current_script = ""
        if REDUCTION_SCRIPT_PATH.exists():
            with REDUCTION_SCRIPT_PATH.open("r") as f:
                current_script = f.read()

        # Compare raw strings instead of inspect.getsource() to avoid  caching bugs
        if latest_script != current_script:
            external_logger.info("New Script detected! Writing to disk and reloading...")

            write_reduction_script(latest_script)

            import reduction_script

            importlib.reload(reduction_script)

            return reduction_script.execute

        external_logger.info("No updates continuing with current script")
        return reduction_function

    except Exception as exc:
        external_logger.warning("Could not get the latest script")
        internal_logger.warning("Reason:", exc_info=exc)

    return reduction_function
