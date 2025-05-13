"""
This module provides functionality for live data processing using the Mantid framework.

It includes utilities for retrieving Mantid-compatible processing scripts for specific instruments,
initiating live data processing, and handling clean shutdowns in response to system signals.
"""

import logging
import os
import signal
import sys
import time
from http import HTTPStatus

import requests
from mantid.api import AlgorithmManager
from mantid.simpleapi import StartLiveData

INSTRUMENT: str = os.environ.get("INSTRUMENT", "MERLIN").upper()

logger = logging.getLogger(__name__)


def get_script() -> str:
    """
    Fetch the latest live data processing script for a specific instrument.

    :param instrument: The name of the instrument for which the live data processing script is required.
    :returns: The content of the live data processing script as a string.
    :raises RuntimeError: If the script could not be retrieved or the response status code indicates an error.
    """
    logger.info("Attempting to get latest %s script...", INSTRUMENT)
    response = requests.get(
        f"https://raw.githubusercontent.com/fiaisis/autoreduction-scripts/main/{INSTRUMENT}/live_data.py",
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        raise RuntimeError("Failed to obtain script from remote, recieved status code: %s", response.status_code)
    return response.text


def start_live_data(script: str, is_event: bool) -> None:
    """
    Start live data processing for the specified script and instrument type.

    :param script: The Mantid processing script to execute for live data.
    :param is_event: A flag indicating if the live data is event-based.
                     If True, the event-based configuration is used; otherwise, histogram-based.
    :return: None
    """
    params = {
        "FromNow": False,
        "FromStartOfRun": True,
        "UpdateEvery": 5,
        "ProcessingScript": script,
        "PreserveEvents": True,
        "OutputWorkspace": "live_data",
    }

    if is_event:
        params["Instrument"] = f"{INSTRUMENT}_EVENT"
    else:
        params["Instrument"] = INSTRUMENT
        params["AccumulationMethod"] = "Append"

    StartLiveData(**params)


def cancel_live_data() -> None:
    """
    Cancel all active Mantid live data algorithms.

    This function calls the Mantid AlgorithmManager to cancel
    all running live data algorithms and waits until all instances
    have completely shut down before proceeding. It ensures a
    clean termination of live data processing.

    :return: None
    """

    AlgorithmManager.CancelAll()
    while AlgorithmManager.runningInstancesOf("StartLiveData") or AlgorithmManager.runningInstancesOf(
        "MonitorLiveData"
    ):
        time.sleep(0.1)


def _shutdown(signum, frame):
    """
    Handle system signals for clean termination of live data processing.

    This function is triggered when a system signal (e.g., SIGTERM or SIGINT)
    is received. It ensures a clean shutdown by stopping all active
    Mantid live data processing algorithms and exiting the program.

    :param signum: The signal number that triggered the function (e.g., SIGTERM, SIGINT).
    :param frame: The current stack frame (unused in this function but required by the signal handler).
    :return: None
    """

    logger.info("Signal %s received, shutting down live data…", signum)
    cancel_live_data()
    sys.exit(0)


def main() -> None:
    """
    Main function to start live data processing for the specified instrument.

    This function initializes signal handling for graceful shutdown, retrieves the
    live data processing script, starts the live data processing for the instrument,
    and periodically checks for updated processing scripts.

    :return: None
    """

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Starting live-data for %s", INSTRUMENT)
    script = get_script()
    is_event = False  # however you determine this
    start_live_data(script, is_event)

    while True:
        time.sleep(15)
        try:
            new_script = get_script()
        except RuntimeError:
            logger.warning("Could not get latest script, continuing with current script")
            new_script = script
        if script != new_script:
            logger.info("New script detected, restarting live data…")
            cancel_live_data()
            script = new_script
            start_live_data(script, is_event)


if __name__ == "__main__":
    main()
