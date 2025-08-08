import pydevd_pycharm
pydevd_pycharm.settrace('94.174.154.158', port=666)
"""
This module provides functionality for live data processing using the Mantid framework.

It includes utilities for retrieving Mantid-compatible processing scripts for specific instruments,
initiating live data processing, and handling clean shutdowns in response to system signals.
"""

import datetime
import logging
import os
import signal
import sys
import time
from http import HTTPStatus
from typing import Any

import requests
from kafka import KafkaConsumer, TopicPartition
from mantid import config
from mantid.api import mtd
from mantid.kernel import DateAndTime
from mantid.simpleapi import LoadEmptyInstrument

from simulator.compiled_schemas.EventMessage import EventMessage
from simulator.compiled_schemas.RunStart import RunStart

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")
KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))
INSTRUMENT: str = os.environ.get("INSTRUMENT", "MERLIN").upper()
SCRIPT_REFRESH_TIME = int(os.environ.get("INGEST_WAIT", "30"))
LIVE_WS_NAME = os.environ.get("LIVE_WS", "lives")
GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN", "shh")


# Local Dev only
# config["defaultsave.directory"] = "~/Downloads"


class TopicIncompleteError(RuntimeError):
    """Raised when the Kafka topic is not yet complete"""


def get_script() -> str:
    """
    Fetch the latest live data processing script for a specific instrument.

    :returns: The content of the live data processing script as a string.
    :raises RuntimeError: If the script could not be retrieved or the response status code indicates an error.
    """
    logger.info("Getting latest sha")
    response = requests.get("https://api.github.com/repos/fiaisis/autoreduction-scripts/branches/main",
                              headers={"Authorization": f"Bearer {GITHUB_API_TOKEN}"})
    script_sha = response.json()["commit" ]["sha"]
    logger.info("Attempting to get latest %s script...", INSTRUMENT)
    response = requests.get(
        f"https://raw.githubusercontent.com/fiaisis/autoreduction-scripts/{script_sha}/{INSTRUMENT}/live_data.py?v=1",
        headers={"Authorization": f"Bearer {GITHUB_API_TOKEN}"},
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        raise RuntimeError("Failed to obtain script from remote, recieved status code: %s", response.status_code)
    logger.info("Successfully obtained %s script", INSTRUMENT)
    return response.text


def find_latest_run_start(runinfo_consumer: KafkaConsumer) -> RunStart | None:
    """
    Find the latest RunStart message in the runinfo topic.

    This function seeks to the end of the runinfo topic, retrieves the last few messages,
    and searches for the most recent RunStart message.

    :param runinfo_consumer: The Kafka consumer for the runinfo topic.
    :return: The latest RunStart message if found, None otherwise.
    :raises TopicIncompleteError: If the topic does not have any messages.
    """
    logger.info("Attempting to find latest run start")
    latest_start = None

    logger.info("Seeking to end of runinfo topic")
    # Set the offset to 3 messages from the end.
    tp = TopicPartition(f"{INSTRUMENT}_runInfo", 0)
    logger.info("Topic partition: %s", tp)
    end_offset = runinfo_consumer.end_offsets([tp])
    runinfo_consumer.seek(tp, end_offset[tp] - 3)

    logger.info("Reading messages from runinfo topic")
    # Grab the last 3 messages, reverse their order then find the latest runstart by iterating through.
    messages = []
    for index, message in enumerate(runinfo_consumer):
        messages.append(message.value)
        if index == 2:
            break
    if len(messages) < 1:
        raise TopicIncompleteError("Topic does not have any messages from which to read. %s", f"{INSTRUMENT}_runInfo")
    logger.info("Found %s messages in runinfo topic", len(messages))
    for message in reversed(messages):
        if RunStart.RunStartBufferHasIdentifier(message, 0):
            latest_start = RunStart.GetRootAs(message, 0)
            break
    return latest_start


def initialize_instrument_workspace(run_start: RunStart) -> None:
    """
    Initialize the instrument workspace for a new run.

    This function extracts the run name from the provided RunStart message, logs the
    start of the new run, and loads an empty instrument workspace for the specified
    instrument. The workspace is configured to handle event data.

    :param run_start: The RunStart message containing information about the new run.
    :return: None
    """
    run_name = run_start.RunName()
    run_name = run_name.decode("utf-8")
    logger.info(f"New run started: {run_name}")
    LoadEmptyInstrument(InstrumentName=INSTRUMENT, OutputWorkspace=LIVE_WS_NAME, MakeEventWorkspace=True)
    # LoadEmptyInstrument(
    #     OutputWorkspace="lives",
    #     Filename="/Users/sham/miniconda3/envs/mantid/instrument/MERLIN_Definition.xml",
    #     MakeEventWorkspace=True,
    # )


def seek_event_consumer_to_runstart(run_start: RunStart, events_consumer: KafkaConsumer) -> None:
    """
    Adjust the given event consumer's position to the beginning of the given run
    :param run_start: The run start message to seek to.
    :param events_consumer: The event consumer to seek to the run start message in.
    :return: None
    """
    logger.info("Seeking event consumer to run start")
    run_start_ms = run_start.StartTime()

    # Find the offset for a given time and seek to it
    topic_partition = TopicPartition(f"{INSTRUMENT}_events", 0)
    offset_info = events_consumer.offsets_for_times({topic_partition: run_start_ms})[topic_partition]
    events_consumer.seek(topic_partition, offset_info.offset)


def process_events(events: EventMessage):
    """
    Process event data by adding events to the live workspace.

    This function extracts time-of-flight, detector IDs, and pulse time from the event message,
    and adds each event to the appropriate spectrum in the live workspace.

    :param events: The EventMessage containing neutron event data.
    :return: None
    """
    times_of_flight = events.TimeOfFlightAsNumpy()
    detector_ids = events.DetectorIdAsNumpy()
    pulse_time = DateAndTime(events.PulseTime())
    ws = mtd[LIVE_WS_NAME]
    detector_ids = ws.getIndicesFromDetectorIDs(detector_ids.tolist())
    if len(times_of_flight) > 0:
        for detector_id, tof in zip(detector_ids, times_of_flight, strict=False):
            spectra = ws.getSpectrum(int(detector_id))
            spectra.addEventQuickly(int(tof), pulse_time)


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
    sys.exit(0)


def setup_consumers(kafka_config: dict[str, Any]) -> tuple[KafkaConsumer, KafkaConsumer]:
    """
    Given a kafka config, setup the consumers for events and runinfo
    :param kafka_config: The kafka config to use for the consumers.
    :return: A tuple of KafkaConsumer objects for events and runinfo.
    """
    events_consumer = KafkaConsumer(f"{INSTRUMENT}_events", **kafka_config)
    runinfo_consumer = KafkaConsumer(f"{INSTRUMENT}_runInfo", **kafka_config)
    return events_consumer, runinfo_consumer


def initialize_run(events_consumer, runinfo_consumer, run_start: RunStart | None = None) -> RunStart:
    """
    Initialize a run by finding the latest run start message, processing it, and seeking the event consumer.

    This function either uses the provided run_start message or finds the latest one,
    processes the run start message, and positions the event consumer at the beginning of the run.

    :param events_consumer: The Kafka consumer for the events topic.
    :param runinfo_consumer: The Kafka consumer for the runinfo topic.
    :param run_start: Optional RunStart message. If None, the latest one will be found.
    :return: The RunStart message that was processed.
    :raises TopicIncompleteError: If no RunStart message is found in the runinfo topic.
    """
    logger.info("Initializing run: %s", str(run_start))
    if run_start is None:
        logger.info("Run Start is None")
        run_start = find_latest_run_start(runinfo_consumer)
        if run_start is None:
            raise TopicIncompleteError("No RunStart found in runInfo")
    initialize_instrument_workspace(run_start)
    seek_event_consumer_to_runstart(run_start, events_consumer)
    return run_start


def start_live_reduction(
    script: str,
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    run_start: RunStart | None = None,
) -> None:
    """
    Start the live data reduction loop for processing neutron event data.

    This function initializes a run, processes event messages from the Kafka consumer,
    periodically checks for updated processing scripts, and monitors for new runs.
    If a new script or run is detected, it restarts the reduction process accordingly.

    :param script: The Mantid processing script to execute for live data reduction.
    :param events_consumer: The Kafka consumer for the events topic.
    :param runinfo_consumer: The Kafka consumer for the runinfo topic.
    :param run_start: Optional RunStart message. If None, the latest one will be found.
    :return: None
    """
    current_run_start = initialize_run(events_consumer, runinfo_consumer, run_start)
    logger.info("Starting live data reduction loop")
    loop_start_time = datetime.datetime.now()
    for message in events_consumer:
        logger.info("Received event message" + str(message.value))
        events = EventMessage.GetRootAs(message.value, 0)
        process_events(events)
        if (datetime.datetime.now() - loop_start_time).total_seconds() > SCRIPT_REFRESH_TIME:
            try:
                new_script = get_script()
                if script != new_script:
                    logger.info("New Script detected, restarting with new script")
                    script = new_script
                    start_live_reduction(script, events_consumer, runinfo_consumer, current_run_start)
            except RuntimeError as exc:
                logger.warning("Could not get latest script, continuing with current script", exc_info=exc)
        try:
            exec(script)
        except Exception as exc:
            logger.warning("Error occurred in reduction, waiting 15 seconds and restarting loop", exc_info=exc)
            time.sleep(15)
            continue

        # Check if we should move onto next workspace
        latest_runstart = find_latest_run_start(runinfo_consumer)
        if latest_runstart != current_run_start:
            start_live_reduction(script, events_consumer, runinfo_consumer, latest_runstart)


def main() -> None:
    """
    Main function to start live data processing for the specified instrument.

    This function initializes signal handling for graceful shutdown, retrieves the
    live data processing script, starts the live data processing for the instrument,
    and periodically checks for updated processing scripts.

    :return: None
    """

    ##################
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Setup consumers
    kafka_config = {
        "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "request_timeout_ms": 60000,
        "session_timeout_ms": 60000,
        "security_protocol": "PLAINTEXT",
        "api_version_auto_timeout_ms": 60000,
    }
    events_consumer, runinfo_consumer = setup_consumers(kafka_config)

    script = get_script()
    start_live_reduction(script, events_consumer, runinfo_consumer)


if __name__ == "__main__":
    main()
