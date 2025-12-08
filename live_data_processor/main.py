"""
Main module for the live data processor.

This module wires together Kafka consumers, Mantid workspaces, and the
reduction script refresh/execute loop to process neutron event data in
near real-time for a selected instrument.
"""
import contextlib
import datetime
import logging
import os
import signal
import sys
from collections.abc import Callable
from types import FrameType
from typing import Any

from kafka import KafkaConsumer
from mantid import ConfigService
from mantid.api import mtd
from mantid.kernel import DateAndTime
from mantid.simpleapi import AddTimeSeriesLog, RemoveWorkspaceHistory
from streaming_data_types import deserialise_f144
from streaming_data_types.fbschemas.eventdata_ev42.EventMessage import EventMessage
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart
from streaming_data_types.utils import get_schema

from live_data_processor.exceptions import TopicIncompleteError
from live_data_processor.kafka_io import (
    datetime_from_record_timestamp,
    find_latest_run_start,
    seek_event_consumer_to_runstart,
    setup_consumers,
)
from live_data_processor.scripts import (
    get_reduction_function,
    refresh_reduction_function,
)
from live_data_processor.workspaces import initialize_instrument_workspace

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
ConfigService.setLogLevel(3)
logger = logging.getLogger(__name__)
UTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "/output")
KAFKA_IP: str = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT: int = int(os.environ.get("KAFKA_PORT", "31092"))
INSTRUMENT: str = os.environ.get("INSTRUMENT", "MERLIN").upper()
SCRIPT_UPDATE_INTERVAL: int = int(os.environ.get("SCRIPT_REFRESH_TIME", "30"))
SCRIPT_EXECUTION_INTERVAL: float = float(os.environ.get("SCRIPT_RUN_INTERVAL", str(60)))
RUN_CHECK_INTERVAL: float = float(os.environ.get("RUN_CHECK_INTERVAL", "3"))
LIVE_WS_NAME: str = os.environ.get("LIVE_WS", "lives")
GITHUB_API_TOKEN: str = os.environ.get("GITHUB_API_TOKEN", "shh")
os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = "20000"
os.environ["EPICS_CA_ADDR_LIST"] = "130.246.39.152:5066"
os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

kafka_config: dict[str, object] = {
    "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
    "auto_offset_reset": "latest",
    "enable_auto_commit": True,
    "request_timeout_ms": 60000,
    "session_timeout_ms": 60000,
    "security_protocol": "PLAINTEXT",
    "api_version_auto_timeout_ms": 60000,
}


def process_events(events: EventMessage) -> None:
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
    if len(times_of_flight) > 0:
        for detector_id, tof in zip(detector_ids, times_of_flight, strict=False):
            spectra = ws.getSpectrum(int(detector_id))
            spectra.addEventQuickly(tof / 1000.0, pulse_time)


def _shutdown(signum: int, frame: FrameType | None) -> None:
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


def initialize_run(
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    run_start: RunStart | None = None,
    streaming_kafka_sample_log: bool = False,
) -> RunStart:
    """
    Initialize a run by finding the latest RunStart, preparing workspace, and seeking consumers.

    This either uses the provided run_start or locates the latest one on the
    runInfo topic. It initializes the Mantid workspace for the instrument and
    seeks the events consumer to the run start timestamp.

    :param events_consumer: Kafka consumer subscribed to the <instrument>_events topic.
    :param runinfo_consumer: Kafka consumer for <instrument>_runInfo used to locate RunStart.
    :param run_start: Optional RunStart message to use; if None, the latest will be fetched.
    :param streaming_kafka_sample_log: If True, sample log topic is also considered for seeking.
    :return: The RunStart message that was resolved and used to initialize the run.
    :raises TopicIncompleteError: If no RunStart message can be found on runInfo.
    """
    logger.info("Initializing run: %s", str(run_start))

    if run_start is None:
        logger.info("Run Start is None")
        run_start = find_latest_run_start(runinfo_consumer, INSTRUMENT)
        if run_start is None:
            raise TopicIncompleteError("No RunStart found in runInfo")
    initialize_instrument_workspace(INSTRUMENT, LIVE_WS_NAME, run_start)
    seek_event_consumer_to_runstart(INSTRUMENT, run_start, events_consumer)
    return run_start


def process_message(message: Any, kafka_sample_streaming: bool = False) -> None:
    """Process a single Kafka message from the events or sample-env topics.

    Depending on the message schema, this will either add event data to the
    live workspace (ev42) or, when enabled, append sample-environment logs
    (f144) as Mantid time series logs.

    :param message: A Kafka message object with a binary payload in message.value.
    :param kafka_sample_streaming: If True, process f144 sample log messages as well.
    :return: None
    """
    schema = get_schema(message.value)
    if schema == "ev42":
        events = EventMessage.GetRootAsEventMessage(message.value, 0)
        process_events(events)
    elif kafka_sample_streaming and schema == "f144":
        log_data = deserialise_f144(message.value)
        source = log_data.source_name.replace("IN:MERLIN:CS:SB:", "").title()
        with contextlib.suppress(TypeError):
            AddTimeSeriesLog(
                "lives",
                source,
                datetime.datetime.fromtimestamp(log_data.timestamp_unix_ns / 1e9, tz=datetime.UTC).isoformat(),
                log_data.value,
            )


def start_live_reduction(
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    run_start: RunStart | None = None,
    kafka_sample_log_streaming: bool = False,
) -> None:
    """
    Start the live data reduction loop for processing neutron event data.

    Initializes a run, consumes Kafka messages, periodically refreshes and
    executes the instrument reduction function, and watches for new runs.
    When a new RunStart is detected it tail-calls itself to switch to the
    next run.

    :param events_consumer: Kafka consumer subscribed to the events topic.
    :param runinfo_consumer: Kafka consumer for the runInfo topic.
    :param run_start: Optional RunStart message to start from; latest will be used if None.
    :param kafka_sample_log_streaming: If True, consume f144 sample log messages and add them as Mantid logs.
    :return: None
    """
    current_run_start = initialize_run(
        events_consumer,
        runinfo_consumer,
        run_start,
        streaming_kafka_sample_log=kafka_sample_log_streaming,
    )
    logger.info("Run began at %s", datetime_from_record_timestamp(current_run_start.StartTime()))
    logger.info("Starting live data reduction loop")

    reduction_function: Callable[[], None] = get_reduction_function(INSTRUMENT)

    script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
    script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
    run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)

    for message in events_consumer:
        process_message(message)
        # Check if we should refresh the reduction function
        if (datetime.datetime.now(tz=datetime.UTC) - script_last_checked_time).total_seconds() > SCRIPT_UPDATE_INTERVAL:
            reduction_function = refresh_reduction_function(reduction_function, INSTRUMENT)
            script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)

        # Check if we should execute the reduction function
        if (
            datetime.datetime.now(tz=datetime.UTC) - script_last_executed_time
        ).total_seconds() > SCRIPT_EXECUTION_INTERVAL:
            try:
                if not kafka_sample_log_streaming:
                    with open("merlin_log.txt") as f:
                        for line in f.readlines():
                            source, value, timestamp = line.split(" - ")
                            AddTimeSeriesLog(
                                "lives",
                                source,
                                timestamp,
                                value,
                            )
                    ws = mtd[LIVE_WS_NAME]
                    RemoveWorkspaceHistory(ws)

                logger.info("Executing reduction script")
                reduction_function()
                logger.info("Reduction script executed")
            except Exception as exc:
                logger.warning("Error occurred in reduction", exc_info=exc)
                continue
            finally:
                script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
                logger.info("Script will execute again in %s seconds", SCRIPT_EXECUTION_INTERVAL)

        # Check if we should move onto next workspace
        if (datetime.datetime.now(tz=datetime.UTC) - run_last_checked_time).total_seconds() > RUN_CHECK_INTERVAL:
            latest_runstart = find_latest_run_start(runinfo_consumer, INSTRUMENT)
            if latest_runstart.RunName() != current_run_start.RunName():
                logger.info(
                    "New run detected: RunStart message at %s",
                    datetime_from_record_timestamp(latest_runstart.StartTime()),
                )

                start_live_reduction(events_consumer, runinfo_consumer, latest_runstart)
            run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)


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
    kafka_config: dict[str, object] = {
        "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "request_timeout_ms": 60000,
        "session_timeout_ms": 60000,
        "security_protocol": "PLAINTEXT",
        "api_version_auto_timeout_ms": 60000,
    }
    events_consumer, runinfo_consumer = setup_consumers(INSTRUMENT, kafka_config)
    kafka_sample_streaming = False

    # init_pvs()  # discover and connect PVs
    # thread, stop_event = start_logging_thread("merlin_log.txt")

    start_live_reduction(events_consumer, runinfo_consumer)

    # stop_event.set()
    # thread.join()


if __name__ == "__main__":
    main()
