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
from pathlib import Path
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

from live_data_processor.epics_streamer import (
    restart_epics_streaming,
    start_logging_process,
)
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
# ConfigService.appendDataSearchDir("~/work/live-data")
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
                datetime.datetime.fromtimestamp(
                    log_data.timestamp_unix_ns / 1e9, tz=datetime.UTC
                ).isoformat(),
                log_data.value,
            )


def start_live_reduction(
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    kafka_sample_log_streaming: bool = False,
    epics_proc=None,
    epics_stop_event=None,
    epics_log_file: str = "sample_log.txt",
) -> None:
    """
    Run the main live data reduction loop for an instrument.

    This function manages the full lifecycle of live reduction across
    successive runs: initializing each run, consuming Kafka event data,
    periodically refreshing and executing the reduction script, and
    detecting new runs to switch in-place without recursion.

    When file-based EPICS sample logging is used (i.e. Kafka sample-log
    streaming is disabled), it also manages the EPICS logging process,
    restarting it on each run change so sample logs are kept run-local.

    The loop runs continuously until the process is terminated.

    :param events_consumer: Kafka consumer subscribed to the <instrument>_events topic.
    :param runinfo_consumer: Kafka consumer subscribed to the <instrument>_runInfo topic.
    :param kafka_sample_log_streaming: If True, sample logs are consumed directly
                                        from Kafka; otherwise EPICS logs are streamed
                                        to file and replayed during reduction.
    :param epics_proc: Active EPICS logging process, if any.
    :param epics_stop_event: Stop event used to shut down the EPICS logging process.
    :param epics_log_file: Path to the EPICS sample-log file when file-based logging is used.
    :return: None
    """
    current_run_start: RunStart | None = None
    reduction_function: Callable[[], None] = get_reduction_function(INSTRUMENT)

    while True:
        current_run_start = initialize_run(
            events_consumer,
            runinfo_consumer,
            current_run_start,
            streaming_kafka_sample_log=kafka_sample_log_streaming,
        )
        logger.info(
            "Run began at %s",
            datetime_from_record_timestamp(current_run_start.StartTime()),
        )
        logger.info("Starting live data reduction loop")

        # Reset per-run timers
        script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
        script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
        run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)

        # Consume events until we detect a new run, then break to reinitialize.
        for message in events_consumer:
            process_message(message, kafka_sample_streaming=kafka_sample_log_streaming)

            now = datetime.datetime.now(tz=datetime.UTC)

            # Refresh reduction function
            if (
                now - script_last_checked_time
            ).total_seconds() > SCRIPT_UPDATE_INTERVAL:
                reduction_function = refresh_reduction_function(
                    reduction_function, INSTRUMENT
                )
                script_last_checked_time = now

            # Execute reduction function periodically
            if (
                now - script_last_executed_time
            ).total_seconds() > SCRIPT_EXECUTION_INTERVAL:
                try:
                    if not kafka_sample_log_streaming:
                        with Path(epics_log_file).open("r", encoding="utf-8") as f:
                            for line in f:
                                source, value, timestamp = line.split(" - ")
                                AddTimeSeriesLog(
                                    LIVE_WS_NAME,
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
                finally:
                    script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
                    logger.info(
                        "Script will execute again in %s seconds",
                        SCRIPT_EXECUTION_INTERVAL,
                    )

            # Check for new run
            now = datetime.datetime.now(tz=datetime.UTC)
            if (now - run_last_checked_time).total_seconds() > RUN_CHECK_INTERVAL:
                latest_runstart = find_latest_run_start(runinfo_consumer, INSTRUMENT)
                if (
                    latest_runstart is not None
                    and latest_runstart.RunName() != current_run_start.RunName()
                ):
                    logger.info(
                        "New run detected: RunStart message at %s",
                        datetime_from_record_timestamp(latest_runstart.StartTime()),
                    )

                    if not kafka_sample_log_streaming:
                        epics_proc, epics_stop_event = restart_epics_streaming(
                            epics_log_file, epics_proc, epics_stop_event
                        )

                    # Switch run_start and break out to reinitialize in-place
                    current_run_start = latest_runstart
                    break

                run_last_checked_time = now

        # If the consumer loop ends (rare), just re-enter the while-loop and attempt to reinitialize.
        current_run_start = None


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

    if not kafka_sample_streaming:
        epics_proc, epics_stop_event = start_logging_process(
            f"{INSTRUMENT.lower()}_log.txt"
        )
    else:
        epics_proc, epics_stop_event = None, None

    try:
        start_live_reduction(
            events_consumer,
            runinfo_consumer,
            kafka_sample_log_streaming=kafka_sample_streaming,
            epics_proc=epics_proc,
            epics_stop_event=epics_stop_event,
            epics_log_file=f"{INSTRUMENT.lower()}_log.txt",
        )

    finally:
        if not kafka_sample_streaming:
            # Clean shutdown of EPICS logging process
            epics_stop_event.set()
            with contextlib.suppress(Exception):
                epics_proc.join(timeout=5)
                if epics_proc.is_alive():
                    epics_proc.terminate()
                    epics_proc.join(timeout=2)


if __name__ == "__main__":
    main()
