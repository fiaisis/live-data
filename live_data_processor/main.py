"""
Main module for the live data processor.

This module wires together Kafka consumers, Mantid workspaces, and the
reduction script refresh/execute loop to process neutron event data in
near real-time for a selected instrument.
"""

import contextlib
import datetime
import os
import queue
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

from kafka import KafkaConsumer
from mantid import ConfigService
from mantid.api import mtd
from mantid.kernel._kernel import DateAndTime
from mantid.simpleapi import (
    AddTimeSeriesLog,
    RemoveWorkspaceHistory,
)
from streaming_data_types import deserialise_f144
from streaming_data_types.fbschemas.eventdata_ev42.EventMessage import EventMessage
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart
from streaming_data_types.utils import get_schema

from live_data_processor.epics_streamer import (
    restart_epics_streaming,
    start_logging_process,
)
from live_data_processor.exceptions import OffsetNotFoundError, TopicIncompleteError
from live_data_processor.kafka_io import (
    datetime_from_record_timestamp,
    find_latest_run_start,
    get_consumer_lag,
    seek_event_consumer_to_runstart,
    setup_consumers,
)
from live_data_processor.loggers import VALKEY_CLIENT, capture_and_tee, setup_loggers
from live_data_processor.scripts import (
    get_initial_reduction_function,
    refresh_reduction_function,
)
from live_data_processor.workspaces import initialize_instrument_workspace

# setup internal and external loggers.
# The external logger will store it's output to valkey to be streamed to the frontend
# If you do not want a log to be visible to a user, use the internal_logger
INSTRUMENT: str = os.environ.get("INSTRUMENT", "MERLIN").upper()
internal_logger, external_logger, stream_key = setup_loggers(INSTRUMENT)


# Silence noisy mantid
ConfigService.setLogLevel(3)

OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "/output")
KAFKA_IP: str = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT: int = int(os.environ.get("KAFKA_PORT", "31092"))

SCRIPT_UPDATE_INTERVAL: float = float(os.environ.get("SCRIPT_REFRESH_TIME", "30"))
SCRIPT_EXECUTION_INTERVAL: float = float(os.environ.get("SCRIPT_RUN_INTERVAL", str(300)))
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

# mantid event data and is based on the epoch being 1990
UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
MANTID_EPOCH = datetime.datetime(1990, 1, 1, tzinfo=datetime.UTC)
EPOCH_DIFFERENCE = MANTID_EPOCH - UNIX_EPOCH
UNIX_TO_MANTID_OFFSET_NS = int(EPOCH_DIFFERENCE.total_seconds() * 1e9)

shutdown_event = threading.Event()


LAST_LOG_TIME = 0.0
LOG_COOLDOWN = 30.0
CATCHUP_START_TIME = time.time()  # Start timing as soon as the script begins
IS_CATCHING_UP = True  # Assume we start in a catch-up state
REALTIME_LAG_THRESHOLD = 1.5


def get_event_temporal_lag(events: EventMessage) -> float:
    """
    Calculate the temporal lag between the event pulse time and the current system time.

    This function computes how far behind real-time the event data is by comparing
    the pulse time embedded in the event message with the current system time.

    :param events: The EventMessage containing event data with an embedded pulse timestamp.
    :return: The lag in seconds (as a float) between the event pulse time and current time.
    """

    event_time_ns = events.PulseTime()
    current_time_ns = time.time_ns()

    # Convert delta to seconds
    return (current_time_ns - event_time_ns) / 1e9


def process_events(events: EventMessage) -> None:
    global LAST_LOG_TIME, IS_CATCHING_UP  # noqa: PLW0603

    raw_pulse_time_ns = events.PulseTime()
    times_of_flight = events.TimeOfFlightAsNumpy()

    if len(times_of_flight) == 0:
        return

    now = time.time()
    event_time_seconds = raw_pulse_time_ns / 1e9
    current_lag = now - event_time_seconds

    # --- Catch-up Tracking Logic ---
    if IS_CATCHING_UP and current_lag <= REALTIME_LAG_THRESHOLD:
        # We were catching up, but now we are within 1.5 seconds of real-time
        total_catchup_duration = now - CATCHUP_START_TIME
        external_logger.info(f"*** Time taken to catch up was: {total_catchup_duration:.2f}s ***")
        IS_CATCHING_UP = False

        # Standard throttled logging so you can still see progress
    if now - LAST_LOG_TIME > LOG_COOLDOWN:
        if current_lag > REALTIME_LAG_THRESHOLD:
            external_logger.info(f"Processing historical data: currently {current_lag:.1f}s behind real-time")
        else:
            # Subtle heartbeat so you know it's still alive
            external_logger.info("Operating in real-time.")
        LAST_LOG_TIME = now
    times_of_flight = events.TimeOfFlightAsNumpy()
    detector_ids = events.DetectorIdAsNumpy()
    pulse_time = DateAndTime(events.PulseTime() - UNIX_TO_MANTID_OFFSET_NS)
    ws = mtd[LIVE_WS_NAME]
    if len(times_of_flight) > 0:
        for detector_id, tof in zip(detector_ids, times_of_flight, strict=False):
            spectra = ws.getSpectrum(int(detector_id))
            spectra.addEventQuickly(tof / 1000.0, pulse_time)


def _shutdown(signum: int, frame: FrameType | None) -> None:
    """
    Handle system signals for clean termination of live data processing.

    This function is triggered when a system signal (e.g., SIGTERM or SIGINT)
    is received. It ensures a clean shutdown by setting the global shutdown event.

    :param signum: The signal number that triggered the function (e.g., SIGTERM, SIGINT).
    :param frame: The current stack frame (unused in this function but required by the signal handler).
    :return: None
    """

    internal_logger.info("Signal %s received, initiating graceful shutdown…", signum)
    shutdown_event.set()


def initialize_run(
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    run_start: RunStart | None = None,
) -> RunStart:
    """
    Initialize a run by finding the latest RunStart, preparing workspace, and seeking consumers.

    This either uses the provided run_start or locates the latest one on the
    runInfo topic. It initializes the Mantid workspace for the instrument and
    seeks the events consumer to the run start timestamp.

    :param events_consumer: Kafka consumer subscribed to the <instrument>_events topic.
    :param runinfo_consumer: Kafka consumer for <instrument>_runInfo used to locate RunStart.
    :param run_start: Optional RunStart message to use; if None, the latest will be fetched.
    :return: The RunStart message that was resolved and used to initialize the run.
    :raises TopicIncompleteError: If no RunStart message can be found on runInfo.
    """
    external_logger.info("Initializing run: %s", str(run_start))

    if run_start is None:
        external_logger.info("Run Start is None")
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


def run_monitor_thread(
    instrument: str,
    kafka_config: dict[str, object],
    run_signal_queue: queue.Queue,
    shutdown_event: threading.Event,
) -> None:
    """
    Background thread that continuously monitors the runInfo topic for new runs.
    Pushes new RunStart messages to the provided thread-safe queue.
    """
    # Python KafkaConsumers are not thread-safe, so we instantiate a dedicated consumer
    consumer_config = kafka_config.copy()
    consumer_config["consumer_timeout_ms"] = 1000  # Allow loop to check shutdown_event

    topic_name = f"{instrument}_runInfo"
    consumer = KafkaConsumer(topic_name, **consumer_config)

    current_run_name = None

    try:
        while not shutdown_event.is_set():
            for message in consumer:
                if shutdown_event.is_set():
                    break

                schema = get_schema(message.value)
                if schema == "pl72":
                    run_start = RunStart.GetRootAsRunStart(message.value, 0)
                    run_name = run_start.RunName()
                    if isinstance(run_name, bytes):
                        run_name = run_name.decode("utf-8")

                    if current_run_name != run_name:
                        current_run_name = run_name
                        run_signal_queue.put(run_start)
    except Exception as e:
        internal_logger.error("Error in run monitor thread: %s", e)
    finally:
        consumer.close()
        internal_logger.info("Run monitor thread shut down cleanly.")


def start_live_reduction(  # noqa: C901, PLR0912, PLR0915
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
    run_signal_queue: queue.Queue,
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
    reduction_function: Callable[[], None] = get_initial_reduction_function(INSTRUMENT)

    while not shutdown_event.is_set():
        try:
            current_run_start = initialize_run(
                events_consumer,
                runinfo_consumer,
                current_run_start,
            )
        except (TopicIncompleteError, OffsetNotFoundError) as ex:
            external_logger.warning("No run could be found. Retrying in %s seconds...", RUN_CHECK_INTERVAL)
            internal_logger.warning("Could not initialize run: %s. Retrying in %s seconds...", ex, RUN_CHECK_INTERVAL)
            time.sleep(RUN_CHECK_INTERVAL)
            continue
        external_logger.info(
            "Run began at %s",
            datetime_from_record_timestamp(current_run_start.StartTime()),
        )
        internal_logger.info(
            "Starting live data reduction loop, script will execute every %s seconds", SCRIPT_EXECUTION_INTERVAL
        )

        # Reset per-run timers
        script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
        script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
        # Note: run_last_checked_time has been removed

        # Consume events until we detect a new run, then break to reinitialize.

        for message in events_consumer:
            if shutdown_event.is_set():
                break

            process_message(message, kafka_sample_streaming=kafka_sample_log_streaming)
            # Optional: Log lag every 1000 messages to avoid spamming the broker
            if message.offset % 100000 == 0:
                lags = get_consumer_lag(events_consumer)
                total_lag = sum(lags.values())
                internal_logger.info(f"Current Kafka Lag: {total_lag} messages")

            now = datetime.datetime.now(tz=datetime.UTC)

            # Refresh reduction function
            if (now - script_last_checked_time).total_seconds() > SCRIPT_UPDATE_INTERVAL:
                reduction_function = refresh_reduction_function(reduction_function, INSTRUMENT)
                script_last_checked_time = now

            # Execute reduction function periodically
            if (now - script_last_executed_time).total_seconds() > SCRIPT_EXECUTION_INTERVAL:
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
                    external_logger.info("%s workspace has %s number of events", LIVE_WS_NAME, ws.getNumberEvents())
                    external_logger.info("Executing reduction script")
                    with capture_and_tee(external_logger):
                        reduction_function()
                    external_logger.info("Reduction script executed")
                except Exception as exc:
                    external_logger.warning("Error occurred in reduction", exc_info=exc)
                finally:
                    script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
                    external_logger.info(
                        "Script will execute again in %s seconds",
                        SCRIPT_EXECUTION_INTERVAL,
                    )

            # Check for new run via non-blocking queue check
            try:
                latest_runstart = run_signal_queue.get_nowait()

                external_logger.info(
                    "New run detected: RunStart message at %s",
                    datetime_from_record_timestamp(latest_runstart.StartTime()),
                )

                if not kafka_sample_log_streaming:
                    epics_proc, epics_stop_event = restart_epics_streaming(epics_log_file, epics_proc, epics_stop_event)

                # Switch run_start and break out to reinitialize in-place
                current_run_start = latest_runstart
                break
            except queue.Empty:
                pass

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
    # Clear previous logs from cache
    VALKEY_CLIENT.delete(stream_key)
    external_logger.info("Starting live data processing for %s", INSTRUMENT)
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
        epics_proc, epics_stop_event = start_logging_process(f"{INSTRUMENT.lower()}_log.txt")
    else:
        epics_proc, epics_stop_event = None, None

    # Start the Background Run Monitor Thread
    run_signal_queue = queue.Queue()
    monitor_thread = threading.Thread(
        target=run_monitor_thread, args=(INSTRUMENT, kafka_config, run_signal_queue, shutdown_event), daemon=True
    )
    monitor_thread.start()

    try:
        start_live_reduction(
            events_consumer,
            runinfo_consumer,
            run_signal_queue,
            kafka_sample_log_streaming=kafka_sample_streaming,
            epics_proc=epics_proc,
            epics_stop_event=epics_stop_event,
            epics_log_file=f"{INSTRUMENT.lower()}_log.txt",
        )

    finally:
        # Trigger shutdown for all threads
        shutdown_event.set()

        # Wait for background thread to exit
        monitor_thread.join(timeout=5)

        # Gracefully close Kafka Consumers
        events_consumer.close()
        runinfo_consumer.close()

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
