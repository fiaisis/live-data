"""
Main module for the live data processor.

This module wires together Kafka consumers, Mantid workspaces, and the
reduction script refresh/execute loop to process neutron event data in
near real-time for a selected instrument.
"""

import datetime
import json
import os
import signal
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Any

from kafka import KafkaConsumer
from mantid import ConfigService
from mantid.api import mtd
from mantid.kernel import FloatTimeSeriesProperty
from mantid.kernel._kernel import DateAndTime
from mantid.simpleapi import (
    AddTimeSeriesLog,
    RemoveWorkspaceHistory,
)
from streaming_data_types.fbschemas.eventdata_ev42.EventMessage import EventMessage
from streaming_data_types.fbschemas.eventdata_ev42.FacilityData import FacilityData
from streaming_data_types.fbschemas.isis_event_info_is84.ISISData import ISISData
from streaming_data_types.fbschemas.isis_event_info_is84.RunState import RunState
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart
from streaming_data_types.utils import get_schema

from live_data_processor.exceptions import OffsetNotFoundError, TopicIncompleteError
from live_data_processor.kafka_io import (
    datetime_from_record_timestamp,
    find_latest_run_start,
    get_consumer_lag,
    seek_event_consumer_to_runstart,
    setup_consumers,
)
from live_data_processor.loggers import VALKEY_CLIENT, capture_and_tee, setup_loggers
from live_data_processor.run_monitor import extract_run_name
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


def extract_isis_proton_charge(events: EventMessage) -> float | None:
    """
    Extract per-frame proton charge (uAh) from ISIS-specific event data, if present and running.
    :param events: The decoded Kafka ev42 EventMessage.
    :return: The proton charge in uAh, or None if unavailable or not in RUNNING state.
    """
    if events.FacilitySpecificDataType() != FacilityData.ISISData:
        return None
    specific = events.FacilitySpecificData()
    if specific is None:
        return None
    isis_data = ISISData()
    isis_data.Init(specific.Bytes, specific.Pos)
    if isis_data.RunState() != RunState.RUNNING:
        return None
    return float(isis_data.ProtonCharge())


def _record_proton_charge(
    ws: Any,
    pulse_time: DateAndTime,
    events: EventMessage,
) -> None:
    """
    Record deduplicated per-pulse proton charge on the workspace's proton_charge log.
    :param ws: The live Mantid EventWorkspace.
    :param pulse_time: The Mantid DateAndTime for the pulse.
    :param events: The decoded Kafka ev42 EventMessage.
    :return: None.
    """
    run = ws.getRun()
    if run.hasProperty("proton_charge"):
        prop = run.getProperty("proton_charge")
        if prop.size() > 0 and prop.lastTime() == pulse_time:
            return
    else:
        prop = FloatTimeSeriesProperty("proton_charge")
        prop.units = "uAh"
        run.addProperty("proton_charge", prop, True)
        prop = run.getProperty("proton_charge")

    proton_charge = extract_isis_proton_charge(events)
    if proton_charge is None:
        return
    prop.addValue(pulse_time, proton_charge)


def _integrate_proton_charge(ws: Any) -> None:
    """
    Integrate the workspace's proton_charge time-series log into gd_prtn_chrg, if present.
    :param ws: The live Mantid EventWorkspace.
    :return: None.
    """
    run = ws.getRun()
    if run.hasProperty("proton_charge"):
        run.integrateProtonCharge()


def _load_epics_logs(ws: Any, last_epics_id: str, run_start_iso: str) -> str:
    """
    Append new EPICS stream updates to the workspace and backfill missing blocks from the latest snapshot.
    :param ws: The live Mantid EventWorkspace.
    :param last_epics_id: The last Valkey stream entry ID already loaded, or "0-0" if none.
    :param run_start_iso: ISO-8601 UTC timestamp of the current run start for backfilled static logs.
    :return: The updated last Valkey stream entry ID.
    """
    epics_stream_key = f"instrument:{INSTRUMENT}:epics_stream"
    epics_latest_key = f"instrument:{INSTRUMENT}:epics_latest"

    min_id = "-" if last_epics_id == "0-0" else f"({last_epics_id}"
    epics_logs = VALKEY_CLIENT.xrange(epics_stream_key, min_id, "+")

    for entry_id, data in epics_logs:
        source = data.get("block_name")
        value = data.get("value")
        timestamp = data.get("timestamp")

        if source and value and timestamp:
            AddTimeSeriesLog(
                LIVE_WS_NAME,
                source,
                timestamp,
                value,
            )
        last_epics_id = entry_id

    run = ws.getRun()
    latest_values = VALKEY_CLIENT.hgetall(epics_latest_key)
    for block_name, value in latest_values.items():
        if block_name and value and not run.hasProperty(block_name):
            AddTimeSeriesLog(
                LIVE_WS_NAME,
                block_name,
                run_start_iso,
                value,
            )

    return last_epics_id


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
    """
    Append neutron events and their pulse times to the live workspace.
    :param events: The decoded Kafka event message.
    :return: None.
    """
    global LAST_LOG_TIME, IS_CATCHING_UP  # noqa: PLW0603

    raw_pulse_time_ns = events.PulseTime()
    pulse_time = DateAndTime(raw_pulse_time_ns - UNIX_TO_MANTID_OFFSET_NS)
    ws = mtd[LIVE_WS_NAME]
    _record_proton_charge(ws, pulse_time, events)

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
    detector_ids = events.DetectorIdAsNumpy()
    for detector_id, tof in zip(detector_ids, times_of_flight, strict=False):
        spectra = ws.getSpectrum(int(detector_id))
        spectra.addEventQuickly(tof / 1000.0, pulse_time)


def _shutdown(signum: int, frame: FrameType | None) -> None:
    """
    Set the shutdown flag in response to an operating system signal.
    :param signum: The signal number.
    :param frame: The current stack frame, if available.
    :return: None.
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
    global CATCHUP_START_TIME, IS_CATCHING_UP  # noqa: PLW0603

    if run_start is None:
        external_logger.info("Initializing latest run...")
        run_start = find_latest_run_start(runinfo_consumer, INSTRUMENT)
        if run_start is None:
            raise TopicIncompleteError("No RunStart found in runInfo")

    run_name = extract_run_name(run_start) or "unknown"
    run_start_str = datetime_from_record_timestamp(run_start.StartTime())
    external_logger.info("Initializing run %s (started at %s)", run_name, run_start_str)

    initialize_instrument_workspace(INSTRUMENT, LIVE_WS_NAME, run_start)
    seek_event_consumer_to_runstart(INSTRUMENT, run_start, events_consumer)
    CATCHUP_START_TIME = time.time()
    IS_CATCHING_UP = True
    return run_start


def process_message(message: Any) -> None:
    """
    Decode an event record and append its neutron events to the workspace.
    :param message: The Kafka record. Payloads without the ev42 identifier are ignored.
    :return: None.
    """
    schema = get_schema(message.value)
    if schema == "ev42":
        events = EventMessage.GetRootAsEventMessage(message.value, 0)
        process_events(events)


def find_new_run_start(
    runinfo_consumer: KafkaConsumer,
    current_run_start: RunStart,
    current_run_name: str | None,
) -> RunStart | None:
    """
    Resolve a run change reported by Valkey to a matching Kafka RunStart.
    :param runinfo_consumer: The Kafka consumer for the instrument's run-info topic.
    :param current_run_start: The RunStart record for the active workspace.
    :param current_run_name: The decoded name of the active run, if available.
    :return: The matching newer RunStart record, or None if no change is confirmed.
    """
    latest_run_data = VALKEY_CLIENT.get(f"instrument:{INSTRUMENT}:current_run")
    if latest_run_data is None:
        return None

    try:
        latest_run_name = json.loads(latest_run_data)["run_name"]
    except (ValueError, KeyError, TypeError):
        internal_logger.warning("Invalid current run data in Valkey")
        return None
    if not latest_run_name or latest_run_name == current_run_name:
        return None

    try:
        candidate = find_latest_run_start(runinfo_consumer, INSTRUMENT)
    except TopicIncompleteError:
        return None
    if (
        candidate is not None
        and extract_run_name(candidate) == latest_run_name
        and candidate.StartTime() > current_run_start.StartTime()
    ):
        return candidate
    return None


def start_live_reduction(  # noqa: PLR0915
    events_consumer: KafkaConsumer,
    runinfo_consumer: KafkaConsumer,
) -> None:
    """
    Initialize the workspace and process events for successive instrument runs.

    Load EPICS time-series logs from Valkey before invoking the reduction script.

    :param events_consumer: The Kafka consumer for the instrument's event topic.
    :param runinfo_consumer: The Kafka consumer used to resolve run starts.
    :return: None.
    """
    current_run_start: RunStart | None = None
    reduction_function: Callable[[], None] = get_initial_reduction_function(INSTRUMENT)

    while True:
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

        # Delete previous run's EPICS stream data from Valkey cache if we are using file/Valkey-based logging
        epics_stream_key = f"instrument:{INSTRUMENT}:epics_stream"
        VALKEY_CLIENT.delete(epics_stream_key)
        internal_logger.info("Cleared Valkey EPICS stream: %s", epics_stream_key)

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
        run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
        first_reduction_due = script_last_executed_time + datetime.timedelta(seconds=SCRIPT_EXECUTION_INTERVAL)
        external_logger.info(
            "First reduction expected around %s UTC (%.0f seconds after initialization), on the next event",
            first_reduction_due.strftime("%Y-%m-%d %H:%M:%S"),
            SCRIPT_EXECUTION_INTERVAL,
        )

        next_run_start = None
        pending_run_start = None
        current_run_name = extract_run_name(current_run_start)
        run_start_iso = datetime_from_record_timestamp(current_run_start.StartTime()).replace(" ", "T")
        last_epics_id = "0-0"

        # Consume events until we detect a new run, then break to reinitialize.
        for message in events_consumer:
            now = datetime.datetime.now(tz=datetime.UTC)
            if pending_run_start is None and (now - run_last_checked_time).total_seconds() > RUN_CHECK_INTERVAL:
                run_last_checked_time = now
                pending_run_start = find_new_run_start(runinfo_consumer, current_run_start, current_run_name)

            # Use the same Kafka record timestamp as the seek for the next run.
            if pending_run_start is not None and message.timestamp >= pending_run_start.StartTime():
                next_run_start = pending_run_start
                external_logger.info(
                    "New run detected: RunStart message at %s",
                    datetime_from_record_timestamp(next_run_start.StartTime()),
                )
                break

            process_message(message)
            # Optional: Log lag every 100000 messages to avoid spamming the broker
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
                    ws = mtd[LIVE_WS_NAME]
                    last_epics_id = _load_epics_logs(ws, last_epics_id, run_start_iso)
                    _integrate_proton_charge(ws)
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

        current_run_start = next_run_start


def main() -> None:
    """
    Start live data processing for the configured instrument.

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

    start_live_reduction(
        events_consumer,
        runinfo_consumer,
    )


if __name__ == "__main__":
    main()
