import datetime
import logging
import multiprocessing
import os
import signal
import sys
import time

from kafka import KafkaConsumer
from mantid.api import mtd
from mantid.kernel import DateAndTime

from live_data_processor.collectors import get_misc_data_collector
from live_data_processor.exceptions import TopicIncompleteError
from live_data_processor.kafka_io import find_latest_run_start, seek_event_consumer_to_runstart, setup_consumers
from live_data_processor.runs import RunContext
from live_data_processor.scripts import get_script
from live_data_processor.workspaces import initialize_instrument_workspace
from schemas.compiled_schemas.EventMessage import EventMessage
from schemas.compiled_schemas.RunStart import RunStart

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
UTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")
KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))
INSTRUMENT: str = os.environ.get("INSTRUMENT", "MERLIN").upper()
SCRIPT_UPDATE_INTERVAL = int(os.environ.get("SCRIPT_REFRESH_TIME", "30"))
SCRIPT_EXECUTION_INTERVAL = float(os.environ.get("SCRIPT_RUN_INTERVAL", "10"))
RUN_CHECK_INTERVAL = float(os.environ.get("RUN_CHECK_INTERVAL", "3"))
LIVE_WS_NAME = os.environ.get("LIVE_WS", "lives")
GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN", "shh")
os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = "20000"
os.environ["EPICS_CA_ADDR_LIST"] = "130.246.39.152:5066"
os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

kafka_config = {
    "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
    "auto_offset_reset": "latest",
    "enable_auto_commit": True,
    "request_timeout_ms": 60000,
    "session_timeout_ms": 60000,
    "security_protocol": "PLAINTEXT",
    "api_version_auto_timeout_ms": 60000,
}

# Local Dev only
# config["defaultsave.directory"] = "~/Downloads"  # noqa: ERA001


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
        run_start = find_latest_run_start(runinfo_consumer, INSTRUMENT)
        if run_start is None:
            raise TopicIncompleteError("No RunStart found in runInfo")
    initialize_instrument_workspace(INSTRUMENT, LIVE_WS_NAME, run_start)
    seek_event_consumer_to_runstart(INSTRUMENT, run_start, events_consumer)
    return run_start


def start_live_reduction(  # noqa: C901
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

    manager = multiprocessing.Manager()
    shared_dict = manager.dict(current_run_start=current_run_start)
    current_run_lock = multiprocessing.Lock()

    def get_current_run() -> RunStart:
        with current_run_lock:
            return shared_dict["current_run_start"]

    run_context = RunContext(get_current_run)

    misc_data_collector = get_misc_data_collector(INSTRUMENT)

    misc_data_collector.on_run_start(run_context)

    stop_event: multiprocessing.Event | None = None
    collector_process: multiprocessing.Process | None = None

    if misc_data_collector.will_run_forever:
        stop_event = multiprocessing.Event()
        collector_process = multiprocessing.Process(
            target=misc_data_collector.run_forever,
            args=(run_context, stop_event),
            name=f"{INSTRUMENT}-misc-collector",
            daemon=True,
        )
        collector_process.start()

    script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
    script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
    run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
    for message in events_consumer:
        events = EventMessage.GetRootAs(message.value, 0)
        process_events(events)
        if (datetime.datetime.now(tz=datetime.UTC) - script_last_checked_time).total_seconds() > SCRIPT_UPDATE_INTERVAL:
            try:
                new_script = get_script(INSTRUMENT)
                if script != new_script:
                    logger.info("New Script detected, Continuing with new script")
                    script = new_script
            except RuntimeError as exc:
                logger.warning("Could not get latest script, continuing with current script", exc_info=exc)
            script_last_checked_time = datetime.datetime.now(tz=datetime.UTC)
        if (
            datetime.datetime.now(tz=datetime.UTC) - script_last_executed_time
        ).total_seconds() > SCRIPT_EXECUTION_INTERVAL:
            try:
                misc_data_collector.on_pre_exec(run_context)
                exec(script)  # noqa: S102
            except Exception as exc:
                logger.warning("Error occurred in reduction, waiting 15 seconds and continuing loop", exc_info=exc)
                time.sleep(15)
                continue
            script_last_executed_time = datetime.datetime.now(tz=datetime.UTC)
        # Check if we should move onto next workspace
        if (datetime.datetime.now(tz=datetime.UTC) - run_last_checked_time).total_seconds() > RUN_CHECK_INTERVAL:
            latest_runstart = find_latest_run_start(runinfo_consumer, INSTRUMENT)
            if latest_runstart.RunName() != current_run_start.RunName():
                logger.info("New run detected, restarting with new run")

                logger.info("Stopping current collector")
                if stop_event is not None:
                    stop_event.set()
                    collector_process.join(timeout=2)
                misc_data_collector.on_run_end(run_context)

                with current_run_lock:
                    current_run_start = latest_runstart

                start_live_reduction(script, events_consumer, runinfo_consumer, latest_runstart)
            run_last_checked_time = datetime.datetime.now(tz=datetime.UTC)


def main() -> None:
    """
    Main function to start live data processing for the specified instrument.

    This function initializes signal handling for graceful shutdown, retrieves the
    live data processing script, starts the live data processing for the instrument,
    and periodically checks for updated processing scripts.

    :return: None
    """
    multiprocessing.set_start_method("fork")
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
    events_consumer, runinfo_consumer = setup_consumers(INSTRUMENT, kafka_config)
    script = get_script(INSTRUMENT)
    start_live_reduction(script, events_consumer, runinfo_consumer)


if __name__ == "__main__":
    main()
