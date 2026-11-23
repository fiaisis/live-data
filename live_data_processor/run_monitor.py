"""
Run monitor for Live Data Processor.

This lightweight script watches the instrument-specific Kafka <instrument>_runInfo
topic and publishes run state changes to Valkey. It is intentionally small and
isolated so that Kafka consumer network handling does not run inside the main
Mantid data reduction loop.
"""

import json
import logging
import os
from typing import Any

import redis
from kafka import KafkaConsumer
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart
from streaming_data_types.utils import get_schema

from live_data_processor.kafka_io import datetime_from_record_timestamp, find_latest_run_start

INSTRUMENT = os.environ.get("INSTRUMENT", os.environ.get("INSTRUMENT_NAME", "MERLIN")).upper()
VALKEY_HOST = os.environ.get("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("VALKEY_PORT", "6379"))
KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))
RUN_MONITOR_GROUP = os.environ.get("RUN_MONITOR_GROUP", f"{INSTRUMENT}_run_monitor")
POLL_TIMEOUT_MS = int(os.environ.get("RUN_MONITOR_POLL_TIMEOUT_MS", "1000"))

RUNINFO_TOPIC = f"{INSTRUMENT}_runInfo"
CURRENT_RUN_KEY = f"instrument:{INSTRUMENT}:current_run"


logger = logging.getLogger(f"internal_run_monitor_{INSTRUMENT}")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s - INTERNAL - %(message)s"))
    logger.addHandler(_handler)


def _decode_value(value: Any) -> str:
    """
    Convert a FlatBuffers field to text for JSON publication.
    :param value: The field value to decode or convert to text.
    :return: The field as a string.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def extract_run_name(run_start: RunStart | None) -> str | None:
    """
    Extract the run name from a Kafka RunStart record.
    :param run_start: The decoded RunStart record, if available.
    :return: The run name as text, or None if the record or name is missing.
    """
    if run_start is None:
        return None

    raw_name = run_start.RunName()
    if raw_name is None:
        return None
    decoded_name = _decode_value(raw_name)
    return str(decoded_name)


def _build_payload(run_start: RunStart) -> dict[str, Any]:
    """
    Build the current-run summary for Valkey.
    :param run_start: The decoded Kafka RunStart record.
    :return: A dictionary with the run name, UTC start time, and available optional metadata.
    """
    payload: dict[str, Any] = {
        "run_name": extract_run_name(run_start),
        "start_timestamp": datetime_from_record_timestamp(run_start.StartTime()),
    }

    for attribute in ("RunNumber", "RunTitle", "SampleName", "InstrumentName"):
        if hasattr(run_start, attribute):
            value = getattr(run_start, attribute)()
            if value is not None:
                payload[attribute[0].lower() + attribute[1:]] = _decode_value(value)

    return payload


def _get_valkey_client() -> redis.Redis:
    """Create a Valkey client that decodes responses as text."""
    return redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)


def _get_base_kafka_config() -> dict[str, object]:
    """Return the Kafka settings shared by the discovery and monitor consumers."""
    return {
        "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "request_timeout_ms": 70000,
        "session_timeout_ms": 60000,
        "security_protocol": "PLAINTEXT",
        "api_version_auto_timeout_ms": 60000,
    }


def _publish_run_state(valkey_client: redis.Redis, run_start: RunStart) -> None:
    """
    Publish the current-run summary as JSON.
    :param valkey_client: The Valkey connection.
    :param run_start: The decoded RunStart record to publish.
    :return: None.
    """
    payload = _build_payload(run_start)
    json_payload = json.dumps(payload, ensure_ascii=False)
    logger.info("Publishing run state to %s: %s", CURRENT_RUN_KEY, payload)
    valkey_client.set(CURRENT_RUN_KEY, json_payload)


def _discover_latest_run_start() -> RunStart | None:
    """
    Read the latest run start with a short-lived Kafka consumer.
    :return: The latest RunStart record, or None if the records inspected contain no run start.
    """
    temp_config = _get_base_kafka_config()
    temp_config["consumer_timeout_ms"] = 10000
    consumer = KafkaConsumer(RUNINFO_TOPIC, **temp_config)
    try:
        return find_latest_run_start(consumer, INSTRUMENT)
    finally:
        consumer.close()


def _configure_consumer() -> KafkaConsumer:
    """
    Subscribe to the instrument's run-info topic with automatic commits disabled.
    :return: The Kafka consumer for the run-monitor group.
    """
    kafka_config = _get_base_kafka_config()
    # A new group must see records published between discovery and its first poll.
    kafka_config.update(group_id=RUN_MONITOR_GROUP, auto_offset_reset="earliest", enable_auto_commit=False)
    consumer = KafkaConsumer(**kafka_config)
    consumer.subscribe([RUNINFO_TOPIC])
    return consumer


def _is_run_start_message(message: Any) -> bool:
    """
    Check whether the Kafka message uses the RunStart schema.

    The RunStart FlatBuffers schema declares `pl72` as its file identifier.

    :param message: The Kafka record to inspect.
    :return: True if the payload has the RunStart schema identifier.
    """
    return get_schema(message.value) == "pl72"


def main() -> None:  # noqa: PLR0912, C901
    """
    Publish the latest run at startup and monitor Kafka for newer run starts.

    Commit offsets after processing each batch so a failed publication can be retried.

    :return: None.
    """
    logger.info("Starting Run Monitor for %s on topic %s", INSTRUMENT, RUNINFO_TOPIC)
    valkey_client = _get_valkey_client()

    current_run_name = None
    current_start_time = None
    consumer = None
    try:
        latest_start = _discover_latest_run_start()
        if latest_start is not None:
            current_run_name = extract_run_name(latest_start)
            if current_run_name is not None:
                _publish_run_state(valkey_client, latest_start)
                current_start_time = latest_start.StartTime()

        consumer = _configure_consumer()
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for partition_records in records.values():
                for message in partition_records:
                    if not _is_run_start_message(message):
                        continue

                    run_start = RunStart.GetRootAsRunStart(message.value, 0)
                    run_name = extract_run_name(run_start)
                    start_time = run_start.StartTime()
                    if run_name is None or (current_start_time is not None and start_time <= current_start_time):
                        continue

                    _publish_run_state(valkey_client, run_start)
                    current_start_time = start_time
                    logger.info("Detected new run %s", run_name)

            if records:
                consumer.commit()
    except KeyboardInterrupt:
        logger.info("Run Monitor shutdown requested")
    except Exception:
        logger.exception("Run Monitor failed")
        raise
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception as e:
                logger.exception(msg=f"Failed to close Kafka consumer cleanly: {e}", exc_info=e)


if __name__ == "__main__":
    main()
