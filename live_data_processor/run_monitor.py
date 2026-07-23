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
import time
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


logger = logging.getLogger("live_data_processor.run_monitor")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def extract_run_name(run_start: RunStart | None) -> str | None:
    if run_start is None:
        return None

    raw_name = run_start.RunName()
    if raw_name is None:
        return None
    decoded_name = _decode_value(raw_name)
    return str(decoded_name)


def _build_payload(run_start: RunStart) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "run_name": extract_run_name(run_start),
        "start_time": datetime_from_record_timestamp(run_start.StartTime()),
        "start_timestamp": int(run_start.StartTime()),
    }

    for attribute in ("RunNumber", "RunTitle", "SampleName", "InstrumentName"):
        if hasattr(run_start, attribute):
            value = getattr(run_start, attribute)()
            if value is not None:
                payload[attribute[0].lower() + attribute[1:]] = _decode_value(value)

    return payload


def _get_valkey_client() -> redis.Redis:
    return redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)


def _get_base_kafka_config() -> dict[str, Any]:
    return {
        "bootstrap_servers": f"{KAFKA_IP}:{KAFKA_PORT}",
        "auto_offset_reset": "latest",
        "enable_auto_commit": False,
        "request_timeout_ms": 60000,
        "session_timeout_ms": 60000,
        "api_version_auto_timeout_ms": 60000,
    }


def _publish_run_state(valkey_client: redis.Redis, run_start: RunStart) -> None:
    payload = _build_payload(run_start)
    json_payload = json.dumps(payload, ensure_ascii=False)
    logger.info("Publishing run state to %s: %s", CURRENT_RUN_KEY, payload)
    valkey_client.set(CURRENT_RUN_KEY, json_payload)


def _discover_latest_run_start() -> RunStart | None:
    temp_config = _get_base_kafka_config()
    temp_config["consumer_timeout_ms"] = 10000
    consumer = KafkaConsumer(RUNINFO_TOPIC, **temp_config)
    try:
        return find_latest_run_start(consumer, INSTRUMENT)
    finally:
        consumer.close()


def _configure_consumer() -> KafkaConsumer:
    kafka_config = _get_base_kafka_config()
    kafka_config["group_id"] = RUN_MONITOR_GROUP
    consumer = KafkaConsumer(**kafka_config)
    consumer.subscribe([RUNINFO_TOPIC])
    return consumer


def _is_run_start_message(message: Any) -> bool:
    return get_schema(message.value) == "pl72"


def main() -> None:
    _configure_logging()
    logger.info("Starting Run Monitor for %s on topic %s", INSTRUMENT, RUNINFO_TOPIC)
    valkey_client = _get_valkey_client()

    current_run_name = None
    consumer = None
    try:
        latest_start = _discover_latest_run_start()
        if latest_start is not None:
            current_run_name = extract_run_name(latest_start)
            if current_run_name is not None:
                _publish_run_state(valkey_client, latest_start)

        consumer = _configure_consumer()
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for partition_records in records.values():
                for message in partition_records:
                    if not _is_run_start_message(message):
                        continue

                    run_start = RunStart.GetRootAsRunStart(message.value, 0)
                    run_name = extract_run_name(run_start)
                    if run_name is None or run_name == current_run_name:
                        continue

                    current_run_name = run_name
                    _publish_run_state(valkey_client, run_start)
                    logger.info("Detected new run %s", current_run_name)

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
            except Exception:
                logger.exception("Failed to close Kafka consumer cleanly")


if __name__ == "__main__":
    main()
