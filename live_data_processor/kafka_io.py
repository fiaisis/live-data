"""
Kafka IO utilities for live data processing.

This module provides helper functions to configure Kafka consumers,
seek to relevant offsets based on run start messages, and locate the
most recent RunStart message for an instrument.
"""

import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart
from streaming_data_types.fbschemas.run_stop_6s4t.RunStop import RunStop

from live_data_processor.exceptions import TopicIncompleteError

logger = logging.getLogger(__name__)

KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))


def datetime_from_record_timestamp(timestamp: int) -> str:
    """Convert a Kafka record timestamp (ms since epoch) to a readable string.

    :param timestamp: Milliseconds since Unix epoch from a Kafka record.
    :return: Timestamp formatted as YYYY-MM-DD HH:MM:SS in UTC.
    """
    return datetime.fromtimestamp(timestamp / 1000.0, tz=UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def find_latest_run_start(
    runinfo_consumer: KafkaConsumer, instrument: str
) -> RunStart | None:
    """
    Find the latest RunStart message in the runinfo topic.

    This function seeks to the end of the runinfo topic, retrieves the last few messages,
    and searches for the most recent RunStart message.

    :param runinfo_consumer: The Kafka consumer for the runinfo topic.
    :return: The latest RunStart message if found, None otherwise.
    :raises TopicIncompleteError: If the topic does not have any messages.
    """
    latest_start = None

    # Set the offset to 3 messages from the end.
    tp = TopicPartition(f"{instrument}_runInfo", 0)
    end_offset = runinfo_consumer.end_offsets([tp])
    runinfo_consumer.seek(tp, end_offset[tp] - 3)

    # Grab the last 3 messages, reverse their order, then find the latest runstart by iterating through.
    messages = []
    for index, message in enumerate(runinfo_consumer):
        messages.append(message.value)
        if index == 3:  # Final index = 2 for 3 messages  # noqa: PLR2004
            break
    if len(messages) < 1:
        raise TopicIncompleteError(
            "Topic does not have any messages from which to read. %s",
            f"{instrument}_runInfo",
        )
    for message in reversed(messages):
        if RunStart.RunStartBufferHasIdentifier(message, 0):
            latest_start = RunStart.GetRootAsRunStart(message, 0)
            break
    else:
        messages = [
            f"Run Stop at {datetime_from_record_timestamp(RunStop.GetRootAsRunStop(message, 0).StopTime())}"
            if RunStop.RunStopBufferHasIdentifier(message, 0)
            else "Unknown message (Not run start or run stop)"
            for message in messages
        ]
        logger.warning("No run start found in runinfo topic. Messages: %s", messages)
    return latest_start


def seek_event_consumer_to_runstart(
    instrument: str,
    run_start: RunStart,
    events_consumer: KafkaConsumer,
    streaming_kafka_sample_log: bool = False,
) -> None:
    """
    Adjust the given event consumer's position to the beginning of the given run
    :param run_start: The run start message to seek to.
    :param events_consumer: The event consumer to seek to the run start message in.
    :return: None
    """

    run_start_ms = run_start.StartTime()
    timestamp = datetime_from_record_timestamp(run_start_ms)
    logger.info("Seeking event consumer to run start: %s", timestamp)
    topics = (
        [f"{instrument}_events", f"{instrument}_sampleEnv"]
        if streaming_kafka_sample_log
        else [f"{instrument}_events"]
    )
    # Find the offset for a given time and seek to it
    for topic in topics:
        tp = TopicPartition(topic, 0)
        offset_info = events_consumer.offsets_for_times({tp: run_start_ms})[tp]
        if offset_info is None or offset_info.offset is None:
            logger.warning(
                "There are no events at the offset time for %s. Is the instrument waiting? See: "
                "https://isiscomputinggroup.github.io/WebDashboard/instruments",
                topic,
            )
            sys.exit(0)
        events_consumer.seek(tp, offset_info.offset)


def setup_consumers(
    instrument: str,
    kafka_config: dict[str, Any],
    kafka_sample_log_streaming: bool = False,
) -> tuple[KafkaConsumer, KafkaConsumer]:
    """
    Create the consumers for events and runinfo
    :return: A tuple of KafkaConsumer objects for events and runinfo.
    """
    events_consumer = KafkaConsumer(**kafka_config)
    runinfo_consumer = KafkaConsumer(
        f"{instrument}_runInfo", consumer_timeout_ms=10000, **kafka_config
    )
    if kafka_sample_log_streaming:
        events_consumer.subscribe([f"{instrument}_events", f"{instrument}_sampleEnv"])
    else:
        events_consumer.subscribe([f"{instrument}_events"])
    return events_consumer, runinfo_consumer
