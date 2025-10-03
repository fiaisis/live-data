import logging
import os
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaConsumer, TopicPartition

from live_data_processor.exceptions import TopicIncompleteError
from schemas.compiled_schemas.RunStart import RunStart

logger = logging.getLogger(__name__)

KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))


def datetime_from_record_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp / 1000.0, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def find_latest_run_start(runinfo_consumer: KafkaConsumer, instrument: str) -> RunStart | None:
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
        if index == 2:  # noqa: PLR2004 # Final index = 2 if 3 messages
            break
    if len(messages) < 1:
        raise TopicIncompleteError("Topic does not have any messages from which to read. %s", f"{instrument}_runInfo")
    for message in reversed(messages):
        if RunStart.RunStartBufferHasIdentifier(message, 0):
            latest_start = RunStart.GetRootAs(message, 0)
            break
    return latest_start


def seek_event_consumer_to_runstart(instrument: str, run_start: RunStart, events_consumer: KafkaConsumer) -> None:
    """
    Adjust the given event consumer's position to the beginning of the given run
    :param run_start: The run start message to seek to.
    :param events_consumer: The event consumer to seek to the run start message in.
    :return: None
    """
    logger.info("Seeking event consumer to run start")
    run_start_ms = run_start.StartTime()

    # Find the offset for a given time and seek to it
    topic_partition = TopicPartition(f"{instrument}_events", 0)
    offset_info = events_consumer.offsets_for_times({topic_partition: run_start_ms})[topic_partition]
    events_consumer.seek(topic_partition, offset_info.offset)


def setup_consumers(instrument: str, kafka_config: dict[str, Any]) -> tuple[KafkaConsumer, KafkaConsumer]:
    """
    Create the consumers for events and runinfo
    :return: A tuple of KafkaConsumer objects for events and runinfo.
    """
    events_consumer = KafkaConsumer(f"{instrument}_events", **kafka_config)
    runinfo_consumer = KafkaConsumer(f"{instrument}_runInfo", **kafka_config)
    return events_consumer, runinfo_consumer
