import logging
import os
from datetime import datetime

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndTimestamp

from live_data_processor.exceptions import TopicIncompleteError
from schemas.compiled_schemas.RunStart import RunStart


logger = logging.getLogger(__name__)

KAFKA_IP = os.environ.get("KAFKA_IP", "livedata.isis.cclrc.ac.uk")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "31092"))


def datetime_from_record_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp/1000.).strftime("%Y-%m-%d %H:%M:%S")

def find_latest_run_start(instrument: str, runinfo_consumer: KafkaConsumer, lookback=50) -> RunStart | None:
    """
    Find the latest RunStart message in the runinfo topic.

    This function seeks to the end of the runinfo topic, retrieves the last few messages,
    and searches for the most recent RunStart message.

    :param runinfo_consumer: The Kafka consumer for the runinfo topic.
    :return: The latest RunStart message if found, None otherwise.
    :raises TopicIncompleteError: If the topic does not have any messages.
    """
    logger.info("==== Starting find_latest_runstarts ====")
    topic=f"{instrument}_runInfo"
    # Discover partitions
    partition_ids = runinfo_consumer.partitions_for_topic(topic)
    logger.debug("Discovered partitions for topic %s: %s", topic, partition_ids)
    if not partition_ids:
        raise TopicIncompleteError(f"Topic {topic} has no partitions")

    partitions = [TopicPartition(topic, pid) for pid in partition_ids]

    # Find the end offset for each partition
    end_offsets = runinfo_consumer.end_offsets(partitions)  # {TopicPartition: end_offset}
    logger.debug("End offsets: %s", {p.partition: end_offsets[p] for p in partitions})
    if all(end_offsets[p] == 0 for p in partitions):
        raise TopicIncompleteError(f"Topic {topic} has no messages")

    # Assign and seek near the end of each partition
    runinfo_consumer.assign(partitions)
    for partition in partitions:
        start_offset = max(0, end_offsets[partition] - lookback)
        runinfo_consumer.seek(partition, start_offset)
        logger.info(
            "Seeking partition %d to offset %d (end=%d, lookback=%d)",
            partition.partition, start_offset, end_offsets[partition], lookback
        )

    # Read forward until we reach the end of each partition
    finished_partitions = set()
    runstarts = []  # (timestamp_ms, raw_bytes)

    logger.info("Polling for messages...")
    while len(finished_partitions) < len(partitions):
        records_map = runinfo_consumer.poll(timeout_ms=500)
        if not records_map:
            logger.debug("No messages returned in this poll")
            break

        for partition_obj, records in records_map.items():
            logger.debug("Got %d records from partition %d", len(records), partition_obj.partition)
            for record in records:
                # Collect Messages
                if RunStart.RunStartBufferHasIdentifier(record.value, 0):
                    ts_ms = record.timestamp
                    runstarts.append((ts_ms, record.value))
                    logger.info(
                        "Found RunStart at offset %d in partition %d at %s",
                        record.offset, partition_obj.partition, datetime_from_record_timestamp(record.timestamp)
                    )
                # Mark partition finished when we hit its last offset
                if record.offset >= end_offsets[partition_obj] - 1:
                    logger.info("Finished reading partition %d", partition_obj.partition)
                    finished_partitions.add(partition_obj)

    # Sort globally by timestamp and return the newest N
    runstarts.sort(key=lambda item: item[0])

    if len(runstarts) > 3:
        latest = runstarts[-3:]
    else:
        latest = runstarts

    logger.info("Total RunStarts found: %d", len(runstarts))
    logger.info("Returning %d latest RunStarts", len(latest))
    logger.info("==== Finished find_latest_runstarts ====")

    return [RunStart.GetRootAs(raw, 0) for _, raw in reversed(latest)][0] if latest else None


def seek_event_consumer_to_runstart(instrument: str, run_start: RunStart, events_consumer: KafkaConsumer) -> None:
    """
    Adjust the given event consumer's position to the beginning of the given run
    :param run_start: The run start message to seek to.
    :param events_consumer: The event consumer to seek to the run start message in.
    :return: None
    """
    logger.info("Seeking event consumer to run start")
    run_start_ms = run_start.StartTime() // 1000


    logger.info("Seeking event consumer to run start")
    logger.info("RunStart raw ts=%s, using ts_ms=%s", run_start.StartTime(), run_start_ms)

    topic = f"{instrument}_events"

    # Ensure we know partitions
    partition_ids = events_consumer.partitions_for_topic(topic)
    if not partition_ids:
        raise TopicIncompleteError(f"Topic {topic} has no partitions")

    partitions = [TopicPartition(topic, pid) for pid in partition_ids]

    events_consumer.assign(partitions)
    events_consumer.poll(timeout_ms=0)

    query = {tp: run_start_ms for tp in partitions}
    results = events_consumer.offsets_for_times(query)

    begins = events_consumer.beginning_offsets(partitions)
    ends = events_consumer.end_offsets(partitions)

    for tp in partitions:
        oat: OffsetAndTimestamp | None = results.get(tp)
        if oat is None:
            fallback = begins[tp]
            logger.warning(
                "No offset found at/after %s ms in %s. Seeking to beginning offset %s (end=%s).",
                run_start_ms,
                tp,
                fallback,
                ends[tp],
            )
            events_consumer.seek(tp, fallback)
        else:
            logger.info("Seeking %s to offset %s at event timestamp %s.", tp, oat.offset, oat.timestamp)
            events_consumer.seek(tp, oat.offset)


def setup_consumers() -> tuple[KafkaConsumer, KafkaConsumer]:
    """
    Create the consumers for events and runinfo
    :return: A tuple of KafkaConsumer objects for events and runinfo.
    """
    events_consumer = KafkaConsumer(bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], auto_offset_reset="earliest")
    runinfo_consumer = KafkaConsumer(bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], auto_offset_reset="earliest")
    return events_consumer, runinfo_consumer
