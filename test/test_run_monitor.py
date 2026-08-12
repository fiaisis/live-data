import json
from unittest.mock import MagicMock, patch

import pytest

from live_data_processor import run_monitor


@patch("live_data_processor.run_monitor.find_latest_run_start")
@patch("live_data_processor.run_monitor.redis.Redis")
@patch("live_data_processor.run_monitor.KafkaConsumer")
def test_main_publishes_initial_run_state(
    mock_kafka_consumer, mock_redis, mock_find_latest
):
    mock_run_start = MagicMock()
    mock_run_start.RunName.return_value = b"MERLIN-001"
    mock_run_start.StartTime.return_value = 1700000000000
    mock_find_latest.return_value = mock_run_start

    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    first_consumer = MagicMock()
    second_consumer = MagicMock()
    second_consumer.poll.side_effect = [{}, KeyboardInterrupt()]
    mock_kafka_consumer.side_effect = [first_consumer, second_consumer]

    run_monitor.main()

    mock_redis_client.set.assert_called_once()
    key, payload = mock_redis_client.set.call_args[0]
    assert "instrument:MERLIN:current_run" in key
    loaded = json.loads(payload)
    assert loaded["run_name"] == "MERLIN-001"
    assert loaded["start_timestamp"] == 1700000000000


@patch("live_data_processor.run_monitor.RunStart.GetRootAsRunStart")
@patch("live_data_processor.run_monitor.get_schema")
@patch("live_data_processor.run_monitor.find_latest_run_start")
@patch("live_data_processor.run_monitor.redis.Redis")
@patch("live_data_processor.run_monitor.KafkaConsumer")
def test_main_detects_new_run_start_message(
    mock_kafka_consumer,
    mock_redis,
    mock_find_latest,
    mock_get_schema,
    mock_get_root,
):
    mock_find_latest.return_value = None

    run_start = MagicMock()
    run_start.RunName.return_value = b"MERLIN-002"
    run_start.StartTime.return_value = 1710000000000
    mock_get_root.return_value = run_start
    mock_get_schema.return_value = "pl72"

    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    message = MagicMock()
    message.value = b"fake"
    poll_consumer = MagicMock()
    poll_consumer.poll.side_effect = [{None: [message]}, KeyboardInterrupt()]
    mock_kafka_consumer.side_effect = [MagicMock(), poll_consumer]

    run_monitor.main()

    mock_redis_client.set.assert_called_once()
    payload = mock_redis_client.set.call_args[0][1]
    loaded = json.loads(payload)
    assert loaded["run_name"] == "MERLIN-002"
