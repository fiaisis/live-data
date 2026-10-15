import json
from unittest.mock import MagicMock, patch


from live_data_processor import run_monitor


@patch("live_data_processor.run_monitor.find_latest_run_start")
@patch("live_data_processor.run_monitor.redis.Redis")
@patch("live_data_processor.run_monitor.KafkaConsumer")
def test_main_publishes_initial_run_state(
    mock_kafka_consumer, mock_redis, mock_find_latest
):
    mock_run_start = MagicMock()
    mock_run_start.RunName.return_value = b"MERLIN-001"
    mock_run_start.StartTime.return_value = 1710000000000
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
    assert loaded["start_timestamp"] == "2024-03-09 16:00:00"


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


def test_decode_value_returns_value_for_string():
    """Test that _decode_value returns the value if it's a string."""
    value = "test_string"
    result = run_monitor._decode_value(value)
    assert result == value


def test_extract_run_name_returns_none_for_no_run_start():
    """Test that extract_run_name returns None if run_start is None."""
    result = run_monitor.extract_run_name(None)
    assert result is None


def test_extract_run_name_returns_none_if_raw_name_is_none():
    """Test that extract_run_name returns None if RunName() returns None."""
    mock_run_start = MagicMock()
    mock_run_start.RunName.return_value = None
    result = run_monitor.extract_run_name(mock_run_start)
    assert result is None


def test_run_monitor_main_detects_run_start_message_in_partition_records():
    """Test that _is_run_start_message correctly identifies a run start message."""
    mock_message = MagicMock()
    mock_message.value = b"fake"
    with patch("live_data_processor.run_monitor.get_schema", return_value="pl72"):
        assert run_monitor._is_run_start_message(mock_message) is True

    with patch("live_data_processor.run_monitor.get_schema", return_value="other"):
        assert run_monitor._is_run_start_message(mock_message) is False
