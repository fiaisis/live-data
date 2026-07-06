from unittest.mock import MagicMock, patch

import pytest
import redis

from live_data_processor.exceptions import SampleLogError
from live_data_processor.epics_streamer import main


@patch("live_data_processor.epics_streamer.init_pvs")
@patch("live_data_processor.epics_streamer.VALKEY_CLIENT")
def test_main_valkey_xadd(mock_valkey_client, mock_init_pvs):
    """Test that the main loop reads from the event queue and calls Valkey XADD."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    # Simulate queue returning an item, then raising Exception to break the infinite loop
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    with patch(
        "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
    ):
        with pytest.raises(Exception, match="Break loop"):
            main()

    mock_valkey_client.xadd.assert_called_once()
    args, kwargs = mock_valkey_client.xadd.call_args
    # args[0] is STREAM_KEY, args[1] is the fields dict
    assert "epics_stream" in args[0]
    fields = args[1]
    assert fields["block_name"] == "TestBlock"
    assert fields["value"] == "42.5"
    assert fields["timestamp"] == "2023-11-14T22:13:20+00:00"
    assert kwargs["maxlen"] == 1000


@patch("live_data_processor.epics_streamer.init_pvs")
@patch("live_data_processor.epics_streamer.VALKEY_CLIENT")
def test_main_valkey_connection_error_handled(mock_valkey_client, mock_init_pvs):
    """Test that Valkey connection errors are caught and logged."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    # Force XADD to raise a ConnectionError, which should be caught and sleep for 1 sec
    mock_valkey_client.xadd.side_effect = redis.ConnectionError("Connection lost")

    with patch(
        "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
    ):
        with patch("live_data_processor.epics_streamer.time.sleep") as mock_sleep:
            with pytest.raises(Exception, match="Break loop"):
                main()
            mock_sleep.assert_called_once_with(1)


@patch("live_data_processor.epics_streamer.init_pvs")
@patch("live_data_processor.epics_streamer.VALKEY_CLIENT")
def test_main_valkey_other_error_raises(mock_valkey_client, mock_init_pvs):
    """Test that non-connection Valkey errors raise SampleLogError."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    mock_valkey_client.xadd.side_effect = Exception("Unexpected error")

    with patch(
        "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
    ):
        with pytest.raises(SampleLogError, match="Failed to write to Valkey stream"):
            main()
