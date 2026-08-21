from unittest.mock import MagicMock, patch

import pytest
import redis

from live_data_processor.exceptions import SampleLogError
from live_data_processor.epics_streamer import main


@pytest.fixture
def valkey_client_mock(monkeypatch):
    """Provide a shared mock that replaces the module-level VALKEY_CLIENT.

    This mimics the valkey client behavior used by epics_streamer and allows
    tests to control xadd side effects and assertions in a single place.

    Also patch _format_timestamp to a deterministic ISO string to avoid any
    environment-dependent timezone/formatting issues during tests.
    """
    client = MagicMock()
    monkeypatch.setattr("live_data_processor.epics_streamer.VALKEY_CLIENT", client)
    monkeypatch.setattr(
        "live_data_processor.epics_streamer._format_timestamp",
        lambda ts: "2023-11-14T22:13:20+00:00",
    )
    return client


@patch("live_data_processor.epics_streamer.init_pvs")
def test_main_valkey_xadd(mock_init_pvs, valkey_client_mock):
    """Test that the main loop reads from the event queue and calls Valkey XADD."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    # Simulate queue returning an item, then raising Exception to break the infinite loop
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    # Prevent setup_loggers from creating Valkey handlers that use queue.Queue
    with patch(
        "live_data_processor.epics_streamer.setup_loggers",
        return_value=(MagicMock(), MagicMock(), "stream"),
    ):
        with patch(
            "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
        ):
            with pytest.raises(Exception, match="Break loop"):
                main()

    valkey_client_mock.xadd.assert_called_once()
    args, kwargs = valkey_client_mock.xadd.call_args
    # args[0] is STREAM_KEY, args[1] is the fields dict
    assert "epics_stream" in args[0]
    fields = args[1]
    assert fields["block_name"] == "TestBlock"
    assert fields["value"] == "42.5"
    assert fields["timestamp"] == "2023-11-14T22:13:20+00:00"
    assert kwargs["maxlen"] == 10000


@patch("live_data_processor.epics_streamer.init_pvs")
def test_main_valkey_connection_error_handled(mock_init_pvs, valkey_client_mock):
    """Test that Valkey connection errors are caught and logged."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    # Force XADD to raise a ConnectionError, which should be caught and sleep for 1 sec
    valkey_client_mock.xadd.side_effect = redis.ConnectionError("Connection lost")

    # Prevent setup_loggers from creating Valkey handlers that use queue.Queue
    with patch(
        "live_data_processor.epics_streamer.setup_loggers",
        return_value=(MagicMock(), MagicMock(), "stream"),
    ):
        with patch(
            "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
        ):
            with patch("live_data_processor.epics_streamer.time.sleep") as mock_sleep:
                with pytest.raises(Exception, match="Break loop"):
                    main()
                mock_sleep.assert_called_once_with(1)


@patch("live_data_processor.epics_streamer.init_pvs")
def test_main_valkey_other_error_raises(mock_init_pvs, valkey_client_mock):
    """Test that non-connection Valkey errors raise SampleLogError."""
    mock_init_pvs.return_value = {"pv1": MagicMock()}

    mock_queue = MagicMock()
    mock_queue.get.side_effect = [
        ("TestBlock", 42.5, 1700000000000000000),
        Exception("Break loop"),
    ]

    valkey_client_mock.xadd.side_effect = Exception("Unexpected error")

    # Prevent setup_loggers from creating Valkey handlers that use queue.Queue
    with patch(
        "live_data_processor.epics_streamer.setup_loggers",
        return_value=(MagicMock(), MagicMock(), "stream"),
    ):
        with patch(
            "live_data_processor.epics_streamer.queue.Queue", return_value=mock_queue
        ):
            with pytest.raises(
                SampleLogError, match="Failed to write to Valkey stream"
            ):
                main()
