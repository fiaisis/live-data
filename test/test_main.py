import logging
from unittest.mock import MagicMock, patch

import pytest
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart

from live_data_processor.exceptions import TopicIncompleteError
from live_data_processor.main import initialize_run

# clear to prevent redis conn
logging.getLogger("EXTERNAL").handlers.clear()


@pytest.fixture(scope="module", autouse=True)
def main_mocks():
    with (
        patch("live_data_processor.main.VALKEY_CLIENT"),
        patch("live_data_processor.main.external_logger"),
        patch("live_data_processor.main.internal_logger"),
    ):
        yield


@patch("live_data_processor.main.seek_event_consumer_to_runstart")
@patch("live_data_processor.main.initialize_instrument_workspace")
@patch("live_data_processor.main.find_latest_run_start")
def test_initialize_run_success(mock_find, mock_workspace, mock_seek):
    """Test initializing a run connects everything correctly."""
    consumer = MagicMock()
    runinfo = MagicMock()
    mock_run_start = MagicMock(spec=RunStart)

    # We don't provide run start, it should fall back to find_latest_run_start
    mock_find.return_value = mock_run_start

    result = initialize_run(consumer, runinfo, run_start=None)

    assert result == mock_run_start
    mock_find.assert_called_once_with(runinfo, "MERLIN")
    mock_workspace.assert_called_once_with("MERLIN", "lives", mock_run_start)
    mock_seek.assert_called_once_with("MERLIN", mock_run_start, consumer)


@patch("live_data_processor.main.find_latest_run_start")
def test_initialize_run_missing_start(mock_find):
    """Test TopicIncompleteError is raised if no RunStart can be found."""
    consumer = MagicMock()
    runinfo = MagicMock()

    mock_find.return_value = None

    with pytest.raises(TopicIncompleteError):
        initialize_run(consumer, runinfo, run_start=None)


@patch("live_data_processor.main.initialize_run")
@patch("live_data_processor.main.process_message")
@patch("live_data_processor.main.refresh_reduction_function")
@patch("live_data_processor.main.get_initial_reduction_function")
@patch("live_data_processor.main.find_latest_run_start")
@patch("live_data_processor.main.AddTimeSeriesLog")
@patch("live_data_processor.main.RemoveWorkspaceHistory")
@patch("live_data_processor.main.mtd")
def test_start_live_reduction_reads_valkey(
    mock_mtd,
    mock_remove,
    mock_add,
    mock_find_latest,
    mock_get_initial,
    mock_refresh,
    mock_process,
    mock_init_run,
):
    """Test that start_live_reduction reads from Valkey when execution interval is reached."""
    from live_data_processor.main import start_live_reduction, VALKEY_CLIENT
    import datetime

    run_start_mock = MagicMock(spec=RunStart)
    # First call: return a valid RunStart. Second call: raise to exit the while True loop.
    mock_init_run.side_effect = [run_start_mock, StopIteration("break out of while loop")]

    # Mock events_consumer to yield exactly one message per iteration
    mock_message = MagicMock()
    mock_events_consumer = MagicMock()
    mock_events_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
    mock_runinfo_consumer = MagicMock()

    # Return None so the "new run" check doesn't trigger a break (we exit via init_run instead)
    mock_find_latest.return_value = None

    # Mock Valkey xrange to return some data
    VALKEY_CLIENT.xrange = MagicMock()
    VALKEY_CLIENT.xrange.return_value = [
        (
            "123-0",
            {
                "block_name": "TestBlock",
                "value": "10.5",
                "timestamp": "2023-01-01T12:00:00Z",
            },
        )
    ]

    # Force datetime to trigger execution interval immediately.
    # The first 3 calls to now() set the per-run timers (lines 235-237 in main.py).
    # Subsequent calls must return a later time so that (now - timer) > interval.
    now_call_count = 0

    class MockDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            nonlocal now_call_count
            now_call_count += 1
            if now_call_count <= 3:
                # Timer initialization: return a baseline time
                return datetime.datetime(2050, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            # Subsequent calls: return a time far enough ahead to trigger all intervals
            return datetime.datetime(2050, 1, 2, 0, 0, 0, tzinfo=datetime.UTC)

    with patch("live_data_processor.main.datetime.datetime", MockDatetime):
        with pytest.raises(StopIteration):
            start_live_reduction(
                mock_events_consumer,
                mock_runinfo_consumer,
                kafka_sample_log_streaming=False,
                epics_proc=None,
                epics_stop_event=None,
            )

    # Verify xrange was called with the correct stream key
    VALKEY_CLIENT.xrange.assert_called_once()
    args, _ = VALKEY_CLIENT.xrange.call_args
    assert "epics_stream" in args[0]
    assert args[1] == "-"
    assert args[2] == "+"

    # Verify the values were added to the workspace log
    mock_add.assert_called_once()
    mock_add.assert_called_with("lives", "TestBlock", "2023-01-01T12:00:00Z", "10.5")
