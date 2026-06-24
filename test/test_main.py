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
@patch("live_data_processor.main.find_latest_run_start")
@patch("live_data_processor.main.AddTimeSeriesLog")
@patch("live_data_processor.main.RemoveWorkspaceHistory")
@patch("live_data_processor.main.mtd")
def test_start_live_reduction_reads_valkey(
    mock_mtd,
    mock_remove,
    mock_add,
    mock_find_latest,
    mock_refresh,
    mock_process,
    mock_init_run,
):
    """Test that start_live_reduction reads from Valkey when execution interval is reached."""
    from live_data_processor.main import start_live_reduction, VALKEY_CLIENT
    import datetime

    mock_init_run.return_value = MagicMock(spec=RunStart)

    # Mock events_consumer to yield exactly one message
    mock_events_consumer = MagicMock()
    mock_events_consumer.__iter__.return_value = [MagicMock()]
    mock_runinfo_consumer = MagicMock()

    # Make find_latest_run_start return a new RunStart to break the main `while True` loop
    new_run_start = MagicMock(spec=RunStart)
    mock_find_latest.return_value = new_run_start

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

    # Force datetime to trigger execution interval immediately
    class MockDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            # Return a time far in the future so that the interval > SCRIPT_EXECUTION_INTERVAL is True
            return datetime.datetime(2050, 1, 1, tzinfo=datetime.UTC)

    with patch("live_data_processor.main.datetime.datetime", MockDatetime):
        # We need to catch the StopIteration or just let it break out of the while loop
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
