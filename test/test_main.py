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
