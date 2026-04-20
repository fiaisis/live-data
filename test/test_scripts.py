from http import HTTPStatus
from unittest.mock import MagicMock, mock_open, patch

from live_data_processor.scripts import (
    get_reduction_function,
    get_script,
    refresh_reduction_function,
    write_reduction_script,
)


@patch("live_data_processor.scripts.requests.get")
def test_get_script_success(mock_get):
    """Test getting script returns script on success."""
    mock_response = MagicMock()
    mock_response.status_code = HTTPStatus.OK
    mock_response.json.return_value = "new script content"
    mock_get.return_value = mock_response

    script = get_script("MERLIN")

    assert script == "new script content"
    mock_get.assert_called_once()
    assert "merlin" in mock_get.call_args[0][0]


@patch("live_data_processor.scripts.requests.get")
def test_get_script_failure(mock_get):
    """Test getting script returns None on failure."""
    mock_response = MagicMock()
    mock_response.status_code = HTTPStatus.NOT_FOUND
    mock_get.return_value = mock_response

    script = get_script("MERLIN")

    assert script is None
    mock_get.assert_called_once()


@patch("pathlib.Path.open", new_callable=mock_open)
def test_write_reduction_script(mock_file_open):
    """Test that writing the script writes to the file successfully."""
    write_reduction_script("def test_func():\n    pass")
    mock_file_open.assert_called_once_with("w")
    mock_file_open().write.assert_called_once_with("def test_func():\n    pass")


@patch("live_data_processor.scripts.importlib.reload")
@patch("live_data_processor.scripts.write_reduction_script")
@patch("live_data_processor.scripts.get_script")
def test_get_reduction_function(mock_get_script, mock_write, mock_reload):
    """Test get_reduction_function downloads and reloads the script."""
    mock_get_script.return_value = "script body"

    # Do something better than this test
    import sys

    mock_script_module = MagicMock()
    sys.modules["reduction_script"] = mock_script_module

    fun = get_reduction_function("MERLIN")

    assert fun == mock_script_module.execute
    mock_get_script.assert_called_with("MERLIN")
    mock_write.assert_called_with("script body")
    mock_reload.assert_called_once_with(mock_script_module)


@patch("live_data_processor.scripts.inspect.getsource")
@patch("live_data_processor.scripts.get_reduction_function")
def test_refresh_reduction_function_new_script(mock_get_reduction, mock_getsource):
    """Test that refresh updates the function if the source is different."""

    def old_fun():
        pass

    def new_fun():
        pass

    mock_get_reduction.return_value = new_fun
    # first call is new script, second is old script (for comparison)
    mock_getsource.side_effect = ["new source", "old source"]

    result = refresh_reduction_function(old_fun, "MERLIN")

    assert result is new_fun


@patch("live_data_processor.scripts.inspect.getsource")
@patch("live_data_processor.scripts.get_reduction_function")
def test_refresh_reduction_function_same_script(mock_get_reduction, mock_getsource):
    """Test that refresh ignores updates if the source is the same."""

    def old_fun():
        pass

    def new_fun():
        pass

    mock_get_reduction.return_value = new_fun
    mock_getsource.side_effect = ["same source", "same source"]

    result = refresh_reduction_function(old_fun, "MERLIN")

    assert result is old_fun and result is not new_fun


@patch("live_data_processor.scripts.get_reduction_function")
def test_refresh_reduction_function_error(mock_get_reduction):
    """Test that refresh ignores updates and logs warning on error."""

    def old_fun():
        pass

    mock_get_reduction.side_effect = RuntimeError("Network Error")

    result = refresh_reduction_function(old_fun, "MERLIN")

    assert result is old_fun
