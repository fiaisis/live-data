import sys
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest

import live_data_processor.scripts as scripts


@pytest.fixture
def script_env(tmp_path, monkeypatch):
    """
    Give each test an isolated script file and import path.

    Disable bytecode writes so rapid script updates in tests cannot reuse stale cached code.

    :param tmp_path: The temporary directory for the script.
    :param monkeypatch: The pytest fixture used to override the script path and bytecode settings.
    :return: A fixture that yields the temporary script path.
    """
    # Disable bytecode caching to prevent timestamp race conditions during testing
    monkeypatch.setattr(sys, "dont_write_bytecode", True)

    temp_script_path = tmp_path / "reduction_script.py"

    monkeypatch.setattr(scripts, "REDUCTION_SCRIPT_PATH", temp_script_path)
    sys.path.insert(0, str(tmp_path))

    yield temp_script_path

    sys.path.remove(str(tmp_path))
    if "reduction_script" in sys.modules:
        del sys.modules["reduction_script"]


def test_get_script_success():
    """Test that get_script correctly extracts the JSON on a 200 OK response."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = "def execute(): print('hello')"
        mock_get.return_value = mock_response

        result = scripts.get_script("MERLIN")

        assert result == "def execute(): print('hello')"
        mock_get.assert_called_once()
        assert "merlin" in mock_get.call_args[0][0]


def test_get_script_failure():
    """Test that get_script gracefully returns None if the API fails."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = HTTPStatus.NOT_FOUND
        mock_get.return_value = mock_response

        result = scripts.get_script("LARMOR")

        assert result is None


def test_write_reduction_script(script_env):
    """Test that the script writes exactly to the configured Path."""
    script_content = "def execute(): pass"
    scripts.write_reduction_script(script_content)

    assert script_env.exists()
    assert script_env.read_text() == script_content


def test_get_initial_reduction_function(script_env):
    """Test fetching and loading the initial script."""
    api_script = "def execute(): return 'initial'"

    with patch("live_data_processor.scripts.get_script", return_value=api_script):
        # Call the initial fetch
        execute_func = scripts.get_initial_reduction_function("MERLIN")

        # Verify it wrote to disk and loaded correctly
        assert script_env.read_text() == api_script
        assert execute_func() == "initial"


def test_refresh_reduction_function_no_update(script_env):
    """Test that if the API matches the disk, no reload occurs."""
    # 1. Setup the existing state
    current_script = "def execute(): return 'version_1'"
    script_env.write_text(current_script)

    # Dummy function to represent our current state in memory
    def dummy_func():
        pass

    # 2. Mock API to return the EXACT same string as what is on disk
    with patch("live_data_processor.scripts.get_script", return_value=current_script):
        with patch("importlib.reload") as mock_reload:
            returned_func = scripts.refresh_reduction_function(dummy_func, "MERLIN")

            # Assert we returned the exact same function memory reference
            assert returned_func is dummy_func
            # Assert importlib.reload was NOT called
            mock_reload.assert_not_called()


def test_refresh_reduction_function_with_update(script_env):
    """Test that a new script from the API triggers a disk write and a module reload."""
    # 1. Setup the old state on disk
    old_script = "def execute(): return 'old_version'"
    script_env.write_text(old_script)

    # We must actually import it first so importlib.reload has something to reload
    import reduction_script

    old_func = reduction_script.execute

    # 2. Setup the new script that the API will return
    new_script = "def execute(): return 'new_version'"

    # 3. Mock the API and call refresh
    with patch("live_data_processor.scripts.get_script", return_value=new_script):
        returned_func = scripts.refresh_reduction_function(old_func, "MERLIN")

        # Verify the file on disk was overwritten
        assert script_env.read_text() == new_script

        # Verify the returned function is different from the old one
        assert returned_func is not old_func

        # Verify the newly loaded function actually runs the new code
        assert returned_func() == "new_version"


def test_refresh_reduction_api_failure_keeps_old_func(script_env):
    """Test that an API outage doesn't crash the loop or lose the function."""

    def old_func():
        pass

    # Mock get_script returning None (API failure)
    with patch("live_data_processor.scripts.get_script", return_value=None):
        returned_func = scripts.refresh_reduction_function(old_func, "MERLIN")

        # Should return the same function reference
        assert returned_func is old_func
