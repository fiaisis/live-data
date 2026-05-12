import sys
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest

import live_data_processor.scripts as scripts


@pytest.fixture
def script_env(tmp_path, monkeypatch):
    """
    Safely test dynamic imports and file I/O by rerouting REDUCTION_SCRIPT_PATH
    to a temporary directory and adding it to sys.path.

    CRITICAL FIX: BYTECODE CACHE RACE CONDITION - RE: test_refresh_reduction_function_with_update
    - Bug: Unit tests write the "old" and "new" scripts within the same second.
      Because the OS file modification timestamp resolution is 1 second, the timestamps
      match exactly. `importlib.reload()` sees the matching timestamp and incorrectly
      loads the old cached `.pyc` bytecode instead of the newly written file.
    - Fix: `sys.dont_write_bytecode = True` forces Python to bypass the cache
      and read from disk every time. (This is a test-only issue; production updates
      happen on longer timescales, naturally avoiding the timestamp collision).

    THE SLEEP EXPERIMENT (To verify):
    1. Comment out the `dont_write_bytecode` line below.
    2. Run the tests. The update test will fail.
    3. Add `import time; time.sleep(1.5)` right before writing the new script in the test.
    4. Run the tests again. They will pass, proving the OS timestamp resolution is the culprit.
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
