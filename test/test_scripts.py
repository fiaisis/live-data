from unittest.mock import MagicMock, patch

import pytest

MODULE = "live_data_processor.scripts"


@patch(f"{MODULE}.requests.get")
def test_get_script_success(mock_get):
    # First call returns branch info with commit sha
    branch_resp = MagicMock()
    branch_resp.json.return_value = {"commit": {"sha": "abc123"}}

    # Second call returns the raw script content
    raw_resp = MagicMock()
    raw_resp.status_code = 200
    raw_resp.text = "print('hello world')\n"

    mock_get.side_effect = [branch_resp, raw_resp]

    from live_data_processor.scripts import get_script

    script = get_script("MERLIN")

    # Returned script content is as provided by second response
    assert script == "print('hello world')\n"

    # Verify the two calls and URLs
    assert mock_get.call_count == 2
    first_call_args, first_call_kwargs = mock_get.call_args_list[0]
    second_call_args, second_call_kwargs = mock_get.call_args_list[1]

    # First call: GitHub API for branch details
    assert first_call_args[0] == ("https://api.github.com/repos/fiaisis/autoreduction-scripts/branches/main")
    # Second call: raw script with sha and instrument path; includes timeout
    assert "abc123" in second_call_args[0]
    assert "/MERLIN/live_data.py" in second_call_args[0]
    assert second_call_kwargs.get("timeout") == 30


@patch(f"{MODULE}.requests.get")
def test_get_script_raises_on_non_ok_status(mock_get):
    # First call returns a sha
    branch_resp = MagicMock()
    branch_resp.json.return_value = {"commit": {"sha": "deadbeef"}}

    # Second call simulates a failure (e.g., 404)
    raw_resp = MagicMock()
    raw_resp.status_code = 404
    raw_resp.text = "Not Found"

    mock_get.side_effect = [branch_resp, raw_resp]

    from live_data_processor.scripts import get_script

    with pytest.raises(RuntimeError):
        _ = get_script("SciDemo")
