from unittest.mock import MagicMock, Mock

import pytest

from live_data_processor.runs import RunContext, _create_run_identifier


@pytest.fixture()
def mock_runstart():
    mock = Mock()
    mock.StartTime.return_value = 1234567890
    mock.RunName.return_value = "test_run"
    return mock


def test_create_run_identifier_returns_none_if_input_is_none():
    assert _create_run_identifier(None) is None


def test_create_run_identifier_returns_tuple_of_strings(mock_runstart):
    result = _create_run_identifier(mock_runstart)
    assert result == ("1234567890", "test_run")
    assert all(isinstance(x, str) for x in result)


def test_create_run_identifier_propagates_values_from_runstart():
    mock = MagicMock()
    mock.StartTime.return_value = 42
    mock.RunName.return_value = "foo"
    result = _create_run_identifier(mock)
    assert result == ("42", "foo")


def test_runcontext_initialization_stores_callable(mock_runstart):
    def fake_get_run():
        return mock_runstart

    ctx = RunContext(get_current_run=fake_get_run)
    assert ctx.get_current_run is fake_get_run


def test_runcontext_get_current_run_returns_expected_value(mock_runstart):
    def fake_get_run():
        return mock_runstart

    ctx = RunContext(fake_get_run)
    assert ctx.get_current_run() is mock_runstart
