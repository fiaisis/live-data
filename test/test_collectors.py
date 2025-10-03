import types
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from live_data_processor.collectors import MerlinCollector, get_misc_data_collector

MODULE = "live_data_processor.collectors"


class FakeCtx:
    """Minimal RunContext double with a stable get_current_run()."""

    def __init__(self, run_obj=None):
        self._run = run_obj if run_obj is not None else object()

    def get_current_run(self):
        return self._run


def test_merlincollector_will_run_forever():
    mc = MerlinCollector()
    assert mc.will_run_forever is True


@patch(f"{MODULE}.Path")
@patch(f"{MODULE}.caget", return_value=12.34)
@patch(f"{MODULE}._create_run_identifier", side_effect=["RID", "RID"])
@patch(f"{MODULE}.datetime")
@patch(f"{MODULE}.time.sleep")
@patch(f"{MODULE}.time.monotonic")
def test_run_forever_logs_on_success(
    mock_monotonic, mock_sleep, mock_datetime, mock_create_id, mock_caget, mock_path, tmp_path
):
    # Arrange monotonic so that: next_tick = 0, delay = 1.0 -> sleep called once.
    mock_monotonic.side_effect = [0.0, 0.0]

    # Make sleep set the stop event so we exit after one loop
    stop_event_set = {"called": False}

    def sleep_side_effect(delay):
        assert delay == pytest.approx(1.0)
        stop_event_set["called"] = True
        # nothing else

    mock_sleep.side_effect = sleep_side_effect

    # Fix the timestamp
    mock_now = MagicMock()
    mock_now.isoformat.return_value = "2025-01-01T00:00:00"
    mock_datetime.datetime.now.return_value = mock_now

    ctx = FakeCtx()
    stop_event = types.SimpleNamespace(is_set=lambda: stop_event_set["called"])

    # Set log file
    mock_path.return_value = Path(tmp_path, "merlin-rot-log")

    # Execute
    MerlinCollector().run_forever(ctx, stop_event)

    # Assert EPICS was read and Mantid was written once with expected args
    mock_caget.assert_called_once_with("IN:MERLIN:CS:SB:Rot", timeout=0.5)
    with Path(tmp_path, "merlin-rot-log").open("r") as fle:
        assert fle.read() == "2025-01-01T00:00:00,12.34\n"


@patch(f"{MODULE}.Path")
@patch(f"{MODULE}.caget", side_effect=TimeoutError)
@patch(f"{MODULE}._create_run_identifier", side_effect=["RID", "RID"])
@patch(f"{MODULE}.time.sleep")
@patch(f"{MODULE}.time.monotonic")
def test_run_forever_skips_on_timeout(
    mock_monotonic,
    mock_sleep,
    mock_create_id,
    mock_caget,
    mock_path,
):
    mock_monotonic.side_effect = [0.0, 0.0]

    stop = {"stop": False}

    def sleep_side_effect(delay):
        stop["stop"] = True

    mock_sleep.side_effect = sleep_side_effect

    ctx = FakeCtx()
    stop_event = types.SimpleNamespace(is_set=lambda: stop["stop"])

    MerlinCollector().run_forever(ctx, stop_event)

    mock_caget.assert_called_once()
    mock_path.return_value.open.assert_not_called()


@patch(f"{MODULE}.logger")
@patch(f"{MODULE}.Path")
@patch(f"{MODULE}.caget", side_effect=Exception("EPICS down"))
@patch(f"{MODULE}._create_run_identifier", side_effect=["RID", "RID"])
@patch(f"{MODULE}.time.sleep")
@patch(f"{MODULE}.time.monotonic")
def test_run_forever_logs_warning_on_exception(
    mock_monotonic,
    mock_sleep,
    mock_create_id,
    mock_caget,
    mock_path,
    mock_logger,
):
    mock_monotonic.side_effect = [0.0, 0.0]

    stop = {"stop": False}
    mock_sleep.side_effect = lambda d: stop.update(stop=True)
    ctx = FakeCtx()
    stop_event = Mock()
    stop_event.is_set.side_effect = lambda: stop["stop"]

    MerlinCollector().run_forever(ctx, stop_event)

    mock_caget.assert_called_once()
    mock_path.return_value.open.assert_not_called()
    # Ensure a warning was emitted with exc_info
    assert mock_logger.warning.call_count == 1
    _, kwargs = mock_logger.warning.call_args
    assert "exc_info" in kwargs
    assert isinstance(kwargs["exc_info"], Exception)


@patch(f"{MODULE}.Path")
@patch(f"{MODULE}.caget", return_value=1.23)
@patch(f"{MODULE}._create_run_identifier", side_effect=["A", "B"])
@patch(f"{MODULE}.time.sleep")
@patch(f"{MODULE}.time.monotonic")
def test_run_forever_skips_on_run_id_change(
    mock_monotonic,
    mock_sleep,
    mock_create_id,
    mock_caget,
    mock_path,
):
    mock_monotonic.side_effect = [0.0, 0.0]
    stop = {"stop": False}
    mock_sleep.side_effect = lambda d: stop.update(stop=True)

    ctx = FakeCtx()
    stop_event = Mock()
    stop_event.is_set.side_effect = lambda: stop["stop"]

    MerlinCollector().run_forever(ctx, stop_event)

    mock_caget.assert_called_once()
    mock_path.return_value.open.assert_not_called()


@pytest.mark.parametrize(
    ("name", "expected_cls"),
    [
        ("merlin", "MerlinCollector"),
        ("MERLIN", "MerlinCollector"),
        ("scidemo", "MerlinCollector"),
        ("SciDemo", "MerlinCollector"),
    ],
)
def test_get_misc_data_collector_supported(name, expected_cls):
    collector = get_misc_data_collector(name)
    assert isinstance(collector, MerlinCollector), f"{name} should map to {expected_cls}"


@pytest.mark.parametrize("name", ["", "unknown", "isis", "mari", "NONSENSE"])
def test_get_misc_data_collector_unsupported_raises(name):
    with pytest.raises(ValueError) as excinfo:  # noqa: PT011
        _ = get_misc_data_collector(name)
    assert "Unsupported instrument" in str(excinfo.value)
