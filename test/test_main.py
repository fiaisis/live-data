import logging
from unittest.mock import MagicMock, patch

import pytest
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart

from live_data_processor.exceptions import TopicIncompleteError
from live_data_processor.main import find_new_run_start, initialize_run

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
    run_start_mock.RunName.return_value = b"MERLIN00075500"
    VALKEY_CLIENT.get.return_value = '{"run_name": "MERLIN00075500"}'
    # First call: return a valid RunStart. Second call: raise to exit the while True loop.
    mock_init_run.side_effect = [
        run_start_mock,
        StopIteration("break out of while loop"),
    ]

    # Check the run once before the reduction interval expires.
    mock_message = MagicMock()
    mock_events_consumer = MagicMock()
    mock_events_consumer.__iter__ = MagicMock(
        return_value=iter([mock_message, mock_message])
    )
    mock_runinfo_consumer = MagicMock()

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
            if now_call_count <= 5:
                return datetime.datetime(2050, 1, 1, 0, 0, 4, tzinfo=datetime.UTC)
            # The next message arrives after the reduction interval.
            return datetime.datetime(2050, 1, 2, 0, 0, 0, tzinfo=datetime.UTC)

    with patch("live_data_processor.main.datetime.datetime", MockDatetime):
        with pytest.raises(StopIteration):
            start_live_reduction(
                mock_events_consumer,
                mock_runinfo_consumer,
            )

    mock_find_latest.assert_not_called()

    # Verify xrange was called with the correct stream key
    VALKEY_CLIENT.xrange.assert_called_once()
    args, _ = VALKEY_CLIENT.xrange.call_args
    assert "epics_stream" in args[0]
    assert args[1] == "-"
    assert args[2] == "+"

    # Verify the Valkey EPICS stream key was deleted at the run boundary
    VALKEY_CLIENT.delete.assert_called_with("instrument:MERLIN:epics_stream")

    # Verify the values were added to the workspace log
    mock_add.assert_called_once()
    mock_add.assert_called_with("lives", "TestBlock", "2023-01-01T12:00:00Z", "10.5")


@pytest.mark.parametrize(
    ("reported_name", "candidate_name", "candidate_time", "should_switch"),
    [
        ("MERLIN00075472", "MERLIN00075500", 1000, False),
        ("MERLIN00075472", "MERLIN00075472", 500, False),
        ("MERLIN00075501", "MERLIN00075501", 2000, True),
    ],
)
@patch("live_data_processor.main.find_latest_run_start")
def test_find_new_run_start(
    mock_find, reported_name, candidate_name, candidate_time, should_switch
):
    from live_data_processor.main import VALKEY_CLIENT

    current = MagicMock(spec=RunStart)
    current.StartTime.return_value = 1000
    candidate = MagicMock(spec=RunStart)
    candidate.RunName.return_value = candidate_name.encode()
    candidate.StartTime.return_value = candidate_time
    mock_find.return_value = candidate
    VALKEY_CLIENT.get.return_value = f'{{"run_name": "{reported_name}"}}'

    result = find_new_run_start(MagicMock(), current, "MERLIN00075500")

    assert result is (candidate if should_switch else None)


def test_run_boundary_keeps_events_in_their_own_workspace(monkeypatch):
    import datetime
    from types import SimpleNamespace

    import live_data_processor.main as processor

    old_run = MagicMock(spec=RunStart)
    old_run.RunName.return_value = b"old-run"
    next_run = MagicMock(spec=RunStart)
    next_run.RunName.return_value = b"next-run"
    next_run.StartTime.return_value = 2000

    old_events = [
        SimpleNamespace(timestamp=timestamp, offset=timestamp)
        for timestamp in (1998, 1999)
    ]
    next_event = SimpleNamespace(timestamp=2000, offset=2000)
    events = MagicMock()
    # Reinitializing seeks back to the first record for the new run.
    events.__iter__.side_effect = [iter([*old_events, next_event]), iter([next_event])]
    initialize = MagicMock(side_effect=[old_run, next_run, StopIteration])
    process = MagicMock()
    lifecycle = MagicMock()
    lifecycle.attach_mock(initialize, "initialize")
    lifecycle.attach_mock(process, "process")
    detect = MagicMock(side_effect=[next_run, None])

    real_datetime = datetime.datetime
    baseline = real_datetime(2050, 1, 1, tzinfo=datetime.UTC)
    ticks = iter(range(0, 100, 5))

    class Clock(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return baseline + datetime.timedelta(seconds=next(ticks))

    monkeypatch.setattr(processor.datetime, "datetime", Clock)
    monkeypatch.setattr(processor, "initialize_run", initialize)
    monkeypatch.setattr(processor, "process_message", process)
    monkeypatch.setattr(processor, "find_new_run_start", detect)
    monkeypatch.setattr(processor, "get_initial_reduction_function", MagicMock())

    with pytest.raises(StopIteration):
        processor.start_live_reduction(events, MagicMock())

    assert [(call[0], call.args[-1]) for call in lifecycle.mock_calls] == [
        ("initialize", None),
        ("process", old_events[0]),
        ("process", old_events[1]),
        ("initialize", next_run),
        ("process", next_event),
        ("initialize", None),
    ]


def _make_ev42_event(
    pulse_time_ns: int,
    tofs: list[int],
    det_ids: list[int],
    isis_specific: dict[str, int | float] | None = None,
):
    """Build a real ev42 EventMessage FlatBuffer with optional is84 ISISData."""
    from streaming_data_types.eventdata_ev42 import serialise_ev42
    from streaming_data_types.fbschemas.eventdata_ev42.EventMessage import EventMessage

    buf = serialise_ev42(
        source_name="merlin",
        message_id=1,
        pulse_time=pulse_time_ns,
        time_of_flight=tofs,
        detector_id=det_ids,
        isis_specific=isis_specific,
    )
    return EventMessage.GetRootAsEventMessage(buf, 0)


def test_extract_isis_proton_charge_running_setup_and_none():
    """Verify is84 proton_charge extraction for RUNNING, SETUP, and missing ISISData."""
    from streaming_data_types.fbschemas.isis_event_info_is84.RunState import RunState

    from live_data_processor.main import extract_isis_proton_charge

    running_ev = _make_ev42_event(
        pulse_time_ns=1_795_684_408_723_078_656,
        tofs=[1000],
        det_ids=[1],
        isis_specific={
            "period_number": 0,
            "run_state": RunState.RUNNING,
            "proton_charge": 0.00125,
        },
    )
    setup_ev = _make_ev42_event(
        pulse_time_ns=1_795_684_408_723_078_656,
        tofs=[1000],
        det_ids=[1],
        isis_specific={
            "period_number": 0,
            "run_state": RunState.SETUP,
            "proton_charge": 0.00125,
        },
    )
    plain_ev = _make_ev42_event(
        pulse_time_ns=1_795_684_408_723_078_656,
        tofs=[1000],
        det_ids=[1],
        isis_specific=None,
    )

    assert extract_isis_proton_charge(running_ev) == pytest.approx(0.00125)
    assert extract_isis_proton_charge(setup_ev) is None
    assert extract_isis_proton_charge(plain_ev) is None


class _FakeTimeSeriesProp:
    """Minimal stand-in for Mantid FloatTimeSeriesProperty."""

    def __init__(self) -> None:
        self.entries: list[tuple[object, float]] = []

    def size(self) -> int:
        return len(self.entries)

    def lastTime(self) -> object:  # noqa: N802
        return self.entries[-1][0]

    def addValue(self, timestamp: object, value: float) -> None:  # noqa: N802
        self.entries.append((timestamp, value))


def test_process_events_deduplicates_proton_charge_per_pulse(monkeypatch):
    """Verify multiple ev42 cards sharing a pulse timestamp only record proton_charge once."""
    from streaming_data_types.fbschemas.isis_event_info_is84.RunState import RunState

    import live_data_processor.main as processor

    fake_prop = _FakeTimeSeriesProp()
    mock_run = MagicMock()
    mock_run.hasProperty.return_value = True
    mock_run.getProperty.return_value = fake_prop
    mock_ws = MagicMock()
    mock_ws.getRun.return_value = mock_run

    monkeypatch.setattr(processor, "mtd", {processor.LIVE_WS_NAME: mock_ws})
    monkeypatch.setattr(processor, "DateAndTime", lambda ns: f"dt:{ns}")

    pulse_1 = 1_795_684_408_723_078_656
    pulse_2 = 1_795_684_408_743_001_600
    isis_1 = {
        "period_number": 0,
        "run_state": RunState.RUNNING,
        "proton_charge": 0.00116,
    }
    isis_2 = {
        "period_number": 0,
        "run_state": RunState.RUNNING,
        "proton_charge": 0.00117,
    }

    # Three DAE cards for pulse_1, then one card for pulse_2
    processor.process_events(_make_ev42_event(pulse_1, [1000], [1], isis_1))
    processor.process_events(_make_ev42_event(pulse_1, [2000], [2], isis_1))
    processor.process_events(_make_ev42_event(pulse_1, [3000], [3], isis_1))
    processor.process_events(_make_ev42_event(pulse_2, [4000], [4], isis_2))

    assert len(fake_prop.entries) == 2
    assert (
        fake_prop.entries[0][0] == f"dt:{pulse_1 - processor.UNIX_TO_MANTID_OFFSET_NS}"
    )
    assert fake_prop.entries[0][1] == pytest.approx(0.00116)
    assert (
        fake_prop.entries[1][0] == f"dt:{pulse_2 - processor.UNIX_TO_MANTID_OFFSET_NS}"
    )
    assert fake_prop.entries[1][1] == pytest.approx(0.00117)


def test_process_events_records_proton_charge_on_empty_event_frame(monkeypatch):
    """Verify frames with zero neutron events still record proton_charge."""
    from streaming_data_types.fbschemas.isis_event_info_is84.RunState import RunState

    import live_data_processor.main as processor

    fake_prop = _FakeTimeSeriesProp()
    mock_run = MagicMock()
    mock_run.hasProperty.return_value = True
    mock_run.getProperty.return_value = fake_prop
    mock_ws = MagicMock()
    mock_ws.getRun.return_value = mock_run

    monkeypatch.setattr(processor, "mtd", {processor.LIVE_WS_NAME: mock_ws})
    monkeypatch.setattr(processor, "DateAndTime", lambda ns: f"dt:{ns}")

    pulse = 1_795_684_408_803_294_720
    empty_ev = _make_ev42_event(
        pulse,
        tofs=[],
        det_ids=[],
        isis_specific={
            "period_number": 0,
            "run_state": RunState.RUNNING,
            "proton_charge": 0.0012,
        },
    )

    processor.process_events(empty_ev)

    assert len(fake_prop.entries) == 1
    assert fake_prop.entries[0][1] == pytest.approx(0.0012)
    mock_ws.getSpectrum.assert_not_called()


def test_load_epics_logs_reads_stream_incrementally_and_backfills_from_hash(
    monkeypatch,
):
    """Verify _load_epics_logs drains stream incrementally and handles pre-run vs mid-run hash timestamps."""
    import datetime
    import json

    import live_data_processor.main as processor

    present_props: set[str] = set()

    def fake_add_log(_ws_name, log_name, timestamp, value):
        present_props.add(log_name)
        recorded_calls.append((log_name, timestamp, value))

    recorded_calls: list[tuple[str, str, str]] = []
    mock_run = MagicMock()
    mock_run.hasProperty.side_effect = lambda name: name in present_props
    mock_ws = MagicMock()
    mock_ws.getRun.return_value = mock_run

    mock_valkey = MagicMock()
    mock_valkey.xrange.side_effect = [
        [
            (
                "100-0",
                {
                    "block_name": "Chopper_Speed",
                    "value": "250.0",
                    "timestamp": "2026-11-26T09:01:00+00:00",
                },
            )
        ],
        [],
    ]
    mock_valkey.hgetall.return_value = {
        "Chopper_Speed": json.dumps(
            {"value": "250.0", "timestamp": "2026-11-26T09:01:00+00:00"}
        ),
        # Static config set before run start -> clamped to run_start_iso
        "Disc_Delay": json.dumps(
            {"value": "12400.0", "timestamp": "2026-11-25T15:00:00+00:00"}
        ),
        # Dynamic value changed mid-run -> preserved at its true timestamp, NOT backdated
        "Sample_Temp": json.dumps(
            {"value": "50.0", "timestamp": "2026-11-26T09:05:00+00:00"}
        ),
        # Malformed entry -> skipped safely
        "Bad_Block": "not-json",
    }

    monkeypatch.setattr(processor, "VALKEY_CLIENT", mock_valkey)
    monkeypatch.setattr(processor, "AddTimeSeriesLog", fake_add_log)

    run_start_dt = datetime.datetime(2026, 11, 26, 9, 0, 0, tzinfo=datetime.UTC)
    cursor = processor._EpicsCursor()
    processor._load_epics_logs(mock_ws, cursor, run_start_dt)

    assert cursor.last_id == "100-0"
    assert recorded_calls == [
        ("Chopper_Speed", "2026-11-26T09:01:00+00:00", "250.0"),
        ("Disc_Delay", "2026-11-26T09:00:00", "12400.0"),
        ("Sample_Temp", "2026-11-26T09:05:00+00:00", "50.0"),
    ]
    assert mock_valkey.xrange.call_args_list[0].args[1:] == ("-", "+")

    # Second reduction: only reads entries strictly after 100-0 and does not re-add backfilled blocks
    recorded_calls.clear()
    processor._load_epics_logs(mock_ws, cursor, run_start_dt)

    assert cursor.last_id == "100-0"
    assert recorded_calls == []
    assert mock_valkey.xrange.call_args_list[1].args[1:] == ("(100-0", "+")


def test_load_epics_logs_partial_failure_does_not_duplicate_applied_entries(
    monkeypatch,
):
    """Verify cursor advances per applied entry so partial failures resume without duplicating prior logs."""
    import datetime

    import live_data_processor.main as processor

    recorded_calls: list[tuple[str, str, str]] = []
    fail_once = {"triggered": False}

    def flaky_add_log(_ws_name, log_name, timestamp, value):
        if log_name == "Chopper_Delay" and not fail_once["triggered"]:
            fail_once["triggered"] = True
            raise RuntimeError("transient Mantid failure")
        recorded_calls.append((log_name, timestamp, value))

    mock_run = MagicMock()
    mock_run.hasProperty.return_value = True
    mock_ws = MagicMock()
    mock_ws.getRun.return_value = mock_run

    entry_1 = (
        "100-0",
        {
            "block_name": "Chopper_Speed",
            "value": "250.0",
            "timestamp": "2026-11-26T09:01:00+00:00",
        },
    )
    entry_2 = (
        "101-0",
        {
            "block_name": "Chopper_Delay",
            "value": "2573.0",
            "timestamp": "2026-11-26T09:01:05+00:00",
        },
    )
    entry_3 = (
        "102-0",
        {
            "block_name": "Disc_Speed",
            "value": "50.0",
            "timestamp": "2026-11-26T09:01:10+00:00",
        },
    )

    mock_valkey = MagicMock()
    mock_valkey.xrange.side_effect = [
        [entry_1, entry_2, entry_3],
        [entry_2, entry_3],
    ]
    mock_valkey.hgetall.return_value = {}

    monkeypatch.setattr(processor, "VALKEY_CLIENT", mock_valkey)
    monkeypatch.setattr(processor, "AddTimeSeriesLog", flaky_add_log)

    run_start_dt = datetime.datetime(2026, 11, 26, 9, 0, 0, tzinfo=datetime.UTC)
    cursor = processor._EpicsCursor()

    # First attempt fails on entry_2 after entry_1 succeeded
    with pytest.raises(RuntimeError, match="transient Mantid failure"):
        processor._load_epics_logs(mock_ws, cursor, run_start_dt)

    assert cursor.last_id == "100-0"
    assert recorded_calls == [("Chopper_Speed", "2026-11-26T09:01:00+00:00", "250.0")]

    # Next reduction resumes strictly after 100-0; entry_1 is NOT duplicated, entry_2 and entry_3 succeed
    processor._load_epics_logs(mock_ws, cursor, run_start_dt)

    assert cursor.last_id == "102-0"
    assert mock_valkey.xrange.call_args_list[1].args[1:] == ("(100-0", "+")
    assert recorded_calls == [
        ("Chopper_Speed", "2026-11-26T09:01:00+00:00", "250.0"),
        ("Chopper_Delay", "2026-11-26T09:01:05+00:00", "2573.0"),
        ("Disc_Speed", "2026-11-26T09:01:10+00:00", "50.0"),
    ]


@patch("live_data_processor.main.seek_event_consumer_to_runstart")
@patch("live_data_processor.main.initialize_instrument_workspace")
def test_initialize_run_logs_clean_run_info_and_resets_catchup(
    _mock_workspace, _mock_seek, monkeypatch
):
    """Verify initialize_run logs run name/timestamp cleanly and resets catch-up tracking."""
    import live_data_processor.main as processor

    mock_logger = MagicMock()
    monkeypatch.setattr(processor, "external_logger", mock_logger)
    monkeypatch.setattr(processor, "CATCHUP_START_TIME", 0.0)
    monkeypatch.setattr(processor, "IS_CATCHING_UP", False)
    monkeypatch.setattr(processor.time, "time", lambda: 1234.5)

    run_start = MagicMock(spec=RunStart)
    run_start.RunName.return_value = b"MERLIN00075560"
    run_start.StartTime.return_value = 1_795_686_879_000  # 2026-11-26 09:54:39 UTC

    processor.initialize_run(MagicMock(), MagicMock(), run_start=run_start)

    assert processor.CATCHUP_START_TIME == 1234.5
    assert processor.IS_CATCHING_UP is True
    mock_logger.info.assert_called_once_with(
        "Initializing run %s (started at %s)",
        "MERLIN00075560",
        "2026-11-26 09:54:39",
    )
