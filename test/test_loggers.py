import logging
import sys
from unittest.mock import MagicMock, patch

import pytest

from live_data_processor.loggers import (
    TeeLoggerWriter,
    ValkeyStreamHandler,
    capture_and_tee,
    setup_loggers,
)


def test_valkey_stream_handler_emit_success():
    """Test that the ValkeyStreamHandler emits logs via XADD correctly."""
    mock_client = MagicMock()
    handler = ValkeyStreamHandler(
        client=mock_client, stream_key="test_stream", maxlen=100
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test log message",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    mock_client.xadd.assert_called_once_with(
        "test_stream", {"msg": "Test log message", "level": "INFO"}, maxlen=100
    )


@patch("live_data_processor.loggers.ValkeyStreamHandler.handleError")
def test_valkey_stream_handler_emit_failure(mock_handle_error):
    """Test that exceptions during emission are delegated to handleError."""
    mock_client = MagicMock()
    mock_client.xadd.side_effect = Exception("Valkey disconnected")
    handler = ValkeyStreamHandler(
        client=mock_client,
        stream_key="test_stream",
    )

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    mock_handle_error.assert_called_once_with(record)


@patch("live_data_processor.loggers.VALKEY_CLIENT")
def test_setup_loggers(mock_valkey_client):
    """Test that setup_loggers configures and returns the correct loggers and key."""
    # Ensure fresh test context
    if "internal_MERLIN" in logging.Logger.manager.loggerDict:
        logging.getLogger("internal_MERLIN").handlers.clear()
        logging.getLogger("external_MERLIN").handlers.clear()

    internal, external, stream_key = setup_loggers("MERLIN")

    assert stream_key == "MERLIN_live_data_processor_logs"
    assert internal.name == "internal_MERLIN"
    assert external.name == "external_MERLIN"

    # Internal should have 1 handler (Console)
    assert len(internal.handlers) == 1
    assert isinstance(internal.handlers[0], logging.StreamHandler)

    # External should have 2 handlers (ValkeyStreamHandler, Console)
    assert len(external.handlers) == 2
    assert isinstance(external.handlers[0], ValkeyStreamHandler)
    assert isinstance(external.handlers[1], logging.StreamHandler)

    # Re-running should not duplicate handlers
    setup_loggers("MERLIN")
    assert len(internal.handlers) == 1
    assert len(external.handlers) == 2


def test_tee_logger_writer():
    """Test that TeeLoggerWriter writes to both original stream and the logger."""
    mock_stream = MagicMock()
    mock_logger = MagicMock()

    tee = TeeLoggerWriter(original_stream=mock_stream, logger=mock_logger)

    # Write a string with whitespace to ensure stripping
    bytes_written = tee.write("   Valid output \n")

    assert bytes_written == 17
    mock_stream.write.assert_called_once_with("   Valid output \n")
    mock_stream.flush.assert_called_once()
    mock_logger.info.assert_called_once_with("Valid output")


def test_tee_logger_writer_empty_string():
    """Test that TeeLoggerWriter skips logging for empty/whitespace strings."""
    mock_stream = MagicMock()
    mock_logger = MagicMock()

    tee = TeeLoggerWriter(original_stream=mock_stream, logger=mock_logger)

    tee.write("   \n")

    mock_stream.write.assert_called_once_with("   \n")
    mock_logger.info.assert_not_called()


def test_capture_and_tee():
    """Test that context manager successfully redirects and restores stdout."""
    original_stdout = sys.stdout
    mock_logger = MagicMock()

    with capture_and_tee(mock_logger):
        assert isinstance(sys.stdout, TeeLoggerWriter)
        assert sys.stdout.logger == mock_logger

        # Test an actual write through the hijacked sys.stdout
        sys.stdout.write("Test print execution")
        mock_logger.info.assert_called_once_with("Test print execution")

    # Assert stdout is restored after exiting block
    assert sys.stdout is original_stdout


class _FakeValkeyClient:
    """Lightweight fake Valkey client for high-volume emit tests."""

    def __init__(self) -> None:
        self.count = 0
        self.last_call: tuple[str, dict[str, str], int | None] | None = None

    def xadd(self, name: str, fields: dict[str, str], maxlen: int | None = None) -> str:
        """Record an XADD call without retaining every historical payload."""
        self.count += 1
        self.last_call = (name, fields, maxlen)
        return f"{self.count}-0"


@pytest.mark.parametrize("total_records", [1, 5_005, 50_000])
def test_valkey_stream_handler_does_not_freeze_at_scale(total_records: int) -> None:
    """Test that ValkeyStreamHandler continues publishing 1:1 at arbitrary scale."""
    fake_client = _FakeValkeyClient()
    handler = ValkeyStreamHandler(
        client=fake_client, stream_key="test_stream", maxlen=2000
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    for idx in range(total_records):
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Log message {idx}",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

    assert fake_client.count == total_records
    assert fake_client.last_call == (
        "test_stream",
        {"msg": f"Log message {total_records - 1}", "level": "INFO"},
        2000,
    )


@patch("live_data_processor.loggers.ValkeyStreamHandler.handleError")
def test_valkey_stream_handler_recovers_after_transient_error(
    mock_handle_error: MagicMock,
) -> None:
    """Test that a transient Valkey error does not stop subsequent records from publishing."""
    mock_client = MagicMock()
    mock_client.xadd.side_effect = [None, ConnectionError("temporary blip"), None]
    handler = ValkeyStreamHandler(
        client=mock_client, stream_key="test_stream", maxlen=2000
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    records = [
        logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Message {idx}",
            args=(),
            exc_info=None,
        )
        for idx in range(3)
    ]

    for record in records:
        handler.emit(record)

    mock_handle_error.assert_called_once_with(records[1])
    assert mock_client.xadd.call_count == len(records)
    mock_client.xadd.assert_any_call(
        "test_stream", {"msg": "Message 2", "level": "INFO"}, maxlen=2000
    )


def test_capture_and_tee_exception_recovery():
    """Test that context manager restores stdout even if an exception occurs."""
    original_stdout = sys.stdout
    mock_logger = MagicMock()

    try:
        with capture_and_tee(mock_logger):
            raise RuntimeError("Something broke!")
    except RuntimeError:
        pass

    assert sys.stdout is original_stdout
