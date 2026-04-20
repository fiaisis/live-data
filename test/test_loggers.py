import logging
import sys
from unittest.mock import MagicMock, patch

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
        "test_stream", {"msg": "Test log message"}, maxlen=100
    )


@patch("live_data_processor.loggers.ValkeyStreamHandler.handleError")
def test_valkey_stream_handler_emit_failure(mock_handle_error):
    """Test that exceptions during emission are delegated to handleError."""
    mock_client = MagicMock()
    mock_client.xadd.side_effect = Exception("Valkey disconnected")
    handler = ValkeyStreamHandler(client=mock_client, stream_key="test_stream")

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
