"""Configure live-data logging and publish log records to Valkey."""

import io
import logging
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager

import redis

VALKEY_HOST: str = os.environ.get("VALKEY_HOST", "localhost")
VALKEY_PORT: int = int(os.environ.get("VALKEY_PORT", "6379"))

# Global client for convenience; individual handlers will manage their own reconnection logic.
VALKEY_CLIENT = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)


class ValkeyStreamHandler(logging.Handler):
    """Logging handler that publishes formatted records to a Valkey stream."""

    def __init__(
        self,
        client: redis.Redis,
        stream_key: str,
        maxlen: int = 2000,
    ) -> None:
        """Configure the stream destination and maximum stream length."""
        super().__init__()
        self.client = client
        self.stream_key = stream_key
        self.maxlen = maxlen

    def emit(self, record: logging.LogRecord) -> None:
        """
        Publish a formatted log record to Valkey.
        :param record: The log record to format and publish.
        :return: None.
        """
        try:
            msg = self.format(record)
            self.client.xadd(self.stream_key, {"msg": msg, "level": record.levelname}, maxlen=self.maxlen)
        except Exception:
            # Avoid any exception escaping from emit
            self.handleError(record)


def setup_loggers(instrument_name: str) -> tuple[logging.Logger, logging.Logger, str]:
    """
    Configure and return the internal and external loggers for a specific instrument.

    :param instrument_name: The name of the instrument (CR) to construct logger and stream names.
    :return: A tuple containing the internal logger, external logger, and the Valkey stream key.
    """
    # Construct the specific stream key based on the CR
    stream_key = f"{instrument_name}_live_data_processor_logs"

    # --- Internal Logger (Console) ---
    internal = logging.getLogger(f"internal_{instrument_name}")
    internal.setLevel(logging.INFO)

    if not internal.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - INTERNAL - %(message)s"))
        internal.addHandler(console_handler)

    # --- External Logger (Valkey Stream) ---
    external = logging.getLogger(f"external_{instrument_name}")
    external.setLevel(logging.INFO)

    if not external.handlers:
        # 1. Add the Valkey Handler
        valkey_handler = ValkeyStreamHandler(VALKEY_CLIENT, stream_key)
        valkey_handler.setFormatter(logging.Formatter("%(message)s"))
        external.addHandler(valkey_handler)

        # 2. Add a Console Handler so it still shows up in Pod logs
        external_console = logging.StreamHandler()
        external_console.setFormatter(logging.Formatter("%(asctime)s - EXTERNAL - %(message)s"))
        external.addHandler(external_console)

    return internal, external, stream_key


class TeeLoggerWriter(io.TextIOBase):
    """File-like writer that mirrors standard output to a logger."""

    def __init__(self, original_stream: io.TextIOBase, logger: logging.Logger):
        """
        Initialize the TeeLoggerWriter.

        :param original_stream: The original output stream (typically sys.stdout).
        :param logger: The logger instance to send captured output to.
        """
        self.original_stream = original_stream
        self.logger = logger

    def write(self, s: str) -> int:
        """
        Write string to original stream and pass cleaned version to logger.

        :param s: The string to be written.
        :return: The number of characters written.
        """
        # 1. Pass the exact string to the original stdout (pod logs)
        self.original_stream.write(s)
        self.original_stream.flush()

        # 2. Clean the string and send to the Valkey logger
        cleaned_str = s.strip()
        if cleaned_str:
            self.logger.info(cleaned_str)

        return len(s)


@contextmanager
def capture_and_tee(logger: logging.Logger) -> Generator[None, None, None]:
    """
    Mirror standard output to the supplied logger until the context exits.
    :param logger: The logger to receive a copy of standard output.
    :return: None.
    """
    original_stdout = sys.stdout
    # We pass the original sys.stdout and our external logger into the Tee
    sys.stdout = TeeLoggerWriter(original_stdout, logger)
    try:
        yield
    finally:
        sys.stdout = original_stdout
