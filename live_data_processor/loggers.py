import io
import logging
import os
import queue
import sys
import threading
from collections.abc import Generator
from contextlib import contextmanager, suppress

import redis

VALKEY_HOST: str = os.environ.get("VALKEY_HOST", "localhost")
VALKEY_PORT: int = int(os.environ.get("VALKEY_PORT", "6379"))
MAX_DELIVERY_ATTEMPTS: int = 10

# Global client for convenience; individual handlers will manage their own reconnection logic.
VALKEY_CLIENT = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)


class ValkeyStreamHandler(logging.Handler):
    """
    Robust logging handler that pushes logs to a Valkey stream (Redis).

    Behavior:
    - Non-blocking emit(): places formatted log records on an internal bounded queue.
    - Background worker drains the queue and performs XADD calls to Redis.
    - On Redis errors the worker retries with backoff; records are not discarded immediately.
    - If the internal queue is full, oldest messages are dropped in favor of newest to avoid unbounded memory growth.
    """

    def __init__(
        self,
        client: redis.Redis,
        stream_key: str,
        maxlen: int = 2000,
        queue_maxsize: int = 5000,
        max_delivery_attempts: int = MAX_DELIVERY_ATTEMPTS,
        backoff_base: float = 0.5,
    ) -> None:
        super().__init__()
        self.client = client
        self.stream_key = stream_key
        self.maxlen = maxlen
        self._queue: queue.Queue[tuple[str, str, logging.LogRecord]] = queue.Queue(maxsize=queue_maxsize)
        self._stop_event = threading.Event()
        # Delivery retry configuration (exposed for testing)
        self.max_delivery_attempts = max_delivery_attempts
        self.backoff_base = backoff_base

    def emit(self, record: logging.LogRecord) -> None:
        """
        Quickly enqueue a formatted record for background delivery to Redis.
        """
        try:
            msg = self.format(record)
            entry = (msg, record.levelname, record)
            try:
                # If queue is full, drop the oldest item to make room (discarding oldest is preferable
                # to blocking or crashing the application).
                self._queue.put(entry, block=False)
                self.client.xadd(self.stream_key, {"msg": msg, "level": record.levelname}, maxlen=self.maxlen)
            except queue.Full:
                with suppress(Exception):
                    # Remove one oldest item then enqueue current
                    _ = self._queue.get(block=False)
                try:
                    self._queue.put(entry, block=False)
                except Exception:
                    # As a final fallback write to stderr so logs aren't silently lost
                    with suppress(Exception):
                        sys.stderr.write(f"[VALKEY DROP] {msg}\n")
        except Exception:
            # Avoid any exception escaping from emit
            self.handleError(record)



def setup_loggers(instrument_name: str) -> tuple[logging.Logger, logging.Logger, str]:
    """
    Configures and returns the internal and external loggers for a specific instrument.

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
    """
    A file-like object that acts like a Tee.
    It writes to the original stdout and sends a copy to a logger.
    """

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
    Context manager to redirect sys.stdout to both the console and the provided logger.

    :param logger: The logger to mirror the stdout output to.
    :yield: None
    """
    original_stdout = sys.stdout
    # We pass the original sys.stdout and our external logger into the Tee
    sys.stdout = TeeLoggerWriter(original_stdout, logger)
    try:
        yield
    finally:
        sys.stdout = original_stdout
