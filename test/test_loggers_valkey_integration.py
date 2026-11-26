"""Integration tests for ValkeyStreamHandler against a live Valkey instance."""

import logging
import os
import uuid
from collections.abc import Generator

import pytest
import redis

from live_data_processor.loggers import ValkeyStreamHandler

TOTAL_RECORDS = 5050
STREAM_MAXLEN = 2000
MAX_APPROX_OVERHEAD = 200


@pytest.fixture
def valkey_client() -> Generator[redis.Redis, None, None]:
    """Return a connected Valkey client, skipping locally if Valkey is unavailable."""
    host = os.environ.get("VALKEY_HOST", "localhost")
    port = int(os.environ.get("VALKEY_PORT", "6379"))
    client = redis.Redis(host=host, port=port, decode_responses=True)
    try:
        client.ping()
    except redis.RedisError as exc:
        if os.environ.get("CI"):
            pytest.fail(f"Valkey is not reachable in CI at {host}:{port}: {exc}")
        pytest.skip(f"Valkey not reachable locally at {host}:{port}: {exc}")

    yield client
    client.close()


def test_valkey_stream_handler_publishes_past_5000_entries_in_valkey(
    valkey_client: redis.Redis,
) -> None:
    """Verify logs continue arriving in Valkey after more than 5000 records are emitted."""
    stream_key = f"test_merlin_logs_{uuid.uuid4().hex}"
    handler = ValkeyStreamHandler(
        client=valkey_client, stream_key=stream_key, maxlen=STREAM_MAXLEN
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    try:
        for idx in range(TOTAL_RECORDS):
            record = logging.LogRecord(
                name="integration_logger",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"Integration log message {idx}",
                args=(),
                exc_info=None,
            )
            handler.emit(record)

        latest_entries = valkey_client.xrevrange(stream_key, "+", "-", count=1)
        assert latest_entries, "Expected entries in Valkey stream"
        _, latest_payload = latest_entries[0]
        assert latest_payload["msg"] == f"Integration log message {TOTAL_RECORDS - 1}"
        assert latest_payload["level"] == "INFO"

        stream_len = valkey_client.xlen(stream_key)
        assert STREAM_MAXLEN <= stream_len <= STREAM_MAXLEN + MAX_APPROX_OVERHEAD
    finally:
        valkey_client.delete(stream_key)
