"""
EPICS process-variable streaming and logging utilities.

This module discovers instrument sample-block PVs via the EPICS BlockServer,
subscribes to them using auto-monitor callbacks, and continuously records
value updates to a timestamped log file.

Key characteristics:

- EPICS configuration is applied at import time and must occur before any
  EPICS calls are made.
- Block names are fetched once from the BlockServer, de-hexed and decompressed,
  then expanded into individual sample-block PVs.
- PV updates are captured via EPICS monitor callbacks and queued with
  nanosecond-resolution timestamps.
- A dedicated background *process* drains the queue and writes updates to disk,
  isolating EPICS I/O from the main application and avoiding threading issues.
- Log output is line-oriented and intended for downstream consumption by the
  live data processor as a time-series sample log.

The primary public entry point is `start_logging_process`, which spawns the
background process and returns both the process handle and a stop event used
for coordinated shutdown.

Failure to discover PVs or write to the log file is considered fatal to the
reduction workflow and results in `SampleLogError` being raised.
"""

import binascii
import datetime
import logging
import os
import queue
import time
import zlib
from typing import Any

import redis
from epics import PV, caget

from live_data_processor.exceptions import SampleLogError

INSTRUMENT = os.environ.get("INSTRUMENT_NAME", "Unknown Instrument").upper()
VALKEY_HOST = os.environ.get("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("VALKEY_PORT", "6379"))
VALKEY_CLIENT = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)
STREAM_KEY = f"instrument:{INSTRUMENT}:epics_stream"

internal_logger = logging.getLogger(f"internal_{INSTRUMENT}")

# EPICS configuration (must be set before any EPICS calls)
os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = "20000"
os.environ["EPICS_CA_ADDR_LIST"] = "130.246.39.152:5066"
os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

# Type alias for the queue entries
EventT = tuple[str, Any, int]


def dehex_and_decompress(value: bytes) -> bytes:
    """
    Dehex and decompress the given byte string
    :param value: The string to dehex and decompress
    :return: The decompressed bytes
    """
    return zlib.decompress(binascii.unhexlify(value))


def _load_block_names() -> list[str]:
    """Fetch and parse block names from BLOCKSERVER once."""
    raw = bytes(caget("IN:MERLIN:CS:BLOCKSERVER:BLOCKNAMES"))
    decoded = dehex_and_decompress(raw).decode()
    return [n.replace("[", "").replace("]", "").replace(" ", "").replace('"', "") for n in decoded.split(",")]


def _make_monitor_callback(event_queue: "queue.Queue[EventT]"):
    """
    Build an EPICS monitor callback that closes over the given queue.
    """

    def _monitor_callback(pvname=None, value=None, timestamp=None, **kws):
        """
        EPICS monitor callback.

        Enqueue every update as (block_name, value, timestamp_ns).
        """
        if pvname is None:
            return

        # pvname expected: "IN:MERLIN:CS:SB:<BLOCK_NAME>"
        try:
            block_name = pvname.rsplit(":", 1)[-1]
        except Exception:
            # Fallback to full pvname if unexpected format
            block_name = pvname

        timestamp_ns = int(float(timestamp) * 1e9) if timestamp else time.time_ns()
        event_queue.put((block_name, value, timestamp_ns))

    return _monitor_callback


def init_pvs(
    event_queue: "queue.Queue[EventT]",
    wait_timeout: float = 1.0,
) -> dict[str, PV]:
    """
    Load blocknames and create PVs with auto_monitor and callbacks.

    Returns a mapping of block_name -> PV instance.
    """
    block_names = _load_block_names()
    pv_map: dict[str, PV] = {}

    callback = _make_monitor_callback(event_queue)

    for name in block_names:
        pvname = f"IN:MERLIN:CS:SB:{name}"
        pv = PV(
            pvname,
            auto_monitor=True,
            callback=callback,
        )

        # Short connection wait; if it fails, skip this PV
        if not pv.wait_for_connection(timeout=wait_timeout):
            continue

        pv_map[name] = pv

    return pv_map


def _format_timestamp(timestamp_ns: int) -> str:
    """
    Format timestamp to ISO 8601 using the required pattern:

        datetime.datetime.fromtimestamp(
            timestamp_unix_ns / 1e9, tz=datetime.UTC
        ).isoformat()
    """
    dt = datetime.datetime.fromtimestamp(timestamp_ns / 1e9, tz=datetime.UTC)
    return dt.isoformat()


def main(wait_timeout: float = 1.0) -> None:
    """
    Child process entrypoint: clears file, initialises PVs and drains the queue to file.
    The EPICS callbacks will enqueue updates; we drain and write until stop_event is set.
    """
    # Per-process state lives here
    event_queue: queue.Queue[EventT] = queue.Queue()

    try:
        pv_map = init_pvs(event_queue=event_queue, wait_timeout=wait_timeout)
        if not pv_map:
            internal_logger.critical("Discovered no PVs, NO EPICS VALUES WILL BE STREAMED - Reduction will be useless")
            raise SampleLogError("No PVs were discovered, therefore no epics values will be streamed.")
    except Exception as exc:
        internal_logger.critical(
            "Failed to discover any PVs, NO EPICS VALUES WILL BE STREAMED - Reduction will be useless"
        )
        raise SampleLogError("Failed to discover any PVs, therefore no epics values will be streamed.") from exc

    # Use a local loop; do not spawn extra threads in the child process for simplicity
    while True:
        try:
            block_name, value, timestamp_ns = event_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if value is None or value == "None":
            continue

        ts_str = _format_timestamp(timestamp_ns)
        try:
            VALKEY_CLIENT.xadd(
                STREAM_KEY,
                {
                    "block_name": block_name,
                    "value": str(value),
                    "timestamp": ts_str,
                },
                maxlen=10000,  # Keep only the last 10000 entries
            )
        except redis.ConnectionError:
            internal_logger.error("Lost connection to Valkey, Retrying...")
            time.sleep(1)
        except Exception as exc:
            raise SampleLogError("Failed to write to Valkey stream.") from exc


if __name__ == "__main__":
    main()
