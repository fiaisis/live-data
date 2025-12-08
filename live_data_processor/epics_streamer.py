"""
EPICS streamer utilities for capturing PV updates to a log file.

This module discovers EPICS block PVs, subscribes to updates, queues
changes, and writes them asynchronously to a simple text log that can be
consumed by the live data processor as time series logs.
"""
import binascii
import datetime
import os
import queue
import threading
import time
import zlib
from typing import Any

from epics import PV, caget

# EPICS configuration (must be set before any EPICS calls)
os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = "20000"
os.environ["EPICS_CA_ADDR_LIST"] = "130.246.39.152:5066"
os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

block_names: list[str] = []
pv_map: dict[str, PV] = {}

# Queue of (block_name, value, timestamp_ns)
_event_queue: "queue.Queue[tuple[str, Any, int]]" = queue.Queue()


def dehex_and_decompress(value: bytes) -> bytes:
    """Decompresses the inputted string, assuming it is in hex encoding."""
    assert isinstance(value, bytes), (
        "Non-bytes argument passed to dehex_and_decompress\n"
        f"Argument was type {value.__class__.__name__} with value {value}"
    )
    return zlib.decompress(binascii.unhexlify(value))


def _load_block_names() -> list[str]:
    """Fetch and parse block names from BLOCKSERVER once."""
    raw = bytes(caget("IN:MERLIN:CS:BLOCKSERVER:BLOCKNAMES"))
    decoded = dehex_and_decompress(raw).decode()
    return [n.replace("[", "").replace("]", "").replace(" ", "").replace('"', "") for n in decoded.split(",")]


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

    if timestamp is None:
        # Fallback to local clock if EPICS timestamp missing
        timestamp_ns = time.time_ns()
        print("Using local time")
    else:
        # EPICS timestamp is seconds since epoch (float)
        timestamp_ns = int(float(timestamp) * 1e9)

    _event_queue.put((block_name, value, timestamp_ns))


def init_pvs(wait_timeout: float = 1.0) -> None:
    """
    Load block names and create PVs with auto_monitor and callbacks.

    Must be called before starting the logging thread.
    """
    global block_names, pv_map

    block_names = _load_block_names()
    pv_map = {}

    for name in block_names:
        pvname = f"IN:MERLIN:CS:SB:{name}"
        pv = PV(
            pvname,
            auto_monitor=True,
            callback=_monitor_callback,
        )

        # Short connection wait; if it fails, skip this PV
        if not pv.wait_for_connection(timeout=wait_timeout):
            continue

        pv_map[name] = pv


def _format_timestamp(timestamp_ns: int) -> str:
    """
    Format timestamp to ISO 8601 using the required pattern:

        datetime.datetime.fromtimestamp(
            timestamp_unix_ns / 1e9, tz=datetime.UTC
        ).isoformat()
    """
    dt = datetime.datetime.fromtimestamp(timestamp_ns / 1e9, tz=datetime.UTC)
    return dt.isoformat()


def _writer_loop(file_path: str, stop_event: threading.Event) -> None:
    """
    Background loop that drains the event queue and writes to file.
    """
    with open(file_path, "a", encoding="utf-8") as f:
        while not stop_event.is_set() or not _event_queue.empty():
            try:
                block_name, value, timestamp_ns = _event_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if value is None or value == "None":
                continue

            ts_str = _format_timestamp(timestamp_ns)
            line = f"{block_name} - {value} - {ts_str}\n"
            f.write(line)


def start_logging_thread(file_path: str):
    """
    Start a background logging thread that writes EVERY PV update.

    REQUIREMENT:
        init_pvs() must have been called first, so monitors are active.

    Returns:
        (thread, stop_event)

    To stop cleanly:
        stop_event.set()
        thread.join()
    """
    if not pv_map:
        raise RuntimeError("PV map is empty — call init_pvs() before starting logging.")

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_writer_loop,
        args=(file_path, stop_event),
        daemon=True,
    )
    thread.start()
    return thread, stop_event
