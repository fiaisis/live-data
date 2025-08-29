"""
Collectors for auxiliary (non-event) live data used during reduction.

This module defines a simple interface for sampling and recording instrument-specific
miscellaneous data alongside event data, and provides a MERLIN implementation that reads
an EPICS process variable and stores it as a Mantid time-series log. The collectors are
constructed and driven by live_data_processor.main.start_live_reduction():

- A MiscDataCollector can perform one-off setup when a new run starts (on_run_start).
- If will_run_forever is True, main will start a background daemon thread that calls
  run_forever(ctx, stop_event) until stop_event is set, typically when the run changes
  or the application shuts down.

External dependencies used by implementations here:
- EPICS Channel Access (epics.caget) for reading process variables.
- Mantid (AddTimeSeriesLog) for writing values into the live workspace logs.
"""

import datetime
import threading
import time
from abc import ABC, abstractmethod

from epics import caget
from mantid.simpleapi import AddTimeSeriesLog

from live_data_processor.main import RunContext, _create_run_identifier, logger


class MiscDataCollector(ABC):
    """
    Abstract interface for collecting auxiliary instrument data during a run.

    Implementations may record EPICS values, environment readings, or other metadata
    associated with the current run and write them to the live Mantid workspace or
    other sinks.

    Life-cycle:
    - on_run_start(ctx) is called synchronously when a run is initialized.
    - If will_run_forever is True, the main loop will run run_forever(ctx, stop_event)
      on a background thread until stop_event is set.
    """

    @abstractmethod
    @property
    def will_run_forever(self) -> bool:
        """
        Indicates whether this collector should run continuously on a background thread.

        When True, the main loop will create and start a daemon thread that calls
        run_forever and will provide a threading.Event to request a clean shutdown.
        """
        ...

    def on_run_start(self, ctx: RunContext) -> None:
        """
        Hook that is invoked when a new run is initialized.

        Override to perform any one-off setup that should occur once per run before
        any background sampling starts.

        :param ctx: RunContext giving access to the current RunStart and helpers.
        :return: None
        """
        return

    def run_forever(self, ctx: RunContext, stop_event: threading.Event) -> None:
        """
        Run the collector loop until stop_event is set.

        Implementations are expected to periodically sample auxiliary data and record
        it appropriately. The loop should check stop_event.is_set() and exit promptly
        once set.

        :param ctx: RunContext providing access to the current run state.
        :param stop_event: Event that signals when to stop the loop.
        :return: None
        """
        return


class MerlinCollector(MiscDataCollector):
    """
    MERLIN-specific collector that samples the sample rotation angle from EPICS and
    writes it into the live Mantid workspace as a time-series log named 'Rot'.

    The collector reads the PV 'IN:MERLIN:CS:SB:Rot' approximately once per second.
    To avoid cross-run contamination, it verifies the current run identifier both
    before and after the EPICS read, only logging the value if the run has not changed.
    """

    @property
    def will_run_forever(self):
        """This collector runs continuously on a background thread."""
        return True

    def run_forever(self, ctx: RunContext, stop_event: threading.Event) -> None:
        """
        Periodically sample MERLIN rotation from EPICS and record to a Mantid time-series log.

        - Polls PV 'IN:MERLIN:CS:SB:Rot' approximately once per second (using monotonic scheduling).
        - Performs a run-identity check before and after reading to avoid attributing a sample to
          the wrong run if a new run starts between reads.
        - On successful read, appends a value to the 'Rot' log of the 'lives' workspace via Mantid.
        - Logs a warning when EPICS read fails and skips that tick.

        :param ctx: RunContext providing access to the current RunStart.
        :param stop_event: Event used to request termination of the loop.
        :return: None
        """
        next_tick = time.monotonic()
        while not stop_event.is_set():
            # 1st run check
            tick_id = _create_run_identifier(ctx.get_current_run())
            try:
                rot = caget("IN:MERLIN:CS:SB:Rot", timeout=0.5)
            except Exception as exc:
                logger.warning("MERLIN EPICS read failed", exc_info=exc)
                rot = None

            # 2nd run check, this ensures time series log will only be added if the rotation is for the same run
            if rot is not None and _create_run_identifier(ctx.get_current_run()) == tick_id:
                AddTimeSeriesLog(
                    "lives",
                    "Rot",
                    datetime.datetime.now().isoformat(),
                    rot,
                )

            next_tick += 1.0
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)


def get_misc_data_collector(instrument: str) -> MiscDataCollector:
    """
    Factory for a MiscDataCollector suitable for the given instrument name.

    :param instrument: The instrument name (case-insensitive), e.g. "merlin".
    :return: A concrete MiscDataCollector implementation for the instrument.
    :raises ValueError: If the instrument is not supported.
    """
    match instrument.lower():
        case "merlin":
            return MerlinCollector()
        case _:
            raise ValueError(f"Unsupported instrument for MiscDataCollector: {instrument}")
