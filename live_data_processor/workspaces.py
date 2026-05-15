import datetime
import logging
import os
from pathlib import Path

import numpy as np
from mantid.api import mtd
from mantid.simpleapi import AddSampleLog, GroupDetectors, LoadEmptyInstrument
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart

from live_data_processor.loggers import capture_and_tee

INSTRUMENT = os.environ.get("INSTRUMENT", "MERLIN").upper()


def initialize_instrument_workspace(instrument: str, workspace_name: str, run_start: RunStart) -> None:
    """
    Initialize the instrument workspace for a new run.

    This function extracts the run name from the provided RunStart message, logs the
    start of the new run, and loads an empty instrument workspace for the specified
    instrument. The workspace is configured to handle event data.

    :param run_start: The RunStart message containing information about the new run.
    :return: None
    """
    external_logger = logging.getLogger(f"external_{INSTRUMENT}")
    external_logger.info("Initializing workspace for run")
    with capture_and_tee(external_logger):
        LoadEmptyInstrument(InstrumentName=instrument, OutputWorkspace=workspace_name, MakeEventWorkspace=True)
        detector_ids = run_start.DetectorSpectrumMap().DetectorIdAsNumpy()
        spectrum = run_start.DetectorSpectrumMap().SpectrumAsNumpy()
        ws = mtd[workspace_name]
        ws.getAxis(0).setUnit("TOF")

        start_time_ms = run_start.StartTime()
        if start_time_ms > 0:
            start_iso = datetime.datetime.fromtimestamp(start_time_ms / 1000.0, tz=datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            AddSampleLog(Workspace=workspace_name, LogName="start_time", LogText=start_iso, LogType="String")

        detector_ids = np.array(ws.getIndicesFromDetectorIDs(detector_ids.tolist()))
        complete_spectrum = np.array(ws.getSpectrumNumbers())
        ids = [complete_spectrum[detector_ids[np.where(spectrum == sp)[0]]] for sp in np.unique(spectrum)]
        outstr = f"{len(ids)}\n" + "\n".join(
            [
                f"{sp}\n{len(detector_id)}\n" + " ".join([str(v) for v in detector_id])
                for sp, detector_id in enumerate(ids)
            ]
        )
        mapfile = Path("insttemp.map")
        with mapfile.open("w") as f:
            f.write(outstr)
        GroupDetectors(ws, str(mapfile), OutputWorkspace=workspace_name)
