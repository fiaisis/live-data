from pathlib import Path

import numpy as np
from mantid.api import mtd
from mantid.simpleapi import GroupDetectors, LoadEmptyInstrument
from streaming_data_types.fbschemas.run_start_pl72.RunStart import RunStart


def initialize_instrument_workspace(instrument: str, workspace_name: str, run_start: RunStart) -> None:
    """
    Initialize the instrument workspace for a new run.

    This function extracts the run name from the provided RunStart message, logs the
    start of the new run, and loads an empty instrument workspace for the specified
    instrument. The workspace is configured to handle event data.

    :param run_start: The RunStart message containing information about the new run.
    :return: None
    """
    LoadEmptyInstrument(InstrumentName=instrument, OutputWorkspace=workspace_name, MakeEventWorkspace=True)
    detector_ids = run_start.DetectorSpectrumMap().DetectorIdAsNumpy()
    spectrum = run_start.DetectorSpectrumMap().SpectrumAsNumpy()
    ws = mtd[workspace_name]
    ws.getAxis(0).setUnit("TOF")
    detector_ids = np.array(ws.getIndicesFromDetectorIDs(detector_ids.tolist()))
    complete_spectrum = np.array(ws.getSpectrumNumbers())
    ids = [complete_spectrum[detector_ids[np.where(spectrum == sp)[0]]] for sp in np.unique(spectrum)]
    outstr = f"{len(ids)}\n" + "\n".join(
        [f"{sp}\n{len(detector_id)}\n" + " ".join([str(v) for v in detector_id]) for sp, detector_id in enumerate(ids)]
    )
    mapfile = Path("insttemp.map")
    with mapfile.open("w") as f:
        f.write(outstr)
    GroupDetectors(ws, str(mapfile), OutputWorkspace=workspace_name)
