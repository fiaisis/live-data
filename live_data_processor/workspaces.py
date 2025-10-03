import numpy as np

from schemas.compiled_schemas.RunStart import RunStart

from mantid.api import mtd
from mantid.kernel import DateAndTime
from mantid.simpleapi import LoadEmptyInstrument, GroupDetectors


def initialize_instrument_workspace(instrument: str, workspace_name: str, run_start: RunStart) -> None:
    """
    Initialize the instrument workspace for a new run.

    This function extracts the run name from the provided RunStart message, logs the
    start of the new run, and loads an empty instrument workspace for the specified
    instrument. The workspace is configured to handle event data.

    :param run_start: The RunStart message containing information about the new run.
    :return: None
    """
    run_name = run_start.RunName()
    run_name = run_name.decode("utf-8")
    # LoadEmptyInstrument(InstrumentName=INSTRUMENT, OutputWorkspace=LIVE_WS_NAME, MakeEventWorkspace=True)
    LoadEmptyInstrument(
        OutputWorkspace="lives",
        Filename="/Users/sham/miniconda3/envs/mantid/instrument/MERLIN_Definition.xml",
        MakeEventWorkspace=True,
    )
    # Call instrument specific setup function
    additional_setup_func = get_additional_workspace_setup(instrument)
    additional_setup_func(run_start, workspace_name)


def setup_merlin_workspace(run_start, workspace_name):
    # merlin specific
    detector_ids = run_start.DetectorSpectrumMap().DetectorIdAsNumpy()
    spectrum = run_start.DetectorSpectrumMap().SpectrumAsNumpy()
    ws = mtd[workspace_name]
    detector_ids = np.array(ws.getIndicesFromDetectorIDs(detector_ids.tolist()))
    complete_spectrum = np.array(ws.getSpectrumNumbers())
    ids = [complete_spectrum[detector_ids[np.where(spectrum == sp)[0]]] for sp in np.unique(spectrum)]
    outstr = f"{len(ids)}\n" + "\n".join(
        [f"{sp}\n{len(id)}\n" + " ".join([str(v) for v in id]) for sp, id in enumerate(ids)]
    )
    mapfile = "insttemp.map"
    with open(mapfile, "w") as f:
        f.write(outstr)
    GroupDetectors(ws, mapfile, OutputWorkspace=workspace_name)


def get_additional_workspace_setup(instrument: str):
    match instrument.upper():
        case "MERLIN":
            return setup_merlin_workspace
        case _:
            # noop setup
            return lambda *args, **kwargs: None
