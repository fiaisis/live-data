# BEGIN FIA BLOCK

import os
import sys
import time

import requests
from mantid import ConfigService
from mantid.simpleapi import RemoveWorkspaceHistory, SaveNexusProcessed

sys.path.append(os.path.dirname(__file__))


def execute():
    ConfigService.setLogLevel(5)

    def get_file_from_request(url: str, path: str) -> None:
        success = False
        attempts = 0
        wait_time_seconds = 15
        while attempts < 3:
            print(f"Attempting to get resource {url}", flush=True)
            response = requests.get(url)
            if not response.ok:
                print(f"Failed to get resource from: {url}", flush=True)
                print(f"Waiting {wait_time_seconds}...", flush=True)
                time.sleep(wait_time_seconds)
                attempts += 1
                wait_time_seconds *= 3
            else:
                with open(path, "w+") as fle:
                    fle.write(response.text)
                success = True
                break

        if not success:
            raise RuntimeError(f"Reduction not possible with missing resource {url}")

    RemoveWorkspaceHistory("lives")
    SaveNexusProcessed(Filename="~/work/live-data/output-lives-new.nxs", InputWorkspace="lives")
    ConfigService.setLogLevel(3)
    git_sha = "0543a069385fc479204d23c8f42a3bde140003b9"
    print(f"mantidproject/direct_reduction sha is {git_sha}")
    # Only needed for fixes with regards to reductions during MARI issues
    get_file_from_request(
        f"https://raw.githubusercontent.com/mantidproject/direct_reduction/{git_sha}/reduction_files/reduction_utils.py",
        "reduction_utils.py",
    )
    get_file_from_request(
        f"https://raw.githubusercontent.com/mantidproject/direct_reduction/{git_sha}/reduction_files/DG_reduction.py",
        "DG_reduction.py",
    )
    get_file_from_request(
        f"https://raw.githubusercontent.com/mantidproject/direct_reduction/{git_sha}/reduction_files/DG_whitevan.py",
        "DG_whitevan.py",
    )
    get_file_from_request(
        f"https://raw.githubusercontent.com/mantidproject/direct_reduction/{git_sha}/reduction_files/DG_monovan.py",
        "DG_monovan.py",
    )
    get_file_from_request(
        "https://raw.githubusercontent.com/pace-neutrons/InstrumentFiles/addf56cf27043dbae5c67a1f26ad76ecf7800324/merlin/mask_24_5.xml",
        "mask_24_5.xml",
    )

    # END FIA BLOCK

    ############ User defined parameters ##############
    ############ User defined parameters ##############
    rotation_block_name = "Rot"
    rotation_bin_size = 5  # In degrees

    # For silicon data:
    # runno = 69266
    # wbvan = 69168
    # ei_list = 'auto'
    # lattice_pars = [5.43, 5.43, 5.43]  # In Angstrom
    # lattice_ang = [90, 90, 90]         # In degrees
    # uvector = '1, 0, 0'                # Reciprocal lattice vector parallel to incident beam direction when rotation=psi0
    # vvector = '0, 1, 0'                # Reciprocal lattice vector perpendicular to u in the horizontal plane
    # rotation_zero_angle = -2           # psi0 in Horace notation

    # For live data on 20250516 (MnO):
    runno = "lives"
    wbvan = 71617
    ei_list = 30  # Auto-ei currently doesn't work for live (streamed) data
    lattice_pars = [4.446, 4.446, 4.446]
    lattice_ang = [90, 90, 90]
    uvector = "1,1,0"
    vvector = "0,0,1"
    rotation_zero_angle = 132.5  # psi0 in Horace notation

    ###################################################

    output_dir = "~/work/live-data/output"  # Do not change, lol watch me

    import importlib

    sys.path.append(os.path.dirname(__file__))

    import reduction_utils

    importlib.reload(reduction_utils)
    import mantid
    from mantid.simpleapi import (
        BinMD,
        CompactMD,
        Load,
        MergeMD,
        SaveMD,
        mtd,
    )

    # Default save directory (/output only for autoreduction as the RBNumber/autoreduced dir is mounted here)
    mantid.config["defaultsave.directory"] = output_dir  # data_dir

    os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = "20000"
    os.environ["EPICS_CA_ADDR_LIST"] = "130.246.39.152:5066"
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

    wsname = runno
    if wsname not in mtd:
        ws = Load(wsname, OutputWorkspace=wsname)
    else:
        ws = mtd[wsname]

    if ei_list == "auto" or (hasattr(ei_list, "__iter__") and ei_list[0] == "auto"):
        ei_list = reduction_utils.autoei(ws)
    if not hasattr(ei_list, "__iter__"):
        ei_list = [ei_list]

    for ei in ei_list:
        output_ws = reduction_utils.iliad(
            runno=wsname,
            wbvan=wbvan,
            ei=ei,
            FixEi=True,
            Erange=[-0.5, 0.01, 0.85],
            cs_block=rotation_block_name,
            cs_bin_size=rotation_bin_size,
            cs_conv_to_md=True,
            cs_conv_pars={
                "lattice_pars": lattice_pars,
                "lattice_ang": lattice_ang,
                "u": uvector,
                "v": vvector,
                "psi0": rotation_zero_angle,
            },
            hard_mask_file="mask_24_5.xml",
            inst="MERLIN",
            powdermap="MERLIN_rings_251.xml",
        )
        allws = []
        print(f"{ei:g}meV:")
        print(mtd.getObjectNames())
        for w in mtd.getObjectNames():
            if f"{wsname}_{ei:g}meV" in w and w.endswith("_ang_md"):
                allws.append(w)
        if len(allws) > 1:
            wsout = MergeMD(",".join(allws), OutputWorkspace=f"MER{runno}_{ei:g}meV_1to1_md")
        else:
            wsout = allws[0]
        mn = [wsout.getDimension(i).getMinimum() for i in range(4)]
        mx = [wsout.getDimension(i).getMaximum() for i in range(4)]
        wsbin = BinMD(
            wsout,
            AlignedDim0=f"[H,0,0],{mn[0]},{mx[0]},100",
            AlignedDim1=f"[0,K,0],{mn[1]},{mx[1]},100",
            AlignedDim2=f"[0,0,L],{mn[2]},{mx[2]},100",
            AlignedDim3=f"DeltaE,{mn[3]},{mx[3]},50",
        )
        wsbin = CompactMD(wsbin)
        SaveMD(
            wsbin,
            Filename=os.path.join(output_dir, f"MER{runno}_{ei:g}meV_1to1_mdhisto.nxs"),
        )
        mtd.remove(wsbin.name())
        # mtd.remove(wsout.name())
        for ws in allws:
            mtd.remove(ws)
