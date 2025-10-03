# Live Data (Operator + Processor)

This repository contains a small platform for running live neutron data reduction:

- live_data_operator: a Kubernetes Operator (based on Kopf) that deploys and manages a Live Data Processor per instrument.
- live_data_processor: a Python service that consumes live event data from Kafka, maintains a Mantid live workspace, executes an instrument-specific reduction script.


## Overview
- The Operator watches for a custom resource (one per instrument) and creates a Deployment running the Live Data Processor. It also ensures the necessary PersistentVolumes and PersistentVolumeClaims exist for output and read-only archive access.
- The Processor connects to a Kafka cluster, locates the latest RunStart on the <INSTRUMENT>_runInfo topic, seeks the <INSTRUMENT>_events topic to the run start timestamp, streams events into a Mantid event workspace, periodically fetches the latest reduction script from GitHub, and executes it. It can spawn a background "misc" data collector to log auxiliary values (e.g., EPICS PVs) to the live Mantid workspace.


## Running the Operator locally
You can run the operator against any kube context your kubectl is configured for.

- Ensure required secrets and RBAC exist (see Kubernetes deployment model below).
- Run from source:
  - kopf run --liveness=http://0.0.0.0:8080/healthz live_data_operator/main.py --verbose
- Or build and run via container (see Docker images).

The operator reacts to livedataprocessors resources; its handlers are in create_fn and resume_fn.


## Running the Processor locally
Recommended: run the processor inside its dedicated image, which already contains Mantid and dependencies. A development Dockerfile is at live_data_processor/Dockerfile.

- Build the image:
  - docker build -t local/live-data-processor -f live_data_processor/Dockerfile .
- Run it (example with environment):
  - docker run --rm \
    -e INSTRUMENT=SCIDEMO \
    -e KAFKA_IP=localhost \
    -e KAFKA_PORT=9092 \
    -e LIVE_WS=lives \
    -e INGEST_WAIT=30 \
    -e GITHUB_API_TOKEN=shh \
    -v $(pwd)/output:/output \
    local/live-data-processor

Native run (requires Mantid available to Python):
- pip install kafka-python==2.2.15 flatbuffers==25.2.10 pyepics
- export INSTRUMENT=SCIDEMO (or your instrument)
- export KAFKA_IP=localhost; export KAFKA_PORT=9092
- python -m live_data_processor.main

What it does:
- Fetches the latest live_data.py reduction script for the INSTRUMENT from fiaisis/autoreduction-scripts (main branch) via GitHub.
- Finds the latest RunStart in <INSTRUMENT>_runInfo and seeks <INSTRUMENT>_events to the StartTime.
- Streams EventMessage payloads into the Mantid workspace, executes the reduction script on a loop, and watches for new RunStart or script updates.


## Compiling FlatBuffers schemas
The repository contains FlatBuffers schema files in schemas/*.fbs and their generated Python modules in schemas/compiled_schemas.

To recompile the schemas:
- Run from the repository root (important, the script uses relative paths):
  - bash schemas/script.sh
- Ensure flatc (the FlatBuffers compiler) is available on your PATH.
- The generated Python files will be written to schemas/compiled_schemas.
- If needed, make the script executable first: `chmod +x schemas/script.sh`
- Optional: clean previous outputs before regenerating: `rm -rf schemas/compiled_schemas`


## MiscDataCollectors (live_data_processor)
MiscDataCollectors are instrument-specific helpers that collect auxiliary (non-event) data alongside the Kafka event stream. They are intended to be defined one per instrument. The exact data source is up to the instrument: a collector might read EPICS PVs, query hardware, aggregate metadata, or even do nothing.

### Design and lifecycle:
- `MiscDataCollector` is an abstract base class in `live_data_processor/collectors.py`.
- Default behavior: `on_run_start(ctx)` and `run_forever(ctx, stop_event)` are no-ops by default, so nothing happens unless an implementation overrides them.
- The `will_run_forever` property tells the processor whether to start a background loop via run_forever. Therefore, when defining a `run_forever` this property should be set to True.

### How they are used at runtime:
- `start_live_reduction()` calls `get_misc_data_collector(INSTRUMENT)` to obtain the instrument’s collector (one per instrument).
- `on_run_start(ctx)` is called once when a run initializes, allowing per-run setup.
- If `collector.will_run_forever` is True, the processor starts a background daemon thread running `run_forever(ctx, stop_event)`. When a new run is detected, stop_event is set and the thread is joined briefly before starting the next run.

### Example implementation:
- MerlinCollector (in live_data_processor/collectors.py):
  - Instruments: "MERLIN" map to MerlinCollector via `get_misc_data_collector()`.
  - Samples the EPICS PV IN:MERLIN:CS:SB:Rot approx. once per second.
  - Intended to write values to a Mantid time-series log named "Rot" on the live workspace (lives) using `AddTimeSeriesLog`.
  - Uses a run-identity check before and after the EPICS read to avoid attributing samples to the wrong run if a run changes between checks.

### Threading and run safety:
- Collectors that run forever execute on a background daemon thread.
- RunContext (live_data_processor/runs.py) provides `ctx.get_current_run()`; `_create_run_identifier` creates a stable identifier per run.
- The run identity is checked around collection to ensure logs apply to the correct run.

### Extending for a new instrument:
1. Create a subclass of `MiscDataCollector` in `live_data_processor/collectors.py` for your instrument.
   - Optionally override `on_run_start(ctx)` for per-run setup. By default, it is a no-op.
   - Optionally/typically override `run_forever(ctx, stop_event)` to perform continuous work. By default, it is a no-op.
   - Implement `will_run_forever` (`return True` if continuous `run_forever` is implemented).
2. Update `get_misc_data_collector(instrument)` to return your new collector for that instrument name (case-insensitive). This factory must be kept in sync whenever a new instrument-specific collector is added.
3. Ensure any required environment is available at runtime (e.g., EPICS_CA_* vars if you use EPICS, or other connectivity needed by your collector).

Minimal outline:
```python
class MyInstrumentCollector(MiscDataCollector):
@property
def will_run_forever(self) -> bool: return True

    def on_run_start(self, ctx):
        pass  # optional per-run setup

    def run_forever(self, ctx, stop_event):
        while not stop_event.is_set():
            # collect from your data source (does not have to be EPICS)
            value = ...
            # optionally write to Mantid logs, files, etc.
            AddTimeSeriesLog("lives", "MyLog", datetime.datetime.now().isoformat(), value)
            time.sleep(1.0)
```

## Configuration
### Operator environment
Environment variables used by live_data_operator/main.py:
- DEV_MODE (default: true)
- CEPH_CREDS_SECRET_NAME (default: ceph-creds)
- CEPH_CREDS_SECRET_NAMESPACE (default: fia)
- CLUSTER_ID (Ceph cluster ID; default set in source)
- FS_NAME (CephFS name; default: deneb)
- LIVE_DATA_PROCESSOR_IMAGE_SHA (digest for processor image)
- GITHUB_API_TOKEN (passed through to the processor)

Operator behavior:
- Creates PV/PVC for Ceph at /output and SMB archive at /archive.
- Deploys ghcr.io/fiaisis/live-data-processor@sha256:${LIVE_DATA_PROCESSOR_IMAGE_SHA} with env INSTRUMENT=<CR name> and GITHUB_API_TOKEN.

### Processor environment
Environment variables used by live_data_processor/main.py:
- OUTPUT_DIR (default: /output)
- KAFKA_IP (default: livedata.isis.cclrc.ac.uk)
- KAFKA_PORT (default: 31092)
- INSTRUMENT (default: SCIDEMO)
- INGEST_WAIT (seconds between script refresh checks; default: 30)
- LIVE_WS (Mantid live workspace name; default: lives)
- GITHUB_API_TOKEN (currently requested by the code path but the public GitHub endpoints are used)
- EPICS_CA_MAX_ARRAY_BYTES, EPICS_CA_ADDR_LIST, EPICS_CA_AUTO_ADDR_LIST are set within the processor for EPICS Channel Access.


## Docker images
Two Dockerfiles are provided:
- container/Dockerfile: builds the Operator image. It installs this package and runs kopf.
- live_data_processor/Dockerfile: builds the Processor image on top of ghcr.io/fiaisis/runner (includes Mantid) and installs runtime deps.


## Kubernetes deployment model
- Custom Resource: livedataprocessors.<group> (CRD manifest is not in this repo; ensure it exists in the cluster). The resource name is the instrument identifier; the Operator uses it as INSTRUMENT.
- Storage:
  - Ceph PV/PVC (read-write) for /output, created programmatically with CSI source configured from CEPH_* env and parameters.
  - SMB archive PV/PVC (read-only) for /archive; requires a secret named archive-creds with SMB credentials in CEPH_CREDS_SECRET_NAMESPACE.
- ServiceAccount: the Processor pods run as service account live-data-operator and tolerate a node taint key=staging, value=big, effect=NoSchedule.


