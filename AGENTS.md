# AGENTS.md

Guidance for coding agents working in this repository.

## What this repo is

The `live-data` repository contains two components that provide real-time streaming neutron data
reduction in the Flexible Interactive Automation (FIA) system (see `README.md`):

1. **`live-data-operator` (`live_data_operator/`)**: A Kopf-based Kubernetes operator that watches
   `LiveDataProcessor` Custom Resources (`livedataprocessors`). For each CR (where
   `metadata.name` is the instrument name, e.g. `merlin`, `mari`), it provisions the per-instrument
   CephFS `PersistentVolume`/`PersistentVolumeClaim` (`/isis/instrument/GENERIC/livereduce/<INST>[-staging]`
   mounted at `/output`), SMB archive PV/PVC (mounted at `/archive`), and the
   `livedataprocessor-<instrument>-deployment` running `ghcr.io/fiaisis/live-data-processor@sha256:<LIVE_DATA_PROCESSOR_IMAGE_SHA>`.
2. **`live-data-processor` (`live_data_processor/`)**: A Mantid-powered worker (built `FROM
   ghcr.io/fiaisis/mantid:<VERSION>`) that consumes FlatBuffers messages (`ess-streaming-data-types`:
   `pl72` `RunStart`, `ev42` `EventMessage`, `f144` sample logs) from instrument Kafka topics
   (`<INSTRUMENT>_events`, `<INSTRUMENT>_runInfo`, `<INSTRUMENT>_sampleEnv`) alongside EPICS sample
   logging (`pyepics`), maintains a live Mantid event workspace (`LIVE_WS`, default `lives`),
   periodically fetches/refreshes and executes `execute()` from the instrument's live reduction
   script via `FIA-API` (`GET {FIA_API_URL}/live-data/{instrument}/script`), streams user-visible
   logs to Valkey (`external_logger`), and rolls over when a new `RunStart` is detected by its
   background monitor thread.

*(Note: `README.md` still references the older layout `./pyproject.toml`, `./container/Dockerfile`,
and `./live_data_processor/Dockerfile`. On `main`, the two `pyproject.toml` files live at
`live_data_operator/pyproject.toml` and `live_data_processor/pyproject.toml`, and both Dockerfiles
live in `container/operator.Dockerfile` and `container/processor.Dockerfile`.)*

## Position in the FIA service

### Overarching FIA architecture

FIA is a multi-repository system (all checked out as siblings under the same parent workspace
directory) that automatically reduces instrument data, and also lets users browse, plot, and rerun
that data interactively:

```text
[ISIS Instrument Archive (/archive, /imat) & lastrun.txt]
        │
        ▼
  file-watcher (Kopf Operator + Per-Instrument Watcher Pods)
        │  publishes file path to RabbitMQ: `watched-files`
        ▼
  run-detection ◄──── fetches instrument rules (`GET /instrument/{inst}/specification`) ────► FIA-API
        │  ingests .nxs metadata, verifies rules, publishes JobRequest JSON
        │  to RabbitMQ: `scheduled-jobs` (or retries via `failed-watched-files`)
        ▼
  jobcontroller (job_creator) ◄──── requests PreScript (`GET /script/{inst}`) & posts Job ──► FIA-API ◄──► autoreduction-scripts
        │  spawns Kubernetes Job in cluster:
        ├──► Main container: runners (`ghcr.io/fiaisis/mantid` or `mantid-imaging`) -> writes reduced data to `/ceph`
        └──► Sidecar container: jobcontroller (job_watcher) -> updates job status/outputs in FIA-API (`PATCH /job/{id}`)

[Interactive / Web & Live Data Paths]
  scigateway (Host App) ──► frontend (FIA React Plugin)
                                ├──► fia-auth (UOWS login, JWT verify/refresh, `/experiments` RB lookup)
                                ├──► FIA-API (runs, jobs, scripts, specifications, reruns -> `watched-files` / `scheduled-jobs`)
                                └──► plotting-service (h5grove NeXus/HDF5 & IMAT image serving from `/ceph` & `/imat`)

  Kafka (`<INST>_events`, `<INST>_sampleEnv`, `<INST>_runInfo`)
        └──► live-data (THIS REPO) (Live Data Operator + Mantid Live Data Processor fetching `/live-data/{inst}/script` from FIA-API)

  LLSP (Long Lived Script Processors: `LLSP-API` + RabbitMQ + Celery `LLSP-Worker` built on `ghcr.io/fiaisis/mantid`)

[Infrastructure & Deployment]
  k8s (Terraform + Ansible for OpenStack clusters, Redpanda, local Minikube)
  gitops (ArgoCD App-of-Apps & Kustomize `base`/`envs`/`variants` manifests for all FIA services)
```

### This repository's location (upstream, downstream & siblings)

- **Role**: Manages and executes continuous real-time Mantid reductions on live neutron event
  streams per instrument.
- **Upstream inputs**:
  - Kafka topics `<INSTRUMENT>_events` (`ev42`), `<INSTRUMENT>_runInfo` (`pl72`), and
    `<INSTRUMENT>_sampleEnv` (`f144`) from the ISIS live data Kafka broker (`KAFKA_IP`:`KAFKA_PORT`),
    plus EPICS Channel Access sample logs.
  - `LiveDataProcessor` Custom Resources (`livedataprocessors`) applied via `gitops` (`../gitops`).
  - Base container image `ghcr.io/fiaisis/mantid:<VERSION>` built by `runners` (`../runners`), used
    both by `container/processor.Dockerfile` and by `.github/workflows/tests.yml` to run `pytest`.
- **Downstream outputs**:
  - `FIA-API` (`../FIA-API`) — `live_data_processor` fetches live reduction scripts from
    `GET {FIA_API_URL}/live-data/{instrument}/script` (and `FIA-API` streams live logs from Valkey
    to `frontend`).
  - CephFS (`/output` inside the processor container, mapped to
    `/isis/instrument/GENERIC/livereduce/<INSTRUMENT>[-staging]`) — reduced live data files read by
    `plotting-service` (`../plotting-service`) for live plotting in `frontend` (`../frontend`).
  - Valkey (`VALKEY_HOST`:`VALKEY_PORT`) — `external_logger` writes user-facing live reduction logs
    streamed to `frontend` (`../frontend`).
  - `gitops` (`../gitops`) — `.github/workflows/build-push.yml` builds and pushes both
    `ghcr.io/fiaisis/live-data-operator` and `ghcr.io/fiaisis/live-data-processor` on every push to
    `main`, then automatically updates the operator image digest and `LIVE_DATA_PROCESSOR_IMAGE_SHA`
    in `./components/live-data-operator/envs/staging/live-data-operator.yml`.
- **Sibling repositories to check when changing contracts**: `../FIA-API` (`/live-data/{inst}/script`
  and live log streaming), `../plotting-service` and `../frontend` (live data output files and
  Valkey log stream), `../runners` (`ghcr.io/fiaisis/mantid` base image), and `../gitops`
  (`components/live-data-operator/`).

### Core technologies
- **Python**: version/minimum is defined by `requires-python` in
  `live_data_operator/pyproject.toml` and `live_data_processor/pyproject.toml` — do not hardcode a
  version here; check those files and `.github/workflows/` before creating an environment.
- **kopf** + **kubernetes**: Operator framework and Kubernetes API client (`live_data_operator`).
- **Mantid** (`mantid.simpleapi`, `mantid.api`): Neutron data workspace and algorithm engine
  (`live_data_processor`, provided by the `ghcr.io/fiaisis/mantid` base image).
- **kafka-python** + **ess-streaming-data-types**: Kafka consumer and FlatBuffers deserialization
  (`ev42`, `pl72`, `f144`).
- **pyepics**: EPICS Channel Access sample environment logging.
- **redis** (Valkey client) + **requests**: Streaming external logs to Valkey and fetching live
  scripts from `FIA-API`.
- **pytest** (+ `pytest-cov`, `pytest-random-order`, `pytest-env`): Unit test suite.
- **ruff**: Linting and formatting (`line-length = 120`, `exclude = ["schemas"]`).
- **mypy**: Static type checking (`mypy --strict live_data_operator` in CI).
- **Docker**: Two Dockerfiles (`container/operator.Dockerfile` and
  `container/processor.Dockerfile`).

---

## Repository structure

```
live_data_operator/          Kopf operator package — reconciles `livedataprocessors` CRs, provisions
                              per-instrument CephFS (/output) and SMB (/archive) PVs/PVCs, and
                              creates/recreates `livedataprocessor-<inst>-deployment`; includes its
                              own `pyproject.toml`
live_data_processor/         Mantid live processor package — Kafka consumers & offset seeking,
                              background `runInfo` monitor thread, Mantid workspace initialization
                              and event ingestion, periodic FIA-API live script refresh/execution,
                              EPICS sample log streaming, and dual internal/external (Valkey)
                              loggers; includes its own `pyproject.toml`
test/                        Pytest unit test suite covering operator, processor main loop, script
                              fetching/refreshing, and loggers
container/                   Dockerfiles for the operator (`operator.Dockerfile`) and processor
                              (`processor.Dockerfile`)
schemas/, simulator/         Compiled FlatBuffers schema helpers and local stream simulator utilities
.github/workflows/           CI/CD workflows: tests.yml, formatting_and_linting.yml, build-push.yml,
                              dependency-review.yml
```

---

## Dual `pyproject.toml` files — read these first

`live_data_operator/pyproject.toml` and `live_data_processor/pyproject.toml` are the sources of
truth for their respective components (there is no root `pyproject.toml`):

- Runtime dependencies and optional extras (`formatting`, `test`, `all`) are pinned with `==` in
  both files — keep new dependencies pinned the same way, and keep shared dev/test tool versions
  (`pytest`, `ruff`, `mypy`) synchronized across both files.
- Note that `live_data_processor` also requires `mantid` at runtime and during `pytest` collection,
  which is supplied by the `ghcr.io/fiaisis/mantid` container/conda environment rather than PyPI.
- Both `pyproject.toml` files configure `required_plugins = "pytest-env"` and Ruff (`line-length = 120`,
  `exclude = ["schemas"]`).
- CI runs `mypy --strict live_data_operator` from the repository root.

---

## Configuration & logging conventions

### `live_data_processor` environment variables
| Variable | Default | Description |
| :--- | :--- | :--- |
| `INSTRUMENT` | `MERLIN` | Instrument name (uppercased at startup) |
| `KAFKA_IP` / `KAFKA_PORT` | `livedata.isis.cclrc.ac.uk` / `31092` | Kafka bootstrap server host and port |
| `LIVE_WS` | `lives` | Name of the Mantid event workspace maintained in memory |
| `SCRIPT_REFRESH_TIME` | `30` | Seconds between checks to `FIA-API` for updated live reduction scripts |
| `SCRIPT_RUN_INTERVAL` | `300` | Seconds between executions of the reduction script's `execute()` function |
| `RUN_CHECK_INTERVAL` | `3` | Seconds between retries when waiting for a valid `RunStart` |
| `OUTPUT_DIR` | `/output` | Output directory inside the processor container |
| `FIA_API_URL` | `https://dev.reduce.isis.cclrc.ac.uk/api` *(or passed by operator)* | Base URL for `GET {FIA_API_URL}/live-data/{instrument}/script` |
| `VALKEY_HOST` / `VALKEY_PORT` | `valkey.valkey.svc.cluster.local` / `6379` | Valkey host and port used by `external_logger` |
| `GITHUB_API_TOKEN` | `shh` | Token passed through if needed by scripts |

### Internal vs. external loggers in `live_data_processor`
`live_data_processor/loggers.py` sets up two loggers:
- `external_logger`: Streams log entries to Valkey so they are visible to end users in the
  `frontend` live log viewer (also captures stdout/stderr during `reduction_function()` via
  `capture_and_tee(external_logger)`).
- `internal_logger`: Logs only to container stdout for operator/developer diagnostics. Use
  `internal_logger` for any message that should **not** be exposed to end users.

### `live_data_operator` environment variables
| Variable | Default | Description |
| :--- | :--- | :--- |
| `LIVE_DATA_PROCESSOR_IMAGE_SHA` | *(default SHA in `main.py`)* | Digest for `ghcr.io/fiaisis/live-data-processor@sha256:...` |
| `DEV_MODE` | `true` | Appends `-staging` to the CephFS `rootPath` when `"true"` |
| `CEPH_CREDS_SECRET_NAME` / `CEPH_CREDS_SECRET_NAMESPACE` | `ceph-creds` / `fia` | Secret name and target namespace for deployments/PVCs |
| `CLUSTER_ID` / `FS_NAME` | `ba68226a-672f-4ba5-97bc-22840318b2ec` / `deneb` | Ceph cluster ID and filesystem name |
| `FIA_API_URL` | `http://localhost:8000` | Passed down to the processor container |
| `VALKEY_HOST` / `VALKEY_PORT` | `valkey.valkey.svc.cluster.local` / `6379` | Passed down to the processor container |
| `GITHUB_API_TOKEN` | `shh` | Passed down to the processor container |

---

## Environment & running commands

Do not assume any particular machine already has a conda env, kubectl context, or virtualenv set up
— check what is available first. Because `live_data_processor` and `test/` import `mantid` (which is
distributed via Conda/`ghcr.io/fiaisis/mantid`, not PyPI), **a Conda environment with `mantid`
installed (or the `ghcr.io/fiaisis/mantid` container, as used in `.github/workflows/tests.yml`) is
required to run the full test suite**.

1. Check existing conda environments:
   ```shell
   conda env list
   ```
   If a matching env exists (e.g. `live-data`), use it with `conda run -n <env-name> <command>`.
2. If creating a new conda environment, install `mantid` from the `mantid`/`conda-forge` channels
   and then install the dependencies from both `live_data_operator/pyproject.toml` and
   `live_data_processor/pyproject.toml`.
3. For running `ruff` and `mypy --strict live_data_operator` alone (which do not import `mantid`),
   either Conda or a standard `venv` with `./live_data_operator[all]` installed works:
   ```shell
   conda run -n <env-name> pip install "./live_data_operator[all]"
   ```

### Running tests, linting & containers
- **Unit tests (requires `mantid` in the active Conda env or `ghcr.io/fiaisis/mantid` container,
  with `PYTHONPATH=.` from repo root)**:
  ```shell
  PYTHONPATH=. conda run -n <env-name> pytest test/ --random-order --random-order-bucket=global
  ```
- **Linting, formatting & type checking (from repo root, matching CI)**:
  ```shell
  conda run -n <env-name> ruff check .
  conda run -n <env-name> ruff format --check .
  conda run -n <env-name> mypy --strict live_data_operator
  ```
- **Docker builds** (from repo root, matching `.github/workflows/build-push.yml`):
  ```shell
  docker build -f ./container/operator.Dockerfile . -t ghcr.io/fiaisis/live-data-operator
  docker build -f ./container/processor.Dockerfile . -t ghcr.io/fiaisis/live-data-processor
  ```
  *(Note: `container/processor.Dockerfile` resets `ENTRYPOINT []` from the `ghcr.io/fiaisis/mantid`
  base image and sets `CMD ["python", "main.py"]`; keep that reset intact so the processor does not
  inherit the runner's `python -c "$@"` entrypoint.)*

## Kubernetes commands

Available kubectl contexts vary per machine/operator — always run `kubectl config get-contexts`
first before targeting a cluster. Never rely on the current context; always pass `--context`
explicitly at the **end** of the command:

```shell
kubectl get pods -n fia --context <context-name>
kubectl get livedataprocessors -n fia --context <context-name>
kubectl logs deploy/live-data-operator -n fia --context <context-name>
```

Double- and triple-check the context name before running anything against a production-like cluster.

## Managing issues

This repo has no local `.github/ISSUE_TEMPLATE/`; GitHub serves the org-wide default template from
`fiaisis/.github` instead. Always use it rather than inventing an ad hoc issue format:

```shell
gh issue create --template Issue
```

Check `gh issue list` / `gh issue view <number>` first to avoid duplicates.

## Branch naming

Branches use `snake_case` descriptions of the work being done. Do not use category prefixes like
`feature/` or `fix/`.

- **Format**: `description_of_work` (e.g. `seperate_run_checking`, `update_agent_guidelines`).

## Commit messages

`main`'s history (`git log --oneline main`) uses **squash merges** for feature/fix PRs (`<PR title>
(#<PR number>)`, e.g. `Seperate run checking (#34)`) alongside merge commits for some Dependabot
PRs. This has two practical consequences:

- **The PR title is what survives as the main summary line.** Individual commits on a working branch
  are squashed on merge, so getting the PR title right (see "Pull request process" below) matters
  more than crafting a commit-by-commit history.
- **Individual commit messages still matter for review and for the squash commit body.** By default,
  GitHub's squash merge concatenates every commit subject on the branch into the squashed commit's
  body. Write each commit as a focused, reviewable unit of work with a clear, imperative-mood
  summary line (and a short *why* body paragraph on non-trivial commits) — avoid noise like `wip` or
  `fix` that would leak into the squashed body. Leave auto-generated Dependabot commit messages as-is.

## Requirements for code changes

Only run tests, linters, and type checkers when they are **relevant to the files you actually
changed** — if a change only touches documentation (e.g. `.md` files), skip `pytest`, `ruff`, and
`mypy`. When you do change Python code, dependency manifests, or configuration, before considering
the change complete:
1. **Add or update unit tests** in `test/` (covering `live_data_operator` or `live_data_processor`,
   whichever you changed) for any new or changed behavior. There is no separate E2E suite in this
   repo beyond these Mantid-container-backed unit tests.
2. **Run the linter and type checker** (`ruff check .`, `ruff format .`,
   `mypy --strict live_data_operator`) and fix everything reported.
3. **Run the full test suite with coverage** (requires `mantid` — see "Environment & running
   commands") and confirm coverage has not regressed compared to `main` (CI uploads results to
   Codecov; locally, add `--cov --cov-report=term-missing` to the `pytest` command).

## Pull request process

1. **Local validation** before opening a PR (when `.py` / `pyproject.toml` / container files are
   changed; skip for `.md`-only changes):
   ```shell
   PYTHONPATH=. conda run -n <env-name> pytest test/ --random-order --random-order-bucket=global
   conda run -n <env-name> ruff check .
   conda run -n <env-name> ruff format .
   conda run -n <env-name> mypy --strict live_data_operator
   ```
   Note: the CI workflow (`.github/workflows/formatting_and_linting.yml`) also runs `ruff format .`,
   `ruff check --fix`, and `mypy --strict live_data_operator` on every non-`main` push and
   auto-commits any fixes back to the branch as `"Formatting and linting commit"`.
2. **PR title**: a clear, concise description in normal grammar/sentence case. Do not use
   `snake_case` for PR titles.
3. **PR description**: use the org-wide default template from `fiaisis/.github`
   (`.github/pull_request_template.md` — no local override in this repo), which is just
   `Closes # <Issue Number>` followed by a `## Description` section. Ask the user for the issue
   number if one isn't evident from the task, or confirm there is none to link.
4. **AI-assistance label**: attach a label to the PR indicating that it was made with the assistance
   of AI. Check existing labels first with `gh label list` (this repo already defines the `AI`
   label) and apply it via `gh pr create --label AI` or `gh pr edit <number> --add-label AI`.

## Handling Dependabot PRs

Dependabot creates PRs grouped as `python-packages` (weekly), `action-packages` (weekly, GitHub
Actions), and `docker` (daily) per `.github/dependabot.yml`. When asked to handle these:

1. **List and identify**: `gh pr list` to find open Dependabot PRs.
2. **Review changes**: `gh pr diff <number>`, paying close attention to major version bumps in
   `live_data_operator/pyproject.toml`, `live_data_processor/pyproject.toml`,
   `container/*.Dockerfile`, or `.github/workflows/`.
   *(Note: When a shared test/lint dependency like `pytest`, `ruff`, or `mypy` is bumped in only one
   `pyproject.toml`, update the matching pin in the other `pyproject.toml` as well so CI's combined
   dependency install in `.github/workflows/tests.yml` does not hit conflicting `==` pins.)*
3. **Validate**:
   - Python/dependency changes: `gh pr checkout <number>`, run `ruff check .`,
     `ruff format --check .`, `mypy --strict live_data_operator`, and the `pytest` suite (or verify
     the Mantid container CI test run via `gh pr checks <number>`).
   - Docker/Action changes: verify CI checks pass with `gh pr checks <number>`.
4. **Approve and merge** once validation passes:
   ```shell
   gh pr review <number> --approve
   gh pr merge <number> --merge
   ```
   Use `--admin` if blocked by branch protection (`gh pr merge <number> --merge --admin`).
5. **Clean up**: close redundant/superseded PRs with `gh pr close <number> --delete-branch`.
6. **Final verification**: checkout `main`, pull latest changes, and confirm tests/checks are green.

## General working practices

- Prefer the `read` tool (with `offset`/`limit`) over `sed -n`, `cat`, or similar shell one-liners
  when inspecting file contents, and prefer the `write` and `edit` tools over `bash` (`cat <<EOF`,
  `sed -i`, `echo >`), Python one-liners, or other non-tool-based commands whenever creating or
  modifying files.
- Ask the user a clarifying question whenever a task is ambiguous (e.g. whether a change applies to
  `live_data_operator` or `live_data_processor`, whether a log message should go to
  `external_logger` vs. `internal_logger`, or whether an issue number exists to link in a PR) rather
  than guessing.
- Keep dependency versions pinned with `==` in both `pyproject.toml` files consistent with the
  existing style.

## Keeping this document up to date

If a change you make would make any information in this `AGENTS.md` inaccurate or incomplete —
e.g. a new environment variable, a changed Kafka topic/schema or Valkey logging contract, a changed
directory or Dockerfile layout, a new/renamed CI workflow, an updated Dependabot grouping, a new
sibling-repo contract, or a different testing/linting command — update the relevant section of this
file as part of the same change. Treat it the same way you would treat `README.md`: documentation
that goes stale is worse than no documentation, so don't leave it to a future agent or PR to notice
the drift.
