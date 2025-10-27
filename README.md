# live-data

This repository contains two components that work together:
- Live Data Operator: A Kopf-based Kubernetes operator that provisions and manages per-instrument Live Data Processor deployments and their storage (CephFS and/or archive volumes).
- Live Data Processor: A Mantid-powered worker that consumes instrument Kafka topics, maintains a live event workspace, periodically executes a dynamically fetched reduction script, and rolls over when new runs start.

![License: GPL-3.0](https://img.shields.io/github/license/fiaisis/live-data)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![Kopf](https://img.shields.io/badge/operator-kopf-28a)](https://kopf.readthedocs.io/)


## Project layout
- container/Dockerfile — Image for the Live Data Operator (runs kopf)
- live_data_operator/ — Operator code that creates/updates a Deployment for the processor and sets up storage
- live_data_processor/ — Processor code that connects to Kafka, updates a Mantid workspace, and runs reduction scripts
- live_data_processor/Dockerfile — Image for the Live Data Processor (Mantid base image)
- pyproject.toml — Root Python package metadata for the operator distribution


## Local install
Requires Python 3.11+.

This repo contains two separate Python packages, each with its own pyproject.toml:
- Root (./pyproject.toml): Live Data Operator only
- Processor (./live_data_processor/pyproject.toml): Live Data Processor only

Install the operator (root project):
- pip install .
- For development extras: pip install -e .[formatting,test]

Install the processor (subproject):
- cd live_data_processor
- pip install .
- For development extras: pip install -e .[formatting,test]

Note: The operator and processor are released independently. Installing at the repo root does not install the processor package, and installing the processor does not install the operator.


## Running locally for development
You can run components independently.

### Live Data Processor (standalone)
The processor consumes instrument topics from Kafka, keeps a live Mantid event workspace, periodically executes a fetched reduction script, and moves to the next run when detected.

Required/used environment variables:
- INSTRUMENT: Instrument name (e.g., MERLIN). Default: MERLIN
- KAFKA_IP: Kafka bootstrap host. Default: livedata.isis.cclrc.ac.uk
- KAFKA_PORT: Kafka bootstrap port. Default: 31092
- LIVE_WS: Mantid workspace name to maintain. Default: lives
- SCRIPT_REFRESH_TIME: Seconds between checks for script updates. Default: 30
- SCRIPT_RUN_INTERVAL: Seconds between script executions. Default: 300 (5 minutes)
- RUN_CHECK_INTERVAL: Seconds between checks for run rollover. Default: 3
- OUTPUT_DIR: Output directory inside the container/environment. Default: /output
- FIA_API_URL: Base URL for the FIA API to fetch the live script. Default: https://dev.reduce.isis.cclrc.ac.uk/api
- GITHUB_API_TOKEN: Token if your reduction scripts or dependencies need GitHub access. Default: shh

Run:
- python live_data_processor/main.py

Notes:
- The processor expects Kafka topics named <INSTRUMENT>_events, <INSTRUMENT>_sampleEnv, and <INSTRUMENT>_runInfo.
- Mantid is required. For local development outside the container, ensure Mantid is installed and available on PYTHONPATH. The processor container image already includes Mantid.
- The processor fetches reduction scripts from: {FIA_API_URL}/live-data/{instrument}/script and writes them to reduction_script.py. It then imports and executes a function named execute(), refreshing when the source changes.


### Live Data Operator (Kopf)
The operator runs in-cluster (or locally) and manages a Deployment for each instrument’s Live Data Processor, including CephFS and archive volumes.

How it decides what to deploy:
- The operator watches a Custom Resource (CR) kind named "LiveDataProcessor" (CRD: livedataprocessors).
- Each CR’s metadata.name is the instrument identifier (e.g., merlin, mari).
- When a CR exists, the operator creates/updates a single Deployment for that instrument’s processor. On resume, it reconciles to the latest image/config.

Key environment variables:
- LIVE_DATA_PROCESSOR_IMAGE_SHA: Digest SHA of the processor image to deploy (without prefix). Used to pin image updates.
- CEPH_CREDS_SECRET_NAME: Name of the Kubernetes Secret with Ceph credentials. Default: ceph-creds
- CEPH_CREDS_SECRET_NAMESPACE: Namespace holding the Ceph credentials Secret. Default: fia
- CLUSTER_ID: Ceph cluster id. Default: ba68226a-672f-4ba5-97bc-22840318b2ec
- FS_NAME: CephFS name. Default: deneb
- FIA_API_URL: Base URL for the FIA API (passed through to the processor). Default: http://localhost:8000
- DEV_MODE: When true, enables developer-friendly defaults in the operator. Default: true
- GITHUB_API_TOKEN: Token passed down if needed by scripts.

Run locally (requires kubeconfig and cluster access):
- kopf run --liveness=http://0.0.0.0:8080/healthz live_data_operator/main.py --verbose

What it does:
- Creates/updates a Deployment named livedataprocessor-<instrument>-deployment with the current processor image ref: ghcr.io/fiaisis/live-data-processor@sha256:<LIVE_DATA_PROCESSOR_IMAGE_SHA>
- Provisions CephFS PV/PVC and archive PV/PVC mounts for the processor Pod
- Supports recreating the Deployment to roll image updates


## Containers
Two images are provided/defined by Dockerfiles in this repo.

### Build the Live Data Operator image
From repository root:
- docker build -f ./container/Dockerfile . -t ghcr.io/fiaisis/live-data:operator

Run locally (listens on 8080 for liveness):
- docker run --rm -p 8080:8080 \
    -e KUBECONFIG=/root/.kube/config \
    -v $HOME/.kube:/root/.kube:ro \
    ghcr.io/fiaisis/live-data:operator

### Build the Live Data Processor image
From repository root:
- docker build -f ./live_data_processor/Dockerfile . -t ghcr.io/fiaisis/live-data-processor

Run pointing at a Kafka broker and instrument:
- docker run --rm \
    -e INSTRUMENT=MERLIN \
    -e KAFKA_IP=broker.example.org \
    -e KAFKA_PORT=9092 \
    -e FIA_API_URL=https://dev.reduce.isis.cclrc.ac.uk/api \
    ghcr.io/fiaisis/live-data-processor


In general, topics are named <INSTRUMENT>_events, <INSTRUMENT>_sampleEnv, and <INSTRUMENT>_runInfo.

The processor seeks to the RunStart timestamp across the subscribed topics, processes ev42 events into a Mantid event workspace, deserialises f144 logs into time series (AddTimeSeriesLog), and periodically executes the latest reduction script.
