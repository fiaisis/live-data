"""
Main Module
"""

import logging
import os
import sys
from http import HTTPStatus
from typing import Any

import kopf
from kubernetes.client import (  # type: ignore
    ApiClient,
    ApiException,
    AppsV1Api,
    CoreV1Api,
    V1Container,
    V1CSIPersistentVolumeSource,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1LabelSelector,
    V1ObjectMeta,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PersistentVolumeSpec,
    V1PodSpec,
    V1PodTemplateSpec,
    V1ResourceRequirements,
    V1SecretReference,
    V1Volume,
    V1VolumeMount,
)


class EndpointFilter(logging.Filter):
    """
    A logging filter to exclude health and readiness probe messages.

    Filters out log messages containing "/healthz" or "/ready" from aiohttp.access logs.

    :param logging.Filter: Base class for all logging filters.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/healthz") == -1 and record.getMessage().find("/ready") == -1


stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("LiveDataOperator")
logging.getLogger("aiohttp.access").addFilter(EndpointFilter())

DEV_MODE = os.environ.get("DEV_MODE", "true").lower() == "true"


CEPH_CREDS_SECRET_NAME = os.environ.get("CEPH_CREDS_SECRET_NAME", "ceph-creds")
PROCESSOR_IMAGE = os.environ.get("LIVE_DATA_PROCESSOR_IMAGE_SHA", "50f170947badb84cac95e094cdd245df6ca3bfb6")
CEPH_CREDS_SECRET_NAMESPACE = os.environ.get("CEPH_CREDS_SECRET_NAMESPACE", "fia")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "ba68226a-672f-4ba5-97bc-22840318b2ec")
FS_NAME = os.environ.get("FS_NAME", "deneb")


def _setup_ceph_pv(
    ceph_creds_k8s_secret_name: str,
    ceph_creds_k8s_namespace: str,
    cluster_id: str,
    fs_name: str,
    instrument: str,
) -> None:
    """
    Sets up a Ceph PersistentVolume (PV) in Kubernetes.

    Creates a static CephFS-based PV with the specified attributes, including storage, access modes, and volume settings

    :param ceph_creds_k8s_secret_name: Name of the Kubernetes secret containing Ceph credentials.
    :param ceph_creds_k8s_namespace: Namespace where the secret is located.
    :param cluster_id: Unique ID for the Ceph cluster.
    :param fs_name: CephFS file system name.
    :param instrument: Name of the instrument, used to create volume attributes.
    :return: None
    """
    try:
        pv_name = f"livedataprocessor-{instrument}-ceph-pv"
        metadata = V1ObjectMeta(name=pv_name)
        secret_ref = V1SecretReference(name=ceph_creds_k8s_secret_name, namespace=ceph_creds_k8s_namespace)
        csi = V1CSIPersistentVolumeSource(
            driver="cephfs.csi.ceph.com",
            node_stage_secret_ref=secret_ref,
            volume_handle=pv_name,
            volume_attributes={
                "clusterID": cluster_id,
                "mounter": "fuse",
                "fsName": fs_name,
                "staticVolume": "true",
                "rootPath": f"/isis/instrument/GENERIC/livereduce/{instrument.upper()}{'-staging' if DEV_MODE else ''}",
            },
        )
        spec = V1PersistentVolumeSpec(
            capacity={"storage": "1000Gi"},
            storage_class_name="",
            access_modes=["ReadWriteMany"],
            persistent_volume_reclaim_policy="Retain",
            volume_mode="Filesystem",
            csi=csi,
        )
        ceph_pv = V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
        CoreV1Api().create_persistent_volume(ceph_pv)
    except ApiException as exc:
        if exc.status == HTTPStatus.CONFLICT:
            logger.info("PV already exists, skipping creation")
        else:
            raise


def _setup_ceph_pvc(instrument: str, namespace: str) -> None:
    """
    Sets up a Ceph PersistentVolumeClaim (PVC) in Kubernetes.

    Creates a PVC that binds to a pre-existing Ceph PV, ensuring storage access for the specified instrument.

    :param instrument: Name of the instrument, used to define the PVC name and volume binding.
    :param namespace: Namespace in which the PVC is created.
    :return: None
    """
    try:
        pvc_name = f"livedataprocessor-{instrument}-ceph-pvc"
        metadata = V1ObjectMeta(name=pvc_name)
        resources = V1ResourceRequirements(requests={"storage": "1000Gi"})
        spec = V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteMany"],
            resources=resources,
            volume_name=f"livedataprocessor-{instrument}-ceph-pv",
            storage_class_name="",
        )
        ceph_pvc = V1PersistentVolumeClaim(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=metadata,
            spec=spec,
        )
        CoreV1Api().create_namespaced_persistent_volume_claim(namespace=namespace, body=ceph_pvc)
    except ApiException as exc:
        if exc.status == HTTPStatus.CONFLICT:
            logger.info("PVC Already exists, skipping creation")
        else:
            raise


def setup_deployment(
    ceph_creds_k8s_secret_name: str, cluster_id: str, instrument: str, namespace: str, fs_name: str
) -> V1Deployment:
    """
    Sets up a Kubernetes Deployment for the Live Data Processor.

    Configures a deployment with a containerized live data processor and its associated storage volumes, environment
    variables, and restart policies.

    :param ceph_creds_k8s_secret_name: Name of the Kubernetes secret containing Ceph credentials.
    :param cluster_id: Unique ID of the Ceph cluster.
    :param instrument: Name of the instrument for which the deployment will process live data.
    :param namespace: Namespace in which the deployment will be created.
    :param fs_name: CephFS file system name to be used by the deployment.
    :return: Configured V1Deployment object ready for deployment in Kubernetes.
    """

    _setup_ceph_pv(ceph_creds_k8s_secret_name, namespace, cluster_id, fs_name, instrument)
    _setup_ceph_pvc(instrument, namespace)

    container = V1Container(
        name=f"livedataprocessor-{instrument}",
        image=f"ghcr.io/fiaisis/live-data-processor@sha256:{PROCESSOR_IMAGE}",
        volume_mounts=[V1VolumeMount(name="ceph-mount", mount_path="/output")],
        env=[V1EnvVar(name="INSTRUMENT", value=instrument)],
    )
    pod_spec = V1PodSpec(
        containers=[container],
        restart_policy="Always",
        service_account="live-data-operator",
        volumes=[
            V1Volume(
                name="ceph-mount",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"livedataprocessor-{instrument}-ceph-pvc", read_only=False
                ),
            )
        ],
    )
    labels = {"app": f"livedataprocessor-{instrument}"}
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels=labels), spec=pod_spec)
    deployment_spec = V1DeploymentSpec(replicas=1, selector=V1LabelSelector(match_labels=labels), template=template)

    return V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        spec=deployment_spec,
        metadata=V1ObjectMeta(
            name=f"livedataprocessor-{instrument}-deployment", labels={"processor-image-sha": PROCESSOR_IMAGE[0:12]}
        ),
    )


def start_live_data_processor(instrument: str) -> None:
    """
    Initialize and start the Live Data Processor for a given instrument.

    This function sets up and deploys a Kubernetes Deployment to process live data
    for the specified instrument. It manages any existing deployment by deleting
    it and recreating a new one in case of conflicts.

    :param instrument: The name of the instrument for which the live data processor is deployed.
    :return: None
    """

    body = setup_deployment(CEPH_CREDS_SECRET_NAME, CLUSTER_ID, instrument, CEPH_CREDS_SECRET_NAMESPACE, FS_NAME)
    body = ApiClient().sanitize_for_serialization(body)  # serialize so kopf may adopt it
    kopf.adopt(body)

    try:
        AppsV1Api().create_namespaced_deployment(namespace=CEPH_CREDS_SECRET_NAMESPACE, body=body)
    except ApiException as exc:
        if exc.status == HTTPStatus.CONFLICT:
            logger.info("Deployment already exists, Deleting previous deployment and creating new one")
            AppsV1Api().delete_namespaced_deployment(
                name=f"livedataprocessor-{instrument}-deployment", namespace=CEPH_CREDS_SECRET_NAMESPACE
            )
            start_live_data_processor(instrument)
        else:
            raise


@kopf.on.create("livedataprocessors")
def create_fn(body: Any, spec: Any, **kwargs: Any) -> None:
    """
    Handles the creation of a LiveDataProcessor custom resource in Kubernetes.

    This function is triggered when a LiveDataProcessor custom resource is created
    in the Kubernetes cluster. It retrieves the instrument identifier from the resource
    metadata and initializes the corresponding live data processor deployment.

    :param body: A dictionary representation of the custom resource's body, containing metadata,
                 specifications, and other details about the resource.
    :param spec: UNUSED The specifications of the custom resource, as provided in its definition.
                 Typically used to configure the resource's behavior.
    :param kwargs: UNUSED Additional parameters provided by Kopf's create handler, such as event-specific
                   context and resource information.
    :return: None
    """
    instrument = body["metadata"]["name"]
    logger.info(f"Creating LiveDataProcessor {body['metadata']['name']}")
    start_live_data_processor(instrument)


@kopf.on.resume("livedataprocessors")
def resume_fn(body: Any, spec: Any, **kwargs: Any) -> None:
    """
    Handles the resumption of a LiveDataProcessor custom resource in Kubernetes.

    This function is invoked when a LiveDataProcessor custom resource is resumed in the Kubernetes cluster,
    such as after a pod restart or operator downtime. It checks if the processor image has changed since the
    last deployment and redeploys the processor if necessary.

    :param body: A dictionary representation of the custom resource's body, containing metadata,
                 specifications, and other details about the resource.
    :param spec: UNUSED The specifications of the custom resource, as provided in its definition.
                 Typically used to configure the resource's behavior.
    :param kwargs: UNUSED Additional parameters provided by the Kopf's resume handler, such as event-specific
                   context and resource information.
    :return: None
    """
    instrument = body["metadata"]["name"]
    logger.info(f"Resuming LiveDataProcessor {instrument}")

    deployed_image = body.get("metadata", {}).get("labels", {}).get("processor-image-sha", "None")

    if deployed_image != PROCESSOR_IMAGE:
        logger.info(f"Image changed for {instrument}, redeploying.")
        start_live_data_processor(instrument)
    else:
        logger.info(f"No image change for {instrument}, skipping redeploy.")
