"""
Main Module
"""

import logging
import os
import sys
import time
from collections.abc import Callable
from functools import wraps
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
    V1DeleteOptions,
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
    V1Toleration,
    V1Volume,
    V1VolumeMount,
)

GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN", "shh")


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
logging.getLogger("kopf").setLevel(logging.INFO)
logging.getLogger("kopf.objects").setLevel(logging.INFO)
logging.getLogger("kopf._cogs.clients.w").setLevel(logging.INFO)
logging.getLogger("kubernetes").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)


DEV_MODE = os.environ.get("DEV_MODE", "true").lower() == "true"


CEPH_CREDS_SECRET_NAME = os.environ.get("CEPH_CREDS_SECRET_NAME", "ceph-creds")
PROCESSOR_IMAGE = os.environ.get("LIVE_DATA_PROCESSOR_IMAGE_SHA", "50f170947badb84cac95e094cdd245df6ca3bfb6")
CEPH_CREDS_SECRET_NAMESPACE = os.environ.get("CEPH_CREDS_SECRET_NAMESPACE", "fia")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "ba68226a-672f-4ba5-97bc-22840318b2ec")
FS_NAME = os.environ.get("FS_NAME", "deneb")
FIA_API_URL = os.environ.get("FIA_API_URL", "http://localhost:8000")


def get_processor_image_sha() -> str:
    """
    Get the processor image sha from the environment variable.
    We use a function so that if the image update changes, a reload is guaranteed
    """
    return os.environ.get("LIVE_DATA_PROCESSOR_IMAGE_SHA", "50f170947badb84cac95e094cdd245df6ca3bfb6")


def processor_image_ref(sha: str) -> str:
    """
    Given a sha, return the image reference

    :param sha: The sha of the image
    :return: The image reference
    """
    return f"ghcr.io/fiaisis/live-data-processor@sha256:{sha}"


def recreate_processor_deployment(instrument: str, namespace: str) -> None:
    """
    Deletes the processor Deployment if it exists, waits for deletion to complete,
    then recreates it using the current image SHA and spec from setup_deployment().

    :param instrument: The instrument for which the deployment is being recreated
    :param namespace: The namespace in which the deployment is being recreated

    :return: None
    """
    api = AppsV1Api()
    name = f"livedataprocessor-{instrument}-deployment"

    # --- delete if exists ---
    try:
        logger.info("Deleting Deployment %s/%s (if it exists)...", namespace, name)
        api.delete_namespaced_deployment(
            name=name,
            namespace=namespace,
            body=V1DeleteOptions(
                propagation_policy="Foreground",
                grace_period_seconds=30,
            ),
        )
    except ApiException as exc:
        if exc.status == HTTPStatus.NOT_FOUND:
            logger.info("Deployment %s/%s not found; will create fresh.", namespace, name)
        else:
            raise

    # wait until it's really gone
    start = time.time()
    delete_timeout = 60
    while True:
        try:
            api.read_namespaced_deployment(name=name, namespace=namespace)
            if time.time() - start > delete_timeout:
                raise TimeoutError(f"Timed out waiting for deletion of {namespace}/{name}")
            time.sleep(1)
        except ApiException as exc:
            if exc.status == HTTPStatus.NOT_FOUND:
                break
            raise

    body = setup_deployment(
        ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
        cluster_id=CLUSTER_ID,
        instrument=instrument,
        namespace=namespace,
        fs_name=FS_NAME,
    )

    body = ApiClient().sanitize_for_serialization(body)
    kopf.adopt(body)

    logger.info("Creating Deployment %s/%s with current image SHA...", namespace, name)
    AppsV1Api().create_namespaced_deployment(namespace=namespace, body=body)


def skip_conflict(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to skip the creation of a resource that already exists
    :param func: The func to wrap
    :return: The wrapped function
    """

    @wraps(func)
    def wrapper(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any: 
        try:
            logger.info("Inside decorator")
            return func(*args, **kwargs)
        except ApiException as exc:
            if exc.status == HTTPStatus.CONFLICT:
                logger.info("resource created by %s already exists, skipping creation", func.__name__)
            else:
                raise

    return wrapper


@skip_conflict
def _setup_archive_pv(instrument: str, secret_namespace: str) -> None:
    """
    Sets up the archive PV using the loaded kubeconfig as a destination
    :param instrument: str, the name of the instrument for the livedataprocessor pv
    :param secret_namespace: str, the namespace of the secret for mounting
    :return: str, the name of the archive PV
    """

    pv_name = f"livedataprocessor-{instrument}-archive-pv-smb"
    metadata = V1ObjectMeta(name=pv_name, annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"})
    secret_ref = V1SecretReference(name="archive-creds", namespace=secret_namespace)
    csi = V1CSIPersistentVolumeSource(
        driver="smb.csi.k8s.io",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"source": "//isisdatar55.isis.cclrc.ac.uk/inst$/"},
        node_stage_secret_ref=secret_ref,
    )
    spec = V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        persistent_volume_reclaim_policy="Retain",
        mount_options=["noserverino", "_netdev", "vers=2.1"],
        csi=csi,
    )
    archive_pv = V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    CoreV1Api().create_persistent_volume(archive_pv)


@skip_conflict
def _setup_archive_pvc(instrument: str, job_namespace: str) -> None:
    """
    Sets up the archive PVC using the loaded kubeconfig as a destination
    :param instrument: str, the name of the instrument that the livedataprocessor PVC is made for
    :param job_namespace: str, the namespace that the job is in
    :return: str, the name of the PVC
    """
    pvc_name = f"livedataprocessor-{instrument}-archive-pvc"
    metadata = V1ObjectMeta(name=pvc_name)
    resources = V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = V1PersistentVolumeClaimSpec(
        access_modes=["ReadOnlyMany"],
        resources=resources,
        volume_name=f"livedataprocessor-{instrument}-archive-pv-smb",
        storage_class_name="",
    )
    archive_pvc = V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=metadata,
        spec=spec,
    )
    CoreV1Api().create_namespaced_persistent_volume_claim(namespace=job_namespace, body=archive_pvc)


@skip_conflict
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


@skip_conflict
def _setup_ceph_pvc(instrument: str, namespace: str) -> None:
    """
    Sets up a Ceph PersistentVolumeClaim (PVC) in Kubernetes.

    Creates a PVC that binds to a pre-existing Ceph PV, ensuring storage access for the specified instrument.

    :param instrument: Name of the instrument, used to define the PVC name and volume binding.
    :param namespace: Namespace in which the PVC is created.
    :return: None
    """
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
    _setup_archive_pv(instrument, namespace)
    _setup_archive_pvc(instrument, namespace)

    container = V1Container(
        name=f"livedataprocessor-{instrument}",
        image=f"ghcr.io/fiaisis/live-data-processor@sha256:{PROCESSOR_IMAGE}",
        resources=V1ResourceRequirements(requests={"memory": "32Gi"}, limits={"memory": "128Gi"}),
        volume_mounts=[
            V1VolumeMount(name="ceph-mount", mount_path="/output"),
            V1VolumeMount(name="archive-mount", mount_path="/archive"),
        ],
        env=[
            V1EnvVar(name="INSTRUMENT", value=instrument),
            V1EnvVar(name="GITHUB_API_TOKEN", value=GITHUB_API_TOKEN),
            V1EnvVar(name="FIA_API_URL", value=FIA_API_URL),
        ],
    )
    pod_spec = V1PodSpec(
        containers=[container],
        restart_policy="Always",
        service_account="live-data-operator",
        tolerations=[V1Toleration(key="staging", operator="Equal", value="big", effect="NoSchedule")],
        volumes=[
            V1Volume(
                name="ceph-mount",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"livedataprocessor-{instrument}-ceph-pvc", read_only=False
                ),
            ),
            V1Volume(
                name="archive-mount",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"livedataprocessor-{instrument}-archive-pvc", read_only=True
                ),
            ),
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
    such as after a pod restart or operator downtime and redeploys the processor.

    :param body: A dictionary representation of the custom resource's body, containing metadata,
                 specifications, and other details about the resource.
    :param spec: UNUSED The specifications of the custom resource, as provided in its definition.
                 Typically used to configure the resource's behavior.
    :param kwargs: UNUSED Additional parameters provided by the Kopf's resume handler, such as event-specific
                   context and resource information.
    :return: None
    """
    instrument = body["metadata"]["name"]
    logger.info(f"Operator resumed, redeploying {instrument} LiveDataProcessor")
    recreate_processor_deployment(instrument, CEPH_CREDS_SECRET_NAMESPACE)
