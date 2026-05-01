import logging
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client import ApiException

from live_data_operator.main import (
    EndpointFilter,
    _setup_archive_pv,
    setup_deployment,
    skip_conflict,
)


def test_endpoint_filter():
    """Test that the filter excludes health and ready probes."""
    filt = EndpointFilter()

    # Excluded matches
    record_health = logging.LogRecord(
        "test", logging.INFO, "path", 1, "GET /healthz HTTP/1.1", (), None
    )
    record_ready = logging.LogRecord(
        "test", logging.INFO, "path", 1, "GET /ready HTTP/1.1", (), None
    )

    assert filt.filter(record_health) is False
    assert filt.filter(record_ready) is False

    # Allowed matches
    record_normal = logging.LogRecord(
        "test", logging.INFO, "path", 1, "GET /status HTTP/1.1", (), None
    )
    assert filt.filter(record_normal) is True


def test_skip_conflict():
    """Test that the skip_conflict decorator suppresses HTTPStatus.CONFLICT."""
    mock_func = MagicMock()
    mock_func.__name__ = "dummy"

    # Test it suppresses CONFLICT
    mock_func.side_effect = ApiException(status=HTTPStatus.CONFLICT, reason="Conflict")
    decorated = skip_conflict(mock_func)
    decorated()  # Should not raise

    # Test it raises other exceptions
    mock_func.side_effect = ApiException(
        status=HTTPStatus.NOT_FOUND, reason="Not Found"
    )
    with pytest.raises(ApiException):
        decorated()


@patch("live_data_operator.main.CoreV1Api")
def test_setup_archive_pv(mock_core_api):
    """Test setting up archive pv sends correct payload to CoreV1Api."""
    mock_api_instance = mock_core_api.return_value

    _setup_archive_pv("MERLIN", "my-namespace")

    mock_api_instance.create_persistent_volume.assert_called_once()
    payload = mock_api_instance.create_persistent_volume.call_args[0][0]

    assert payload.metadata.name == "livedataprocessor-MERLIN-archive-pv-smb"
    assert payload.spec.csi.driver == "smb.csi.k8s.io"


@patch("live_data_operator.main._setup_archive_pvc")
@patch("live_data_operator.main._setup_archive_pv")
@patch("live_data_operator.main._setup_ceph_pvc")
@patch("live_data_operator.main._setup_ceph_pv")
def test_setup_deployment(mock_ceph_pv, mock_ceph_pvc, mock_arch_pv, mock_arch_pvc):
    """Test that setup_deployment invokes the PV setups and returns a correct V1Deployment."""
    deployment = setup_deployment(
        "ceph-creds-secret", "cluster-id-123", "MERLIN", "test-namespace", "my-fs"
    )

    mock_ceph_pv.assert_called_once()
    mock_ceph_pvc.assert_called_once()
    mock_arch_pv.assert_called_once()
    mock_arch_pvc.assert_called_once()

    assert deployment.metadata.name == "livedataprocessor-MERLIN-deployment"
    assert deployment.spec.replicas == 1

    container = deployment.spec.template.spec.containers[0]
    assert container.name == "livedataprocessor-MERLIN"

    env_vars = {e.name: e.value for e in container.env}
    assert env_vars["INSTRUMENT"] == "MERLIN"
