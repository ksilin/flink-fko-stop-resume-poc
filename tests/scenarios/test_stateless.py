"""Tests for stateless jobs."""
import pytest
import time
import logging
from ..framework import KubernetesClient, FlinkDeploymentManager

logger = logging.getLogger(__name__)

@pytest.fixture
def k8s_client(k8s_namespace):
    return KubernetesClient(namespace=k8s_namespace)

@pytest.fixture
def flink_manager(k8s_client):
    return FlinkDeploymentManager(k8s_client)

@pytest.mark.stateless
def test_stateless_basic_suspend_resume(flink_manager, k8s_client, deployment_name, project_root, cleanup_after_test):
    """Test basic suspend/resume for stateless job."""
    deployment_file = project_root / "k8s/deployments/stateless.yaml"

    try:
        name = flink_manager.deploy_from_file(str(deployment_file), overrides={"metadata.name": deployment_name})
        assert flink_manager.wait_for_stable(name, timeout=300)
        time.sleep(20)

        flink_manager.suspend(name)
        time.sleep(15)

        flink_manager.resume(name)
        assert flink_manager.wait_for_stable(name, timeout=300)

        logger.info("✓ Stateless job suspend/resume successful")
    finally:
        if cleanup_after_test:
            flink_manager.cleanup(deployment_name)
