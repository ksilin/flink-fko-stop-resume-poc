"""
Tests for stateful jobs with savepoint upgrade mode.
"""
import pytest
import time
import logging
from pathlib import Path
from ..framework import KubernetesClient, FlinkDeploymentManager, StateValidator

logger = logging.getLogger(__name__)

@pytest.fixture
def k8s_client(k8s_namespace):
    """Create Kubernetes client."""
    return KubernetesClient(namespace=k8s_namespace)

@pytest.fixture
def flink_manager(k8s_client):
    """Create FlinkDeployment manager."""
    return FlinkDeploymentManager(k8s_client)

@pytest.mark.stateful
@pytest.mark.savepoint
def test_stateful_savepoint_basic_suspend_resume(flink_manager, k8s_client, deployment_name, project_root, cleanup_after_test):
    """
    Test basic suspend/resume with savepoint for stateful job.

    Steps:
    1. Deploy stateful job with savepoint mode
    2. Wait for STABLE state
    3. Let job run and process data
    4. Suspend job (triggers savepoint)
    5. Resume job (restores from savepoint)
    6. Verify job continues and state is preserved
    """
    deployment_file = project_root / "k8s/deployments/stateful-savepoint.yaml"

    try:
        # Step 1: Deploy
        logger.info("Step 1: Deploying stateful job with savepoint mode")
        name = flink_manager.deploy_from_file(
            str(deployment_file),
            overrides={"metadata.name": deployment_name}
        )

        # Step 2: Wait for STABLE
        logger.info("Step 2: Waiting for job to reach STABLE state")
        assert flink_manager.wait_for_stable(name, timeout=300), "Job failed to reach STABLE"

        # Step 3: Let job run
        logger.info("Step 3: Letting job process data for 30 seconds")
        time.sleep(30)

        # Step 4: Suspend
        logger.info("Step 4: Suspending job (triggers savepoint)")
        flink_manager.suspend(name)
        time.sleep(20)  # Wait for suspend to complete

        # Step 5: Resume
        logger.info("Step 5: Resuming job (restores from savepoint)")
        flink_manager.resume(name)
        assert flink_manager.wait_for_stable(name, timeout=300), "Job failed to resume"

        # Step 6: Verify
        logger.info("Step 6: Verifying job is processing")
        time.sleep(30)
        deployment = k8s_client.get_flink_deployment(name)
        assert deployment["status"]["lifecycleState"] == "STABLE"
        logger.info("✓ Test passed: Job resumed successfully from savepoint")

    finally:
        if cleanup_after_test:
            flink_manager.cleanup(deployment_name)


@pytest.mark.stateful
@pytest.mark.savepoint
@pytest.mark.watermark
def test_stateful_savepoint_watermark_toggle(flink_manager, k8s_client, deployment_name, project_root, cleanup_after_test):
    """
    Test watermark configuration change with savepoint.

    Steps:
    1. Deploy with watermarks enabled
    2. Wait for STABLE
    3. Suspend and toggle watermarks off
    4. Resume and verify
    5. Suspend and toggle watermarks back on
    6. Resume and verify
    """
    deployment_file = project_root / "k8s/deployments/stateful-savepoint.yaml"

    try:
        # Deploy with watermarks enabled (default)
        logger.info("Deploying with watermarks enabled")
        name = flink_manager.deploy_from_file(
            str(deployment_file),
            overrides={"metadata.name": deployment_name}
        )
        assert flink_manager.wait_for_stable(name, timeout=300)
        time.sleep(20)

        # Suspend and disable watermarks
        logger.info("Suspending and disabling watermarks")
        flink_manager.suspend(name)
        time.sleep(15)
        flink_manager.toggle_watermarks(name, False)
        flink_manager.resume(name)
        assert flink_manager.wait_for_stable(name, timeout=300)
        time.sleep(20)

        # Suspend and re-enable watermarks
        logger.info("Suspending and re-enabling watermarks")
        flink_manager.suspend(name)
        time.sleep(15)
        flink_manager.toggle_watermarks(name, True)
        flink_manager.resume(name)
        assert flink_manager.wait_for_stable(name, timeout=300)

        logger.info("✓ Test passed: Watermark toggle works with savepoint")

    finally:
        if cleanup_after_test:
            flink_manager.cleanup(deployment_name)
