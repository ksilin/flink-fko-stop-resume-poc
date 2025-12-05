"""
Pytest configuration and shared fixtures for Flink stop/resume tests.
"""

import os
import logging
import pytest
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def k8s_namespace():
    """Return the Kubernetes namespace for tests."""
    return os.environ.get("FLINK_TEST_NAMESPACE", "flink")


@pytest.fixture(scope="session")
def kafka_bootstrap():
    """Return the Kafka bootstrap servers."""
    return os.environ.get(
        "KAFKA_BOOTSTRAP",
        "kafka.flink.svc.cluster.local:9092"
    )


@pytest.fixture(scope="session")
def flink_image():
    """Return the Flink Docker image to use."""
    return os.environ.get("FLINK_IMAGE", "flink-deployment-test:latest")


@pytest.fixture(scope="function")
def deployment_name(request):
    """Generate a unique deployment name for each test."""
    test_name = request.node.name.replace("test_", "").replace("[", "-").replace("]", "")
    test_name = test_name.lower().replace("_", "-")[:50]  # K8s name length limit
    return f"test-{test_name}"


@pytest.fixture(scope="function")
def test_timeout():
    """Default timeout for test operations (seconds)."""
    return int(os.environ.get("TEST_TIMEOUT", "300"))  # 5 minutes


@pytest.fixture(scope="session")
def cleanup_after_test():
    """Whether to cleanup resources after each test."""
    return os.environ.get("CLEANUP_AFTER_TEST", "true").lower() == "true"


def pytest_configure(config):
    """Pytest configuration hook."""
    # Add custom markers
    config.addinivalue_line(
        "markers", "integration: Integration test requiring full cluster setup"
    )
    config.addinivalue_line(
        "markers", "slow: Slow test (> 1 minute)"
    )

    # Check for required environment variables in integration tests
    if config.getoption("markexpr") != "not integration":
        # These are checked at import time for integration tests
        pass


def pytest_collection_modifyitems(config, items):
    """Modify test collection."""
    # Add markers automatically based on test names
    for item in items:
        # Add slow marker to tests that are likely slow
        if "kafka" in item.nodeid.lower() or "draining" in item.nodeid.lower():
            item.add_marker(pytest.mark.slow)

        # Add integration marker to all tests in scenarios/
        if "scenarios/" in item.nodeid:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session", autouse=True)
def test_environment_info(request):
    """Print test environment information."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("Flink Test Environment")
    logger.info("=" * 60)
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Python version: {pytest.__version__}")
    logger.info(f"Namespace: {os.environ.get('FLINK_TEST_NAMESPACE', 'flink')}")
    logger.info(f"Kafka: {os.environ.get('KAFKA_BOOTSTRAP', 'kafka.flink.svc.cluster.local:9092')}")
    logger.info(f"Cleanup: {os.environ.get('CLEANUP_AFTER_TEST', 'true')}")
    logger.info("=" * 60)
