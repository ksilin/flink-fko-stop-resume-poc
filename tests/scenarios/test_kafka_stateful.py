"""Tests for Kafka-based stateful jobs."""
import pytest
import time
import logging
from ..framework import KubernetesClient, FlinkDeploymentManager, KafkaTestClient

logger = logging.getLogger(__name__)

@pytest.fixture
def k8s_client(k8s_namespace):
    return KubernetesClient(namespace=k8s_namespace)

@pytest.fixture
def flink_manager(k8s_client):
    return FlinkDeploymentManager(k8s_client)

@pytest.fixture
def kafka_client(kafka_bootstrap):
    return KafkaTestClient(kafka_bootstrap)

@pytest.mark.kafka
@pytest.mark.stateful
@pytest.mark.slow
def test_kafka_stateful_savepoint(flink_manager, kafka_client, deployment_name, project_root, cleanup_after_test):
    """Test Kafka stateful job with savepoint recovery."""
    deployment_file = project_root / "k8s/deployments/kafka-stateful-savepoint.yaml"

    try:
        # Produce test messages
        messages = [f"key-{i%10}|message-{i}" for i in range(100)]
        kafka_client.produce_messages("stateful-test-input", messages)

        # Deploy and wait
        name = flink_manager.deploy_from_file(str(deployment_file), overrides={"metadata.name": deployment_name})
        assert flink_manager.wait_for_stable(name, timeout=300)
        time.sleep(30)

        # Suspend/resume
        flink_manager.suspend(name)
        time.sleep(20)
        flink_manager.resume(name)
        assert flink_manager.wait_for_stable(name, timeout=300)

        # Verify output
        time.sleep(20)
        output = kafka_client.consume_messages("stateful-test-output", max_messages=50)
        assert len(output) > 0, "No output messages found"

        logger.info("✓ Kafka stateful job test passed")
    finally:
        if cleanup_after_test:
            flink_manager.cleanup(deployment_name)
