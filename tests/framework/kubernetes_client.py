"""
Kubernetes API client wrapper for Flink testing.
"""

import logging
import time
from typing import Dict, List, Optional, Any
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class KubernetesClient:
    """Wrapper for Kubernetes API operations."""

    def __init__(self, namespace: str = "flink"):
        """Initialize Kubernetes client."""
        try:
            config.load_kube_config()
        except Exception:
            config.load_incluster_config()

        self.namespace = namespace
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()

    def get_pod(self, pod_name: str) -> Optional[client.V1Pod]:
        """Get a pod by name."""
        try:
            return self.core_v1.read_namespaced_pod(pod_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def list_pods(self, label_selector: str = None) -> List[client.V1Pod]:
        """List pods with optional label selector."""
        try:
            result = self.core_v1.list_namespaced_pod(
                self.namespace,
                label_selector=label_selector
            )
            return result.items
        except ApiException as e:
            logger.error(f"Error listing pods: {e}")
            return []

    def wait_for_pod_ready(self, pod_name: str, timeout: int = 300) -> bool:
        """Wait for a pod to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            pod = self.get_pod(pod_name)
            if pod and pod.status.phase == "Running":
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        if condition.type == "Ready" and condition.status == "True":
                            logger.info(f"Pod {pod_name} is ready")
                            return True
            time.sleep(5)

        logger.error(f"Pod {pod_name} not ready after {timeout}s")
        return False

    def get_pod_logs(self, pod_name: str, tail_lines: int = 100) -> str:
        """Get logs from a pod."""
        try:
            return self.core_v1.read_namespaced_pod_log(
                pod_name,
                self.namespace,
                tail_lines=tail_lines
            )
        except ApiException as e:
            logger.error(f"Error getting logs from {pod_name}: {e}")
            return ""

    def delete_pod(self, pod_name: str):
        """Delete a pod."""
        try:
            self.core_v1.delete_namespaced_pod(pod_name, self.namespace)
            logger.info(f"Deleted pod {pod_name}")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Error deleting pod {pod_name}: {e}")

    def get_flink_deployment(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a FlinkDeployment custom resource."""
        try:
            return self.custom_api.get_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                namespace=self.namespace,
                plural="flinkdeployments",
                name=name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def create_flink_deployment(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Create a FlinkDeployment custom resource."""
        return self.custom_api.create_namespaced_custom_object(
            group="flink.apache.org",
            version="v1beta1",
            namespace=self.namespace,
            plural="flinkdeployments",
            body=body
        )

    def delete_flink_deployment(self, name: str):
        """Delete a FlinkDeployment custom resource."""
        try:
            self.custom_api.delete_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                namespace=self.namespace,
                plural="flinkdeployments",
                name=name
            )
            logger.info(f"Deleted FlinkDeployment {name}")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Error deleting FlinkDeployment {name}: {e}")

    def patch_flink_deployment(self, name: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        """Patch a FlinkDeployment custom resource."""
        return self.custom_api.patch_namespaced_custom_object(
            group="flink.apache.org",
            version="v1beta1",
            namespace=self.namespace,
            plural="flinkdeployments",
            name=name,
            body=patch
        )

    def wait_for_deployment_state(self, name: str, expected_state: str, timeout: int = 300) -> bool:
        """Wait for FlinkDeployment to reach expected state."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            deployment = self.get_flink_deployment(name)
            if deployment and deployment.get("status"):
                current_state = deployment["status"].get("lifecycleState")
                if current_state == expected_state:
                    logger.info(f"FlinkDeployment {name} reached state {expected_state}")
                    return True
                logger.debug(f"FlinkDeployment {name} state: {current_state}")
            time.sleep(5)

        logger.error(f"FlinkDeployment {name} did not reach state {expected_state} after {timeout}s")
        return False
