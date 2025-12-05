"""FlinkDeployment manager for test operations."""
import logging
import yaml
from typing import Dict, Optional
from .kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)

class FlinkDeploymentManager:
    """Manages FlinkDeployment lifecycle for testing."""

    def __init__(self, k8s_client: KubernetesClient):
        self.k8s = k8s_client

    def deploy_from_file(self, yaml_path: str, overrides: Optional[Dict] = None) -> str:
        """Deploy from YAML file with optional overrides."""
        with open(yaml_path) as f:
            spec = yaml.safe_load(f)

        if overrides:
            spec = self._apply_overrides(spec, overrides)

        result = self.k8s.create_flink_deployment(spec)
        name = result["metadata"]["name"]
        logger.info(f"Deployed FlinkDeployment {name}")
        return name

    def suspend(self, name: str):
        """Suspend a running job."""
        patch = {"spec": {"job": {"state": "suspended"}}}
        self.k8s.patch_flink_deployment(name, patch)
        logger.info(f"Suspended {name}")

    def resume(self, name: str):
        """Resume a suspended job."""
        patch = {"spec": {"job": {"state": "running"}}}
        self.k8s.patch_flink_deployment(name, patch)
        logger.info(f"Resumed {name}")

    def toggle_watermarks(self, name: str, enabled: bool):
        """Toggle watermarks configuration."""
        patch = {"spec": {"job": {"args": [f"--watermarks={str(enabled).lower()}"]}}}
        self.k8s.patch_flink_deployment(name, patch)

    def wait_for_stable(self, name: str, timeout: int = 300) -> bool:
        """Wait for deployment to reach STABLE state."""
        return self.k8s.wait_for_deployment_state(name, "STABLE", timeout)

    def cleanup(self, name: str):
        """Delete deployment and wait for cleanup."""
        self.k8s.delete_flink_deployment(name)

    def _apply_overrides(self, spec: Dict, overrides: Dict) -> Dict:
        """Apply overrides to deployment spec."""
        for key, value in overrides.items():
            keys = key.split(".")
            current = spec
            for k in keys[:-1]:
                current = current.setdefault(k, {})
            current[keys[-1]] = value
        return spec
