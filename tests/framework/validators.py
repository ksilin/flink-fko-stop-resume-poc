"""Validators for test assertions."""
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class StateValidator:
    """Validate state recovery across restarts."""

    def __init__(self):
        self.expected_counts = {}
        self.actual_counts = {}

    def record_expected(self, key: str, count: int):
        """Record expected count before restart."""
        self.expected_counts[key] = count

    def record_actual(self, key: str, count: int):
        """Record actual count after restart."""
        self.actual_counts[key] = count

    def validate(self) -> bool:
        """Validate that actual >= expected."""
        for key, expected in self.expected_counts.items():
            actual = self.actual_counts.get(key, 0)
            if actual < expected:
                logger.error(f"Validation failed: {key} expected>={expected}, got {actual}")
                return False
        logger.info("State validation passed")
        return True

class JobStatusValidator:
    """Validate job status transitions."""

    @staticmethod
    def is_running(deployment: Dict) -> bool:
        """Check if job is running."""
        return deployment.get("status", {}).get("jobStatus", {}).get("state") == "RUNNING"

    @staticmethod
    def is_stable(deployment: Dict) -> bool:
        """Check if deployment is stable."""
        return deployment.get("status", {}).get("lifecycleState") == "STABLE"
