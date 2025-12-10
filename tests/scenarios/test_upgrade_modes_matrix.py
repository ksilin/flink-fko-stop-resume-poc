"""
Comprehensive multi-dimensional tests for Flink suspend/resume behavior.

This test suite evaluates the behavior of Flink jobs across multiple dimensions:
- Job type: Stateless vs Stateful
- Upgrade mode: stateless, last-state, savepoint
- Watermarks: enabled vs disabled

For each combination, we document:
- What state/data is preserved or lost
- How the job behaves on resume
- Any observed issues or gotchas
"""
import pytest
import time
import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple
from ..framework import KubernetesClient

logger = logging.getLogger(__name__)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def k8s_client(k8s_namespace):
    """Create Kubernetes client."""
    return KubernetesClient(namespace=k8s_namespace)


# =============================================================================
# Helper Functions
# =============================================================================

def deploy_from_yaml(k8s_client: KubernetesClient, yaml_path: Path, name: str):
    """Deploy a FlinkDeployment from YAML file with custom name."""
    import yaml
    with open(yaml_path) as f:
        manifest = yaml.safe_load(f)

    manifest['metadata']['name'] = name
    k8s_client.create_flink_deployment(manifest)
    logger.info(f"Deployed FlinkDeployment: {name}")


def wait_for_stable(k8s_client: KubernetesClient, name: str, timeout: int = 300) -> bool:
    """Wait for FlinkDeployment to reach STABLE state."""
    start = time.time()
    while time.time() - start < timeout:
        deployment = k8s_client.get_flink_deployment(name)
        lifecycle = deployment.get('status', {}).get('lifecycleState')
        job_status = deployment.get('status', {}).get('jobStatus', {}).get('state')

        if lifecycle == 'STABLE' and job_status == 'RUNNING':
            logger.info(f"FlinkDeployment {name} reached state STABLE")
            return True
        time.sleep(5)

    logger.error(f"FlinkDeployment {name} did not reach state STABLE after {timeout}s")
    return False


def suspend_deployment(k8s_client: KubernetesClient, name: str):
    """Suspend a FlinkDeployment."""
    k8s_client.patch_flink_deployment(name, {'spec': {'job': {'state': 'suspended'}}})
    logger.info(f"Suspended: {name}")


def resume_deployment(k8s_client: KubernetesClient, name: str):
    """Resume a FlinkDeployment."""
    k8s_client.patch_flink_deployment(name, {'spec': {'job': {'state': 'running'}}})
    logger.info(f"Resumed: {name}")


def toggle_watermarks(k8s_client: KubernetesClient, name: str, enabled: bool):
    """Toggle watermarks for a deployment."""
    arg_value = f"--watermarks={str(enabled).lower()}"
    deployment = k8s_client.get_flink_deployment(name)
    current_args = deployment['spec']['job'].get('args', [])

    # Remove existing watermarks arg
    new_args = [arg for arg in current_args if not arg.startswith('--watermarks=')]
    new_args.append(arg_value)

    k8s_client.patch_flink_deployment(name, {'spec': {'job': {'args': new_args}}})
    logger.info(f"Toggled watermarks to {enabled} for {name}")


def cleanup_deployment(k8s_client: KubernetesClient, name: str):
    """Delete a FlinkDeployment."""
    try:
        k8s_client.delete_flink_deployment(name)
        logger.info(f"Cleaned up: {name}")
    except Exception as e:
        logger.warning(f"Cleanup failed for {name}: {e}")


def get_job_metrics(k8s_client: KubernetesClient, name: str) -> Dict:
    """
    Extract job metrics from logs.

    Returns:
        Dict with message counts, state values, counters, and state reset indicators
    """
    logs = k8s_client.get_taskmanager_logs(name, tail_lines=500)

    # Count processed messages (from print() output)
    message_count = len(re.findall(r'processedAt=', logs))

    # Extract state counter values (for stateful jobs)
    # Match format: key-0|count=100|index=123|processedAt=1234567890
    counter_pattern = r'(key-\d+)\|count=(\d+)\|'
    counters = {}
    for match in re.finditer(counter_pattern, logs):
        key = match.group(1)
        value = int(match.group(2))
        counters[key] = max(counters.get(key, 0), value)

    # Check for state reset indicators - "First message for key" logs
    # This appears when state is null (fresh start)
    first_message_pattern = r'First message for key: (key-\d+)'
    keys_with_fresh_state = set(re.findall(first_message_pattern, logs))

    # Extract minimum counter values seen (to detect if counting from ~1)
    min_counters = {}
    for match in re.finditer(counter_pattern, logs):
        key = match.group(1)
        value = int(match.group(2))
        if key not in min_counters or value < min_counters[key]:
            min_counters[key] = value

    return {
        'message_count': message_count,
        'state_counters': counters,
        'total_state_sum': sum(counters.values()) if counters else 0,
        'fresh_start_keys': keys_with_fresh_state,
        'min_counter_values': min_counters,
        'has_fresh_start_indicator': len(keys_with_fresh_state) > 0
    }


def evaluate_resume_behavior(
    pre_suspend_metrics: Dict,
    post_resume_metrics: Dict,
    upgrade_mode: str,
    job_type: str
) -> Dict:
    """
    Evaluate what happened during suspend/resume.

    Returns:
        Dict with evaluation results and observations.
    """
    observations = []

    # Check state preservation
    if job_type == 'stateful':
        pre_state = pre_suspend_metrics.get('state_counters', {})
        post_state = post_resume_metrics.get('state_counters', {})

        if upgrade_mode in ['savepoint', 'last-state']:
            if post_state and pre_state:
                # State should be preserved or increased
                preserved = all(
                    post_state.get(k, 0) >= v
                    for k, v in pre_state.items()
                )
                if preserved:
                    observations.append("✓ State PRESERVED: counters continued from previous values")
                else:
                    observations.append("✗ State LOST: counters reset to lower values")
            elif not post_state:
                observations.append("✗ State LOST: no state found after resume")
        elif upgrade_mode == 'stateless':
            # Check multiple indicators of state reset
            post_has_fresh_start = post_resume_metrics.get('has_fresh_start_indicator', False)
            post_min_values = post_resume_metrics.get('min_counter_values', {})
            post_fresh_keys = post_resume_metrics.get('fresh_start_keys', set())

            # State reset indicators (any of these suggests fresh start):
            # 1. "First message for key" logs present
            # 2. Minimum counter values are low (< 10)
            # 3. Post-resume values don't correlate with pre-suspend values

            has_first_message_logs = post_has_fresh_start
            has_low_min_counters = any(v < 10 for v in post_min_values.values()) if post_min_values else False
            fresh_keys_count = len(post_fresh_keys)

            if has_first_message_logs:
                observations.append(f"✓ State RESET (confirmed): 'First message' logs found for {fresh_keys_count} keys")
            elif has_low_min_counters:
                observations.append(f"✓ State RESET (inferred): minimum counter values < 10")
            elif not post_state:
                observations.append("✓ State RESET: no state found after resume")
            else:
                # Heuristic: if post-resume values are in expected range for fresh start
                avg_post = sum(post_state.values()) / len(post_state) if post_state else 0
                if 35 <= avg_post <= 80:  # ~45s accumulation at 1 msg/s
                    observations.append(f"✓ State RESET (likely): average counter {avg_post:.0f} consistent with fresh start")
                else:
                    observations.append(f"? Unclear: average counter {avg_post:.0f}, expected 35-80 for fresh start")

    return {
        'pre_metrics': pre_suspend_metrics,
        'post_metrics': post_resume_metrics,
        'observations': observations,
        'state_preserved': 'State PRESERVED' in ' '.join(observations),
        'state_lost': 'State LOST' in ' '.join(observations)
    }


# =============================================================================
# STATELESS JOB TESTS - Matrix across upgrade modes and watermarks
# =============================================================================

@pytest.mark.parametrize("upgrade_mode,watermarks", [
    ("stateless", True),
    ("stateless", False),
    ("savepoint", True),   # Can stateless job use savepoint mode?
    ("savepoint", False),
    ("last-state", True),  # Can stateless job use last-state mode?
    ("last-state", False),
])
@pytest.mark.integration
@pytest.mark.slow
def test_stateless_suspend_resume_matrix(
    k8s_client,
    project_root,
    upgrade_mode,
    watermarks
):
    """
    Test stateless job suspend/resume across upgrade modes and watermark configs.

    Evaluates:
    - Does the job successfully suspend and resume?
    - Is any data/processing lost?
    - What is the impact of watermarks?
    - What is the impact of upgrade mode on stateless jobs?

    Expected behavior:
    - stateless mode: Job restarts fresh, no state to preserve
    - savepoint/last-state mode: Should work but no state to save (stateless job)
    - watermarks: Should not affect stateless job behavior significantly
    """
    name = f"test-stateless-{upgrade_mode}-wm{int(watermarks)}"
    yaml_path = project_root / "k8s/deployments/stateless.yaml"

    try:
        logger.info(f"=== Test: Stateless job with {upgrade_mode} mode, watermarks={watermarks} ===")

        # Deploy with modifications
        import yaml
        with open(yaml_path) as f:
            manifest = yaml.safe_load(f)

        manifest['metadata']['name'] = name
        manifest['spec']['job']['upgradeMode'] = upgrade_mode
        manifest['spec']['job']['args'] = [f"--watermarks={str(watermarks).lower()}"]

        k8s_client.create_flink_deployment(manifest)
        logger.info(f"Deployed: {name}")

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Failed to reach STABLE"

        # Let job run
        run_time = 30
        logger.info(f"Running for {run_time}s...")
        time.sleep(run_time)

        # Get pre-suspend metrics
        pre_metrics = get_job_metrics(k8s_client, name)
        logger.info(f"Pre-suspend: {pre_metrics['message_count']} messages processed")

        # Suspend
        logger.info("Suspending...")
        suspend_deployment(k8s_client, name)
        time.sleep(20)

        # Check suspended state
        deployment = k8s_client.get_flink_deployment(name)
        lifecycle = deployment.get('status', {}).get('lifecycleState')
        logger.info(f"Lifecycle state after suspend: {lifecycle}")

        # Resume
        logger.info("Resuming...")
        resume_deployment(k8s_client, name)
        assert wait_for_stable(k8s_client, name, timeout=300), "Failed to resume"

        # Let job run after resume
        time.sleep(30)

        # Get post-resume metrics
        post_metrics = get_job_metrics(k8s_client, name)
        logger.info(f"Post-resume: {post_metrics['message_count']} messages processed")

        # Evaluate
        evaluation = evaluate_resume_behavior(pre_metrics, post_metrics, upgrade_mode, 'stateless')

        logger.info("=== EVALUATION ===")
        logger.info(f"Upgrade mode: {upgrade_mode}")
        logger.info(f"Watermarks: {watermarks}")
        logger.info(f"Pre-suspend messages: {pre_metrics['message_count']}")
        logger.info(f"Post-resume messages: {post_metrics['message_count']}")
        for obs in evaluation['observations']:
            logger.info(f"  {obs}")

        # For stateless jobs, we expect:
        # - Job always restarts successfully
        # - No state preservation (it's stateless)
        # - Message processing resumes
        assert post_metrics['message_count'] > 0, "No messages processed after resume"

        logger.info("✓ Test completed")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# STATEFUL JOB TESTS - Matrix across upgrade modes and watermarks
# =============================================================================

@pytest.mark.parametrize("upgrade_mode,watermarks", [
    ("savepoint", True),
    ("savepoint", False),
    ("last-state", True),
    ("last-state", False),
    ("stateless", True),   # Stateful job with stateless mode - should lose state!
    ("stateless", False),
])
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.stateful
def test_stateful_suspend_resume_matrix(
    k8s_client,
    project_root,
    upgrade_mode,
    watermarks
):
    """
    Test stateful job suspend/resume across upgrade modes and watermark configs.

    Evaluates:
    - Is state preserved or lost?
    - What is the impact of upgrade mode?
    - What is the impact of watermarks?
    - Are there any issues with state recovery?

    Expected behavior:
    - savepoint mode: State PRESERVED via savepoint
    - last-state mode: State PRESERVED via checkpoint
    - stateless mode: State LOST (fresh start despite having state)
    - watermarks: Should not affect state preservation
    """
    name = f"test-stateful-{upgrade_mode}-wm{int(watermarks)}"

    # Choose the right YAML based on upgrade mode
    if upgrade_mode == 'savepoint':
        yaml_path = project_root / "k8s/deployments/stateful-savepoint.yaml"
    elif upgrade_mode == 'last-state':
        yaml_path = project_root / "k8s/deployments/stateful-laststate.yaml"
    else:  # stateless
        yaml_path = project_root / "k8s/deployments/stateful-stateless-mode.yaml"

    try:
        logger.info(f"=== Test: Stateful job with {upgrade_mode} mode, watermarks={watermarks} ===")

        # Deploy with modifications
        import yaml
        with open(yaml_path) as f:
            manifest = yaml.safe_load(f)

        manifest['metadata']['name'] = name
        manifest['spec']['job']['args'] = [f"--watermarks={str(watermarks).lower()}"]

        k8s_client.create_flink_deployment(manifest)
        logger.info(f"Deployed: {name}")

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Failed to reach STABLE"

        # Let job run to accumulate state
        run_time = 45
        logger.info(f"Running for {run_time}s to accumulate state...")
        time.sleep(run_time)

        # Get pre-suspend metrics
        pre_metrics = get_job_metrics(k8s_client, name)
        logger.info(f"Pre-suspend state: {pre_metrics['state_counters']}")
        logger.info(f"Pre-suspend total: {pre_metrics['total_state_sum']}")

        # Suspend
        logger.info("Suspending...")
        suspend_deployment(k8s_client, name)
        time.sleep(30)

        # Check suspended state and savepoint info
        deployment = k8s_client.get_flink_deployment(name)
        lifecycle = deployment.get('status', {}).get('lifecycleState')
        savepoint_path = deployment.get('status', {}).get('jobStatus', {}).get('upgradeSavepointPath', 'N/A')
        logger.info(f"Lifecycle state: {lifecycle}")
        logger.info(f"Savepoint path: {savepoint_path}")

        # Resume
        logger.info("Resuming...")
        resume_deployment(k8s_client, name)
        assert wait_for_stable(k8s_client, name, timeout=300), "Failed to resume"

        # Let job run after resume
        time.sleep(45)

        # Get post-resume metrics
        post_metrics = get_job_metrics(k8s_client, name)
        logger.info(f"Post-resume state: {post_metrics['state_counters']}")
        logger.info(f"Post-resume total: {post_metrics['total_state_sum']}")

        # Evaluate
        evaluation = evaluate_resume_behavior(pre_metrics, post_metrics, upgrade_mode, 'stateful')

        logger.info("=== EVALUATION ===")
        logger.info(f"Upgrade mode: {upgrade_mode}")
        logger.info(f"Watermarks: {watermarks}")
        logger.info(f"Pre-suspend state sum: {pre_metrics['total_state_sum']}")
        logger.info(f"Post-resume state sum: {post_metrics['total_state_sum']}")
        for obs in evaluation['observations']:
            logger.info(f"  {obs}")

        # Assertions based on upgrade mode
        if upgrade_mode in ['savepoint', 'last-state']:
            assert evaluation['state_preserved'], \
                f"State should be preserved with {upgrade_mode} mode but was lost"
        elif upgrade_mode == 'stateless':
            # With stateless mode, state will be lost even for stateful jobs
            logger.info("Note: State loss expected with stateless upgrade mode")

        logger.info("✓ Test completed")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# WATERMARK TOGGLE TESTS
# =============================================================================

@pytest.mark.parametrize("job_type,upgrade_mode", [
    ("stateless", "stateless"),
    ("stateful", "savepoint"),
    ("stateful", "last-state"),
])
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.watermark
def test_watermark_toggle_during_suspend(
    k8s_client,
    project_root,
    job_type,
    upgrade_mode
):
    """
    Test watermark configuration toggle during suspend/resume cycles.

    Tests:
    - Enable watermarks -> suspend -> disable watermarks -> resume
    - Verify job continues correctly
    - Check if watermark change affects behavior

    This tests whether watermark configuration can be changed
    dynamically via suspend/resume cycles.
    """
    name = f"test-wm-toggle-{job_type}-{upgrade_mode}"

    if job_type == 'stateless':
        yaml_path = project_root / "k8s/deployments/stateless.yaml"
    elif upgrade_mode == 'savepoint':
        yaml_path = project_root / "k8s/deployments/stateful-savepoint.yaml"
    else:
        yaml_path = project_root / "k8s/deployments/stateful-laststate.yaml"

    try:
        logger.info(f"=== Test: Watermark toggle for {job_type} with {upgrade_mode} ===")

        # Deploy with watermarks ENABLED
        import yaml
        with open(yaml_path) as f:
            manifest = yaml.safe_load(f)

        manifest['metadata']['name'] = name
        manifest['spec']['job']['upgradeMode'] = upgrade_mode
        manifest['spec']['job']['args'] = ["--watermarks=true"]

        k8s_client.create_flink_deployment(manifest)
        logger.info("Deployed with watermarks=true")

        assert wait_for_stable(k8s_client, name, timeout=300)
        time.sleep(30)

        # Suspend and toggle to DISABLED
        logger.info("Suspending to toggle watermarks to false...")
        suspend_deployment(k8s_client, name)
        time.sleep(20)

        toggle_watermarks(k8s_client, name, False)
        resume_deployment(k8s_client, name)
        assert wait_for_stable(k8s_client, name, timeout=300)
        logger.info("Resumed with watermarks=false")
        time.sleep(30)

        # Suspend and toggle back to ENABLED
        logger.info("Suspending to toggle watermarks back to true...")
        suspend_deployment(k8s_client, name)
        time.sleep(20)

        toggle_watermarks(k8s_client, name, True)
        resume_deployment(k8s_client, name)
        assert wait_for_stable(k8s_client, name, timeout=300)
        logger.info("Resumed with watermarks=true")
        time.sleep(30)

        logger.info("✓ Watermark toggle successful across multiple suspend/resume cycles")

    finally:
        cleanup_deployment(k8s_client, name)
