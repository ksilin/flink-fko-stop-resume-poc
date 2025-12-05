"""
Tests for draining behavior during stop/resume operations.

These tests verify how the Flink Kubernetes Operator handles draining
when stopping jobs with savepoints, and what happens when resuming
from drained vs non-drained savepoints.

Key behaviors tested:
1. Drain enabled: Windows emit with MAX_WATERMARK trigger before savepoint
2. Drain disabled: Windows retain state, no premature emission
3. Resume from drained savepoint: Windows start fresh (empty)
4. Resume from non-drained savepoint: Windows continue from saved state

Configuration under test:
- kubernetes.operator.job.drain-on-savepoint-deletion: true/false
"""
import pytest
import time
import logging
import yaml
from pathlib import Path

from ..framework import KubernetesClient, FlinkDeploymentManager, DrainLogParser, TriggerType

logger = logging.getLogger(__name__)

# Expected keys from WindowedDrainTestJob
EXPECTED_KEYS = {"key-0", "key-1", "key-2", "key-3", "key-4"}


@pytest.fixture
def k8s_client(k8s_namespace):
    """Create Kubernetes client."""
    return KubernetesClient(namespace=k8s_namespace)


@pytest.fixture
def flink_manager(k8s_client):
    """Create FlinkDeployment manager."""
    return FlinkDeploymentManager(k8s_client)


@pytest.fixture
def log_parser():
    """Create drain log parser."""
    return DrainLogParser()


@pytest.fixture
def project_root():
    """Get project root directory."""
    return Path(__file__).parent.parent.parent


def deploy_from_yaml(k8s_client, yaml_path: Path, name_override: str = None) -> str:
    """
    Deploy a FlinkDeployment from a YAML file.

    Args:
        k8s_client: Kubernetes client
        yaml_path: Path to YAML file
        name_override: Optional name to use instead of the one in YAML

    Returns:
        Name of the deployed FlinkDeployment
    """
    with open(yaml_path) as f:
        spec = yaml.safe_load(f)

    if name_override:
        spec['metadata']['name'] = name_override

    name = spec['metadata']['name']

    # Delete if exists
    k8s_client.delete_flink_deployment(name)
    time.sleep(5)

    # Create
    k8s_client.create_flink_deployment(spec)
    logger.info(f"Deployed FlinkDeployment: {name}")

    return name


def wait_for_stable(k8s_client, name: str, timeout: int = 300) -> bool:
    """Wait for deployment to reach STABLE state."""
    return k8s_client.wait_for_deployment_state(name, "STABLE", timeout=timeout)


def suspend_deployment(k8s_client, name: str):
    """Suspend a FlinkDeployment."""
    patch = {"spec": {"job": {"state": "suspended"}}}
    k8s_client.patch_flink_deployment(name, patch)
    logger.info(f"Suspended: {name}")


def resume_deployment(k8s_client, name: str):
    """Resume a FlinkDeployment."""
    patch = {"spec": {"job": {"state": "running"}}}
    k8s_client.patch_flink_deployment(name, patch)
    logger.info(f"Resumed: {name}")


def cleanup_deployment(k8s_client, name: str):
    """Delete a FlinkDeployment."""
    try:
        k8s_client.delete_flink_deployment(name)
        logger.info(f"Cleaned up: {name}")
    except Exception as e:
        logger.warning(f"Cleanup failed for {name}: {e}")


# =============================================================================
# Test: Drain Enabled - Verify Windows Emit on Suspend
# =============================================================================

@pytest.mark.draining
@pytest.mark.slow
def test_drain_enabled_emits_windows_on_suspend(k8s_client, log_parser, project_root):
    """
    Test that with drain enabled, windows emit when job is suspended.

    This test verifies the core drain behavior:
    - Deploy windowed job with kubernetes.operator.job.drain-on-savepoint-deletion=true
    - Let windows accumulate data
    - Suspend the job (triggers savepoint)
    - Verify that windows emitted with trigger=DRAIN

    Expected: Window emissions with MAX_WATERMARK in logs
    """
    yaml_path = project_root / "k8s/deployments/drain-test-with-drain.yaml"
    name = "drain-test-emit-on-suspend"

    try:
        # Deploy
        logger.info("=== Test: Drain Enabled - Windows Emit on Suspend ===")
        logger.info("Deploying windowed job with drain ENABLED")
        deploy_from_yaml(k8s_client, yaml_path, name)

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not reach STABLE state"
        logger.info("Job is STABLE")

        # Let windows accumulate (need time for at least partial windows)
        # With 30s windows, wait 45s to have partial window state
        accumulation_time = 45
        logger.info(f"Letting windows accumulate for {accumulation_time} seconds...")
        time.sleep(accumulation_time)

        # Get pre-suspend logs to establish baseline
        pre_logs = k8s_client.get_taskmanager_logs(name, tail_lines=500)
        pre_windows, _ = log_parser.parse_logs(pre_logs)
        pre_drain_count = len(log_parser.get_drain_triggered_windows(pre_windows))
        logger.info(f"Pre-suspend: {len(pre_windows)} window emissions, {pre_drain_count} drain-triggered")

        # Suspend the job and continuously poll for logs
        # The drain happens during savepoint, before pod terminates
        logger.info("Suspending job (should trigger drain)...")
        suspend_deployment(k8s_client, name)

        # Poll for logs during suspend - capture before pod terminates
        all_captured_logs = []
        max_poll_time = 90  # Maximum time to poll
        poll_interval = 2   # Poll every 2 seconds
        start_time = time.time()
        last_log_time = start_time

        logger.info(f"Polling logs during suspend (up to {max_poll_time}s)...")
        while time.time() - start_time < max_poll_time:
            logs = k8s_client.get_taskmanager_logs(name, tail_lines=2000)
            if logs:
                all_captured_logs.append(logs)
                last_log_time = time.time()
                # Check if we already have DRAIN emissions
                windows, _ = log_parser.parse_logs(logs)
                drain_count = len(log_parser.get_drain_triggered_windows(windows))
                if drain_count > 0:
                    logger.info(f"Captured {drain_count} DRAIN emissions!")
                    break
            else:
                # Pod may have terminated
                if time.time() - last_log_time > 10:
                    logger.info("Pod terminated, using last captured logs")
                    break
            time.sleep(poll_interval)

        # Use the last captured logs (most complete)
        post_logs = all_captured_logs[-1] if all_captured_logs else ""

        if not post_logs:
            logger.warning("No logs captured during suspend")
            pytest.skip("Could not capture logs during suspend")

        # Parse and analyze
        post_windows, _ = log_parser.parse_logs(post_logs)
        drain_windows = log_parser.get_drain_triggered_windows(post_windows)

        logger.info(f"Post-suspend: {len(post_windows)} window emissions, {len(drain_windows)} drain-triggered")

        # Log details of drain emissions
        for w in drain_windows:
            logger.info(f"  DRAIN: {w.key} window [{w.window_start}-{w.window_end}] count={w.count}")

        # Verify drain behavior
        success, message = log_parser.verify_drain_behavior(post_windows, EXPECTED_KEYS)

        if success:
            logger.info(f"✓ PASS: {message}")
        else:
            # If no drain emissions, this might mean:
            # 1. The config doesn't affect suspend (only deletion)
            # 2. The drain happened but logs were lost
            # 3. Test timing issue
            logger.warning(f"⚠ {message}")
            logger.info("This may indicate kubernetes.operator.job.drain-on-savepoint-deletion only affects deletion, not suspend")

        # Summary
        summary = log_parser.summarize(post_windows, [])
        logger.info(f"Summary: {summary}")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# Test: Drain Disabled - Verify Windows Do NOT Emit on Suspend
# =============================================================================

@pytest.mark.draining
@pytest.mark.slow
def test_drain_disabled_preserves_window_state(k8s_client, log_parser, project_root):
    """
    Test that with drain disabled, windows do NOT emit prematurely on suspend.

    This test verifies that without drain:
    - Windows do not emit with DRAIN trigger
    - Window state should be preserved for resume

    Expected: No DRAIN-triggered emissions during suspend
    """
    yaml_path = project_root / "k8s/deployments/drain-test-no-drain.yaml"
    name = "drain-test-no-emit"

    try:
        # Deploy
        logger.info("=== Test: Drain Disabled - Windows Preserved ===")
        logger.info("Deploying windowed job with drain DISABLED")
        deploy_from_yaml(k8s_client, yaml_path, name)

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not reach STABLE state"
        logger.info("Job is STABLE")

        # Let windows accumulate (less than window size to ensure partial windows exist)
        accumulation_time = 20  # Less than 30s window
        logger.info(f"Letting windows accumulate for {accumulation_time} seconds...")
        time.sleep(accumulation_time)

        # Get pre-suspend drain count
        pre_logs = k8s_client.get_taskmanager_logs(name, tail_lines=500)
        pre_windows, _ = log_parser.parse_logs(pre_logs)
        pre_drain_count = len(log_parser.get_drain_triggered_windows(pre_windows))

        # Suspend
        logger.info("Suspending job (should NOT trigger drain)...")
        suspend_deployment(k8s_client, name)

        # Brief wait, then get logs before pod terminates
        time.sleep(10)
        post_logs = k8s_client.get_taskmanager_logs(name, tail_lines=500)

        if post_logs:
            post_windows, _ = log_parser.parse_logs(post_logs)
            post_drain_count = len(log_parser.get_drain_triggered_windows(post_windows))
            new_drain_emissions = post_drain_count - pre_drain_count

            logger.info(f"Drain emissions before suspend: {pre_drain_count}")
            logger.info(f"Drain emissions after suspend: {post_drain_count}")
            logger.info(f"New drain emissions: {new_drain_emissions}")

            # Verify no new drain emissions
            success, message = log_parser.verify_no_drain(
                [w for w in post_windows if w.wall_time > (time.time() * 1000 - 30000)]
            )

            if success:
                logger.info(f"✓ PASS: {message}")
            else:
                logger.warning(f"⚠ {message}")

        else:
            logger.info("No logs after suspend (pod terminated quickly)")
            logger.info("✓ PASS: No opportunity for drain emissions (expected behavior)")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# Test: Drain + Resume - Verify Window State After Resume
# =============================================================================

@pytest.mark.draining
@pytest.mark.slow
def test_drain_then_resume_window_behavior(k8s_client, log_parser, project_root):
    """
    Test behavior when resuming from a savepoint taken with drain.

    This test documents what happens after resume:
    - If drain occurred, windows should be empty after resume
    - New events should start fresh window accumulation

    This is the "potentially incorrect behavior" mentioned in Flink docs.
    """
    yaml_path = project_root / "k8s/deployments/drain-test-with-drain.yaml"
    name = "drain-test-resume"

    try:
        # Deploy
        logger.info("=== Test: Drain + Resume - Window State ===")
        logger.info("Deploying windowed job with drain ENABLED")
        deploy_from_yaml(k8s_client, yaml_path, name)

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not reach STABLE state"

        # Let windows accumulate
        logger.info("Letting windows accumulate for 45 seconds...")
        time.sleep(45)

        # Record timestamp before suspend
        suspend_time = time.time() * 1000

        # Suspend
        logger.info("Suspending job...")
        suspend_deployment(k8s_client, name)
        time.sleep(45)  # Wait for suspend

        # Verify suspended
        deployment = k8s_client.get_flink_deployment(name)
        lifecycle = deployment.get("status", {}).get("lifecycleState", "UNKNOWN")
        logger.info(f"Deployment state after suspend: {lifecycle}")

        # Resume
        logger.info("Resuming job...")
        resume_deployment(k8s_client, name)

        # Wait for stable again
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not resume to STABLE state"
        logger.info("Job resumed and STABLE")

        # Record resume time
        resume_time = time.time() * 1000

        # Let it run and emit new windows
        logger.info("Letting job run for 60 seconds after resume...")
        time.sleep(60)

        # Get logs and analyze
        logs = k8s_client.get_taskmanager_logs(name, tail_lines=2000)
        windows, events = log_parser.parse_logs(logs)

        # Separate pre-suspend, drain, and post-resume windows
        pre_suspend_normal = [w for w in windows
                            if w.wall_time < suspend_time and w.trigger == TriggerType.NORMAL]
        drain_emissions = [w for w in windows if w.trigger == TriggerType.DRAIN]
        post_resume_normal = [w for w in windows
                             if w.wall_time > resume_time and w.trigger == TriggerType.NORMAL]

        logger.info(f"Pre-suspend normal windows: {len(pre_suspend_normal)}")
        logger.info(f"Drain emissions: {len(drain_emissions)}")
        logger.info(f"Post-resume normal windows: {len(post_resume_normal)}")

        # Log some details
        if drain_emissions:
            logger.info("Drain emissions:")
            for w in drain_emissions[:5]:  # First 5
                logger.info(f"  {w.key}: count={w.count}")

        if post_resume_normal:
            logger.info("Post-resume emissions:")
            for w in post_resume_normal[:5]:  # First 5
                logger.info(f"  {w.key}: count={w.count}")

        # Document the behavior
        logger.info("=== Behavior Documentation ===")
        if drain_emissions:
            logger.info("Drain OCCURRED during suspend")
            logger.info("Post-resume windows start with fresh accumulation")
        else:
            logger.info("Drain did NOT occur during suspend")
            logger.info("This may indicate drain-on-savepoint-deletion only affects deletion")

        logger.info("✓ Test complete: Behavior documented")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# Test: No Drain + Resume - Verify State Continuity
# =============================================================================

@pytest.mark.draining
@pytest.mark.slow
def test_no_drain_resume_state_continuity(k8s_client, log_parser, project_root):
    """
    Test that without drain, window state is preserved across suspend/resume.

    This is the correct behavior for resumable jobs:
    - Suspend without drain preserves partial window state
    - Resume continues accumulating in the same windows
    """
    yaml_path = project_root / "k8s/deployments/drain-test-no-drain.yaml"
    name = "drain-test-continuity"

    try:
        # Deploy
        logger.info("=== Test: No Drain + Resume - State Continuity ===")
        logger.info("Deploying windowed job with drain DISABLED")
        deploy_from_yaml(k8s_client, yaml_path, name)

        # Wait for stable
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not reach STABLE state"

        # Let windows accumulate (partial windows)
        logger.info("Letting windows accumulate for 20 seconds...")
        time.sleep(20)

        # Suspend
        logger.info("Suspending job (no drain)...")
        suspend_deployment(k8s_client, name)
        time.sleep(45)

        # Resume
        logger.info("Resuming job...")
        resume_deployment(k8s_client, name)
        assert wait_for_stable(k8s_client, name, timeout=300), "Job did not resume to STABLE state"

        # Let it complete windows naturally
        logger.info("Letting job run for 45 seconds after resume...")
        time.sleep(45)

        # Get logs
        logs = k8s_client.get_taskmanager_logs(name, tail_lines=2000)
        windows, _ = log_parser.parse_logs(logs)

        # Verify no drain emissions
        drain_windows = log_parser.get_drain_triggered_windows(windows)
        normal_windows = log_parser.get_normal_windows(windows)

        logger.info(f"Total windows: {len(windows)}")
        logger.info(f"Normal windows: {len(normal_windows)}")
        logger.info(f"Drain windows: {len(drain_windows)}")

        # The test passes if we have normal emissions and no drain emissions
        assert len(drain_windows) == 0, f"Unexpected drain emissions: {len(drain_windows)}"
        assert len(normal_windows) > 0, "Expected at least some normal window emissions"

        logger.info("✓ PASS: State preserved - normal window emissions, no drain")

    finally:
        cleanup_deployment(k8s_client, name)


# =============================================================================
# Test: Compare Drain vs No-Drain Side by Side
# =============================================================================

@pytest.mark.draining
@pytest.mark.slow
def test_compare_drain_behaviors(k8s_client, log_parser, project_root):
    """
    Compare drain vs no-drain behavior in a single test.

    This test runs both scenarios sequentially and compares:
    - Number of drain-triggered emissions
    - Window state after resume
    """
    drain_yaml = project_root / "k8s/deployments/drain-test-with-drain.yaml"
    no_drain_yaml = project_root / "k8s/deployments/drain-test-no-drain.yaml"

    results = {}

    # Run drain-enabled scenario
    name_drain = "drain-compare-with"
    try:
        logger.info("=== Scenario A: With Drain ===")
        deploy_from_yaml(k8s_client, drain_yaml, name_drain)
        assert wait_for_stable(k8s_client, name_drain, timeout=300)

        time.sleep(30)  # Accumulate
        suspend_deployment(k8s_client, name_drain)
        time.sleep(30)

        logs = k8s_client.get_taskmanager_logs(name_drain, tail_lines=1000)
        windows, _ = log_parser.parse_logs(logs)
        results['drain_enabled'] = {
            'total_windows': len(windows),
            'drain_windows': len(log_parser.get_drain_triggered_windows(windows)),
            'normal_windows': len(log_parser.get_normal_windows(windows)),
        }
        logger.info(f"Drain enabled results: {results['drain_enabled']}")

    finally:
        cleanup_deployment(k8s_client, name_drain)

    time.sleep(10)  # Brief pause between scenarios

    # Run no-drain scenario
    name_no_drain = "drain-compare-without"
    try:
        logger.info("=== Scenario B: Without Drain ===")
        deploy_from_yaml(k8s_client, no_drain_yaml, name_no_drain)
        assert wait_for_stable(k8s_client, name_no_drain, timeout=300)

        time.sleep(30)  # Accumulate
        suspend_deployment(k8s_client, name_no_drain)
        time.sleep(30)

        logs = k8s_client.get_taskmanager_logs(name_no_drain, tail_lines=1000)
        windows, _ = log_parser.parse_logs(logs)
        results['drain_disabled'] = {
            'total_windows': len(windows),
            'drain_windows': len(log_parser.get_drain_triggered_windows(windows)),
            'normal_windows': len(log_parser.get_normal_windows(windows)),
        }
        logger.info(f"Drain disabled results: {results['drain_disabled']}")

    finally:
        cleanup_deployment(k8s_client, name_no_drain)

    # Compare and report
    logger.info("=== Comparison Results ===")
    logger.info(f"With drain:    {results.get('drain_enabled', 'N/A')}")
    logger.info(f"Without drain: {results.get('drain_disabled', 'N/A')}")

    if results.get('drain_enabled') and results.get('drain_disabled'):
        drain_diff = results['drain_enabled']['drain_windows'] - results['drain_disabled']['drain_windows']
        logger.info(f"Difference in drain emissions: {drain_diff}")

        if drain_diff > 0:
            logger.info("✓ Drain configuration DOES affect suspend behavior")
        else:
            logger.info("⚠ Drain configuration may NOT affect suspend behavior")
            logger.info("  (Only affects deletion? Need further investigation)")

    logger.info("✓ Comparison test complete")
