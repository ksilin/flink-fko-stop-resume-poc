#!/usr/bin/env python3
"""
Flink Deployment CLI

Unified command-line interface for Flink deployment operations.
Replaces shell scripts: stop-resume.sh, observe.sh

Usage:
    python tests/cli.py suspend <name> [-n namespace]
    python tests/cli.py resume <name> [-n namespace]
    python tests/cli.py status <name> [-n namespace]
    python tests/cli.py watermarks <name> --enable|--disable [-n namespace]
    python tests/cli.py logs <name> [-n namespace] [--lines N]
"""
import argparse
import sys
import json
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(__file__).rsplit('/', 1)[0])

from framework.kubernetes_client import KubernetesClient
from framework.flink_deployment import FlinkDeploymentManager


def print_colored(text: str, color: str = 'default'):
    """Print with ANSI colors."""
    colors = {
        'green': '\033[0;32m',
        'yellow': '\033[1;33m',
        'blue': '\033[0;34m',
        'red': '\033[0;31m',
        'default': '\033[0m',
    }
    reset = '\033[0m'
    print(f"{colors.get(color, '')}{text}{reset}")


def print_status(deployment: dict, verbose: bool = False):
    """Print formatted deployment status."""
    name = deployment.get('metadata', {}).get('name', 'unknown')
    status = deployment.get('status', {})

    lifecycle = status.get('lifecycleState', 'UNKNOWN')
    job_status = status.get('jobStatus', {})
    job_state = job_status.get('state', 'UNKNOWN')

    print_colored("=== FlinkDeployment Status ===", 'blue')
    print(f"Name:           {name}")
    print(f"Lifecycle:      {lifecycle}")
    print(f"Job State:      {job_state}")

    if job_status.get('startTime'):
        print(f"Started:        {job_status['startTime']}")

    savepoint_path = job_status.get('upgradeSavepointPath')
    if savepoint_path:
        print(f"Savepoint:      {savepoint_path}")

    if verbose:
        print_colored("\n=== Full Status ===", 'blue')
        print(json.dumps(status, indent=2, default=str))


def cmd_suspend(args):
    """Suspend a Flink deployment."""
    k8s = KubernetesClient(namespace=args.namespace)
    manager = FlinkDeploymentManager(k8s)

    print_colored(f"Suspending {args.name}...", 'yellow')
    manager.suspend(args.name)
    print_colored(f"✓ Suspend request sent for {args.name}", 'green')

    if args.wait:
        print("Waiting for SUSPENDED state...")
        k8s.wait_for_deployment_state(args.name, 'SUSPENDED', timeout=args.timeout)
        print_colored(f"✓ {args.name} is now SUSPENDED", 'green')

    # Show current status
    deployment = k8s.get_flink_deployment(args.name)
    print_status(deployment)


def cmd_resume(args):
    """Resume a Flink deployment."""
    k8s = KubernetesClient(namespace=args.namespace)
    manager = FlinkDeploymentManager(k8s)

    print_colored(f"Resuming {args.name}...", 'yellow')
    manager.resume(args.name)
    print_colored(f"✓ Resume request sent for {args.name}", 'green')

    if args.wait:
        print("Waiting for STABLE state...")
        k8s.wait_for_deployment_state(args.name, 'STABLE', timeout=args.timeout)
        print_colored(f"✓ {args.name} is now STABLE", 'green')

    # Show current status
    deployment = k8s.get_flink_deployment(args.name)
    print_status(deployment)


def cmd_status(args):
    """Show deployment status."""
    k8s = KubernetesClient(namespace=args.namespace)

    try:
        deployment = k8s.get_flink_deployment(args.name)
        print_status(deployment, verbose=args.verbose)

        # Show pods
        print_colored("\n=== Pods ===", 'blue')
        pods = k8s.list_pods(label_selector=f"app={args.name}")
        if pods:
            for pod in pods:
                status = pod.status.phase
                print(f"  {pod.metadata.name}: {status}")
        else:
            print("  (no pods found)")

    except Exception as e:
        print_colored(f"✗ Error: {e}", 'red')
        sys.exit(1)


def cmd_watermarks(args):
    """Toggle watermarks for a deployment."""
    k8s = KubernetesClient(namespace=args.namespace)
    manager = FlinkDeploymentManager(k8s)

    if args.enable:
        enabled = True
    elif args.disable:
        enabled = False
    else:
        print_colored("✗ Must specify --enable or --disable", 'red')
        sys.exit(1)

    action = "Enabling" if enabled else "Disabling"
    print_colored(f"{action} watermarks for {args.name}...", 'yellow')
    manager.toggle_watermarks(args.name, enabled)
    print_colored(f"✓ Watermarks {'enabled' if enabled else 'disabled'} for {args.name}", 'green')

    # Show current status
    deployment = k8s.get_flink_deployment(args.name)
    print_status(deployment)


def cmd_logs(args):
    """Show TaskManager logs."""
    k8s = KubernetesClient(namespace=args.namespace)

    print_colored(f"=== TaskManager Logs for {args.name} ===", 'blue')

    # Find TaskManager pod
    pods = k8s.list_pods(label_selector=f"app={args.name},component=taskmanager")

    if not pods:
        # Try alternative selector - find any pod with taskmanager in name
        all_pods = k8s.list_pods(label_selector=f"app={args.name}")
        pods = [p for p in all_pods if 'taskmanager' in p.metadata.name]

    if not pods:
        print_colored("✗ No TaskManager pods found", 'red')
        sys.exit(1)

    pod_name = pods[0].metadata.name
    print(f"Pod: {pod_name}\n")

    logs = k8s.get_pod_logs(pod_name, tail_lines=args.lines)
    print(logs)


def cmd_list(args):
    """List all Flink deployments."""
    k8s = KubernetesClient(namespace=args.namespace)

    print_colored(f"=== Flink Deployments in {args.namespace} ===", 'blue')

    deployments = k8s.custom_api.list_namespaced_custom_object(
        group="flink.apache.org",
        version="v1beta1",
        namespace=args.namespace,
        plural="flinkdeployments"
    )

    if not deployments.get('items'):
        print("  (no deployments found)")
        return

    print(f"{'NAME':<25} {'LIFECYCLE':<15} {'JOB STATE':<15}")
    print("-" * 55)

    for d in deployments['items']:
        name = d.get('metadata', {}).get('name', 'unknown')
        lifecycle = d.get('status', {}).get('lifecycleState', 'UNKNOWN')
        job_state = d.get('status', {}).get('jobStatus', {}).get('state', 'UNKNOWN')
        print(f"{name:<25} {lifecycle:<15} {job_state:<15}")


def main():
    parser = argparse.ArgumentParser(
        description='Flink Deployment CLI - Unified operations interface',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/cli.py list                              # List all deployments
  python tests/cli.py status stateful-test              # Show status
  python tests/cli.py suspend stateful-test --wait      # Suspend and wait
  python tests/cli.py resume stateful-test --wait       # Resume and wait
  python tests/cli.py watermarks stateful-test --enable # Enable watermarks
  python tests/cli.py logs stateful-test --lines 20     # Show last 20 log lines
"""
    )
    parser.add_argument('--namespace', '-n', default='flink', help='Kubernetes namespace')

    subparsers = parser.add_subparsers(dest='command', required=True)

    # list
    list_parser = subparsers.add_parser('list', help='List all Flink deployments')
    list_parser.set_defaults(func=cmd_list)

    # suspend
    suspend_parser = subparsers.add_parser('suspend', help='Suspend a deployment')
    suspend_parser.add_argument('name', help='Deployment name')
    suspend_parser.add_argument('--wait', '-w', action='store_true', help='Wait for SUSPENDED state')
    suspend_parser.add_argument('--timeout', '-t', type=int, default=120, help='Wait timeout in seconds')
    suspend_parser.set_defaults(func=cmd_suspend)

    # resume
    resume_parser = subparsers.add_parser('resume', help='Resume a deployment')
    resume_parser.add_argument('name', help='Deployment name')
    resume_parser.add_argument('--wait', '-w', action='store_true', help='Wait for STABLE state')
    resume_parser.add_argument('--timeout', '-t', type=int, default=300, help='Wait timeout in seconds')
    resume_parser.set_defaults(func=cmd_resume)

    # status
    status_parser = subparsers.add_parser('status', help='Show deployment status')
    status_parser.add_argument('name', help='Deployment name')
    status_parser.add_argument('--verbose', '-v', action='store_true', help='Show full status JSON')
    status_parser.set_defaults(func=cmd_status)

    # watermarks
    wm_parser = subparsers.add_parser('watermarks', help='Toggle watermarks')
    wm_parser.add_argument('name', help='Deployment name')
    wm_group = wm_parser.add_mutually_exclusive_group(required=True)
    wm_group.add_argument('--enable', action='store_true', help='Enable watermarks')
    wm_group.add_argument('--disable', action='store_true', help='Disable watermarks')
    wm_parser.set_defaults(func=cmd_watermarks)

    # logs
    logs_parser = subparsers.add_parser('logs', help='Show TaskManager logs')
    logs_parser.add_argument('name', help='Deployment name')
    logs_parser.add_argument('--lines', '-l', type=int, default=50, help='Number of lines to show')
    logs_parser.set_defaults(func=cmd_logs)

    args = parser.parse_args()

    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(130)
    except Exception as e:
        print_colored(f"✗ Error: {e}", 'red')
        sys.exit(1)


if __name__ == '__main__':
    main()
